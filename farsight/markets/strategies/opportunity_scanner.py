"""
OpportunityScanner — finds trading opportunities via feature/signal analysis.

Pipeline:
    TopMarketsSource         → Fetch top markets by 24h volume
    OrderbookEnricher        → Add orderbook depth + spread
    PriceHistoryEnricher     → Backfill price history into MarketState
    FeatureAnalyzer          → Compute 20 streaming features
    SignalScorer             → Run signal detectors, produce Opportunities
    QualityFilter            → Remove low-edge, illiquid candidates

Each stage is a reusable class that can be used independently:
  - TopMarketsSource → agent skill: "what are the top prediction markets?"
  - FeatureAnalyzer → agent skill: "compute features for this market"
  - SignalScorer → agent skill: "what signals are active on this market?"
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from farsight.markets.clients.polymarket.clob_client import ClobClient
from farsight.markets.clients.polymarket.gamma_client import GammaClient
from farsight.markets.services.feature_engine import compute_features
from farsight.markets.services.signal_engine import SignalEngine
from farsight.markets.services.state_engine import MarketState
from farsight.markets.services.theme_service import ThemeService
from farsight.markets.strategies.base import (
    Analyzer,
    Enricher,
    MarketContext,
    Opportunity,
    Scorer,
    Source,
    Strategy,
    StrategyMode,
)

logger = logging.getLogger(__name__)


# ── Pipeline Stages ──────────────────────────────────────────────────


class TopMarketsSource(Source):
    """Fetch and screen active markets from Polymarket.

    Screens by:
    - Volume (24h) — is this market being actively traded?
    - Spread tightness — can we enter/exit cheaply?
    - Bet balance — is this a real two-sided market or one-sided?
    - Price range — skip near-certain (>95%) and near-zero (<5%) markets
    - Market type — skip 5m/15m crypto noise

    Agent skill: "What are the most tradeable prediction markets right now?"
    """

    def __init__(
        self,
        gamma: GammaClient,
        limit: int = 50,
        min_volume_24h: float = 100,     # Minimum $100 24h volume
        min_liquidity: float = 1000,     # Minimum $1K liquidity
        max_spread: float = 0.15,        # Maximum 15% spread
        price_range: tuple[float, float] = (0.05, 0.95),  # Skip near-certain
    ):
        self.gamma = gamma
        self.limit = limit
        self.min_volume_24h = min_volume_24h
        self.min_liquidity = min_liquidity
        self.max_spread = max_spread
        self.price_range = price_range

    async def fetch(self) -> list[MarketContext]:
        # Fetch more than we need — we'll filter down
        raw_markets = await self.gamma.get_markets(
            active=True, closed=False,
            limit=self.limit * 3,
            order="volume_24hr", ascending=False,
        )

        contexts = []
        screened_out = {"noise": 0, "no_outcomes": 0, "price_range": 0,
                        "low_volume": 0, "low_liquidity": 0, "wide_spread": 0}

        for raw in raw_markets:
            slug = raw.get("slug", "")

            # Skip 5-min/15-min crypto noise
            if "updown-5m" in slug or "updown-15m" in slug:
                screened_out["noise"] += 1
                continue

            market = GammaClient.normalize_market(raw)
            if not market.outcomes or len(market.outcomes) < 2:
                screened_out["no_outcomes"] += 1
                continue

            primary = market.outcomes[0]

            # Price range filter — skip near-certain markets (no edge)
            if primary.current_price < self.price_range[0] or primary.current_price > self.price_range[1]:
                screened_out["price_range"] += 1
                continue

            # Volume filter
            vol_24h = float(raw.get("volume24hr") or 0)
            if vol_24h < self.min_volume_24h:
                screened_out["low_volume"] += 1
                continue

            # Liquidity filter
            if market.liquidity < self.min_liquidity:
                screened_out["low_liquidity"] += 1
                continue

            # Spread filter (from Gamma data — bestAsk - bestBid)
            best_bid = float(raw.get("bestBid") or 0)
            best_ask = float(raw.get("bestAsk") or 0)
            spread = best_ask - best_bid if best_ask > best_bid else 0
            if spread > self.max_spread and spread > 0:
                screened_out["wide_spread"] += 1
                continue

            # Compute screening metrics
            # Bet balance: how symmetric is YES vs NO trading?
            # Competitive score from Gamma (0-1, 1 = perfectly balanced)
            competitive = float(raw.get("competitive") or 0)

            ctx = MarketContext(
                market_id=market.condition_id,
                market_question=market.question,
                event_slug=market.slug,
                token_id=primary.token_id,
                outcome_label=primary.label,
                current_price=primary.current_price,
                best_bid=best_bid,
                best_ask=best_ask,
                spread=spread,
                volume_24h=vol_24h,
                liquidity=market.liquidity,
                end_date=market.end_date,
                raw=raw,
            )

            # Attach screening metadata for scoring
            ctx._competitive = competitive
            ctx._price_change_1d = float(raw.get("oneDayPriceChange") or 0)

            contexts.append(ctx)

            if len(contexts) >= self.limit:
                break

        logger.info(
            f"TopMarketsSource: {len(raw_markets)} raw → {len(contexts)} after screening "
            f"(filtered: {screened_out})"
        )
        return contexts


class OrderbookEnricher(Enricher):
    """Add live orderbook data (bid/ask/spread/depth) to a market context.

    Computes:
    - Real spread from L2 book (more accurate than Gamma's estimate)
    - Depth imbalance (bid vs ask volume)
    - Total liquidity (USD on both sides)

    Agent skill: "What's the orderbook look like for this market?"
    """

    def __init__(self, clob: ClobClient):
        self.clob = clob

    async def enrich(self, ctx: MarketContext) -> MarketContext:
        book = await self.clob.get_orderbook(ctx.token_id)
        if book and book.mid > 0:
            ctx.current_price = book.mid
            ctx.best_bid = book.best_bid
            ctx.best_ask = book.best_ask
            ctx.spread = book.spread
            ctx.liquidity = book.total_bid_depth + book.total_ask_depth
            ctx.orderbook_depth = ctx.liquidity

            # Compute depth imbalance from real book
            total = book.total_bid_depth + book.total_ask_depth
            if total > 0:
                ctx._depth_imbalance = (book.total_bid_depth - book.total_ask_depth) / total
            else:
                ctx._depth_imbalance = 0
        return ctx


class PriceHistoryEnricher(Enricher):
    """Backfill price history and build a MarketState with rolling windows.

    Agent skill: "What's the price history for this market?"
    """

    def __init__(self, clob: ClobClient, fidelity: int = 120):
        self.clob = clob
        self.fidelity = fidelity

    async def enrich(self, ctx: MarketContext) -> MarketContext:
        history = await self.clob.get_price_history(
            ctx.token_id, interval="1m", fidelity=self.fidelity,
        )
        ctx.price_history = history
        return ctx


class FeatureAnalyzer(Analyzer):
    """Compute streaming features from price history + orderbook.

    Agent skill: "What are the microstructure/dynamics features for this market?"
    """

    def analyze(self, ctx: MarketContext) -> MarketContext:
        state = MarketState(ctx.token_id)

        # Replay price history into rolling windows
        for point in ctx.price_history:
            try:
                ts = datetime.fromtimestamp(int(point["t"]), tz=timezone.utc).replace(tzinfo=None)
                price = float(point["p"])
                if price > 0:
                    state.update_price(ts, mid=price)
            except (ValueError, TypeError, KeyError):
                continue

        # Overlay current orderbook state
        if ctx.best_bid > 0:
            state.update_price(
                datetime.utcnow(), mid=ctx.current_price,
                bid=ctx.best_bid, ask=ctx.best_ask,
            )
            state.update_book(
                ctx.liquidity / 2, ctx.liquidity / 2,
                ctx.best_bid, ctx.best_ask,
            )

        ctx.features = compute_features(state)
        ctx.features["last_price"] = ctx.current_price
        return ctx


class ThemeAnalyzer(Analyzer):
    """Map market to themes, sectors, and related tickers.

    Agent skill: "What traditional assets are related to this market?"
    """

    def __init__(self, theme_service: Optional[ThemeService] = None):
        self.themes = theme_service or ThemeService()

    def analyze(self, ctx: MarketContext) -> MarketContext:
        ctx.theme = self.themes.get_theme_for_question(ctx.market_question)
        ctx.related_tickers = self.themes.get_tickers_for_question(ctx.market_question)
        return ctx


class SignalScorer(Scorer):
    """Run signal detectors and convert to scored Opportunities.

    Agent skill: "What trading signals are active on this market?"
    """

    def __init__(self, signal_engine: Optional[SignalEngine] = None):
        self.signal_engine = signal_engine or SignalEngine()

    def score(self, ctx: MarketContext) -> list[Opportunity]:
        if not ctx.features:
            return []

        opportunities = []

        # 1. Standard signal detectors (shock, momentum, reversion)
        signals = self.signal_engine.evaluate(ctx.features, ctx.token_id, ctx.market_id)
        for signal in signals:
            opp = Opportunity(
                market_id=ctx.market_id,
                market_question=ctx.market_question,
                event_slug=ctx.event_slug,
                token_id=ctx.token_id,
                outcome=ctx.outcome_label,
                strategy="scanner",
                reasoning=signal.evidence[0].description if signal.evidence else signal.signal_type.value,
                direction="buy" if signal.direction.value == "bullish" else "sell",
                entry_price=ctx.current_price,
                model_price=signal.model_probability,
                edge=signal.edge,
                confidence=signal.confidence,
                liquidity=ctx.liquidity,
                volume_24h=ctx.volume_24h,
                spread=ctx.spread,
                risk_flags=signal.risk_flags,
                resolution_date=ctx.end_date,
                context=ctx,
            )
            opportunities.append(opp)

        # 2. Feature-based opportunities (lower threshold than signal detectors)
        #    These catch interesting markets that don't meet the strict signal thresholds
        feature_opps = self._score_from_features(ctx)
        opportunities.extend(feature_opps)

        return opportunities

    def _score_from_features(self, ctx: MarketContext) -> list[Opportunity]:
        """Score opportunities from raw features — lower bar than signal detectors.

        Catches markets with:
        - Meaningful 1h price movement (>2%)
        - High depth imbalance (>60% one-sided)
        - High volume relative to liquidity (active interest)
        - Deviation from VWAP (reversion potential)
        """
        features = ctx.features
        opps = []

        # Directional momentum: 1h delta > 2% with decent volume
        delta_1h = features.get("delta_1h")
        trade_vel = features.get("trade_velocity", 0)
        if delta_1h is not None and abs(delta_1h) > 0.02 and trade_vel > 0.1:
            direction = "buy" if delta_1h > 0 else "sell"
            edge = abs(delta_1h) * 0.5  # Assume 50% of the move continues
            opps.append(Opportunity(
                market_id=ctx.market_id,
                market_question=ctx.market_question,
                event_slug=ctx.event_slug,
                token_id=ctx.token_id,
                outcome=ctx.outcome_label,
                strategy="scanner",
                reasoning=f"1h momentum: {delta_1h:+.1%} with {trade_vel:.1f} trades/min",
                direction=direction,
                entry_price=ctx.current_price,
                model_price=ctx.current_price + (delta_1h * 0.5),
                edge=edge,
                confidence=min(0.7, 0.3 + abs(delta_1h) * 3),
                liquidity=ctx.liquidity,
                volume_24h=ctx.volume_24h,
                spread=ctx.spread,
                risk_flags=["feature_based"],
                resolution_date=ctx.end_date,
                context=ctx,
            ))

        # Depth imbalance: strong one-sided book (>60%)
        depth_imb = features.get("depth_imbalance", 0)
        if abs(depth_imb) > 0.6 and ctx.liquidity > 5000:
            direction = "buy" if depth_imb > 0 else "sell"
            edge = abs(depth_imb) * 0.03  # Modest edge from order flow
            opps.append(Opportunity(
                market_id=ctx.market_id,
                market_question=ctx.market_question,
                event_slug=ctx.event_slug,
                token_id=ctx.token_id,
                outcome=ctx.outcome_label,
                strategy="scanner",
                reasoning=f"Depth imbalance: {depth_imb:+.0%} ({'buy' if depth_imb > 0 else 'sell'} pressure)",
                direction=direction,
                entry_price=ctx.current_price,
                model_price=ctx.current_price + (0.02 if depth_imb > 0 else -0.02),
                edge=edge,
                confidence=min(0.6, 0.3 + abs(depth_imb) * 0.3),
                liquidity=ctx.liquidity,
                volume_24h=ctx.volume_24h,
                spread=ctx.spread,
                risk_flags=["feature_based", "order_flow"],
                resolution_date=ctx.end_date,
                context=ctx,
            ))

        # VWAP reversion: price extended from recent average
        reversion = features.get("reversion_score")
        if reversion is not None and abs(reversion) > 1.5:
            direction = "sell" if reversion > 0 else "buy"
            edge = abs(reversion) * 0.01
            opps.append(Opportunity(
                market_id=ctx.market_id,
                market_question=ctx.market_question,
                event_slug=ctx.event_slug,
                token_id=ctx.token_id,
                outcome=ctx.outcome_label,
                strategy="scanner",
                reasoning=f"VWAP deviation: {reversion:.1f}σ — potential reversion",
                direction=direction,
                entry_price=ctx.current_price,
                model_price=ctx.current_price - (reversion * 0.01),
                edge=edge,
                confidence=min(0.6, 0.3 + abs(reversion) * 0.1),
                liquidity=ctx.liquidity,
                volume_24h=ctx.volume_24h,
                spread=ctx.spread,
                risk_flags=["feature_based", "mean_reversion"],
                resolution_date=ctx.end_date,
                context=ctx,
            ))

        # RSI extremes: overbought/oversold probability
        rsi = features.get("rsi_1h")
        if rsi is not None:
            if rsi > 70:
                edge = (rsi - 70) / 300  # ~1% edge at RSI 100
                opps.append(Opportunity(
                    market_id=ctx.market_id,
                    market_question=ctx.market_question,
                    event_slug=ctx.event_slug,
                    token_id=ctx.token_id,
                    outcome=ctx.outcome_label,
                    strategy="scanner",
                    reasoning=f"RSI overbought: {rsi:.0f} — probability may have overshot",
                    direction="sell",
                    entry_price=ctx.current_price,
                    model_price=ctx.current_price - edge,
                    edge=edge,
                    confidence=min(0.6, 0.3 + (rsi - 70) / 100),
                    liquidity=ctx.liquidity, volume_24h=ctx.volume_24h, spread=ctx.spread,
                    risk_flags=["feature_based", "rsi_extreme"],
                    resolution_date=ctx.end_date, context=ctx,
                ))
            elif rsi < 30:
                edge = (30 - rsi) / 300
                opps.append(Opportunity(
                    market_id=ctx.market_id,
                    market_question=ctx.market_question,
                    event_slug=ctx.event_slug,
                    token_id=ctx.token_id,
                    outcome=ctx.outcome_label,
                    strategy="scanner",
                    reasoning=f"RSI oversold: {rsi:.0f} — probability may have undershot",
                    direction="buy",
                    entry_price=ctx.current_price,
                    model_price=ctx.current_price + edge,
                    edge=edge,
                    confidence=min(0.6, 0.3 + (30 - rsi) / 100),
                    liquidity=ctx.liquidity, volume_24h=ctx.volume_24h, spread=ctx.spread,
                    risk_flags=["feature_based", "rsi_extreme"],
                    resolution_date=ctx.end_date, context=ctx,
                ))

        # Volume surge + momentum alignment: strong conviction move
        vol_ratio = features.get("volume_ratio")
        momentum = features.get("momentum_score")
        if vol_ratio is not None and momentum is not None and vol_ratio > 2.0 and abs(momentum) > 0.3:
            direction = "buy" if momentum > 0 else "sell"
            edge = abs(momentum) * 0.03 * min(vol_ratio / 5, 1)
            opps.append(Opportunity(
                market_id=ctx.market_id,
                market_question=ctx.market_question,
                event_slug=ctx.event_slug,
                token_id=ctx.token_id,
                outcome=ctx.outcome_label,
                strategy="scanner",
                reasoning=f"Volume surge ({vol_ratio:.1f}x normal) + momentum ({momentum:+.2f})",
                direction=direction,
                entry_price=ctx.current_price,
                model_price=ctx.current_price + (momentum * 0.03),
                edge=edge,
                confidence=min(0.7, 0.4 + abs(momentum) * 0.3 + min(vol_ratio / 10, 0.2)),
                liquidity=ctx.liquidity, volume_24h=ctx.volume_24h, spread=ctx.spread,
                risk_flags=["feature_based", "volume_confirmed"],
                resolution_date=ctx.end_date, context=ctx,
            ))

        # Bollinger breakout: price at band extremes with volume
        bb_pos = features.get("bollinger_position")
        bb_width = features.get("bollinger_width")
        if bb_pos is not None and bb_width is not None:
            if bb_pos > 1.0 and bb_width > 0.05:
                edge = (bb_pos - 1.0) * 0.02
                opps.append(Opportunity(
                    market_id=ctx.market_id,
                    market_question=ctx.market_question,
                    event_slug=ctx.event_slug,
                    token_id=ctx.token_id,
                    outcome=ctx.outcome_label,
                    strategy="scanner",
                    reasoning=f"Bollinger breakout above upper band (pos={bb_pos:.2f}, width={bb_width:.2%})",
                    direction="sell",  # Extended above → potential reversion
                    entry_price=ctx.current_price,
                    model_price=ctx.current_price - edge,
                    edge=edge,
                    confidence=0.5,
                    liquidity=ctx.liquidity, volume_24h=ctx.volume_24h, spread=ctx.spread,
                    risk_flags=["feature_based", "bollinger_breakout"],
                    resolution_date=ctx.end_date, context=ctx,
                ))
            elif bb_pos < 0.0 and bb_width > 0.05:
                edge = abs(bb_pos) * 0.02
                opps.append(Opportunity(
                    market_id=ctx.market_id,
                    market_question=ctx.market_question,
                    event_slug=ctx.event_slug,
                    token_id=ctx.token_id,
                    outcome=ctx.outcome_label,
                    strategy="scanner",
                    reasoning=f"Bollinger breakdown below lower band (pos={bb_pos:.2f}, width={bb_width:.2%})",
                    direction="buy",  # Extended below → potential reversion
                    entry_price=ctx.current_price,
                    model_price=ctx.current_price + edge,
                    edge=edge,
                    confidence=0.5,
                    liquidity=ctx.liquidity, volume_24h=ctx.volume_24h, spread=ctx.spread,
                    risk_flags=["feature_based", "bollinger_breakout"],
                    resolution_date=ctx.end_date, context=ctx,
                ))

        return opps


# ── Composed Strategy ────────────────────────────────────────────────


from typing import Literal as _Literal
from pydantic import BaseModel as _BaseModel, Field as _Field
from farsight.markets.strategies.config import StrategyConfig


class ScannerParams(_BaseModel):
    max_markets: int = 50
    min_volume_24h: float = 100
    price_range_low: float = 0.05
    price_range_high: float = 0.95


class ScannerConfig(StrategyConfig):
    name: _Literal["scanner"] = "scanner"
    params: ScannerParams = _Field(default_factory=ScannerParams)


class OpportunityScanner(Strategy):
    """Scan top markets for trading opportunities.

    Pipeline: TopMarketsSource → OrderbookEnricher → PriceHistoryEnricher
              → FeatureAnalyzer → ThemeAnalyzer → SignalScorer → QualityFilter
    """

    name = "scanner"
    mode = StrategyMode.SCAN
    scan_interval_seconds = 300

    def __init__(
        self,
        gamma: Optional[GammaClient] = None,
        clob: Optional[ClobClient] = None,
        max_markets: int = 50,
        min_edge: float = 0.01,     # feature-based opps have smaller edges
        min_liquidity: float = 3000,
        config: Optional[ScannerConfig] = None,
    ):
        gamma = gamma or GammaClient()
        clob = clob or ClobClient()

        # Config overrides raw kwargs when provided.
        if config is not None:
            max_markets = config.params.max_markets
            min_edge = config.thresholds.min_edge
            min_liquidity = config.thresholds.min_liquidity
            self.scan_interval_seconds = config.scheduling.scan_interval_seconds
        self.config = config

        # Composable stages
        self.source = TopMarketsSource(gamma, limit=max_markets)
        self.orderbook_enricher = OrderbookEnricher(clob)
        self.history_enricher = PriceHistoryEnricher(clob)
        self.feature_analyzer = FeatureAnalyzer()
        self.theme_analyzer = ThemeAnalyzer()
        self.signal_scorer = SignalScorer()
        self.min_edge = min_edge
        self.min_liquidity = min_liquidity

    async def scan(self) -> list[Opportunity]:
        """Execute the full pipeline."""
        # 1. Source
        contexts = await self.source.fetch()
        logger.info(f"OpportunityScanner: sourced {len(contexts)} markets")

        all_opportunities = []

        for ctx in contexts:
            try:
                # 2. Enrich
                ctx = await self.orderbook_enricher.enrich(ctx)
                if ctx.liquidity < 1000:
                    continue
                ctx = await self.history_enricher.enrich(ctx)

                # 3. Analyze
                ctx = self.feature_analyzer.analyze(ctx)
                ctx = self.theme_analyzer.analyze(ctx)

                # 4. Score
                opps = self.signal_scorer.score(ctx)
                all_opportunities.extend(opps)

            except Exception as e:
                logger.debug(f"Scanner: error processing {ctx.market_question[:40]}: {e}")
                continue

        # Inline quality gates (Scorer+Filter collapsed into the emit step).
        # Portfolio-level sizing/caps are Policy's job downstream.
        filtered = [
            o for o in all_opportunities
            if abs(o.edge) >= self.min_edge and o.liquidity >= self.min_liquidity
        ]
        for o in filtered:
            o.compute_score()
        filtered.sort(key=lambda o: o.score, reverse=True)
        logger.info(f"OpportunityScanner: {len(all_opportunities)} raw → {len(filtered)} qualified")
        return filtered

    # Skill extraction
    def get_source(self) -> Source:
        return self.source

    def get_analyzer(self) -> Analyzer:
        return self.feature_analyzer
