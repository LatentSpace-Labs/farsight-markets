"""
CrossEventArb — detects structural mispricing in multi-outcome events.

Pipeline:
    ActiveEventsSource       → Fetch active events with multiple outcomes
    EventPriceEnricher       → Sum outcome prices, compute deviation
    StructuralAnalyzer       → Detect mispricing (sum > 103% or < 97%)
    ArbScorer                → Produce arb opportunities with edge = deviation
    QualityFilter            → Remove low-deviation, illiquid arbs

Mathematical edge: in multi-outcome events, prices MUST sum to 100%.
Any excess is guaranteed profit at resolution (minus fees/slippage).

Agent skill: "Are there any structurally mispriced prediction market events?"
"""

import logging
from datetime import datetime
from typing import Optional

from farsight.markets.clients.polymarket.clob_client import ClobClient
from farsight.markets.clients.polymarket.gamma_client import GammaClient
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


class ActiveEventsSource(Source):
    """Fetch multi-outcome events from Polymarket.

    Agent skill: "What are the active multi-outcome prediction market events?"
    """

    def __init__(self, gamma: GammaClient, limit: int = 30):
        self.gamma = gamma
        self.limit = limit

    async def fetch(self) -> list[MarketContext]:
        raw_events = await self.gamma.get_events(
            active=True, closed=False,
            limit=self.limit,
            order="volume_24hr", ascending=False,
        )

        contexts = []
        for raw_event in raw_events:
            event = GammaClient.normalize_event(raw_event)
            if len(event.markets) < 2:
                continue

            # ONLY consider neg-risk events.
            # neg_risk = Polymarket's explicit flag meaning "these outcomes are
            # mutually exclusive — exactly one wins." Prices SHOULD sum to ~100%.
            # Without this flag, child markets are independent binary contracts
            # where each has its own YES+NO≈$1. Summing YES prices is meaningless.
            has_neg_risk = any(m.neg_risk for m in event.markets)
            if not has_neg_risk:
                continue

            prices = []
            labels = []
            for m in event.markets:
                if m.outcomes:
                    prices.append(m.outcomes[0].current_price)
                    labels.append(m.outcomes[0].label)

            if not prices:
                continue

            ctx = MarketContext(
                market_id=event.slug,
                market_question=event.title,
                event_slug=event.slug,
                source_platform="polymarket",
                event_title=event.title,
                sibling_prices=prices,
                sibling_labels=labels,
                price_sum=sum(prices),
                liquidity=event.liquidity,
                volume_24h=float(raw_event.get("volume24hr") or 0),
                end_date=event.end_date,
                raw=raw_event,
            )

            ctx._child_markets = event.markets
            contexts.append(ctx)

        return contexts


class StructuralAnalyzer(Analyzer):
    """Check if event outcome prices sum to ~100%.

    Agent skill: "Is this prediction market event structurally mispriced?"
    """

    def __init__(self, min_deviation: float = 0.02):
        self.min_deviation = min_deviation

    def analyze(self, ctx: MarketContext) -> MarketContext:
        deviation = ctx.price_sum - 1.0
        ctx.features = {
            "price_sum": ctx.price_sum,
            "deviation": deviation,
            "abs_deviation": abs(deviation),
            "is_overpriced": deviation > self.min_deviation,
            "is_underpriced": deviation < -self.min_deviation,
            "num_outcomes": len(ctx.sibling_prices),
        }
        return ctx


class ArbScorer(Scorer):
    """Convert structural analysis into arb opportunities.

    Agent skill: "What's the arbitrage opportunity on this event?"
    """

    def __init__(self, min_deviation: float = 0.02):
        self.min_deviation = min_deviation

    def score(self, ctx: MarketContext) -> list[Opportunity]:
        deviation = ctx.features.get("deviation", 0)
        if abs(deviation) < self.min_deviation:
            return []

        if not hasattr(ctx, "_child_markets"):
            return []

        num_outcomes = ctx.features.get("num_outcomes", 1)
        opportunities = []

        if deviation > 0:
            # Overpriced — find the most overvalued candidate
            markets_sorted = sorted(
                [m for m in ctx._child_markets if m.outcomes],
                key=lambda m: m.outcomes[0].current_price,
                reverse=True,
            )
            for market in markets_sorted:
                primary = market.outcomes[0]
                if primary.current_price < 0.02:
                    continue

                opp = Opportunity(
                    market_id=market.condition_id,
                    market_question=market.question,
                    event_slug=ctx.event_slug,
                    token_id=primary.token_id,
                    outcome=primary.label,
                    strategy="arb",
                    reasoning=(
                        f"Event '{ctx.event_title}' outcomes sum to {ctx.price_sum:.1%} "
                        f"(overpriced by {deviation:.1%}). "
                        f"{primary.label} at {primary.current_price:.0%} may be inflated."
                    ),
                    direction="sell",
                    entry_price=primary.current_price,
                    model_price=primary.current_price - deviation / num_outcomes,
                    edge=deviation / num_outcomes,
                    confidence=min(0.9, 0.5 + deviation * 2),
                    liquidity=ctx.liquidity,
                    volume_24h=ctx.volume_24h,
                    spread=0.02,
                    risk_flags=_assess_risk(ctx, deviation),
                    resolution_date=ctx.end_date,
                    context=ctx,
                )
                opportunities.append(opp)
                break  # One per event

        else:
            # Underpriced — buy the leader
            markets_sorted = sorted(
                [m for m in ctx._child_markets if m.outcomes],
                key=lambda m: m.outcomes[0].current_price,
                reverse=True,
            )
            if markets_sorted:
                leader = markets_sorted[0]
                primary = leader.outcomes[0]

                opp = Opportunity(
                    market_id=leader.condition_id,
                    market_question=leader.question,
                    event_slug=ctx.event_slug,
                    token_id=primary.token_id,
                    outcome=primary.label,
                    strategy="arb",
                    reasoning=(
                        f"Event '{ctx.event_title}' outcomes sum to {ctx.price_sum:.1%} "
                        f"(underpriced by {abs(deviation):.1%}). "
                        f"Leader {primary.label} at {primary.current_price:.0%} may be undervalued."
                    ),
                    direction="buy",
                    entry_price=primary.current_price,
                    model_price=primary.current_price + abs(deviation) / num_outcomes,
                    edge=abs(deviation) / num_outcomes,
                    confidence=min(0.9, 0.5 + abs(deviation) * 2),
                    liquidity=ctx.liquidity,
                    volume_24h=ctx.volume_24h,
                    spread=0.02,
                    risk_flags=_assess_risk(ctx, deviation),
                    resolution_date=ctx.end_date,
                    context=ctx,
                )
                opportunities.append(opp)

        return opportunities


# ── Composed Strategy ────────────────────────────────────────────────


from typing import Literal as _Literal
from pydantic import BaseModel as _BaseModel, Field as _Field
from farsight.markets.strategies.config import StrategyConfig


class ArbParams(_BaseModel):
    max_events: int = 30
    min_deviation: float = 0.02


class ArbConfig(StrategyConfig):
    name: _Literal["arb"] = "arb"
    params: ArbParams = _Field(default_factory=ArbParams)


class CrossEventArb(Strategy):
    """Scan events for structural mispricing.

    Pipeline: ActiveEventsSource → StructuralAnalyzer → ArbScorer → QualityFilter
    """

    name = "arb"
    mode = StrategyMode.SCAN
    scan_interval_seconds = 600

    def __init__(
        self,
        gamma: Optional[GammaClient] = None,
        clob: Optional[ClobClient] = None,
        max_events: int = 30,
        min_deviation: float = 0.02,
        min_liquidity: float = 5000,
        config: Optional[ArbConfig] = None,
    ):
        gamma = gamma or GammaClient()

        if config is not None:
            max_events = config.params.max_events
            min_deviation = config.params.min_deviation
            min_liquidity = config.thresholds.min_liquidity
            self.scan_interval_seconds = config.scheduling.scan_interval_seconds
        self.config = config

        self.source = ActiveEventsSource(gamma, limit=max_events)
        self.structural_analyzer = StructuralAnalyzer(min_deviation=min_deviation)
        self.arb_scorer = ArbScorer(min_deviation=min_deviation)
        # Arb edges are smaller but more certain, so min_edge is intentionally
        # a fraction of min_deviation.
        self.min_edge = min_deviation / 10
        self.min_liquidity = min_liquidity
        self.min_confidence = (config.thresholds.min_confidence if config else 0.4)

    async def scan(self) -> list[Opportunity]:
        # 1. Source
        contexts = await self.source.fetch()
        logger.info(f"CrossEventArb: sourced {len(contexts)} multi-outcome events")

        all_opportunities = []

        for ctx in contexts:
            try:
                # 2. Analyze
                ctx = self.structural_analyzer.analyze(ctx)

                # 3. Score
                opps = self.arb_scorer.score(ctx)
                all_opportunities.extend(opps)
            except Exception as e:
                logger.debug(f"Arb: error processing {ctx.event_title[:40]}: {e}")

        filtered = [
            o for o in all_opportunities
            if abs(o.edge) >= self.min_edge
            and o.liquidity >= self.min_liquidity
            and o.confidence >= self.min_confidence
        ]
        for o in filtered:
            o.compute_score()
        filtered.sort(key=lambda o: o.score, reverse=True)
        logger.info(f"CrossEventArb: {len(all_opportunities)} raw → {len(filtered)} qualified")
        return filtered

    def get_source(self) -> Source:
        return self.source

    def get_analyzer(self):
        return self.structural_analyzer


# ── Helpers ──────────────────────────────────────────────────────────


def _assess_risk(ctx: MarketContext, deviation: float) -> list[str]:
    flags = []
    if abs(deviation) > 0.10:
        flags.append("extreme_deviation")
    if ctx.end_date:
        days = (ctx.end_date - datetime.utcnow()).total_seconds() / 86400
        if days < 1:
            flags.append("near_resolution")
        elif days > 365:
            flags.append("long_horizon")
    if ctx.liquidity < 50000:
        flags.append("low_liquidity")
    return flags
