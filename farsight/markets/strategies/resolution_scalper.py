"""
ResolutionScalper — captures value as markets approach resolution.

Pipeline:
    NearResolutionSource     → Fetch markets resolving within N days
    OrderbookEnricher        → Add orderbook depth + spread
    ResolutionAnalyzer       → Estimate fair value based on time to resolution
    DiscountScorer           → Score opportunities by discount to fair value
                             (quality gates live inside the scorer)

Hybrid mode:
  - Scan: periodically find near-resolution candidates
  - Stream: watch candidates in real-time for optimal entry timing

Agent skill: "What near-resolution markets have uncertainty discounts?"
"""

import logging
from datetime import datetime, timedelta
from typing import Literal, Optional

from pydantic import BaseModel, Field

from farsight.markets.clients.polymarket.clob_client import ClobClient
from farsight.markets.clients.polymarket.gamma_client import GammaClient, _infer_category
from farsight.markets import telemetry
from farsight.markets.strategies.base import (
    Action,
    ActionType,
    Analyzer,
    MarketContext,
    Opportunity,
    Scorer,
    Source,
    Strategy,
    StrategyMode,
)
from farsight.markets.strategies.config import ScopeConfig, StrategyConfig

logger = logging.getLogger(__name__)


# ── Resolution-specific config ───────────────────────────────────────

# Per-category convergence profiles. Three parameters:
#   horizon_days: the window over which convergence plays out
#   convexity:    shape of the gap-decay curve
#                   <1  flat early, snap late  (event-driven: sports, elections)
#                   =1  linear
#                   >1  drifts early, plateaus late (info-diffusion: geopolitics)
#   strength:     ceiling on the pull — prior that the current leader actually
#                 wins. fv_at_zero = price + strength · (1 - price).
# Tuned priors, not fitted — revisit once we persist resolution outcomes.
CONVERGENCE_PROFILES: dict[str, dict[str, float]] = {
    "sports":      {"horizon_days":  1.0, "convexity": 0.30, "strength": 0.98},
    "elections":   {"horizon_days":  3.0, "convexity": 0.50, "strength": 0.95},
    "politics":    {"horizon_days":  7.0, "convexity": 0.70, "strength": 0.85},
    "geopolitics": {"horizon_days": 14.0, "convexity": 1.50, "strength": 0.70},
}
DEFAULT_CONVERGENCE_PROFILE = {"horizon_days": 7.0, "convexity": 1.0, "strength": 0.80}


class ResolutionParams(BaseModel):
    max_days: int = 14
    min_certainty: float = 0.75
    convergence_profiles: dict[str, dict[str, float]] = Field(
        default_factory=lambda: dict(CONVERGENCE_PROFILES)
    )


class ResolutionConfig(StrategyConfig):
    name: Literal["resolution"] = "resolution"
    params: ResolutionParams = Field(default_factory=ResolutionParams)


# ── Pipeline Stages ──────────────────────────────────────────────────


class NearResolutionSource(Source):
    """Fetch markets resolving within N days, scoped by config.

    Scope precedence: market_slugs → condition_ids → tag_slugs → categories.
    Date and liquidity bounds are pushed to the Gamma API so we don't silently
    miss markets past the 100-row cap.

    Agent skill: "What prediction markets are resolving soon?"
    """

    def __init__(
        self,
        gamma: GammaClient,
        scope: ScopeConfig,
        max_days: int,
        min_liquidity: float,
    ):
        self.gamma = gamma
        self.scope = scope
        self.max_days = max_days
        self.min_liquidity = min_liquidity

    async def fetch(self) -> list[MarketContext]:
        now = datetime.utcnow()
        end_min = now
        end_max = now + timedelta(days=self.max_days)

        raw_markets = await self._fetch_raw(end_min, end_max)
        telemetry.emit(
            "stage.enter", strategy="resolution", stage="source",
            fetched=len(raw_markets),
            params={
                "max_days": self.max_days,
                "min_liquidity": self.min_liquidity,
                "categories": list(self.scope.categories),
                "tag_slugs": list(self.scope.tag_slugs),
            },
        )

        drops: dict[str, list[str]] = {}

        def _drop(reason: str, slug: str) -> None:
            drops.setdefault(reason, []).append(slug or "?")

        contexts: list[MarketContext] = []
        seen: set[str] = set()

        for raw in raw_markets:
            market = GammaClient.normalize_market(raw)
            if not market.condition_id or market.condition_id in seen:
                _drop("duplicate", market.slug or "")
                continue
            seen.add(market.condition_id)

            if not market.end_date:
                _drop("no_end_date", market.slug or "")
                continue

            end_naive = market.end_date.replace(tzinfo=None) if market.end_date.tzinfo else market.end_date
            days_left = (end_naive - now).total_seconds() / 86400
            if days_left < 0 or days_left > self.max_days:
                _drop("outside_date_window", market.slug or "")
                continue

            slug = market.slug or ""
            if "updown-5m" in slug or "updown-15m" in slug:
                _drop("updown_market", slug)
                continue

            if not market.outcomes or len(market.outcomes) < 2:
                _drop("fewer_than_2_outcomes", slug)
                continue

            category = (raw.get("category") or "").lower() or _infer_category(raw)
            if self.scope.categories and category not in self.scope.categories:
                _drop(f"category_not_allowed:{category or 'unknown'}", slug)
                continue

            # Analyze the leader, not outcomes[0]. For a binary market the
            # leader is whichever side is above 0.5; for a multi-candidate
            # market (e.g. "next president") it's the front-runner. Picking
            # outcomes[0] blindly produced nonsense edges on long-tail losers.
            primary = max(market.outcomes, key=lambda o: o.current_price)
            ctx = MarketContext(
                market_id=market.condition_id,
                market_question=market.question,
                event_slug=market.slug,
                token_id=primary.token_id,
                outcome_label=primary.label,
                current_price=primary.current_price,
                volume_24h=float(raw.get("volume24hr") or 0),
                liquidity=market.liquidity,
                end_date=market.end_date,
                raw=raw,
            )
            ctx._days_left = days_left
            ctx._category = category
            contexts.append(ctx)

        for reason, slugs in drops.items():
            telemetry.emit(
                "stage.drop", strategy="resolution", stage="source",
                reason=reason, count=len(slugs), samples=slugs[:3],
            )
        telemetry.emit(
            "stage.keep", strategy="resolution", stage="source",
            count=len(contexts),
        )
        return contexts

    async def _fetch_raw(self, end_min: datetime, end_max: datetime) -> list[dict]:
        """Resolve the configured scope into a flat list of raw market dicts."""
        scope = self.scope

        # Tier 1: explicit market identifiers win outright.
        if scope.market_slugs:
            return await self.gamma.get_markets(
                slugs=scope.market_slugs, limit=len(scope.market_slugs),
            )
        if scope.condition_ids:
            return await self.gamma.get_markets(
                condition_ids=scope.condition_ids, limit=len(scope.condition_ids),
            )

        # Tier 2: event scope — fan out child markets.
        if scope.event_slugs:
            out: list[dict] = []
            for ev_slug in scope.event_slugs:
                ev = await self.gamma.get_event_by_slug(ev_slug)
                if ev:
                    out.extend(ev.get("markets") or [])
            return out

        # Tier 3/5: tag slugs resolve to IDs for /markets. Categories fall
        # back to label-matching in the loop above.
        tag_ids: list[int] = []
        if scope.tag_slugs:
            resolved = await self.gamma.resolve_tag_slugs(scope.tag_slugs)
            tag_ids = list(resolved.values())

        common_kwargs = dict(
            active=True, closed=False,
            limit=200,
            order="end_date", ascending=True,
            end_date_min=end_min,
            end_date_max=end_max,
            liquidity_min=self.min_liquidity if self.min_liquidity > 0 else None,
            include_tag=True,
        )

        if tag_ids:
            out = []
            seen: set[str] = set()
            for tid in tag_ids:
                for m in await self.gamma.get_markets(tag_id=tid, **common_kwargs):
                    cid = m.get("conditionId") or m.get("condition_id")
                    if cid and cid not in seen:
                        seen.add(cid)
                        out.append(m)
            return out

        return await self.gamma.get_markets(**common_kwargs)


class ResolutionAnalyzer(Analyzer):
    """Estimate fair value based on time to resolution.

    As resolution approaches, high-probability outcomes should trade
    closer to 100%. The gap between current price and fair value
    represents an uncertainty discount that shrinks over time.

    Agent skill: "What should this market be priced at given time to resolution?"
    """

    def __init__(
        self,
        min_certainty: float = 0.85,
        profiles: Optional[dict[str, dict[str, float]]] = None,
    ):
        self.min_certainty = min_certainty
        self.profiles = profiles if profiles is not None else CONVERGENCE_PROFILES

    def analyze(self, ctx: MarketContext) -> MarketContext:
        days_left = getattr(ctx, "_days_left", 999)
        category = getattr(ctx, "_category", None)
        price = ctx.current_price

        # Source already selects the leading outcome, so price is the leader's.
        # Only one side to qualify against now.
        if price < self.min_certainty:
            ctx.features = {"fair_value": price, "discount": 0.0, "qualified": False}
            return ctx

        profile = self.profiles.get(category or "", DEFAULT_CONVERGENCE_PROFILE)
        fair_value = self._estimate_fair_value(price, days_left, profile)
        discount = fair_value - price

        ctx.features = {
            "fair_value": fair_value,
            "discount": discount,
            "days_left": days_left,
            "category": category,
            "qualified": discount > 0,
        }
        return ctx

    @staticmethod
    def _estimate_fair_value(
        current_price: float,
        days_left: float,
        profile: Optional[dict[str, float]] = None,
    ) -> float:
        """fair = price + (fv₀ - price) · (1 - t^k)  where t = days_left/horizon."""
        if days_left <= 0:
            return current_price
        p = profile or DEFAULT_CONVERGENCE_PROFILE
        horizon = p["horizon_days"]
        convexity = p.get("convexity", 1.0)
        strength = p["strength"]

        t = max(0.0, min(1.0, days_left / horizon))
        fv_at_zero = min(0.99, current_price + strength * (1.0 - current_price))
        gap_to_close = fv_at_zero - current_price
        return current_price + gap_to_close * (1.0 - t ** convexity)


class DiscountScorer(Scorer):
    """Turn resolution features into qualified Opportunities.

    Rules and quality gates live here, not in a separate Filter stage:
    if the rules pass, emit; otherwise return empty. Portfolio-level
    concerns (Kelly, position caps) are Policy's job downstream.

    Agent skill: "What's the discount on this near-resolution market?"
    """

    def __init__(
        self,
        min_edge: float = 0.02,
        min_confidence: float = 0.40,
        min_liquidity: float = 5_000,
        max_spread: float = 0.10,
    ):
        self.min_edge = min_edge
        self.min_confidence = min_confidence
        self.min_liquidity = min_liquidity
        self.max_spread = max_spread

    def score(self, ctx: MarketContext) -> list[Opportunity]:
        if not ctx.features.get("qualified"):
            return []

        discount = ctx.features.get("discount", 0)
        fair_value = ctx.features["fair_value"]
        days_left = ctx.features.get("days_left", 999)

        # Quality gates — all-or-nothing.
        if discount < self.min_edge:
            return []
        if ctx.liquidity < self.min_liquidity:
            return []
        if (ctx.spread or 0) > self.max_spread:
            return []
        confidence = min(0.95, ctx.current_price + 0.05)
        if confidence < self.min_confidence:
            return []

        opp = Opportunity(
            market_id=ctx.market_id,
            market_question=ctx.market_question,
            event_slug=ctx.event_slug,
            token_id=ctx.token_id,
            outcome=ctx.outcome_label,
            strategy="resolution",
            reasoning=(
                f"[{ctx.features.get('category') or 'uncategorized'}] "
                f"Resolves in {days_left:.1f} days. "
                f"{ctx.outcome_label} at {ctx.current_price:.0%} but fair value ~{fair_value:.0%}. "
                f"Discount: {discount:.1%}."
            ),
            direction="buy",
            entry_price=ctx.current_price,
            model_price=fair_value,
            edge=discount,
            confidence=confidence,
            horizon=f"{days_left:.0f}d",
            liquidity=ctx.liquidity,
            volume_24h=ctx.volume_24h,
            spread=ctx.spread or 0.02,
            risk_flags=_assess_risk(days_left, ctx.current_price, ctx.liquidity),
            resolution_date=ctx.end_date,
            context=ctx,
        )
        return [opp]


# ── Composed Strategy ────────────────────────────────────────────────


class ResolutionScalper(Strategy):
    """Find near-resolution markets with uncertainty discounts.

    Pipeline: NearResolutionSource → OrderbookEnricher → ResolutionAnalyzer
              → DiscountScorer (rules + quality gates inline)
    """

    name = "resolution"
    # SCAN-only: monitor() re-fetches via REST every 60s; no on_state_update
    # consumer is wired, so opening WS streams would just burn bandwidth.
    mode = StrategyMode.SCAN

    def __init__(
        self,
        gamma: Optional[GammaClient] = None,
        clob: Optional[ClobClient] = None,
        config: Optional[ResolutionConfig] = None,
    ):
        self.gamma = gamma or GammaClient()
        self._clob = clob or ClobClient()
        self.config = config or ResolutionConfig.default()

        cfg = self.config
        self.scan_interval_seconds = cfg.scheduling.scan_interval_seconds

        self.source = NearResolutionSource(
            self.gamma,
            scope=cfg.scope,
            max_days=cfg.params.max_days,
            min_liquidity=cfg.thresholds.min_liquidity,
        )
        self.resolution_analyzer = ResolutionAnalyzer(
            min_certainty=cfg.params.min_certainty,
            profiles=cfg.params.convergence_profiles,
        )
        self.discount_scorer = DiscountScorer(
            min_edge=cfg.thresholds.min_edge,
            min_confidence=cfg.thresholds.min_confidence,
            min_liquidity=cfg.thresholds.min_liquidity,
            max_spread=cfg.thresholds.max_spread,
        )

    async def scan(self) -> list[Opportunity]:
        from farsight.markets.strategies.opportunity_scanner import OrderbookEnricher

        orderbook_enricher = OrderbookEnricher(self._clob)
        contexts = await self.source.fetch()
        logger.info(f"ResolutionScalper: sourced {len(contexts)} near-resolution markets")

        # ENRICH + ANALYZE + SCORE. Source already applies liquidity at the
        # Gamma layer; the Scorer re-checks with live orderbook liquidity.
        enriched: list[MarketContext] = []
        enrich_errors: list[str] = []
        for ctx in contexts:
            try:
                ctx = await orderbook_enricher.enrich(ctx)
                enriched.append(ctx)
            except Exception as e:
                enrich_errors.append(f"{ctx.event_slug}:{type(e).__name__}")

        telemetry.emit("stage.enter", strategy="resolution", stage="enricher",
                       input_count=len(contexts))
        if enrich_errors:
            telemetry.emit("stage.drop", strategy="resolution", stage="enricher",
                           reason="enrich_error", count=len(enrich_errors),
                           samples=enrich_errors[:3])
        telemetry.emit("stage.keep", strategy="resolution", stage="enricher",
                       count=len(enriched))

        analyzed: list[MarketContext] = []
        unqualified: list[str] = []
        for ctx in enriched:
            ctx = self.resolution_analyzer.analyze(ctx)
            if ctx.features.get("qualified"):
                analyzed.append(ctx)
            else:
                unqualified.append(ctx.event_slug or "?")
        telemetry.emit("stage.enter", strategy="resolution", stage="analyzer",
                       input_count=len(enriched),
                       params={"min_certainty": self.config.params.min_certainty})
        if unqualified:
            telemetry.emit("stage.drop", strategy="resolution", stage="analyzer",
                           reason="low_certainty_or_discount",
                           count=len(unqualified), samples=unqualified[:3])
        telemetry.emit("stage.keep", strategy="resolution", stage="analyzer",
                       count=len(analyzed))

        all_opportunities: list[Opportunity] = []
        for ctx in analyzed:
            for o in self.discount_scorer.score(ctx):
                all_opportunities.append(o)
                telemetry.emit(
                    "opportunity", strategy="resolution",
                    slug=o.event_slug, outcome=o.outcome,
                    price=o.entry_price, fair_value=o.model_price,
                    edge=o.edge, confidence=o.confidence,
                    liquidity=o.liquidity, horizon=o.horizon,
                    category=ctx.features.get("category"),
                )
        telemetry.emit(
            "stage.enter", strategy="resolution", stage="scorer",
            input_count=len(analyzed),
            params={
                "min_edge": self.config.thresholds.min_edge,
                "min_confidence": self.config.thresholds.min_confidence,
                "min_liquidity": self.config.thresholds.min_liquidity,
                "max_spread": self.config.thresholds.max_spread,
            },
        )
        dropped = len(analyzed) - len(all_opportunities)
        if dropped:
            telemetry.emit("stage.drop", strategy="resolution", stage="scorer",
                           reason="quality_gate", count=dropped)
        telemetry.emit("stage.keep", strategy="resolution", stage="scorer",
                       count=len(all_opportunities))

        # Compute scores so Runner can rank/dedup; Policy handles portfolio-level
        # decisions (Kelly sizing, position caps) downstream.
        for o in all_opportunities:
            o.compute_score()

        logger.info(f"ResolutionScalper: {len(all_opportunities)} qualified opportunities")
        return all_opportunities

    async def monitor(self, open_positions: list[dict]) -> list[Action]:
        actions: list[Action] = []
        take_profit = self.config.risk.take_profit_price
        stop_loss_pct = self.config.risk.stop_loss_pct

        for pos in open_positions:
            if pos.get("strategy") != self.name:
                continue

            token_id = pos.get("token_id", "")
            if not token_id:
                continue

            book = await self._clob.get_orderbook(token_id)
            if not book:
                continue

            current_price = book.mid
            entry = pos.get("entry_price", 0)

            if current_price >= take_profit:
                actions.append(Action(
                    action_type=ActionType.CLOSE,
                    trade_id=pos["id"],
                    reason=f"Price reached {current_price:.0%} — taking profit",
                    exit_price=current_price,
                ))
            elif entry > 0 and current_price < entry - stop_loss_pct:
                actions.append(Action(
                    action_type=ActionType.STOP_LOSS,
                    trade_id=pos["id"],
                    reason=f"Stop loss: {current_price:.0%} < entry {entry:.0%} - {stop_loss_pct:.0%}",
                    exit_price=current_price,
                ))

        return actions

    def get_source(self) -> Source:
        return self.source

    def get_analyzer(self):
        return self.resolution_analyzer


def _assess_risk(days_left: float, price: float, liquidity: float) -> list[str]:
    flags = []
    if days_left < 0.5:
        flags.append("imminent_resolution")
    if price > 0.95:
        flags.append("very_high_certainty")
    if liquidity < 10000:
        flags.append("low_liquidity")
    return flags
