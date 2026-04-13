"""
ResolutionScalper — captures value as markets approach resolution.

Pipeline:
    NearResolutionSource     → Fetch markets resolving within 7 days
    OrderbookEnricher        → Add orderbook depth + spread
    ResolutionAnalyzer       → Estimate fair value based on time to resolution
    DiscountScorer           → Score opportunities by discount to fair value
    QualityFilter            → Remove low-confidence, illiquid candidates

Hybrid mode:
  - Scan: periodically find near-resolution candidates
  - Stream: watch candidates in real-time for optimal entry timing

Agent skill: "What near-resolution markets have uncertainty discounts?"
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from farsight.markets.clients.polymarket.clob_client import ClobClient
from farsight.markets.clients.polymarket.gamma_client import GammaClient, _infer_category
from farsight.markets.strategies.base import (
    Action,
    ActionType,
    Analyzer,
    Enricher,
    Filter,
    MarketContext,
    Opportunity,
    Scorer,
    Source,
    Strategy,
    StrategyMode,
)

logger = logging.getLogger(__name__)


# ── Pipeline Stages ──────────────────────────────────────────────────


# Categories whose resolutions are hard/date-driven — convergence-to-certainty
# holds. Subjective or open-ended categories (entertainment, "will X tweet Y")
# are excluded by default.
DEFAULT_CATEGORY_ALLOWLIST: frozenset[str] = frozenset({
    "politics", "geopolitics", "sports", "elections",
})


class NearResolutionSource(Source):
    """Fetch markets resolving within N days, restricted to categories where
    convergence-to-certainty is a reasonable prior.

    Agent skill: "What prediction markets are resolving soon?"
    """

    def __init__(
        self,
        gamma: GammaClient,
        max_days: int = 7,
        categories: Optional[frozenset[str]] = None,
    ):
        self.gamma = gamma
        self.max_days = max_days
        self.categories = categories if categories is not None else DEFAULT_CATEGORY_ALLOWLIST

    async def fetch(self) -> list[MarketContext]:
        raw_markets = await self.gamma.get_markets(
            active=True, closed=False,
            limit=100,
            order="end_date", ascending=True,
        )

        now = datetime.utcnow()
        contexts = []

        for raw in raw_markets:
            market = GammaClient.normalize_market(raw)

            if not market.end_date:
                continue

            end_naive = market.end_date.replace(tzinfo=None) if market.end_date.tzinfo else market.end_date
            days_left = (end_naive - now).total_seconds() / 86400
            if days_left < 0 or days_left > self.max_days:
                continue

            slug = market.slug or ""
            if "updown-5m" in slug or "updown-15m" in slug:
                continue

            if not market.outcomes or len(market.outcomes) < 2:
                continue

            category = (raw.get("category") or "").lower() or _infer_category(raw)
            if self.categories and category not in self.categories:
                continue

            primary = market.outcomes[0]

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

        return contexts


# Per-category convergence profiles. Three parameters:
#   horizon_days: the window over which convergence plays out
#   convexity:    shape of the gap-decay curve
#                   <1  flat early, snap late  (event-driven: sports, elections)
#                   =1  linear
#                   >1  drifts early, plateaus late (info-diffusion: geopolitics)
#   strength:     ceiling on the pull — our prior that the current leader
#                 actually wins. Fair value at t=0 is capped at
#                 price + strength · (1 - price).
# Tuned priors, not fitted — revisit once we persist resolution outcomes.
CONVERGENCE_PROFILES: dict[str, dict[str, float]] = {
    "sports":      {"horizon_days":  1.0, "convexity": 0.30, "strength": 0.98},
    "elections":   {"horizon_days":  3.0, "convexity": 0.50, "strength": 0.95},
    "politics":    {"horizon_days":  7.0, "convexity": 0.70, "strength": 0.85},
    "geopolitics": {"horizon_days": 14.0, "convexity": 1.50, "strength": 0.70},
}
DEFAULT_CONVERGENCE_PROFILE = {"horizon_days": 7.0, "convexity": 1.0, "strength": 0.80}


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

        # Only works for near-certain outcomes
        if price < self.min_certainty and (1 - price) < self.min_certainty:
            ctx.features = {"fair_value": price, "discount": 0, "qualified": False}
            return ctx

        profile = self.profiles.get(category or "", DEFAULT_CONVERGENCE_PROFILE)
        fair_value = self._estimate_fair_value(price, days_left, profile)
        discount = fair_value - price

        ctx.features = {
            "fair_value": fair_value,
            "discount": discount,
            "days_left": days_left,
            "category": category,
            "current_certainty": max(price, 1 - price),
            "qualified": discount > 0.03,
        }
        return ctx

    @staticmethod
    def _estimate_fair_value(
        current_price: float,
        days_left: float,
        profile: Optional[dict[str, float]] = None,
    ) -> float:
        """Estimate fair value via power-law decay of the remaining gap.

        fair_value = price + (fv_at_zero - price) * (1 - t^k)
            t = clamp(days_left / horizon, 0, 1)
            k = convexity (shape of decay)
            fv_at_zero = min(0.99, price + strength * (1 - price))
        """
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
    """Convert resolution analysis into scored opportunities.

    Agent skill: "What's the discount on this near-resolution market?"
    """

    def __init__(self, min_discount: float = 0.03):
        self.min_discount = min_discount

    def score(self, ctx: MarketContext) -> list[Opportunity]:
        if not ctx.features.get("qualified"):
            return []

        discount = ctx.features.get("discount", 0)
        if discount < self.min_discount:
            return []

        fair_value = ctx.features["fair_value"]
        days_left = ctx.features.get("days_left", 999)

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
            confidence=min(0.95, ctx.current_price + 0.05),
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
              → DiscountScorer → QualityFilter
    """

    name = "resolution"
    mode = StrategyMode.HYBRID
    scan_interval_seconds = 900

    def __init__(
        self,
        gamma: Optional[GammaClient] = None,
        clob: Optional[ClobClient] = None,
        max_days: int = 14,
        min_certainty: float = 0.75,
        min_discount: float = 0.02,
        min_liquidity: float = 5000,
        categories: Optional[frozenset[str]] = None,
        convergence_profiles: Optional[dict[str, dict[str, float]]] = None,
    ):
        gamma = gamma or GammaClient()
        clob = clob or ClobClient()

        self.source = NearResolutionSource(gamma, max_days=max_days, categories=categories)
        self.orderbook_enricher = None  # Import inline to avoid circular
        self._clob = clob
        self.resolution_analyzer = ResolutionAnalyzer(
            min_certainty=min_certainty,
            profiles=convergence_profiles,
        )
        self.discount_scorer = DiscountScorer(min_discount=min_discount)
        self.quality_filter = Filter(min_edge=min_discount, min_liquidity=min_liquidity)

    async def scan(self) -> list[Opportunity]:
        from farsight.markets.strategies.opportunity_scanner import OrderbookEnricher

        orderbook_enricher = OrderbookEnricher(self._clob)

        # 1. Source
        contexts = await self.source.fetch()
        logger.info(f"ResolutionScalper: sourced {len(contexts)} near-resolution markets")

        all_opportunities = []

        for ctx in contexts:
            try:
                # 2. Enrich
                ctx = await orderbook_enricher.enrich(ctx)
                if ctx.liquidity < 1000:
                    continue

                # 3. Analyze
                ctx = self.resolution_analyzer.analyze(ctx)

                # 4. Score
                opps = self.discount_scorer.score(ctx)
                all_opportunities.extend(opps)
            except Exception as e:
                logger.debug(f"Resolution: error on {ctx.market_question[:40]}: {e}")

        # 5. Filter
        filtered = self.quality_filter.filter(all_opportunities)
        logger.info(f"ResolutionScalper: {len(all_opportunities)} raw → {len(filtered)} after filter")
        return filtered

    async def monitor(self, open_positions: list[dict]) -> list[Action]:
        actions = []
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

            if current_price >= 0.97:
                actions.append(Action(
                    action_type=ActionType.CLOSE,
                    trade_id=pos["id"],
                    reason=f"Price reached {current_price:.0%} — taking profit",
                    exit_price=current_price,
                ))
            elif entry > 0 and current_price < entry - 0.05:
                actions.append(Action(
                    action_type=ActionType.STOP_LOSS,
                    trade_id=pos["id"],
                    reason=f"Stop loss: {current_price:.0%} < entry {entry:.0%} - 5%",
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
