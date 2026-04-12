"""
CrossVenueArbitrage — detects price divergences between Polymarket and Kalshi.

Pipeline:
    DualVenueSource          → Fetch open markets from both platforms
    VenueMatchEnricher       → Match equivalent markets across venues
    DivergenceAnalyzer       → Compute spread and identify significant divergences
    ArbScorer                → Score opportunities by spread * liquidity * confidence
    QualityFilter            → Remove low-confidence matches and thin markets

When the same question trades at different prices on two CFTC-regulated
and crypto-native exchanges, one of them is wrong. This is the highest
confidence signal type — it's mathematically grounded.

Example:
    Polymarket: "Fed rate cut June" = 62%
    Kalshi:     "Fed rate cut June" = 55%
    → 7 point divergence. Buy the cheap side, sell the expensive side.

Agent skill: "Are Polymarket and Kalshi disagreeing on any events?"
"""

import logging
from typing import Optional

from farsight.markets.clients.kalshi.rest_client import KalshiClient
from farsight.markets.clients.polymarket.gamma_client import GammaClient
from farsight.markets.strategies.base import (
    Filter,
    MarketContext,
    Opportunity,
    Source,
    Strategy,
    StrategyMode,
)
from farsight.markets.strategies.venue_matcher import VenueMatcher, VenueMatch

logger = logging.getLogger(__name__)


class DualVenueSource(Source):
    """Fetch markets from both Polymarket and Kalshi.

    Agent skill: "What markets are active on both Polymarket and Kalshi?"
    """

    def __init__(
        self,
        gamma: GammaClient,
        kalshi: KalshiClient,
        pm_limit: int = 50,
        kalshi_limit: int = 200,
    ):
        self.gamma = gamma
        self.kalshi = kalshi
        self.pm_limit = pm_limit
        self.kalshi_limit = kalshi_limit

    async def fetch(self) -> list[MarketContext]:
        """Fetch from both venues and return combined contexts."""
        # This source returns a single MarketContext containing both venue datasets
        # (The matcher operates on the combined set, not per-market)

        # Polymarket
        pm_markets_raw = await self.gamma.get_markets(
            active=True, closed=False,
            limit=self.pm_limit,
            order="volume_24hr", ascending=False,
        )
        pm_markets = []
        for raw in pm_markets_raw:
            slug = raw.get("slug", "")
            if "updown-5m" in slug or "updown-15m" in slug:
                continue
            market = GammaClient.normalize_market(raw)
            pm_markets.append({
                "condition_id": market.condition_id,
                "question": market.question,
                "slug": market.slug,
                "outcomes": market.outcomes,
                "volume": market.volume_total,
                "liquidity": market.liquidity,
            })

        # Kalshi
        kalshi_result = await self.kalshi.get_markets(status="open", limit=self.kalshi_limit)
        kalshi_markets = kalshi_result.get("markets", [])

        # Return as a single context carrying both datasets
        ctx = MarketContext(
            market_id="cross_venue_scan",
            market_question="Cross-venue arbitrage scan",
            source_platform="dual",
        )
        ctx._pm_markets = pm_markets
        ctx._kalshi_markets = kalshi_markets
        return [ctx]


class CrossVenueArbitrage(Strategy):
    """Detect price divergences between Polymarket and Kalshi.

    Pipeline: DualVenueSource → VenueMatcher → DivergenceScorer → QualityFilter
    """

    name = "cross_venue"
    mode = StrategyMode.SCAN
    scan_interval_seconds = 900  # Every 15 minutes

    def __init__(
        self,
        gamma: Optional[GammaClient] = None,
        kalshi: Optional[KalshiClient] = None,
        min_spread: float = 0.03,      # 3% minimum divergence
        min_confidence: float = 0.5,    # Match confidence
        min_liquidity: float = 5000,
    ):
        gamma = gamma or GammaClient()
        kalshi = kalshi or KalshiClient()

        self.source = DualVenueSource(gamma, kalshi)
        self.matcher = VenueMatcher(min_confidence=min_confidence)
        self.min_spread = min_spread
        self.quality_filter = Filter(
            min_edge=min_spread,
            min_confidence=min_confidence,
            min_liquidity=min_liquidity,
        )

    async def scan(self) -> list[Opportunity]:
        """Scan for cross-venue divergences."""
        # 1. Fetch from both venues
        contexts = await self.source.fetch()
        if not contexts:
            return []

        ctx = contexts[0]
        pm_markets = getattr(ctx, "_pm_markets", [])
        kalshi_markets = getattr(ctx, "_kalshi_markets", [])

        logger.info(f"CrossVenueArb: {len(pm_markets)} Polymarket, {len(kalshi_markets)} Kalshi markets")

        # 2. Match equivalent markets
        matches = self.matcher.match_markets(pm_markets, kalshi_markets)
        logger.info(f"CrossVenueArb: {len(matches)} cross-venue matches found")

        # 3. Convert significant divergences to opportunities
        opportunities = []
        for match in matches:
            if match.spread < self.min_spread:
                continue

            # Determine which side to buy (cheaper) and sell (more expensive)
            if match.direction == "poly_higher":
                # Polymarket is expensive, Kalshi is cheap → buy on Kalshi
                buy_venue = "kalshi"
                sell_venue = "polymarket"
                buy_price = match.kalshi_price
                sell_price = match.polymarket_price
            else:
                buy_venue = "polymarket"
                sell_venue = "kalshi"
                buy_price = match.polymarket_price
                sell_price = match.kalshi_price

            opp = Opportunity(
                market_id=match.polymarket_id,
                market_question=match.polymarket_question,
                event_slug=match.polymarket_slug,
                outcome="Yes",
                strategy=self.name,
                reasoning=(
                    f"Cross-venue divergence: {match.spread:.1%} spread. "
                    f"Polymarket={match.polymarket_price:.0%}, "
                    f"Kalshi={match.kalshi_price:.0%}. "
                    f"Buy on {buy_venue} @ {buy_price:.0%}, "
                    f"implied sell on {sell_venue} @ {sell_price:.0%}. "
                    f"Match confidence: {match.confidence:.0%} ({match.match_method})"
                ),
                direction="buy",
                entry_price=buy_price,
                model_price=sell_price,
                edge=match.spread,
                confidence=match.confidence,
                liquidity=0,  # Would need orderbook fetch for accurate liquidity
                spread=0.02,  # Estimated execution spread
                risk_flags=self._assess_risk(match),
            )
            opp.compute_score()
            opportunities.append(opp)

        # 4. Filter and rank
        filtered = self.quality_filter.filter(opportunities)
        logger.info(f"CrossVenueArb: {len(opportunities)} divergences → {len(filtered)} after filter")
        return filtered

    @staticmethod
    def _assess_risk(match: VenueMatch) -> list[str]:
        flags = []
        if match.confidence < 0.7:
            flags.append("uncertain_match")
        if match.spread > 0.15:
            flags.append("extreme_divergence")  # Might be different questions
        if match.match_method == "keyword":
            flags.append("keyword_match_only")  # Not manually verified
        return flags

    def get_source(self) -> Source:
        return self.source
