"""
VenueMatcher — matches equivalent markets across Polymarket and Kalshi.

The core challenge: Polymarket uses free-text questions and slugs,
Kalshi uses structured tickers. We need to determine when two markets
on different platforms are asking the same question.

Matching approaches (in priority order):
1. Manual mapping table — curated, highest confidence
2. Keyword overlap — tokenize questions, compare common terms
3. Category + date — same topic area + same resolution timeframe

Each match has a confidence score. Only high-confidence matches
are used for cross-venue arbitrage signals.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class VenueMatch:
    """A matched pair of markets across two venues."""
    polymarket_id: str           # condition_id
    polymarket_question: str
    polymarket_slug: str
    polymarket_price: float      # YES probability

    kalshi_ticker: str
    kalshi_question: str         # yes_sub_title
    kalshi_price: float          # YES probability

    confidence: float            # 0-1, how sure we are these are the same question
    match_method: str            # "manual", "keyword", "category_date"

    # Derived
    spread: float = 0.0         # |polymarket_price - kalshi_price|
    direction: str = ""         # "poly_higher" | "kalshi_higher"

    def compute_spread(self):
        self.spread = abs(self.polymarket_price - self.kalshi_price)
        if self.polymarket_price > self.kalshi_price:
            self.direction = "poly_higher"
        elif self.kalshi_price > self.polymarket_price:
            self.direction = "kalshi_higher"
        else:
            self.direction = "equal"
        return self.spread


# ── Manual Mapping Table ─────────────────────────────────────────────
# High-value known matches. Extend as you discover equivalent markets.

MANUAL_MAPPINGS: list[dict] = [
    # Fed rate decisions
    {
        "polymarket_pattern": r"fed.*cut.*rate|fed.*rate.*cut|fomc.*cut",
        "kalshi_pattern": r"KXFED|fed.*fund.*rate",
        "topic": "fed_rate",
    },
    # Bitcoin price targets
    {
        "polymarket_pattern": r"bitcoin.*\$?\d+k|btc.*above.*\$?\d+",
        "kalshi_pattern": r"KXBTC|bitcoin.*above|btc.*price",
        "topic": "btc_price",
    },
    # US elections
    {
        "polymarket_pattern": r"president.*202[4-9]|presidential.*election|democrat.*nominee|republican.*nominee",
        "kalshi_pattern": r"PRES|president.*election|democrat|republican",
        "topic": "us_election",
    },
    # Recession
    {
        "polymarket_pattern": r"recession.*202[5-7]|gdp.*negative",
        "kalshi_pattern": r"KXRECESSION|recession|gdp.*contract",
        "topic": "recession",
    },
    # Government shutdown
    {
        "polymarket_pattern": r"government.*shutdown",
        "kalshi_pattern": r"KXSHUTDOWN|government.*shutdown",
        "topic": "gov_shutdown",
    },
]


class VenueMatcher:
    """Matches equivalent markets between Polymarket and Kalshi."""

    def __init__(self, min_confidence: float = 0.5):
        self.min_confidence = min_confidence

    def match_markets(
        self,
        polymarket_markets: list[dict],
        kalshi_markets: list[dict],
    ) -> list[VenueMatch]:
        """Find matching market pairs between the two venues.

        Args:
            polymarket_markets: List of normalized Polymarket markets
                (must have: condition_id, question, slug, outcomes[0].current_price)
            kalshi_markets: List of Kalshi market dicts from the API
                (must have: ticker, yes_sub_title, yes_bid_dollars, etc.)

        Returns sorted by spread descending (biggest divergences first).
        """
        matches = []

        for pm in polymarket_markets:
            pm_question = (pm.get("question") or "").lower()
            pm_slug = (pm.get("slug") or "").lower()
            pm_price = self._get_pm_price(pm)
            pm_id = pm.get("condition_id") or pm.get("market_id") or ""

            if pm_price <= 0:
                continue

            for km in kalshi_markets:
                km_question = (km.get("yes_sub_title") or km.get("title") or "").lower()
                km_ticker = km.get("ticker", "")
                km_price = self._get_kalshi_price(km)

                if km_price <= 0:
                    continue

                # Try matching
                confidence, method = self._compute_match_confidence(
                    pm_question, pm_slug, km_question, km_ticker,
                )

                if confidence >= self.min_confidence:
                    match = VenueMatch(
                        polymarket_id=pm_id,
                        polymarket_question=pm.get("question", "")[:100],
                        polymarket_slug=pm.get("slug", ""),
                        polymarket_price=pm_price,
                        kalshi_ticker=km_ticker,
                        kalshi_question=km_question[:100],
                        kalshi_price=km_price,
                        confidence=confidence,
                        match_method=method,
                    )
                    match.compute_spread()
                    matches.append(match)

        # Deduplicate — keep best match per Polymarket market
        best: dict[str, VenueMatch] = {}
        for m in matches:
            key = m.polymarket_id
            if key not in best or m.confidence > best[key].confidence:
                best[key] = m

        result = sorted(best.values(), key=lambda m: m.spread, reverse=True)
        return result

    def _compute_match_confidence(
        self,
        pm_question: str,
        pm_slug: str,
        km_question: str,
        km_ticker: str,
    ) -> tuple[float, str]:
        """Compute matching confidence between a Polymarket and Kalshi market.

        Returns (confidence, method).
        """
        # 1. Manual mapping (highest confidence)
        for mapping in MANUAL_MAPPINGS:
            pm_match = re.search(mapping["polymarket_pattern"], pm_question) or \
                       re.search(mapping["polymarket_pattern"], pm_slug)
            km_match = re.search(mapping["kalshi_pattern"], km_question) or \
                       re.search(mapping["kalshi_pattern"], km_ticker.lower())
            if pm_match and km_match:
                return 0.9, "manual"

        # 2. Keyword overlap
        pm_tokens = self._tokenize(pm_question + " " + pm_slug)
        km_tokens = self._tokenize(km_question + " " + km_ticker.lower())

        if not pm_tokens or not km_tokens:
            return 0.0, ""

        overlap = pm_tokens & km_tokens
        # Remove common stopwords from overlap count
        stopwords = {"will", "the", "be", "in", "by", "on", "of", "a", "to", "for", "and", "or", "is", "at"}
        meaningful_overlap = overlap - stopwords

        if not meaningful_overlap:
            return 0.0, ""

        # Jaccard-like similarity on meaningful tokens
        union = pm_tokens | km_tokens - stopwords
        similarity = len(meaningful_overlap) / len(union) if union else 0

        # Boost for key entity matches (names, numbers, dates)
        entity_boost = 0
        for token in meaningful_overlap:
            if re.match(r"\d+", token):  # Numbers (years, prices, percentages)
                entity_boost += 0.1
            if len(token) > 5:  # Longer words are more specific
                entity_boost += 0.05

        confidence = min(0.85, similarity * 0.6 + entity_boost)

        if confidence >= 0.3:
            return confidence, "keyword"

        return 0.0, ""

    @staticmethod
    def _tokenize(text: str) -> set[str]:
        """Tokenize text into meaningful words."""
        # Remove punctuation, split on whitespace
        cleaned = re.sub(r"[^\w\s]", " ", text.lower())
        tokens = set(cleaned.split())
        # Remove very short tokens
        return {t for t in tokens if len(t) > 1}

    @staticmethod
    def _get_pm_price(market: dict) -> float:
        """Extract probability from a Polymarket market dict."""
        outcomes = market.get("outcomes", [])
        if outcomes and hasattr(outcomes[0], "current_price"):
            return outcomes[0].current_price
        # Try schema dict format
        if outcomes and isinstance(outcomes[0], dict):
            return float(outcomes[0].get("current_price", 0))
        return 0.0

    @staticmethod
    def _get_kalshi_price(market: dict) -> float:
        """Extract probability from a Kalshi market dict."""
        from farsight.markets.clients.kalshi.rest_client import KalshiClient
        return KalshiClient.market_to_probability(market)
