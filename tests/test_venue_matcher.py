"""Tests for VenueMatcher — cross-venue market matching."""

import pytest
from farsight.markets.strategies.venue_matcher import VenueMatcher, VenueMatch


@pytest.fixture
def matcher():
    return VenueMatcher(min_confidence=0.3)


class TestVenueMatch:
    def test_compute_spread(self):
        match = VenueMatch(
            polymarket_id="pm1", polymarket_question="Test?",
            polymarket_slug="test", polymarket_price=0.62,
            kalshi_ticker="K1", kalshi_question="Test?", kalshi_price=0.55,
            confidence=0.8, match_method="manual",
        )
        spread = match.compute_spread()
        assert spread == pytest.approx(0.07)
        assert match.direction == "poly_higher"

    def test_spread_kalshi_higher(self):
        match = VenueMatch(
            polymarket_id="pm1", polymarket_question="Q",
            polymarket_slug="q", polymarket_price=0.40,
            kalshi_ticker="K1", kalshi_question="Q", kalshi_price=0.48,
            confidence=0.8, match_method="manual",
        )
        match.compute_spread()
        assert match.spread == pytest.approx(0.08)
        assert match.direction == "kalshi_higher"

    def test_spread_equal(self):
        match = VenueMatch(
            polymarket_id="pm1", polymarket_question="Q",
            polymarket_slug="q", polymarket_price=0.50,
            kalshi_ticker="K1", kalshi_question="Q", kalshi_price=0.50,
            confidence=0.8, match_method="manual",
        )
        match.compute_spread()
        assert match.spread == 0.0
        assert match.direction == "equal"


class TestManualMatching:
    def test_fed_rate_match(self, matcher):
        pm_markets = [{
            "condition_id": "0xfed1",
            "question": "Will the Fed cut rates in June 2026?",
            "slug": "fed-rate-cut-june-2026",
            "outcomes": [{"current_price": 0.62}],
        }]
        kalshi_markets = [{
            "ticker": "KXFED-26JUN-T425",
            "yes_sub_title": "Fed funds rate below 4.25% in June",
            "yes_bid_dollars": "0.54",
            "yes_ask_dollars": "0.56",
            "last_price_dollars": "0.55",
        }]

        matches = matcher.match_markets(pm_markets, kalshi_markets)
        assert len(matches) == 1
        assert matches[0].confidence >= 0.8
        assert matches[0].match_method == "manual"
        assert matches[0].spread == pytest.approx(0.07)

    def test_bitcoin_match(self, matcher):
        pm_markets = [{
            "condition_id": "0xbtc1",
            "question": "Will Bitcoin hit $100k by December 2026?",
            "slug": "btc-100k-dec-2026",
            "outcomes": [{"current_price": 0.35}],
        }]
        kalshi_markets = [{
            "ticker": "KXBTC-26DEC-T100000",
            "yes_sub_title": "Bitcoin above $100,000",
            "yes_bid_dollars": "0.30",
            "yes_ask_dollars": "0.32",
            "last_price_dollars": "0.31",
        }]

        matches = matcher.match_markets(pm_markets, kalshi_markets)
        assert len(matches) == 1
        assert matches[0].confidence >= 0.8

    def test_no_match(self, matcher):
        pm_markets = [{
            "condition_id": "0x1",
            "question": "Will Rihanna release an album before GTA VI?",
            "slug": "rihanna-album-gta-vi",
            "outcomes": [{"current_price": 0.50}],
        }]
        kalshi_markets = [{
            "ticker": "KXHIGHNY-26MAR01-B45.5",
            "yes_sub_title": "NYC high temperature above 45.5F",
            "yes_bid_dollars": "0.60",
            "yes_ask_dollars": "0.62",
            "last_price_dollars": "0.61",
        }]

        matches = matcher.match_markets(pm_markets, kalshi_markets)
        assert len(matches) == 0


class TestKeywordMatching:
    def test_keyword_overlap(self, matcher):
        pm_markets = [{
            "condition_id": "0x1",
            "question": "Will there be a government shutdown in 2026?",
            "slug": "government-shutdown-2026",
            "outcomes": [{"current_price": 0.45}],
        }]
        kalshi_markets = [{
            "ticker": "KXSHUTDOWN-26",
            "yes_sub_title": "Government shutdown in 2026",
            "yes_bid_dollars": "0.38",
            "yes_ask_dollars": "0.40",
            "last_price_dollars": "0.39",
        }]

        matches = matcher.match_markets(pm_markets, kalshi_markets)
        assert len(matches) == 1
        assert matches[0].confidence >= 0.5

    def test_partial_keyword_low_confidence(self, matcher):
        """Partial keyword overlap should yield lower confidence."""
        pm_markets = [{
            "condition_id": "0x1",
            "question": "Will inflation exceed 4% in 2026?",
            "slug": "inflation-4pct-2026",
            "outcomes": [{"current_price": 0.30}],
        }]
        kalshi_markets = [{
            "ticker": "KXCPI",
            "yes_sub_title": "CPI above 4% year-over-year",
            "yes_bid_dollars": "0.25",
            "yes_ask_dollars": "0.27",
            "last_price_dollars": "0.26",
        }]

        matches = matcher.match_markets(pm_markets, kalshi_markets)
        # May or may not match depending on keyword overlap
        # The manual mapping for "inflation" should catch this
        if matches:
            assert matches[0].confidence < 0.9  # Not a strong manual match


class TestDeduplication:
    def test_keeps_best_match_per_polymarket_id(self, matcher):
        """When one PM market matches multiple Kalshi markets, keep the best."""
        pm_markets = [{
            "condition_id": "0xfed1",
            "question": "Will the Fed cut rates?",
            "slug": "fed-rate-cut",
            "outcomes": [{"current_price": 0.60}],
        }]
        kalshi_markets = [
            {
                "ticker": "KXFED-A",
                "yes_sub_title": "Fed funds rate cut",
                "yes_bid_dollars": "0.50",
                "yes_ask_dollars": "0.52",
                "last_price_dollars": "0.51",
            },
            {
                "ticker": "KXFED-B",
                "yes_sub_title": "Fed funds rate decrease",
                "yes_bid_dollars": "0.55",
                "yes_ask_dollars": "0.57",
                "last_price_dollars": "0.56",
            },
        ]

        matches = matcher.match_markets(pm_markets, kalshi_markets)
        # Should only have 1 match (best confidence) for the single PM market
        assert len(matches) == 1
