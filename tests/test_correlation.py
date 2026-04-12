"""Tests for CorrelationService with fake market data."""

import pytest
from uuid import uuid4

from farsight.markets.schemas.signals import Direction, SignalSchema, SignalType
from farsight.markets.services.correlation_service import CorrelationService
from farsight.markets.services.theme_service import ThemeService


class FakeMarketData:
    """Fake market data provider for testing."""

    def __init__(self, prices: dict[str, dict]):
        self.prices = prices

    async def get_price_change(self, ticker: str) -> dict | None:
        return self.prices.get(ticker)


def _make_signal(direction="bullish", confidence=0.7) -> SignalSchema:
    return SignalSchema(
        id=uuid4(),
        market_id="test_market",
        source="polymarket",
        signal_type=SignalType.PROBABILITY_SHOCK,
        direction=Direction(direction),
        confidence=confidence,
        horizon="1h",
        tradability_score=0.6,
        model_probability=0.60,
        market_price=0.50,
        edge=0.10,
        feature_set_version="v1",
        rule_version="v1",
    )


class TestCorrelation:
    @pytest.mark.asyncio
    async def test_bullish_signal_confirmed_by_rising_asset(self):
        """Bullish PM signal + SPY going up = confirming."""
        fake_data = FakeMarketData({
            "TLT": {"price": 92.0, "change_pct": 1.5},
            "SPY": {"price": 520.0, "change_pct": 0.8},
        })
        svc = CorrelationService(theme_service=ThemeService(), market_data=fake_data)
        signal = _make_signal(direction="bullish")

        result = await svc.correlate_signal(signal, "Will the Fed cut rates?")

        assert len(result["confirmations"]) >= 1
        assert result["composite_confidence"] >= signal.confidence  # Boosted
        assert result["confidence_boost"] > 0

    @pytest.mark.asyncio
    async def test_bearish_signal_confirmed_by_falling_asset(self):
        """Bearish PM signal + SPY going down = confirming."""
        fake_data = FakeMarketData({
            "TLT": {"price": 90.0, "change_pct": -2.0},
            "SPY": {"price": 510.0, "change_pct": -1.0},
        })
        svc = CorrelationService(theme_service=ThemeService(), market_data=fake_data)
        signal = _make_signal(direction="bearish")

        result = await svc.correlate_signal(signal, "Will the Fed cut rates?")

        assert len(result["confirmations"]) >= 1
        assert result["confidence_boost"] > 0

    @pytest.mark.asyncio
    async def test_bullish_signal_diverges_from_falling_asset(self):
        """Bullish PM signal + SPY going down = diverging."""
        fake_data = FakeMarketData({
            "SPY": {"price": 510.0, "change_pct": -1.5},
            "TLT": {"price": 88.0, "change_pct": -0.5},
        })
        svc = CorrelationService(theme_service=ThemeService(), market_data=fake_data)
        signal = _make_signal(direction="bullish")

        result = await svc.correlate_signal(signal, "Will the Fed cut rates?")

        assert len(result["divergences"]) >= 1
        assert result["confidence_boost"] < 0  # Reduced confidence

    @pytest.mark.asyncio
    async def test_no_related_tickers(self):
        """Market with no theme mapping should return unchanged confidence."""
        fake_data = FakeMarketData({})
        svc = CorrelationService(theme_service=ThemeService(), market_data=fake_data)
        signal = _make_signal(confidence=0.7)

        result = await svc.correlate_signal(signal, "Will it rain tomorrow?")

        assert result["related_tickers"] == []
        assert result["composite_confidence"] == 0.7

    @pytest.mark.asyncio
    async def test_evidence_generated(self):
        fake_data = FakeMarketData({
            "BTC-USD": {"price": 95000.0, "change_pct": 3.5},
        })
        svc = CorrelationService(theme_service=ThemeService(), market_data=fake_data)
        signal = _make_signal(direction="bullish")

        result = await svc.correlate_signal(signal, "Will Bitcoin hit $100K?")

        assert len(result["evidence"]) >= 1
        assert "BTC-USD" in result["evidence"][0].description

