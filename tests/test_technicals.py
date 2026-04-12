"""Tests for technical indicator features."""

import pytest
from datetime import datetime, timedelta
from farsight.markets.services.state_engine import MarketState
from farsight.markets.features import technicals


@pytest.fixture
def trending_up_state():
    """Market with clear uptrend over 1 hour."""
    state = MarketState("t1")
    now = datetime.utcnow()
    for i in range(120):
        t = now - timedelta(hours=1) + timedelta(seconds=i * 30)
        price = 0.40 + (i / 120) * 0.15  # 40% → 55%
        state.update_price(t, mid=price)
        if i % 5 == 0:
            state.update_trade(t, price=price, size_usd=100 + i * 10)
    return state


@pytest.fixture
def trending_down_state():
    """Market with clear downtrend."""
    state = MarketState("t2")
    now = datetime.utcnow()
    for i in range(120):
        t = now - timedelta(hours=1) + timedelta(seconds=i * 30)
        price = 0.60 - (i / 120) * 0.15  # 60% → 45%
        state.update_price(t, mid=price)
    return state


@pytest.fixture
def flat_state():
    """Market with no trend — sideways."""
    state = MarketState("t3")
    now = datetime.utcnow()
    import random
    random.seed(42)
    for i in range(120):
        t = now - timedelta(hours=1) + timedelta(seconds=i * 30)
        price = 0.50 + random.uniform(-0.005, 0.005)
        state.update_price(t, mid=price)
    return state


class TestRSI:
    def test_rsi_uptrend_high(self, trending_up_state):
        val = technicals.rsi_1h(trending_up_state)
        assert val is not None
        assert val > 60  # Uptrend should produce high RSI

    def test_rsi_downtrend_low(self, trending_down_state):
        val = technicals.rsi_1h(trending_down_state)
        assert val is not None
        assert val < 40  # Downtrend should produce low RSI

    def test_rsi_flat_neutral(self, flat_state):
        val = technicals.rsi_1h(flat_state)
        assert val is not None
        assert 30 < val < 70  # Flat should be neutral

    def test_rsi_empty_state(self):
        state = MarketState("empty")
        assert technicals.rsi_1h(state) is None


class TestBollinger:
    def test_position_in_uptrend(self, trending_up_state):
        pos = technicals.bollinger_position(trending_up_state)
        assert pos is not None
        assert pos > 0.5  # Price above mean in uptrend

    def test_width_positive(self, trending_up_state):
        width = technicals.bollinger_width(trending_up_state)
        assert width is not None
        assert width > 0

    def test_flat_market_narrow_bands(self, flat_state, trending_up_state):
        width = technicals.bollinger_width(flat_state)
        trending_width = technicals.bollinger_width(trending_up_state)
        if width is not None and trending_width is not None:
            assert width < trending_width  # Flat = narrower bands

    def test_empty_state(self):
        state = MarketState("empty")
        assert technicals.bollinger_position(state) is None


class TestVolume:
    def test_volume_ratio_with_trades(self, trending_up_state):
        val = technicals.volume_ratio(trending_up_state)
        # May be None depending on window structure
        if val is not None:
            assert val > 0

    def test_volume_price_divergence(self, trending_up_state):
        val = technicals.volume_price_divergence(trending_up_state)
        if val is not None:
            assert -1 <= val <= 1

    def test_empty_state(self):
        state = MarketState("empty")
        assert technicals.volume_ratio(state) is None


class TestMomentum:
    def test_momentum_score_uptrend(self, trending_up_state):
        val = technicals.momentum_score(trending_up_state)
        assert val is not None
        assert val > 0  # Positive momentum in uptrend

    def test_momentum_score_downtrend(self, trending_down_state):
        val = technicals.momentum_score(trending_down_state)
        assert val is not None
        assert val < 0  # Negative momentum in downtrend

    def test_rate_of_change(self, trending_up_state):
        val = technicals.rate_of_change(trending_up_state)
        assert val is not None
        assert val > 0  # Price higher than 1h ago


class TestPriceLevels:
    def test_distance_from_midpoint_at_50(self):
        state = MarketState("t")
        state.update_price(datetime.utcnow(), mid=0.50)
        assert technicals.distance_from_midpoint(state) == pytest.approx(0.0)

    def test_distance_from_midpoint_at_extreme(self):
        state = MarketState("t")
        state.update_price(datetime.utcnow(), mid=0.90)
        assert technicals.distance_from_midpoint(state) == pytest.approx(0.40)

    def test_price_percentile(self, trending_up_state):
        val = technicals.price_percentile(trending_up_state)
        assert val is not None
        assert val > 0.7  # Current price near top of 4h range (uptrend)

    def test_price_percentile_empty(self):
        state = MarketState("empty")
        assert technicals.price_percentile(state) is None


class TestComputeAll:
    def test_returns_all_features(self, trending_up_state):
        features = technicals.compute_all(trending_up_state)
        expected_keys = [
            "rsi_1h", "rsi_4h", "bollinger_position", "bollinger_width",
            "volume_ratio", "volume_price_divergence", "rate_of_change",
            "momentum_score", "distance_from_midpoint", "price_percentile",
        ]
        for key in expected_keys:
            assert key in features, f"Missing feature: {key}"

    def test_empty_state_no_crash(self):
        state = MarketState("empty")
        features = technicals.compute_all(state)
        assert "rsi_1h" in features
        assert "momentum_score" in features
