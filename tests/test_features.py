"""Tests for feature computation — microstructure, probability dynamics, quality."""

import pytest
from datetime import datetime, timedelta

from farsight.markets.services.state_engine import MarketState
from farsight.markets.features import microstructure, probability, quality
from farsight.markets.services.feature_engine import compute_features


@pytest.fixture
def active_state():
    """A market state with realistic data populated."""
    state = MarketState("token_1", market_id="market_1")
    now = datetime.utcnow()

    # Simulate a price series: 0.50 → 0.55 over 5 minutes
    for i in range(60):
        t = now - timedelta(minutes=5) + timedelta(seconds=i * 5)
        price = 0.50 + (i / 60) * 0.05
        state.update_price(t, mid=price, bid=price - 0.01, ask=price + 0.01)

    # Add some trades
    for i in range(10):
        t = now - timedelta(minutes=5) + timedelta(seconds=i * 30)
        state.update_trade(t, price=0.52 + i * 0.003, size_usd=100 + i * 50)

    # Set book state
    state.update_book(bid_depth=25000, ask_depth=18000, best_bid=0.54, best_ask=0.56)

    return state


@pytest.fixture
def stale_state():
    """A market with old data."""
    state = MarketState("token_stale")
    old = datetime.utcnow() - timedelta(minutes=15)
    state.update_price(old, mid=0.50, bid=0.49, ask=0.51)
    return state


@pytest.fixture
def empty_state():
    """A freshly created state with no data."""
    return MarketState("token_empty")


class TestMicrostructureFeatures:
    def test_spread_pct(self, active_state):
        result = microstructure.spread_pct(active_state)
        assert result > 0
        assert result < 1.0  # Not a dead market

    def test_depth_imbalance_positive(self, active_state):
        # bid_depth=25000, ask_depth=18000 → positive imbalance (more buy pressure)
        result = microstructure.depth_imbalance(active_state)
        assert result > 0
        assert -1 <= result <= 1

    def test_depth_imbalance_zero_depth(self, empty_state):
        result = microstructure.depth_imbalance(empty_state)
        assert result == 0.0

    def test_trade_imbalance_5m(self, active_state):
        result = microstructure.trade_imbalance_5m(active_state)
        assert -1 <= result <= 1

    def test_quote_velocity(self, active_state):
        result = microstructure.quote_velocity(active_state)
        assert result > 0  # We added 60 price updates

    def test_trade_velocity(self, active_state):
        result = microstructure.trade_velocity(active_state)
        assert result > 0  # We added 10 trades

    def test_compute_all_returns_dict(self, active_state):
        result = microstructure.compute_all(active_state)
        assert "spread_pct" in result
        assert "depth_imbalance" in result
        assert "trade_imbalance_5m" in result
        assert "quote_velocity" in result
        assert "trade_velocity" in result


class TestProbabilityFeatures:
    def test_delta_5m_positive_trend(self, active_state):
        # Price went from 0.50 to 0.55
        delta = probability.delta_5m(active_state)
        assert delta is not None
        assert delta > 0

    def test_delta_on_empty_state(self, empty_state):
        delta = probability.delta_5m(empty_state)
        assert delta is None

    def test_acceleration(self, active_state):
        accel = probability.acceleration(active_state)
        # May or may not be None depending on window contents
        if accel is not None:
            assert isinstance(accel, float)

    def test_drift_score(self, active_state):
        drift = probability.drift_score(active_state)
        if drift is not None:
            assert -3 <= drift <= 3

    def test_reversion_score(self, active_state):
        rev = probability.reversion_score(active_state)
        if rev is not None:
            assert isinstance(rev, float)

    def test_volatility_burst(self, active_state):
        vol = probability.volatility_burst(active_state)
        if vol is not None:
            assert vol > 0

    def test_compute_all(self, active_state):
        result = probability.compute_all(active_state)
        assert "delta_5m" in result
        assert "drift_score" in result
        assert "reversion_score" in result


class TestQualityFeatures:
    def test_liquidity_score_active_market(self, active_state):
        score = quality.liquidity_score(active_state)
        assert 0 <= score <= 1
        assert score > 0.1  # Should have decent liquidity

    def test_liquidity_score_empty_market(self, empty_state):
        score = quality.liquidity_score(empty_state)
        assert score == 0.0  # No depth, no volume, max spread

    def test_stale_score_fresh(self, active_state):
        score = quality.stale_score(active_state)
        assert score < 0.3  # Updated recently

    def test_stale_score_old(self, stale_state):
        score = quality.stale_score(stale_state)
        assert score > 0.5  # 15 minutes old

    def test_stale_score_never_updated(self, empty_state):
        score = quality.stale_score(empty_state)
        assert score == 1.0

    def test_manipulation_heuristic_normal(self, active_state):
        score = quality.manipulation_heuristic(active_state)
        assert score < 0.5  # Normal market, no red flags

    def test_resolution_proximity(self, active_state):
        # End date in 30 days
        future = datetime.utcnow() + timedelta(days=30)
        days = quality.resolution_proximity(active_state, future)
        assert 29 < days < 31

    def test_resolution_proximity_no_date(self, active_state):
        days = quality.resolution_proximity(active_state, None)
        assert days == 999.0

    def test_compute_all(self, active_state):
        result = quality.compute_all(active_state)
        assert "liquidity_score" in result
        assert "stale_score" in result
        assert "manipulation_heuristic" in result
        assert "resolution_proximity_days" in result


class TestComputeFeatures:
    def test_full_feature_vector(self, active_state):
        features = compute_features(active_state)
        # Should have all feature families
        assert "spread_pct" in features  # microstructure
        assert "delta_5m" in features    # probability
        assert "liquidity_score" in features  # quality
        assert len(features) >= 15  # At least 15 features total

    def test_features_on_empty_state(self, empty_state):
        features = compute_features(empty_state)
        assert features["liquidity_score"] == 0.0
        assert features["delta_5m"] is None
