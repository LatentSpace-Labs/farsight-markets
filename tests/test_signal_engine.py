"""Tests for signal detection, filter chain, and signal engine."""

import pytest
from datetime import datetime, timedelta
from uuid import uuid4

from farsight.markets.services.signal_engine import (
    SignalEngine,
    SignalFilter,
    detect_probability_shock,
    detect_momentum_continuation,
    detect_mean_reversion,
    detect_thematic_repricing,
    detect_structural_inconsistency,
)
from farsight.markets.schemas.signals import Direction, SignalType, SignalStatus
from farsight.markets.engine.event_bus import EventBus


# ── Detector Tests ───────────────────────────────────────────────────


class TestProbabilityShockDetector:
    def test_detects_large_positive_move(self):
        features = {"delta_5m": 0.08, "liquidity_score": 0.7, "trade_velocity": 2.0}
        signal = detect_probability_shock(features, "t1", "m1")
        assert signal is not None
        assert signal.signal_type == SignalType.PROBABILITY_SHOCK
        assert signal.direction == Direction.BULLISH
        assert signal.confidence > 0.4

    def test_detects_large_negative_move(self):
        features = {"delta_5m": -0.06, "liquidity_score": 0.5, "trade_velocity": 1.0}
        signal = detect_probability_shock(features, "t1", "m1")
        assert signal is not None
        assert signal.direction == Direction.BEARISH

    def test_ignores_small_move(self):
        features = {"delta_5m": 0.02, "liquidity_score": 0.8}
        signal = detect_probability_shock(features, "t1", "m1")
        assert signal is None

    def test_ignores_none_delta(self):
        features = {"delta_5m": None}
        signal = detect_probability_shock(features, "t1", "m1")
        assert signal is None


class TestMomentumDetector:
    def test_detects_strong_drift(self):
        features = {"drift_score": 2.0, "acceleration": 0.005, "trade_velocity": 1.0}
        signal = detect_momentum_continuation(features, "t1", "m1")
        assert signal is not None
        assert signal.signal_type == SignalType.MOMENTUM_CONTINUATION
        assert signal.direction == Direction.BULLISH

    def test_ignores_weak_drift(self):
        features = {"drift_score": 0.5, "acceleration": 0.001, "trade_velocity": 1.0}
        signal = detect_momentum_continuation(features, "t1", "m1")
        assert signal is None

    def test_ignores_no_trades(self):
        features = {"drift_score": 2.5, "acceleration": 0.01, "trade_velocity": 0.0}
        signal = detect_momentum_continuation(features, "t1", "m1")
        assert signal is None


class TestMeanReversionDetector:
    def test_detects_overextended_up(self):
        features = {"reversion_score": 2.5, "liquidity_score": 0.6}
        signal = detect_mean_reversion(features, "t1", "m1")
        assert signal is not None
        assert signal.signal_type == SignalType.MEAN_REVERSION
        assert signal.direction == Direction.BEARISH  # Revert DOWN from high

    def test_detects_overextended_down(self):
        features = {"reversion_score": -3.0, "liquidity_score": 0.8}
        signal = detect_mean_reversion(features, "t1", "m1")
        assert signal is not None
        assert signal.direction == Direction.BULLISH  # Revert UP from low

    def test_ignores_normal(self):
        features = {"reversion_score": 1.0, "liquidity_score": 0.5}
        signal = detect_mean_reversion(features, "t1", "m1")
        assert signal is None


class TestThematicRepricingDetector:
    def test_detects_broad_positive_move(self):
        features = {"delta_5m": 0.03}
        siblings = [0.04, 0.05, 0.03, 0.06]  # 4 markets moving up
        signal = detect_thematic_repricing(features, "t1", "m1", siblings)
        assert signal is not None
        assert signal.signal_type == SignalType.THEMATIC_REPRICING
        assert signal.direction == Direction.BULLISH

    def test_ignores_insufficient_breadth(self):
        features = {"delta_5m": 0.03}
        siblings = [0.04, 0.05]  # Only 2 — below threshold of 3
        signal = detect_thematic_repricing(features, "t1", "m1", siblings)
        assert signal is None

    def test_ignores_no_siblings(self):
        features = {"delta_5m": 0.03}
        signal = detect_thematic_repricing(features, "t1", "m1", None)
        assert signal is None


class TestStructuralInconsistencyDetector:
    def test_detects_mispricing(self):
        features = {"liquidity_score": 0.7}
        signal = detect_structural_inconsistency(features, "t1", "m1", outcome_prices_sum=1.07)
        assert signal is not None
        assert signal.signal_type == SignalType.STRUCTURAL_INCONSISTENCY
        assert signal.direction == Direction.NEUTRAL

    def test_ignores_normal_sum(self):
        features = {"liquidity_score": 0.7}
        signal = detect_structural_inconsistency(features, "t1", "m1", outcome_prices_sum=1.01)
        assert signal is None

    def test_ignores_no_sum(self):
        features = {"liquidity_score": 0.7}
        signal = detect_structural_inconsistency(features, "t1", "m1", outcome_prices_sum=None)
        assert signal is None


# ── Filter Chain Tests ───────────────────────────────────────────────


class TestSignalFilter:
    @pytest.fixture
    def filt(self):
        return SignalFilter()

    def _make_signal(self, **overrides):
        defaults = {
            "id": uuid4(),
            "market_id": str(uuid4()),
            "source": "polymarket",
            "signal_type": SignalType.PROBABILITY_SHOCK,
            "direction": Direction.BULLISH,
            "confidence": 0.7,
            "horizon": "1h",
            "tradability_score": 0.6,
            "model_probability": 0.60,
            "market_price": 0.50,
            "edge": 0.10,
            "feature_set_version": "v1",
            "rule_version": "v1",
        }
        defaults.update(overrides)
        from farsight.markets.schemas.signals import SignalSchema
        return SignalSchema(**defaults)

    def test_valid_signal_passes(self, filt):
        signal = self._make_signal()
        passed, reason = filt.check(signal)
        assert passed
        assert reason == "passed"

    def test_low_confidence_rejected(self, filt):
        signal = self._make_signal(confidence=0.2)
        passed, reason = filt.check(signal)
        assert not passed
        assert "confidence" in reason

    def test_high_entry_price_rejected(self, filt):
        signal = self._make_signal(market_price=0.95)
        passed, reason = filt.check(signal)
        assert not passed
        assert "market_price" in reason

    def test_low_entry_price_rejected(self, filt):
        signal = self._make_signal(market_price=0.05)
        passed, reason = filt.check(signal)
        assert not passed
        assert "market_price" in reason

    def test_low_edge_rejected(self, filt):
        signal = self._make_signal(edge=0.01, horizon="1h")  # Below 3% threshold for 1h
        passed, reason = filt.check(signal)
        assert not passed
        assert "edge" in reason

    def test_cooldown_enforced(self, filt):
        signal = self._make_signal()
        filt.record_emission(signal)

        # Second signal same market+type — different price so dedup doesn't
        # collide, but cooldown on (market, signal_type) still blocks it.
        signal2 = self._make_signal(id=uuid4(), market_id=signal.market_id, market_price=0.72)
        passed, reason = filt.check(signal2)
        assert not passed
        assert "cooldown" in reason

    def test_reset_clears_state(self, filt):
        signal = self._make_signal()
        filt.record_emission(signal)
        filt.reset()

        signal2 = self._make_signal(id=uuid4(), market_id=signal.market_id)
        passed, _ = filt.check(signal2)
        assert passed


# ── Signal Engine Tests ──────────────────────────────────────────────


class TestSignalEngine:
    @pytest.fixture
    def engine(self):
        return SignalEngine()

    def test_evaluate_detects_shock(self, engine):
        features = {
            "delta_5m": 0.10,
            "liquidity_score": 0.8,
            "trade_velocity": 3.0,
            "stale_score": 0.0,
            "manipulation_heuristic": 0.0,
            "resolution_proximity_days": 30.0,
            "last_price": 0.50,
        }
        signals = engine.evaluate(features, "t1", "m1")
        assert len(signals) >= 1
        types = [s.signal_type for s in signals]
        assert SignalType.PROBABILITY_SHOCK in types

    def test_evaluate_no_signals_on_quiet_market(self, engine):
        features = {
            "delta_5m": 0.01,
            "drift_score": 0.3,
            "reversion_score": 0.5,
            "liquidity_score": 0.5,
            "trade_velocity": 0.5,
        }
        signals = engine.evaluate(features, "t1", "m1")
        assert len(signals) == 0

    @pytest.mark.asyncio
    async def test_on_features_publishes_signals(self):
        bus = EventBus()
        engine = SignalEngine(event_bus=bus)
        engine.wire(bus)

        emitted = []

        async def capture(payload):
            emitted.append(payload)

        bus.subscribe("signal.generated", capture)

        await engine.on_features({
            "token_id": "t1",
            "market_id": "m1",
            "features": {
                "delta_5m": 0.12,
                "liquidity_score": 0.9,
                "trade_velocity": 5.0,
                "stale_score": 0.0,
                "manipulation_heuristic": 0.0,
                "resolution_proximity_days": 30.0,
                "last_price": 0.45,
            },
        })
        import asyncio
        await asyncio.sleep(0.05)

        assert len(emitted) >= 1
        assert emitted[0]["signal_type"] == "probability_shock"

    def test_health(self, engine):
        health = engine.get_health()
        assert health["signals_generated"] == 0
        assert health["signals_suppressed"] == 0

    def test_reset(self, engine):
        features = {
            "delta_5m": 0.10,
            "liquidity_score": 0.8,
            "trade_velocity": 3.0,
            "stale_score": 0.0,
            "manipulation_heuristic": 0.0,
            "resolution_proximity_days": 30.0,
            "last_price": 0.50,
        }
        engine.evaluate(features, "t1")
        engine.reset()
        assert engine.get_health()["signals_generated"] == 0
