"""Tests for ReplayService — backtesting through the live pipeline."""

import pytest
from farsight.markets.services.replay_service import ReplayService, ReplayEvent


class TestReplayService:
    @pytest.fixture
    def svc(self):
        return ReplayService()

    @pytest.mark.asyncio
    async def test_replay_empty_events(self, svc):
        result = await svc.replay([])
        assert result.events_replayed == 0
        assert result.signals_generated == []

    @pytest.mark.asyncio
    async def test_replay_synthetic_flat_price(self, svc):
        """Flat price should generate no signals."""
        events = ReplayService.generate_synthetic_events(
            token_id="t1",
            start_price=0.50,
            end_price=0.50,  # No movement
            duration_minutes=10,
            events_per_minute=5,
        )
        result = await svc.replay(events)

        assert result.events_replayed == 50  # 10 min * 5/min
        assert result.state_updates > 0
        assert len(result.signals_generated) == 0  # No movement = no signals

    @pytest.mark.asyncio
    async def test_replay_shock_generates_signal(self, svc):
        """A price shock should trigger a probability_shock signal."""
        events = ReplayService.generate_shock_events(
            token_id="t1",
            base_price=0.50,
            shock_magnitude=0.10,  # +10% shock (well above 5% threshold)
            calm_minutes=10,
            shock_minutes=5,
            events_per_minute=10,
        )
        result = await svc.replay(events)

        assert result.events_replayed > 100
        assert result.state_updates > 0
        assert result.feature_computations > 0
        # Should have at least one probability_shock signal
        shock_signals = [
            s for s in result.signals_generated
            if s.get("signal_type") == "probability_shock"
        ]
        assert len(shock_signals) >= 1

    @pytest.mark.asyncio
    async def test_replay_respects_market_filter(self, svc):
        """When market_ids filter is set, only those tokens are processed."""
        events = ReplayService.generate_synthetic_events("t1", 0.50, 0.60, 5, 5)
        events += ReplayService.generate_synthetic_events("t2", 0.50, 0.60, 5, 5)

        result = await svc.replay(events, market_ids=["t1"])

        # Only t1 events should be replayed
        assert result.events_replayed == 25  # 5 min * 5/min for t1 only

    @pytest.mark.asyncio
    async def test_replay_result_has_timing(self, svc):
        events = ReplayService.generate_synthetic_events("t1", 0.50, 0.55, 5, 5)
        result = await svc.replay(events)

        assert result.duration_seconds > 0
        assert result.start_time is not None
        assert result.end_time is not None


class TestSyntheticEventGeneration:
    def test_generate_synthetic_events_count(self):
        events = ReplayService.generate_synthetic_events("t1", 0.50, 0.60, 10, 5)
        assert len(events) == 50  # 10 min * 5/min

    def test_generate_synthetic_events_price_path(self):
        events = ReplayService.generate_synthetic_events("t1", 0.30, 0.70, 10, 10)
        prices = [e.payload["mid"] for e in events]
        assert prices[0] == pytest.approx(0.30, abs=0.01)
        assert prices[-1] == pytest.approx(0.70, abs=0.01)
        # Should be monotonically increasing
        for i in range(1, len(prices)):
            assert prices[i] >= prices[i - 1]

    def test_generate_shock_events_structure(self):
        events = ReplayService.generate_shock_events(
            "t1", base_price=0.50, shock_magnitude=0.10,
            calm_minutes=5, shock_minutes=3, events_per_minute=5,
        )
        assert len(events) == (5 + 3) * 5  # 40 events

        # First events should be around base price
        assert events[0].payload["mid"] == pytest.approx(0.50, abs=0.01)
        # Last events should be around base + shock
        assert events[-1].payload["mid"] == pytest.approx(0.60, abs=0.02)

    def test_all_events_have_required_fields(self):
        events = ReplayService.generate_synthetic_events("t1", 0.50, 0.60, 5, 5)
        for e in events:
            assert e.event_type == "raw.price_tick"
            assert "token_id" in e.payload
            assert "mid" in e.payload
            assert "bid" in e.payload
            assert "ask" in e.payload
            assert "timestamp" in e.payload
