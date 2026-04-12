"""Tests for StateEngine — market state reconstruction from raw events."""

import pytest
from datetime import datetime, timedelta
from farsight.markets.engine.event_bus import EventBus
from farsight.markets.services.state_engine import StateEngine, MarketState


@pytest.fixture
def bus():
    return EventBus()


@pytest.fixture
def engine(bus):
    e = StateEngine(event_bus=bus)
    e.wire(bus)
    return e


class TestMarketState:
    def test_initial_state(self):
        state = MarketState("token_1")
        assert state.last_price == 0.0
        assert state.best_bid == 0.0
        assert state.best_ask == 1.0
        assert state.spread == 1.0
        assert state.is_stale is False  # No update yet, not stale
        assert state.seconds_since_last_update is None

    def test_update_price(self):
        state = MarketState("token_1")
        now = datetime.utcnow()
        state.update_price(now, mid=0.55, bid=0.53, ask=0.57)

        assert state.last_price == 0.55
        assert state.best_bid == 0.53
        assert state.best_ask == 0.57
        assert state.spread == pytest.approx(0.04)
        assert state.prices_5m.count() == 1
        assert state.prices_1h.count() == 1

    def test_update_trade(self):
        state = MarketState("token_1")
        now = datetime.utcnow()
        state.update_trade(now, price=0.60, size_usd=500.0)

        assert state.last_price == 0.60
        assert state.last_trade_time == now
        assert state.trades_1h.trade_count() == 1
        assert state.volume_5m.sum() == 500.0
        assert state.volume_1h.sum() == 500.0

    def test_update_book(self):
        state = MarketState("token_1")
        state.update_book(bid_depth=1000.0, ask_depth=800.0, best_bid=0.48, best_ask=0.52)

        assert state.bid_depth == 1000.0
        assert state.ask_depth == 800.0
        assert state.best_bid == 0.48
        assert state.best_ask == 0.52
        assert state.spread == pytest.approx(0.04)

    def test_rolling_deltas(self):
        state = MarketState("token_1")
        now = datetime.utcnow()

        state.update_price(now, mid=0.50)
        state.update_price(now + timedelta(seconds=1), mid=0.55)
        state.update_price(now + timedelta(seconds=2), mid=0.58)

        assert state.prices_5m.delta() == pytest.approx(0.08)
        assert state.prices_1m.delta() == pytest.approx(0.08)

    def test_snapshot_serializable(self):
        state = MarketState("token_1", market_id="mkt_abc")
        now = datetime.utcnow()
        state.update_price(now, mid=0.55, bid=0.53, ask=0.57)
        state.update_trade(now, price=0.55, size_usd=100.0)

        snap = state.to_snapshot()
        assert snap["token_id"] == "token_1"
        assert snap["market_id"] == "mkt_abc"
        assert snap["last_price"] == 0.55
        assert snap["volume_1h"] == 100.0
        assert snap["trade_count_1h"] == 1
        assert snap["price_delta_5m"] is not None


class TestStateEngine:
    @pytest.mark.asyncio
    async def test_process_price_tick(self, engine):
        await engine.on_price_tick({
            "token_id": "t1",
            "market_id": "m1",
            "timestamp": datetime.utcnow().isoformat(),
            "mid": 0.55,
            "bid": 0.53,
            "ask": 0.57,
        })

        state = engine.get_state("t1")
        assert state is not None
        assert state.last_price == 0.55
        assert state.market_id == "m1"

    @pytest.mark.asyncio
    async def test_process_trade(self, engine):
        await engine.on_trade_print({
            "token_id": "t1",
            "timestamp": datetime.utcnow().isoformat(),
            "price": 0.60,
            "size_usd": 250.0,
        })

        state = engine.get_state("t1")
        assert state is not None
        assert state.last_price == 0.60
        assert state.volume_1h.sum() == 250.0

    @pytest.mark.asyncio
    async def test_process_orderbook(self, engine):
        await engine.on_orderbook({
            "token_id": "t1",
            "timestamp": datetime.utcnow().isoformat(),
            "bids": [{"price": 0.48, "size": 500}, {"price": 0.45, "size": 300}],
            "asks": [{"price": 0.52, "size": 400}, {"price": 0.55, "size": 200}],
        })

        state = engine.get_state("t1")
        assert state is not None
        assert state.best_bid == 0.48
        assert state.best_ask == 0.52
        assert state.bid_depth > 0
        assert state.ask_depth > 0

    @pytest.mark.asyncio
    async def test_multiple_tokens_tracked_independently(self, engine):
        now = datetime.utcnow()
        await engine.on_price_tick({"token_id": "t1", "timestamp": now.isoformat(), "mid": 0.40, "bid": 0, "ask": 1})
        await engine.on_price_tick({"token_id": "t2", "timestamp": now.isoformat(), "mid": 0.70, "bid": 0, "ask": 1})

        assert engine.get_state("t1").last_price == 0.40
        assert engine.get_state("t2").last_price == 0.70
        assert len(engine.get_all_states()) == 2

    @pytest.mark.asyncio
    async def test_publishes_state_update(self, bus, engine):
        received = []

        async def capture(payload):
            received.append(payload)

        bus.subscribe("derived.state_update", capture)

        await engine.on_price_tick({
            "token_id": "t1",
            "timestamp": datetime.utcnow().isoformat(),
            "mid": 0.55,
            "bid": 0.53,
            "ask": 0.57,
        })
        import asyncio
        await asyncio.sleep(0.05)

        assert len(received) == 1
        assert received[0]["token_id"] == "t1"
        assert received[0]["last_price"] == 0.55

    @pytest.mark.asyncio
    async def test_ignore_empty_token_id(self, engine):
        await engine.on_price_tick({"token_id": "", "mid": 0.5, "bid": 0, "ask": 1})
        assert len(engine.get_all_states()) == 0

    @pytest.mark.asyncio
    async def test_health(self, engine):
        await engine.on_price_tick({
            "token_id": "t1",
            "timestamp": datetime.utcnow().isoformat(),
            "mid": 0.5, "bid": 0, "ask": 1,
        })

        health = engine.get_health()
        assert health["tracked_tokens"] == 1
        assert health["total_updates"] == 1
        assert health["stale_count"] == 0

    @pytest.mark.asyncio
    async def test_clear_resets_state(self, engine):
        await engine.on_price_tick({"token_id": "t1", "timestamp": datetime.utcnow().isoformat(), "mid": 0.5, "bid": 0, "ask": 1})
        assert len(engine.get_all_states()) == 1

        engine.clear()
        assert len(engine.get_all_states()) == 0
        assert engine.get_health()["total_updates"] == 0
