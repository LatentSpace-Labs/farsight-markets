"""Tests for Polymarket WebSocket client with a fake WebSocket."""

import asyncio
import json
import pytest
from datetime import datetime

from farsight.markets.clients.polymarket.ws_client import PolymarketWsClient
from farsight.markets.engine.checkpoint import MemoryCheckpointStore
from farsight.markets.engine.event_bus import EventBus


class FakeWebSocket:
    """Fake WebSocket that feeds pre-recorded messages then closes."""

    def __init__(self, messages: list[str]):
        self._messages = list(messages)
        self._sent: list[str] = []
        self._closed = False

    async def send(self, message: str):
        self._sent.append(message)

    async def recv(self) -> str:
        if not self._messages:
            # Simulate disconnect after all messages consumed
            raise ConnectionError("no more messages")
        return self._messages.pop(0)

    async def close(self):
        self._closed = True


@pytest.fixture
def bus():
    return EventBus()


@pytest.fixture
def checkpoint():
    return MemoryCheckpointStore()


@pytest.fixture
def make_client(bus, checkpoint):
    """Factory that creates a WsClient wired to a FakeWebSocket."""
    def _make(messages: list[dict]) -> tuple[PolymarketWsClient, FakeWebSocket]:
        raw = [json.dumps(m) for m in messages]
        fake_ws = FakeWebSocket(raw)
        client = PolymarketWsClient(bus, checkpoint)
        # Inject the fake WebSocket connection
        client._connect_func = asyncio.coroutine(lambda: fake_ws)
        return client, fake_ws
    return _make


class TestWsClientMessageHandling:
    """Test _handle_message directly — no actual WebSocket connection needed."""

    @pytest.mark.asyncio
    async def test_price_change_publishes_to_bus(self, bus, checkpoint):
        """price_change has nested price_changes[] array with per-token bid/ask."""
        received = []

        async def capture(payload):
            received.append(payload)

        bus.subscribe("raw.price_tick", capture)

        client = PolymarketWsClient(bus, checkpoint)
        msg = json.dumps({
            "event_type": "price_change",
            "market": "0xabc",
            "timestamp": "1757908892351",
            "price_changes": [
                {
                    "asset_id": "token_yes",
                    "price": "0.5",
                    "size": "200",
                    "side": "BUY",
                    "best_bid": "0.48",
                    "best_ask": "0.52",
                },
                {
                    "asset_id": "token_no",
                    "price": "0.5",
                    "size": "200",
                    "side": "SELL",
                    "best_bid": "0.48",
                    "best_ask": "0.52",
                },
            ],
        })
        await client._handle_message(msg)
        await asyncio.sleep(0.05)

        assert len(received) == 2  # One tick per price_change entry
        assert received[0]["source"] == "polymarket"
        assert received[0]["token_id"] == "token_yes"
        assert received[0]["bid"] == 0.48
        assert received[0]["ask"] == 0.52
        assert received[0]["mid"] == 0.50

    @pytest.mark.asyncio
    async def test_best_bid_ask_event(self, bus, checkpoint):
        """best_bid_ask has top-level fields (requires custom_feature_enabled)."""
        received = []

        async def capture(payload):
            received.append(payload)

        bus.subscribe("raw.price_tick", capture)

        client = PolymarketWsClient(bus, checkpoint)
        msg = json.dumps({
            "event_type": "best_bid_ask",
            "market": "0xabc",
            "asset_id": "token_1",
            "best_bid": "0.73",
            "best_ask": "0.77",
            "spread": "0.04",
            "timestamp": "1766789469958",
        })
        await client._handle_message(msg)
        await asyncio.sleep(0.05)

        assert len(received) == 1
        assert received[0]["bid"] == 0.73
        assert received[0]["ask"] == 0.77
        assert received[0]["mid"] == 0.75

    @pytest.mark.asyncio
    async def test_last_trade_publishes_to_bus(self, bus, checkpoint):
        received = []

        async def capture(payload):
            received.append(payload)

        bus.subscribe("raw.trade_print", capture)

        client = PolymarketWsClient(bus, checkpoint)
        msg = json.dumps({
            "event_type": "last_trade_price",
            "asset_id": "token_xyz",
            "price": "0.456",
            "side": "BUY",
            "size": "219.217767",
            "timestamp": "1750428146322",
        })
        await client._handle_message(msg)
        await asyncio.sleep(0.05)

        assert len(received) == 1
        assert received[0]["price"] == 0.456
        assert received[0]["size_usd"] == pytest.approx(219.217767)
        assert received[0]["side"] == "buy"
        assert received[0]["token_id"] == "token_xyz"

    @pytest.mark.asyncio
    async def test_book_event_publishes_to_bus(self, bus, checkpoint):
        received = []

        async def capture(payload):
            received.append(payload)

        bus.subscribe("raw.orderbook", capture)

        client = PolymarketWsClient(bus, checkpoint)
        msg = json.dumps({
            "event_type": "book",
            "asset_id": "token_book",
            "market": "0xabc",
            "bids": [{"price": "0.48", "size": "500"}],
            "asks": [{"price": "0.52", "size": "400"}],
            "timestamp": "1757908892351",
        })
        await client._handle_message(msg)
        await asyncio.sleep(0.05)

        assert len(received) == 1
        assert len(received[0]["bids"]) == 1
        assert len(received[0]["asks"]) == 1
        assert received[0]["token_id"] == "token_book"

    @pytest.mark.asyncio
    async def test_malformed_json_increments_error(self, bus, checkpoint):
        client = PolymarketWsClient(bus, checkpoint)
        await client._handle_message("not valid json {{{")
        assert client.messages_errors == 1
        assert client.messages_received == 1

    @pytest.mark.asyncio
    async def test_unknown_event_type_ignored(self, bus, checkpoint):
        received_ticks = []
        received_trades = []

        async def cap_tick(p):
            received_ticks.append(p)

        async def cap_trade(p):
            received_trades.append(p)

        bus.subscribe("raw.price_tick", cap_tick)
        bus.subscribe("raw.trade_print", cap_trade)

        client = PolymarketWsClient(bus, checkpoint)
        msg = json.dumps({"event_type": "tick_size_change", "asset_id": "x"})
        await client._handle_message(msg)
        await asyncio.sleep(0.05)

        assert len(received_ticks) == 0
        assert len(received_trades) == 0
        assert client.messages_errors == 0

    @pytest.mark.asyncio
    async def test_checkpoint_updated_on_price(self, bus, checkpoint):
        client = PolymarketWsClient(bus, checkpoint)
        msg = json.dumps({
            "event_type": "price_change",
            "market": "0x1",
            "timestamp": "1757908892351",
            "price_changes": [
                {"asset_id": "t1", "best_bid": "0.48", "best_ask": "0.52"},
            ],
        })
        await client._handle_message(msg)

        last = await checkpoint.get_last("polymarket_ws")
        assert last is not None

    @pytest.mark.asyncio
    async def test_metrics_tracking(self, bus, checkpoint):
        client = PolymarketWsClient(bus, checkpoint)

        for i in range(3):
            msg = json.dumps({
                "event_type": "price_change",
                "market": "0x1",
                "timestamp": "1757908892351",
                "price_changes": [
                    {"asset_id": f"t{i}", "best_bid": "0.48", "best_ask": "0.52"},
                ],
            })
            await client._handle_message(msg)

        assert client.messages_received == 3
        assert client.last_message_time is not None

        health = client.get_health()
        assert health["messages_received"] == 3
        assert health["seconds_since_last_message"] is not None
        assert health["seconds_since_last_message"] < 1.0

    @pytest.mark.asyncio
    async def test_raw_samples_captured(self, bus, checkpoint):
        client = PolymarketWsClient(bus, checkpoint)
        msg = json.dumps({
            "event_type": "last_trade_price",
            "asset_id": "t1",
            "price": "0.5",
            "size": "100",
            "timestamp": "1757908892351",
        })
        await client._handle_message(msg)

        assert len(client.raw_samples) == 1
        assert client.raw_samples[0]["event_type"] == "last_trade_price"


class TestWsClientSubscription:
    @pytest.mark.asyncio
    async def test_pending_subscriptions_when_not_connected(self, bus, checkpoint):
        client = PolymarketWsClient(bus, checkpoint)
        await client.update_subscriptions({"t1", "t2", "t3"})

        assert client._pending_tokens == {"t1", "t2", "t3"}
        assert len(client._subscribed_tokens) == 0
