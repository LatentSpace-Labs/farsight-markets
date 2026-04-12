"""Tests for the async event bus."""

import asyncio
import pytest
from farsight.markets.engine.event_bus import EventBus


@pytest.fixture
def bus():
    return EventBus()


class TestEventBus:
    @pytest.mark.asyncio
    async def test_publish_to_subscriber(self, bus):
        received = []

        async def handler(payload):
            received.append(payload)

        bus.subscribe("test.event", handler)
        await bus.publish("test.event", {"key": "value"})
        await asyncio.sleep(0.05)

        assert len(received) == 1
        assert received[0]["key"] == "value"

    @pytest.mark.asyncio
    async def test_multiple_subscribers(self, bus):
        results_a = []
        results_b = []

        async def handler_a(payload):
            results_a.append(payload)

        async def handler_b(payload):
            results_b.append(payload)

        bus.subscribe("test.event", handler_a)
        bus.subscribe("test.event", handler_b)
        await bus.publish("test.event", {"x": 1})
        await asyncio.sleep(0.05)

        assert len(results_a) == 1
        assert len(results_b) == 1

    @pytest.mark.asyncio
    async def test_no_subscribers_does_not_error(self, bus):
        await bus.publish("unsubscribed.topic", {"data": True})

    @pytest.mark.asyncio
    async def test_error_isolation(self, bus):
        """A failing handler should not prevent other handlers from running."""
        good_results = []

        async def bad_handler(payload):
            raise ValueError("boom")

        async def good_handler(payload):
            good_results.append(payload)

        bus.subscribe("test.event", bad_handler)
        bus.subscribe("test.event", good_handler)
        await bus.publish("test.event", {"data": 1})
        await asyncio.sleep(0.1)

        assert len(good_results) == 1
        assert bus.error_counts.get("test.event", 0) >= 1

    @pytest.mark.asyncio
    async def test_event_counts(self, bus):
        async def noop(payload):
            pass

        bus.subscribe("a", noop)
        await bus.publish("a", {})
        await bus.publish("a", {})
        await bus.publish("b", {})

        assert bus.event_counts["a"] == 2
        assert bus.event_counts["b"] == 1

    @pytest.mark.asyncio
    async def test_seconds_since_last_event(self, bus):
        assert bus.seconds_since_last_event("test") is None

        async def noop(payload):
            pass

        bus.subscribe("test", noop)
        await bus.publish("test", {})

        lag = bus.seconds_since_last_event("test")
        assert lag is not None
        assert lag < 1.0

    @pytest.mark.asyncio
    async def test_health(self, bus):
        async def noop(payload):
            pass

        bus.subscribe("topic_a", noop)
        bus.subscribe("topic_a", noop)
        await bus.publish("topic_a", {})

        health = bus.get_health()
        assert health["subscriber_counts"]["topic_a"] == 2
        assert health["event_counts"]["topic_a"] == 1
        assert "topic_a" in health["seconds_since_last"]
