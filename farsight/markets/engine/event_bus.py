"""
In-process async event bus.

Central nervous system of the pipeline. All components communicate through typed events.
Simple pub/sub — no external dependencies. Proportional to Polymarket's ~76 MB/day volume.

Topics:
    raw.price_tick       — Normalized price update from any source
    raw.trade_print      — Normalized trade from any source
    raw.orderbook        — L2 orderbook snapshot/delta
    derived.state_update — Market state changed
    derived.features     — New feature vector computed
    signal.generated     — New signal created
    alert.triggered      — User alert rule matched
    trade.executed       — Paper or live trade executed
"""

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Callable, Coroutine

logger = logging.getLogger(__name__)

Handler = Callable[[dict], Coroutine[Any, Any, None]]


class EventBus:
    """In-process async pub/sub with error isolation and metrics."""

    def __init__(self):
        self._subscribers: dict[str, list[Handler]] = defaultdict(list)
        self._event_counts: dict[str, int] = defaultdict(int)
        self._error_counts: dict[str, int] = defaultdict(int)
        self._last_event_time: dict[str, float] = {}

    def subscribe(self, event_type: str, handler: Handler):
        """Register a handler for an event type. Multiple handlers per type allowed."""
        self._subscribers[event_type].append(handler)
        logger.debug(f"Subscribed {handler.__qualname__} to {event_type}")

    async def publish(self, event_type: str, payload: dict):
        """Publish an event to all subscribers. Errors in one handler don't affect others."""
        self._event_counts[event_type] += 1
        self._last_event_time[event_type] = time.time()

        handlers = self._subscribers.get(event_type, [])
        for handler in handlers:
            try:
                asyncio.create_task(self._safe_call(event_type, handler, payload))
            except Exception as e:
                self._error_counts[event_type] += 1
                logger.error(f"Failed to create task for {handler.__qualname__} on {event_type}: {e}")

    async def _safe_call(self, event_type: str, handler: Handler, payload: dict):
        """Execute handler with error isolation."""
        try:
            await handler(payload)
        except Exception as e:
            self._error_counts[event_type] += 1
            logger.error(
                f"Handler {handler.__qualname__} failed on {event_type}: {e}",
                exc_info=True,
            )

    # ── Metrics ──────────────────────────────────────────────────────

    @property
    def event_counts(self) -> dict[str, int]:
        return dict(self._event_counts)

    @property
    def error_counts(self) -> dict[str, int]:
        return dict(self._error_counts)

    def seconds_since_last_event(self, event_type: str) -> float | None:
        """Seconds since last event of this type. None if never received."""
        last = self._last_event_time.get(event_type)
        if last is None:
            return None
        return time.time() - last

    def get_health(self) -> dict:
        """Return health summary for observability."""
        return {
            "event_counts": self.event_counts,
            "error_counts": self.error_counts,
            "subscriber_counts": {k: len(v) for k, v in self._subscribers.items()},
            "seconds_since_last": {
                k: round(time.time() - v, 1)
                for k, v in self._last_event_time.items()
            },
        }
