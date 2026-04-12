"""
ReplayService — replays historical events through the live pipeline for backtesting.

The key insight: replay uses the SAME StateEngine → FeatureEngine → SignalEngine
code path as live processing. The only difference is the event source:
  - Live: WebSocket → EventBus
  - Replay: DB/file → EventBus

This ensures backtest results match live behavior (no train/serve skew).

Design for testability:
  - replay() accepts any iterable of events (can be in-memory list)
  - Returns collected signals for comparison against actual outcomes
  - No DB dependency in core replay logic
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import AsyncIterator, Optional

from farsight.markets.config import settings
from farsight.markets.engine.event_bus import EventBus
from farsight.markets.services.feature_engine import FeatureEngine
from farsight.markets.services.signal_engine import SignalEngine
from farsight.markets.services.state_engine import StateEngine

logger = logging.getLogger(__name__)


@dataclass
class ReplayEvent:
    """A single event to replay (price tick or trade)."""
    event_type: str       # "raw.price_tick" | "raw.trade_print" | "raw.orderbook"
    timestamp: datetime
    payload: dict


@dataclass
class ReplayResult:
    """Result of a replay run."""
    events_replayed: int = 0
    signals_generated: list = field(default_factory=list)
    duration_seconds: float = 0.0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    feature_computations: int = 0
    state_updates: int = 0


class ReplayService:
    """Replays historical events through the full signal pipeline."""

    async def replay(
        self,
        events: list[ReplayEvent] | AsyncIterator,
        market_ids: list[str] | None = None,
    ) -> ReplayResult:
        """Replay a sequence of events and collect generated signals.

        Creates isolated instances of StateEngine, FeatureEngine, and SignalEngine
        (NOT the live ones) so replay doesn't affect production state.

        Args:
            events: List of ReplayEvent objects, sorted by timestamp
            market_ids: Optional filter — only process these markets

        Returns:
            ReplayResult with all signals generated during replay
        """
        start = time.time()
        result = ReplayResult()

        # Create isolated pipeline instances
        bus = EventBus()
        state_engine = StateEngine(event_bus=bus)
        feature_engine = FeatureEngine(state_engine, event_bus=bus)
        signal_engine = SignalEngine(event_bus=bus)

        # Wire the pipeline
        state_engine.wire(bus)
        feature_engine.wire(bus)
        signal_engine.wire(bus)

        # Capture signals
        async def collect_signal(payload):
            result.signals_generated.append(payload)

        bus.subscribe("signal.generated", collect_signal)

        # Replay events in order
        if isinstance(events, list):
            for event in events:
                if market_ids:
                    token_id = event.payload.get("token_id", "")
                    if token_id not in market_ids:
                        continue

                await bus.publish(event.event_type, event.payload)
                result.events_replayed += 1

                if result.start_time is None:
                    result.start_time = event.timestamp
                result.end_time = event.timestamp
        else:
            # AsyncIterator (e.g., streaming from DB)
            async for event in events:
                if market_ids:
                    token_id = event.payload.get("token_id", "")
                    if token_id not in market_ids:
                        continue

                await bus.publish(event.event_type, event.payload)
                result.events_replayed += 1

                if result.start_time is None:
                    result.start_time = event.timestamp
                result.end_time = event.timestamp

        # Allow async tasks to complete
        import asyncio
        await asyncio.sleep(0.1)

        result.duration_seconds = round(time.time() - start, 3)
        result.state_updates = state_engine.get_health()["total_updates"]
        result.feature_computations = feature_engine.get_health()["total_computations"]

        logger.info(
            f"Replay complete: {result.events_replayed} events, "
            f"{len(result.signals_generated)} signals, "
            f"{result.duration_seconds}s"
        )

        return result

    @staticmethod
    def generate_synthetic_events(
        token_id: str,
        start_price: float,
        end_price: float,
        duration_minutes: int = 60,
        events_per_minute: int = 10,
    ) -> list[ReplayEvent]:
        """Generate synthetic price events for testing signal detection.

        Creates a linear price path from start_price to end_price.
        Useful for testing: "does a 10% move over 5 minutes trigger a shock signal?"
        """
        from datetime import timedelta

        events = []
        total_events = duration_minutes * events_per_minute
        price_step = (end_price - start_price) / total_events
        time_step = timedelta(minutes=duration_minutes) / total_events
        base_time = datetime.utcnow() - timedelta(minutes=duration_minutes)

        for i in range(total_events):
            t = base_time + time_step * i
            price = start_price + price_step * i

            events.append(ReplayEvent(
                event_type="raw.price_tick",
                timestamp=t,
                payload={
                    "token_id": token_id,
                    "source": "replay",
                    "timestamp": t.isoformat(),
                    "bid": price - 0.005,
                    "ask": price + 0.005,
                    "mid": price,
                    "spread": 0.01,
                },
            ))

        return events

    @staticmethod
    def generate_shock_events(
        token_id: str,
        base_price: float,
        shock_magnitude: float,
        calm_minutes: int = 30,
        shock_minutes: int = 5,
        events_per_minute: int = 10,
    ) -> list[ReplayEvent]:
        """Generate events with a calm period followed by a sudden price shock.

        Useful for testing probability_shock detection:
        - 30 min at base_price (establish baseline)
        - 5 min shock from base_price to base_price + shock_magnitude
        """
        from datetime import timedelta

        events = []
        base_time = datetime.utcnow() - timedelta(minutes=calm_minutes + shock_minutes)

        # Calm period
        for i in range(calm_minutes * events_per_minute):
            t = base_time + timedelta(seconds=i * 60 / events_per_minute)
            # Small noise around base price
            import random
            noise = random.uniform(-0.002, 0.002)
            price = base_price + noise

            events.append(ReplayEvent(
                event_type="raw.price_tick",
                timestamp=t,
                payload={
                    "token_id": token_id,
                    "source": "replay",
                    "timestamp": t.isoformat(),
                    "bid": price - 0.005,
                    "ask": price + 0.005,
                    "mid": price,
                    "spread": 0.01,
                },
            ))

        # Shock period
        shock_start = base_time + timedelta(minutes=calm_minutes)
        total_shock_events = shock_minutes * events_per_minute
        for i in range(total_shock_events):
            t = shock_start + timedelta(seconds=i * 60 / events_per_minute)
            price = base_price + shock_magnitude * (i / total_shock_events)

            events.append(ReplayEvent(
                event_type="raw.price_tick",
                timestamp=t,
                payload={
                    "token_id": token_id,
                    "source": "replay",
                    "timestamp": t.isoformat(),
                    "bid": price - 0.005,
                    "ask": price + 0.005,
                    "mid": price,
                    "spread": 0.01,
                },
            ))

        return events
