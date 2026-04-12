"""
FeatureEngine — computes streaming features on every state update.

Subscribes to: derived.state_update
Publishes:     derived.features

Each state update triggers a full feature vector computation for that token.
Features are pure functions of MarketState — no DB or network access.

Design for testability:
  - compute_features() is a standalone function, callable without the bus
  - FeatureEngine just wires the bus subscription and adds metadata
"""

import logging
import time
from datetime import datetime
from typing import Optional

from farsight.markets.config import settings
from farsight.markets.engine.event_bus import EventBus
from farsight.markets.features import microstructure, probability, quality, technicals
from farsight.markets.services.state_engine import MarketState, StateEngine

logger = logging.getLogger(__name__)


def compute_features(state: MarketState, end_date: datetime | None = None) -> dict:
    """Compute the full feature vector for a market state.

    Pure function: MarketState in → feature dict out. No side effects.
    Returns a flat dict of feature_name → value (float or None).
    """
    features = {}
    features.update(microstructure.compute_all(state))
    features.update(probability.compute_all(state))
    features.update(quality.compute_all(state, end_date))
    features.update(technicals.compute_all(state))
    return features


class FeatureEngine:
    """Streaming feature computation wired to the event bus.

    On each derived.state_update, computes features and publishes derived.features.
    """

    def __init__(
        self,
        state_engine: StateEngine,
        event_bus: Optional[EventBus] = None,
    ):
        self._state_engine = state_engine
        self._bus = event_bus
        self._compute_count = 0
        self._last_compute_ms: float = 0

    def wire(self, bus: EventBus):
        """Subscribe to event bus. Call during startup."""
        self._bus = bus
        bus.subscribe("derived.state_update", self.on_state_update)

    async def on_state_update(self, payload: dict):
        """Handle a state update — compute features and publish."""
        token_id = payload.get("token_id", "")
        if not token_id:
            return

        state = self._state_engine.get_state(token_id)
        if state is None:
            return

        start = time.time()
        features = compute_features(state)
        elapsed_ms = (time.time() - start) * 1000

        self._compute_count += 1
        self._last_compute_ms = elapsed_ms

        if self._bus:
            await self._bus.publish("derived.features", {
                "token_id": token_id,
                "market_id": state.market_id,
                "timestamp": datetime.utcnow().isoformat(),
                "feature_set_version": settings.FEATURE_SET_VERSION,
                "features": features,
                "compute_ms": round(elapsed_ms, 2),
            })

    def compute_for_token(self, token_id: str) -> dict | None:
        """Compute features for a token synchronously (for API/testing)."""
        state = self._state_engine.get_state(token_id)
        if state is None:
            return None
        return compute_features(state)

    def get_health(self) -> dict:
        return {
            "total_computations": self._compute_count,
            "last_compute_ms": round(self._last_compute_ms, 2),
        }
