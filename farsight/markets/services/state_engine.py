"""
StateEngine — Maintains live market state from raw event stream.

Subscribes to: raw.price_tick, raw.trade_print, raw.orderbook
Publishes:     derived.state_update

For each tracked market/outcome, maintains:
  - Last price, bid, ask, spread
  - Rolling windows (1m, 5m, 15m, 1h, 4h) for prices and volume
  - Order book depth and imbalance
  - Staleness tracking

Design for testability:
  - Pure computation — no DB or network access
  - All state accessible via get_state() for assertions
  - EventBus is injected (can be a test spy)
  - process_*() methods can be called directly without EventBus
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

from farsight.markets.config import settings
from farsight.markets.engine.event_bus import EventBus
from farsight.markets.engine.window import RollingWindow, VolumeWeightedWindow

logger = logging.getLogger(__name__)


class MarketState:
    """Live state for a single outcome token, maintained in memory."""

    def __init__(self, token_id: str, market_id: Optional[str] = None):
        self.token_id = token_id
        self.market_id = market_id

        # Latest values
        self.last_price: float = 0.0
        self.last_trade_time: Optional[datetime] = None
        self.last_update_time: Optional[datetime] = None
        self.best_bid: float = 0.0
        self.best_ask: float = 1.0
        self.spread: float = 1.0
        self.bid_depth: float = 0.0
        self.ask_depth: float = 0.0

        # Rolling windows
        maxlen = settings.ROLLING_WINDOW_MAX_ENTRIES
        self.prices_1m = RollingWindow(timedelta(minutes=1), maxlen=maxlen)
        self.prices_5m = RollingWindow(timedelta(minutes=5), maxlen=maxlen)
        self.prices_15m = RollingWindow(timedelta(minutes=15), maxlen=maxlen)
        self.prices_1h = RollingWindow(timedelta(hours=1), maxlen=maxlen)
        self.prices_4h = RollingWindow(timedelta(hours=4), maxlen=maxlen)

        self.trades_1h = VolumeWeightedWindow(timedelta(hours=1), maxlen=maxlen)
        self.volume_5m = RollingWindow(timedelta(minutes=5), maxlen=maxlen)
        self.volume_1h = RollingWindow(timedelta(hours=1), maxlen=maxlen)

    def update_price(self, timestamp: datetime, mid: float, bid: float = 0.0, ask: float = 1.0):
        """Update price state and all rolling windows."""
        self.last_price = mid
        self.last_update_time = timestamp
        self.best_bid = bid
        self.best_ask = ask
        self.spread = ask - bid

        self.prices_1m.add(timestamp, mid)
        self.prices_5m.add(timestamp, mid)
        self.prices_15m.add(timestamp, mid)
        self.prices_1h.add(timestamp, mid)
        self.prices_4h.add(timestamp, mid)

    def update_trade(self, timestamp: datetime, price: float, size_usd: float):
        """Record a trade in rolling windows."""
        self.last_price = price
        self.last_trade_time = timestamp
        self.last_update_time = timestamp

        self.trades_1h.add(timestamp, price, size_usd)
        self.volume_5m.add(timestamp, size_usd)
        self.volume_1h.add(timestamp, size_usd)

        # Also update price windows on trade
        self.prices_1m.add(timestamp, price)
        self.prices_5m.add(timestamp, price)
        self.prices_15m.add(timestamp, price)
        self.prices_1h.add(timestamp, price)
        self.prices_4h.add(timestamp, price)

    def update_book(self, bid_depth: float, ask_depth: float, best_bid: float, best_ask: float):
        """Update orderbook state."""
        self.bid_depth = bid_depth
        self.ask_depth = ask_depth
        self.best_bid = best_bid
        self.best_ask = best_ask
        self.spread = best_ask - best_bid

    def to_snapshot(self) -> dict:
        """Export current state as a serializable dict."""
        return {
            "token_id": self.token_id,
            "market_id": self.market_id,
            "last_price": self.last_price,
            "last_trade_time": self.last_trade_time.isoformat() if self.last_trade_time else None,
            "last_update_time": self.last_update_time.isoformat() if self.last_update_time else None,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread": self.spread,
            "bid_depth": self.bid_depth,
            "ask_depth": self.ask_depth,
            "price_delta_1m": self.prices_1m.delta(),
            "price_delta_5m": self.prices_5m.delta(),
            "price_delta_15m": self.prices_15m.delta(),
            "price_delta_1h": self.prices_1h.delta(),
            "price_delta_4h": self.prices_4h.delta(),
            "vwap_1h": self.trades_1h.vwap(),
            "volume_5m": self.volume_5m.sum(),
            "volume_1h": self.volume_1h.sum(),
            "trade_count_1h": self.trades_1h.trade_count(),
        }

    @property
    def seconds_since_last_update(self) -> Optional[float]:
        if self.last_update_time is None:
            return None
        return (datetime.utcnow() - self.last_update_time).total_seconds()

    @property
    def is_stale(self) -> bool:
        """Market is stale if no update in 10 minutes."""
        secs = self.seconds_since_last_update
        return secs is not None and secs > 600


class StateEngine:
    """Maintains in-memory state for all tracked markets.

    Subscribes to raw events from the event bus, updates per-token state,
    and publishes derived.state_update when state changes.
    """

    def __init__(self, event_bus: Optional[EventBus] = None):
        self._bus = event_bus
        self._states: dict[str, MarketState] = {}  # keyed by token_id
        self._update_count = 0

    def wire(self, bus: EventBus):
        """Subscribe to event bus topics. Call during startup."""
        self._bus = bus
        bus.subscribe("raw.price_tick", self.on_price_tick)
        bus.subscribe("raw.trade_print", self.on_trade_print)
        bus.subscribe("raw.orderbook", self.on_orderbook)

    def _get_or_create(self, token_id: str, market_id: Optional[str] = None) -> MarketState:
        if token_id not in self._states:
            self._states[token_id] = MarketState(token_id, market_id)
        return self._states[token_id]

    # ── Event handlers (called by EventBus or directly in tests) ─────

    async def on_price_tick(self, payload: dict):
        """Handle a normalized price tick."""
        token_id = payload.get("token_id", "")
        if not token_id:
            return

        state = self._get_or_create(token_id, payload.get("market_id"))
        timestamp = _parse_timestamp(payload.get("timestamp"))

        state.update_price(
            timestamp=timestamp,
            mid=payload.get("mid", 0.0),
            bid=payload.get("bid", 0.0),
            ask=payload.get("ask", 1.0),
        )
        self._update_count += 1
        await self._publish_update(token_id, state)

    async def on_trade_print(self, payload: dict):
        """Handle a normalized trade."""
        token_id = payload.get("token_id", "")
        if not token_id:
            return

        state = self._get_or_create(token_id, payload.get("market_id"))
        timestamp = _parse_timestamp(payload.get("timestamp"))

        state.update_trade(
            timestamp=timestamp,
            price=payload.get("price", 0.0),
            size_usd=payload.get("size_usd", 0.0),
        )
        self._update_count += 1
        await self._publish_update(token_id, state)

    async def on_orderbook(self, payload: dict):
        """Handle an orderbook snapshot."""
        token_id = payload.get("token_id", "")
        if not token_id:
            return

        state = self._get_or_create(token_id, payload.get("market_id"))

        # Compute depths from level arrays
        bids = payload.get("bids", [])
        asks = payload.get("asks", [])
        bid_depth = sum(b.get("size", 0) * b.get("price", 0) for b in bids)
        ask_depth = sum(a.get("size", 0) * a.get("price", 0) for a in asks)
        best_bid = bids[0]["price"] if bids else 0.0
        best_ask = asks[0]["price"] if asks else 1.0

        state.update_book(bid_depth, ask_depth, best_bid, best_ask)

        # Also update price from book mid
        mid = (best_bid + best_ask) / 2
        timestamp = _parse_timestamp(payload.get("timestamp"))
        state.update_price(timestamp, mid, best_bid, best_ask)

        self._update_count += 1
        await self._publish_update(token_id, state)

    async def _publish_update(self, token_id: str, state: MarketState):
        """Publish a state update to the event bus."""
        if self._bus:
            await self._bus.publish("derived.state_update", {
                "token_id": token_id,
                "market_id": state.market_id,
                **state.to_snapshot(),
            })

    # ── Queries ──────────────────────────────────────────────────────

    def get_state(self, token_id: str) -> Optional[MarketState]:
        """Get the current state for a token. Returns None if not tracked."""
        return self._states.get(token_id)

    def get_all_states(self) -> dict[str, MarketState]:
        """Get all tracked states."""
        return dict(self._states)

    def get_stale_tokens(self) -> list[str]:
        """Get token IDs with stale data (no update in 10+ minutes)."""
        return [tid for tid, s in self._states.items() if s.is_stale]

    def get_health(self) -> dict:
        """Return health summary."""
        return {
            "tracked_tokens": len(self._states),
            "total_updates": self._update_count,
            "stale_count": len(self.get_stale_tokens()),
        }

    def clear(self):
        """Reset all state. Used in testing and replay."""
        self._states.clear()
        self._update_count = 0


def _parse_timestamp(val) -> datetime:
    """Parse a timestamp from various formats."""
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.utcnow()
