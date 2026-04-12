"""
Polymarket CLOB WebSocket client.

Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market
and streams real-time events:
  - book: L2 orderbook updates
  - price_change: mid-price movements
  - last_trade_price: trade executions

Design for testability:
  - WebSocket connection is injected (can be mocked)
  - All messages go through _handle_message() which publishes to EventBus
  - Reconnect logic is isolated in connect() loop
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Optional, Protocol

from farsight.markets.config import settings
from farsight.markets.engine.checkpoint import CheckpointStore
from farsight.markets.engine.event_bus import EventBus
from farsight.markets.schemas.ticks import (
    OrderbookSnapshot,
    PriceTick,
    TradePrint,
)

logger = logging.getLogger(__name__)


class WebSocketLike(Protocol):
    """Protocol for WebSocket connections — enables testing with fakes."""

    async def send(self, message: str) -> None: ...
    async def recv(self) -> str: ...
    async def close(self) -> None: ...


class PolymarketWsClient:
    """Persistent WebSocket connection to Polymarket CLOB market channel."""

    def __init__(
        self,
        event_bus: EventBus,
        checkpoint: CheckpointStore,
        ws_url: Optional[str] = None,
    ):
        self._url = ws_url or settings.POLYMARKET_WS_URL
        self._bus = event_bus
        self._checkpoint = checkpoint
        self._subscribed_tokens: set[str] = set()
        self._pending_tokens: set[str] = set()  # Tokens to subscribe on next reconnect
        self._reconnect_delay = settings.WS_RECONNECT_DELAY_INITIAL
        self._running = False
        self._ws: Optional[WebSocketLike] = None
        self._connect_func = None  # Injectable for testing

        # Metrics
        self.messages_received = 0
        self.messages_errors = 0
        self.reconnect_count = 0
        self.last_message_time: Optional[float] = None
        self.raw_samples: list[dict] = []  # Capture first N raw messages for debugging
        self._max_raw_samples = 10

    async def connect(self):
        """Main loop: connect -> subscribe -> consume -> reconnect on failure."""
        self._running = True
        while self._running:
            try:
                ws = await self._open_connection()
                self._ws = ws
                self._reconnect_delay = settings.WS_RECONNECT_DELAY_INITIAL

                # Subscribe to all tracked tokens
                if self._subscribed_tokens or self._pending_tokens:
                    tokens = self._subscribed_tokens | self._pending_tokens
                    await self._send_subscribe(ws, tokens)
                    self._subscribed_tokens = tokens
                    self._pending_tokens.clear()

                logger.info(f"WebSocket connected, tracking {len(self._subscribed_tokens)} tokens")

                # Start heartbeat task (PING every 10s, required by Polymarket)
                heartbeat = asyncio.create_task(self._heartbeat(ws))

                # Consume messages until disconnection
                try:
                    while self._running:
                        raw = await ws.recv()
                        if raw == "PONG" or raw == "pong":
                            continue
                        if raw == "INVALID OPERATION":
                            logger.debug("Server rejected a message (INVALID OPERATION)")
                            continue
                        await self._handle_message(raw)
                finally:
                    heartbeat.cancel()

            except Exception as e:
                if not self._running:
                    break
                self.reconnect_count += 1
                logger.warning(
                    f"WebSocket disconnected: {e}, "
                    f"reconnecting in {self._reconnect_delay:.1f}s "
                    f"(reconnect #{self.reconnect_count})"
                )
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    settings.WS_RECONNECT_DELAY_MAX,
                )

    async def _heartbeat(self, ws: WebSocketLike):
        """Send PING every 10 seconds to keep the connection alive."""
        try:
            while self._running:
                await asyncio.sleep(10)
                await ws.send("PING")
        except Exception:
            pass  # Connection closed, heartbeat stops

    async def _open_connection(self) -> WebSocketLike:
        """Open a WebSocket connection. Override _connect_func for testing."""
        if self._connect_func:
            return await self._connect_func()
        import websockets
        return await websockets.connect(self._url)

    async def stop(self):
        """Gracefully stop the WebSocket connection."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    # ── Subscription management ──────────────────────────────────────

    async def update_subscriptions(self, token_ids: set[str]):
        """Update which tokens we're streaming. Handles add/remove dynamically."""
        to_add = token_ids - self._subscribed_tokens
        to_remove = self._subscribed_tokens - token_ids

        if self._ws:
            if to_add:
                await self._send_subscribe(self._ws, to_add)
            if to_remove:
                await self._send_unsubscribe(self._ws, to_remove)
            self._subscribed_tokens = token_ids
        else:
            # Not connected yet — queue for next connect
            self._pending_tokens = token_ids

    async def _send_subscribe(self, ws: WebSocketLike, token_ids: set[str]):
        """Send subscription message for a set of token IDs.

        First subscription uses: {"assets_ids": [...], "type": "market"}
        Additional tokens use:   {"assets_ids": [...], "operation": "subscribe"}
        """
        token_list = list(token_ids)

        if not self._subscribed_tokens:
            # Initial subscription — send all at once with "type": "market"
            msg = json.dumps({
                "assets_ids": token_list,
                "type": "market",
            })
            await ws.send(msg)
            logger.debug(f"Initial subscription: {len(token_list)} tokens")
        else:
            # Adding to existing subscription
            msg = json.dumps({
                "assets_ids": token_list,
                "operation": "subscribe",
            })
            await ws.send(msg)
            logger.debug(f"Added {len(token_list)} tokens to subscription")

    async def _send_unsubscribe(self, ws: WebSocketLike, token_ids: set[str]):
        """Send unsubscribe message."""
        msg = json.dumps({
            "assets_ids": list(token_ids),
            "operation": "unsubscribe",
        })
        await ws.send(msg)

    # ── Message handling ─────────────────────────────────────────────

    async def _handle_message(self, raw: str):
        """Parse and route a WebSocket message to the event bus.

        This is the core testable method — given raw JSON, produce typed events.
        """
        self.last_message_time = time.time()
        self.messages_received += 1

        try:
            msgs = json.loads(raw)
        except json.JSONDecodeError:
            self.messages_errors += 1
            logger.warning(f"Malformed WebSocket message: {raw[:200]}")
            return

        # Polymarket sends arrays of events
        if not isinstance(msgs, list):
            msgs = [msgs]

        for msg in msgs:
            # Capture raw samples for debugging
            if len(self.raw_samples) < self._max_raw_samples:
                # Truncate large fields for readability
                sample = {}
                for k, v in msg.items():
                    if isinstance(v, list) and len(v) > 5:
                        sample[k] = f"[{len(v)} items]"
                    elif isinstance(v, str) and len(v) > 100:
                        sample[k] = v[:100] + "..."
                    else:
                        sample[k] = v
                self.raw_samples.append(sample)

            event_type = msg.get("event_type")

            try:
                if event_type == "price_change":
                    # price_change has nested price_changes[] array with per-token data
                    ticks = PriceTick.all_from_polymarket_ws(msg)
                    for tick in ticks:
                        await self._bus.publish("raw.price_tick", tick.model_dump())
                    if ticks:
                        await self._checkpoint.update("polymarket_ws", ticks[0].timestamp)

                elif event_type == "best_bid_ask":
                    # Simpler format with top-level fields (requires custom_feature_enabled)
                    tick = PriceTick.from_polymarket_ws(msg)
                    await self._bus.publish("raw.price_tick", tick.model_dump())
                    await self._checkpoint.update("polymarket_ws", tick.timestamp)

                elif event_type == "last_trade_price":
                    trade = TradePrint.from_polymarket_ws(msg)
                    await self._bus.publish("raw.trade_print", trade.model_dump())
                    await self._checkpoint.update("polymarket_ws", trade.timestamp)

                elif event_type == "book":
                    book = OrderbookSnapshot.from_polymarket_book(msg)
                    await self._bus.publish("raw.orderbook", book.model_dump())

                # Ignore: tick_size_change, new_market, market_resolved

            except Exception as e:
                self.messages_errors += 1
                logger.warning(f"Error processing {event_type}: {e}")

    # ── Health ───────────────────────────────────────────────────────

    def get_health(self) -> dict:
        """Return health metrics for observability."""
        lag = None
        if self.last_message_time:
            lag = round(time.time() - self.last_message_time, 1)

        return {
            "connected": self._ws is not None and self._running,
            "subscribed_tokens": len(self._subscribed_tokens),
            "messages_received": self.messages_received,
            "messages_errors": self.messages_errors,
            "reconnect_count": self.reconnect_count,
            "seconds_since_last_message": lag,
            "raw_samples": self.raw_samples,
        }
