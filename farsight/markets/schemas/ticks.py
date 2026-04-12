"""
Canonical schemas for price ticks, trades, and orderbook snapshots.

These are the normalized event types that flow through the event bus.
Each client normalizes raw API/WebSocket data into these schemas.
"""

from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class TradeSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class PriceTick(BaseModel):
    """Normalized price update from any source."""
    market_id: Optional[UUID] = None       # Internal FK, set after catalog lookup
    outcome_id: Optional[UUID] = None
    source: str                             # "polymarket", "kalshi"
    token_id: str                           # Source-native token identifier
    timestamp: datetime

    bid: float
    ask: float
    mid: float
    spread: float                           # ask - bid

    # Dedup key: source + token_id + timestamp (truncated to ms)
    @property
    def dedup_key(self) -> str:
        ts_ms = int(self.timestamp.timestamp() * 1000)
        return f"{self.source}:{self.token_id}:{ts_ms}"

    @classmethod
    def from_polymarket_ws(cls, msg: dict) -> "PriceTick":
        """Normalize a Polymarket price_change WebSocket event.

        price_change messages have a NESTED price_changes[] array:
        {
          "event_type": "price_change",
          "market": "0x...",
          "price_changes": [
            {"asset_id": "...", "price": "0.5", "best_bid": "0.48", "best_ask": "0.52", ...}
          ],
          "timestamp": "1757908892351"
        }

        We extract the first price_change entry. The caller (ws_client) handles
        iterating if there are multiple entries per message.
        """
        # For best_bid_ask events (top-level fields)
        if msg.get("event_type") == "best_bid_ask":
            bid = float(msg.get("best_bid", 0))
            ask = float(msg.get("best_ask", 1))
            return cls(
                source="polymarket",
                token_id=msg.get("asset_id", ""),
                timestamp=_parse_ws_timestamp(msg.get("timestamp")),
                bid=bid,
                ask=ask,
                mid=(bid + ask) / 2 if (bid + ask) > 0 else 0,
                spread=ask - bid,
            )

        # For price_change events (nested price_changes array)
        changes = msg.get("price_changes", [])
        if not changes:
            # Fallback: try top-level fields
            bid = float(msg.get("best_bid", 0))
            ask = float(msg.get("best_ask", 0))
            return cls(
                source="polymarket",
                token_id=msg.get("asset_id", ""),
                timestamp=_parse_ws_timestamp(msg.get("timestamp")),
                bid=bid, ask=ask,
                mid=(bid + ask) / 2 if (bid + ask) > 0 else 0,
                spread=ask - bid,
            )

        change = changes[0]
        bid = float(change.get("best_bid", 0))
        ask = float(change.get("best_ask", 0))
        mid = (bid + ask) / 2 if (bid + ask) > 0 else 0
        return cls(
            source="polymarket",
            token_id=change.get("asset_id", ""),
            timestamp=_parse_ws_timestamp(msg.get("timestamp")),
            bid=bid,
            ask=ask,
            mid=mid,
            spread=ask - bid,
        )

    @classmethod
    def all_from_polymarket_ws(cls, msg: dict) -> list["PriceTick"]:
        """Extract ALL price ticks from a price_change message.

        A single price_change message can contain updates for multiple tokens
        (e.g., both YES and NO outcomes of the same market).
        """
        changes = msg.get("price_changes", [])
        ts = _parse_ws_timestamp(msg.get("timestamp"))
        ticks = []
        for change in changes:
            bid = float(change.get("best_bid", 0))
            ask = float(change.get("best_ask", 0))
            mid = (bid + ask) / 2 if (bid + ask) > 0 else 0
            ticks.append(cls(
                source="polymarket",
                token_id=change.get("asset_id", ""),
                timestamp=ts,
                bid=bid, ask=ask, mid=mid,
                spread=ask - bid,
            ))
        return ticks


class TradePrint(BaseModel):
    """Normalized trade execution from any source."""
    market_id: Optional[UUID] = None
    outcome_id: Optional[UUID] = None
    source: str
    token_id: str
    timestamp: datetime

    price: float
    size_usd: float
    side: TradeSide
    taker_address: Optional[str] = None    # On-chain address (Polymarket)

    @property
    def dedup_key(self) -> str:
        ts_ms = int(self.timestamp.timestamp() * 1000)
        return f"{self.source}:{self.token_id}:{ts_ms}:{self.price}:{self.size_usd}"

    @classmethod
    def from_polymarket_ws(cls, msg: dict) -> "TradePrint":
        """Normalize a Polymarket last_trade_price WebSocket event.

        Format:
        {
          "asset_id": "...", "event_type": "last_trade_price",
          "price": "0.456", "side": "BUY", "size": "219.217767",
          "timestamp": "1750428146322"
        }
        """
        side_str = msg.get("side", "BUY").upper()
        return cls(
            source="polymarket",
            token_id=msg.get("asset_id", ""),
            timestamp=_parse_ws_timestamp(msg.get("timestamp")),
            price=float(msg.get("price", 0)),
            size_usd=float(msg.get("size", 0)),
            side=TradeSide.BUY if side_str == "BUY" else TradeSide.SELL,
        )


class OrderbookLevel(BaseModel):
    """Single price level in an orderbook."""
    price: float
    size: float  # In shares/contracts


class OrderbookSnapshot(BaseModel):
    """Normalized L2 orderbook snapshot."""
    market_id: Optional[UUID] = None
    outcome_id: Optional[UUID] = None
    source: str
    token_id: str
    timestamp: datetime

    bids: list[OrderbookLevel] = Field(default_factory=list)
    asks: list[OrderbookLevel] = Field(default_factory=list)

    @property
    def best_bid(self) -> float:
        return self.bids[0].price if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0].price if self.asks else 1.0

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2

    @property
    def total_bid_depth(self) -> float:
        return sum(level.size * level.price for level in self.bids)

    @property
    def total_ask_depth(self) -> float:
        return sum(level.size * level.price for level in self.asks)

    @classmethod
    def from_polymarket_book(cls, data: dict, token_id: str = "") -> "OrderbookSnapshot":
        """Normalize a Polymarket book WebSocket event or REST response.

        WS format: {"event_type": "book", "asset_id": "...", "bids": [...], "asks": [...], "timestamp": "..."}
        REST format: {"bids": [...], "asks": [...]}
        Prices and sizes are strings in WS messages.
        """
        asset_id = data.get("asset_id", token_id)
        bids = [
            OrderbookLevel(price=float(b["price"]), size=float(b["size"]))
            for b in data.get("bids", [])
        ]
        asks = [
            OrderbookLevel(price=float(a["price"]), size=float(a["size"]))
            for a in data.get("asks", [])
        ]
        return cls(
            source="polymarket",
            token_id=asset_id,
            timestamp=_parse_ws_timestamp(data.get("timestamp")),
            bids=sorted(bids, key=lambda x: x.price, reverse=True),
            asks=sorted(asks, key=lambda x: x.price),
        )


# ── Helpers ──────────────────────────────────────────────────────────


def _parse_ws_timestamp(val) -> datetime:
    """Parse a Polymarket WebSocket timestamp (epoch milliseconds as string)."""
    if val is None:
        return datetime.utcnow()
    try:
        # WS timestamps are epoch milliseconds as strings: "1757908892351"
        ts_ms = int(val)
        return datetime.utcfromtimestamp(ts_ms / 1000)
    except (ValueError, TypeError, OSError):
        return datetime.utcnow()
