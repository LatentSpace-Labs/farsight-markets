"""
Polymarket CLOB API client (read-only).

The CLOB API provides orderbook, price, and trade data.
Public for reads, no auth needed. Base URL: https://clob.polymarket.com

Used for:
- Orderbook snapshots (gap-fill, reconciliation)
- Price queries (fallback when WS is down)
- Trade history (backfill, gap detection)
- Price history (historical series)
"""

import logging
from datetime import datetime
from typing import Optional

import httpx

from farsight.markets.config import settings
from farsight.markets.schemas.ticks import (
    OrderbookLevel,
    OrderbookSnapshot,
    PriceTick,
    TradePrint,
    TradeSide,
)

logger = logging.getLogger(__name__)

# CLOB API rate limit: 9,000/10s — comfortable for gap-fill + reconciliation
DEFAULT_TIMEOUT = 30.0


class ClobClient:
    """Async read-only client for the Polymarket CLOB API."""

    def __init__(self, base_url: Optional[str] = None):
        self._base_url = base_url or settings.POLYMARKET_CLOB_URL
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=DEFAULT_TIMEOUT,
                headers={"Accept": "application/json"},
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ── Orderbook ────────────────────────────────────────────────────

    async def get_orderbook(self, token_id: str) -> Optional[OrderbookSnapshot]:
        """Fetch L2 orderbook for a specific outcome token."""
        client = await self._get_client()
        try:
            resp = await client.get("/book", params={"token_id": token_id})
            resp.raise_for_status()
            data = resp.json()
            return OrderbookSnapshot.from_polymarket_book(data, token_id)
        except httpx.HTTPStatusError as e:
            logger.error(f"CLOB API error fetching book for {token_id[:20]}: {e.response.status_code}")
            return None
        except httpx.RequestError as e:
            logger.error(f"CLOB API request error: {e}")
            return None

    # ── Price ────────────────────────────────────────────────────────

    async def get_price(self, token_id: str) -> Optional[float]:
        """Fetch current mid price for a token.

        Note: CLOB /price endpoint may return 400 for some tokens.
        Falls back to extracting price from /book if /price fails.
        """
        client = await self._get_client()
        try:
            resp = await client.get("/price", params={"token_id": token_id})
            resp.raise_for_status()
            data = resp.json()
            return float(data.get("price", 0))
        except httpx.HTTPStatusError:
            # Fallback: compute from orderbook
            book = await self.get_orderbook(token_id)
            if book:
                return book.mid
            return None
        except httpx.RequestError as e:
            logger.error(f"CLOB API error fetching price: {e}")
            return None

    async def get_midpoint(self, token_id: str) -> Optional[float]:
        """Fetch midpoint price for a token."""
        client = await self._get_client()
        try:
            resp = await client.get("/midpoint", params={"token_id": token_id})
            resp.raise_for_status()
            data = resp.json()
            return float(data.get("mid", 0))
        except httpx.HTTPStatusError:
            book = await self.get_orderbook(token_id)
            if book:
                return book.mid
            return None
        except httpx.RequestError as e:
            logger.error(f"CLOB API error fetching midpoint: {e}")
            return None

    async def get_spread(self, token_id: str) -> Optional[dict]:
        """Fetch bid-ask spread for a token."""
        client = await self._get_client()
        try:
            resp = await client.get("/spread", params={"token_id": token_id})
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError:
            book = await self.get_orderbook(token_id)
            if book:
                return {"bid": book.best_bid, "ask": book.best_ask, "spread": book.spread}
            return None
        except httpx.RequestError as e:
            logger.error(f"CLOB API error fetching spread: {e}")
            return None

    # ── Trades ───────────────────────────────────────────────────────

    async def get_trades(
        self,
        condition_id: str,
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> list[TradePrint]:
        """Fetch recent trades for a market (by condition_id).

        Note: The CLOB /trades endpoint requires API key authentication.
        For unauthenticated use, trade data comes from the WebSocket stream
        (last_trade_price events) or Goldsky subgraph.
        """
        client = await self._get_client()
        params: dict = {"market": condition_id, "limit": limit}
        if before:
            params["before"] = before
        if after:
            params["after"] = after

        try:
            resp = await client.get("/trades", params=params)
            resp.raise_for_status()
            raw_trades = resp.json()

            trades = []
            for t in raw_trades:
                try:
                    trades.append(TradePrint(
                        source="polymarket",
                        token_id=str(t.get("asset_id", "")),
                        timestamp=datetime.fromisoformat(t["match_time"].replace("Z", "+00:00"))
                        if t.get("match_time") else datetime.utcnow(),
                        price=float(t.get("price", 0)),
                        size_usd=float(t.get("size", 0)),
                        side=TradeSide.BUY if t.get("side") == "BUY" else TradeSide.SELL,
                        taker_address=t.get("taker_address"),
                    ))
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Skipping malformed trade: {e}")
            return trades
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.error(f"CLOB API error fetching trades: {e}")
            return []

    # ── Price History ────────────────────────────────────────────────

    async def get_price_history(
        self,
        token_id: str,
        interval: str = "1h",
        fidelity: int = 60,
    ) -> list[dict]:
        """Fetch historical price series for a token.

        Args:
            token_id: The outcome token ID
            interval: Time interval (e.g., "1m", "5m", "1h", "1d")
            fidelity: Number of data points

        Returns list of dicts with 't' (epoch seconds) and 'p' (price).
        The raw API response structure varies — we normalize to {t, p}.
        """
        client = await self._get_client()
        try:
            resp = await client.get("/prices-history", params={
                "market": token_id,
                "interval": interval,
                "fidelity": fidelity,
            })
            resp.raise_for_status()
            data = resp.json()

            # Handle different response shapes
            history = data.get("history", data) if isinstance(data, dict) else data
            if not isinstance(history, list):
                return []

            # Normalize to [{t, p}] format
            normalized = []
            for point in history:
                if isinstance(point, dict):
                    normalized.append(point)
                elif isinstance(point, (list, tuple)) and len(point) >= 2:
                    normalized.append({"t": point[0], "p": point[1]})
            return normalized
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.error(f"CLOB API error fetching price history: {e}")
            return []

    # ── Markets (CLOB view) ──────────────────────────────────────────

    async def get_markets(self, next_cursor: str = "MA==") -> tuple[list[dict], Optional[str]]:
        """Fetch markets from CLOB API with cursor pagination.

        Returns (markets, next_cursor). next_cursor is None when no more pages.
        """
        client = await self._get_client()
        try:
            resp = await client.get("/markets", params={"next_cursor": next_cursor})
            resp.raise_for_status()
            data = resp.json()
            markets = data.get("data", [])
            cursor = data.get("next_cursor")
            # CLOB returns "LTE=" when there are no more pages
            if cursor == "LTE=":
                cursor = None
            return markets, cursor
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.error(f"CLOB API error fetching markets: {e}")
            return [], None
