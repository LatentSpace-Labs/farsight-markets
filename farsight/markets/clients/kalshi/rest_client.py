"""
Kalshi REST API client (public endpoints — no auth needed).

Base URL: https://api.elections.kalshi.com/trade-api/v2
CFTC-regulated prediction market exchange.

Public endpoints used:
  GET /events                    — List events (cursor-paginated)
  GET /events/{event_ticker}     — Single event detail
  GET /markets                   — List markets (cursor-paginated)
  GET /markets/{ticker}          — Single market detail
  GET /markets/trades            — Public trade history
  GET /markets/candlesticks      — OHLCV candles (1m, 1h, 1d)
  GET /exchange/status           — Exchange status
  GET /exchange/schedule         — Trading hours

Key differences from Polymarket:
  - Prices in DOLLARS (e.g., "0.55" = 55 cents = 55% probability)
  - Tickers are structured: "KXHIGHNY-26MAR01-B45.5" = NYC high temp > 45.5F
  - Status lifecycle: unopened → open → closed → settled
  - Cursor pagination (not offset/limit)
  - Native OHLCV candlesticks built in
  - volume/liquidity fields use fixed-point strings (e.g., "1234.5678")
"""

import logging
from datetime import datetime
from typing import Optional

import httpx

from farsight.markets.config import settings

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0


class KalshiClient:
    """Async client for Kalshi's public REST API."""

    def __init__(self, base_url: Optional[str] = None):
        self._base_url = base_url or settings.KALSHI_API_URL
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

    # ── Events ───────────────────────────────────────────────────────

    async def get_events(
        self,
        status: Optional[str] = "open",
        series_ticker: Optional[str] = None,
        with_nested_markets: bool = True,
        limit: int = 200,
        cursor: Optional[str] = None,
    ) -> dict:
        """Fetch events with optional nested markets.

        Returns {"events": [...], "cursor": "next_cursor_or_empty"}.
        """
        client = await self._get_client()
        params: dict = {"limit": limit, "with_nested_markets": str(with_nested_markets).lower()}
        if status:
            params["status"] = status
        if series_ticker:
            params["series_ticker"] = series_ticker
        if cursor:
            params["cursor"] = cursor

        try:
            resp = await client.get("/events", params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.debug("Kalshi events endpoint requires auth — skipping")
            else:
                logger.error(f"Kalshi API error fetching events: {e.response.status_code}")
            return {"events": [], "cursor": ""}
        except httpx.RequestError as e:
            logger.error(f"Kalshi API request error: {e}")
            return {"events": [], "cursor": ""}

    async def get_event(self, event_ticker: str) -> Optional[dict]:
        """Fetch a single event by ticker."""
        client = await self._get_client()
        try:
            resp = await client.get(f"/events/{event_ticker}")
            resp.raise_for_status()
            return resp.json().get("event")
        except httpx.HTTPStatusError:
            return None

    # ── Markets ──────────────────────────────────────────────────────

    async def get_markets(
        self,
        status: Optional[str] = "open",
        event_ticker: Optional[str] = None,
        series_ticker: Optional[str] = None,
        tickers: Optional[list[str]] = None,
        limit: int = 200,
        cursor: Optional[str] = None,
    ) -> dict:
        """Fetch markets.

        Returns {"markets": [...], "cursor": "..."}.
        """
        client = await self._get_client()
        params: dict = {"limit": limit}
        if status:
            params["status"] = status
        if event_ticker:
            params["event_ticker"] = event_ticker
        if series_ticker:
            params["series_ticker"] = series_ticker
        if tickers:
            params["tickers"] = ",".join(tickers)
        if cursor:
            params["cursor"] = cursor

        try:
            resp = await client.get("/markets", params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.debug("Kalshi markets endpoint requires auth — skipping")
            else:
                logger.error(f"Kalshi API error fetching markets: {e.response.status_code}")
            return {"markets": [], "cursor": ""}
        except httpx.RequestError as e:
            logger.error(f"Kalshi API request error: {e}")
            return {"markets": [], "cursor": ""}

    async def get_market(self, ticker: str) -> Optional[dict]:
        """Fetch a single market by ticker."""
        client = await self._get_client()
        try:
            resp = await client.get(f"/markets/{ticker}")
            resp.raise_for_status()
            return resp.json().get("market")
        except httpx.HTTPStatusError:
            return None

    # ── Trades ───────────────────────────────────────────────────────

    async def get_trades(
        self,
        ticker: Optional[str] = None,
        limit: int = 100,
        cursor: Optional[str] = None,
        min_ts: Optional[int] = None,
        max_ts: Optional[int] = None,
    ) -> dict:
        """Fetch public trade history."""
        client = await self._get_client()
        params: dict = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        if cursor:
            params["cursor"] = cursor
        if min_ts:
            params["min_ts"] = min_ts
        if max_ts:
            params["max_ts"] = max_ts

        try:
            resp = await client.get("/markets/trades", params=params)
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.error(f"Kalshi API error fetching trades: {e}")
            return {"trades": [], "cursor": ""}

    # ── Candlesticks ─────────────────────────────────────────────────

    async def get_candlesticks(
        self,
        ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int = 60,
    ) -> list[dict]:
        """Fetch OHLCV candlesticks for a market.

        Args:
            ticker: Market ticker
            start_ts: Unix timestamp (seconds)
            end_ts: Unix timestamp (seconds)
            period_interval: 1 (1 minute), 60 (1 hour), 1440 (1 day)
        """
        client = await self._get_client()
        try:
            resp = await client.get(f"/markets/{ticker}/candlesticks", params={
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
            })
            resp.raise_for_status()
            return resp.json().get("candlesticks", [])
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.error(f"Kalshi API error fetching candlesticks: {e}")
            return []

    # ── Exchange Info ────────────────────────────────────────────────

    async def get_exchange_status(self) -> dict:
        """Get exchange operating status."""
        client = await self._get_client()
        try:
            resp = await client.get("/exchange/status")
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.error(f"Kalshi API error: {e}")
            return {}

    async def get_exchange_schedule(self) -> dict:
        """Get trading hours schedule."""
        client = await self._get_client()
        try:
            resp = await client.get("/exchange/schedule")
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.error(f"Kalshi API error: {e}")
            return {}

    # ── Normalization Helpers ────────────────────────────────────────

    @staticmethod
    def market_to_probability(market: dict) -> float:
        """Extract implied probability from a Kalshi market object.

        Kalshi prices are in dollars (0.01 to 0.99).
        yes_bid + no_bid should approximate 1.00.
        """
        yes_bid = _safe_float(market.get("yes_bid_dollars"))
        yes_ask = _safe_float(market.get("yes_ask_dollars"))
        last = _safe_float(market.get("last_price_dollars"))

        if yes_bid > 0 and yes_ask > 0:
            return (yes_bid + yes_ask) / 2
        return last or 0.0

    @staticmethod
    def market_to_dict(market: dict) -> dict:
        """Normalize a Kalshi market to a common format for comparison with Polymarket."""
        return {
            "source": "kalshi",
            "ticker": market.get("ticker", ""),
            "event_ticker": market.get("event_ticker", ""),
            "question": market.get("yes_sub_title", ""),
            "status": market.get("status", ""),
            "probability": KalshiClient.market_to_probability(market),
            "yes_bid": _safe_float(market.get("yes_bid_dollars")),
            "yes_ask": _safe_float(market.get("yes_ask_dollars")),
            "no_bid": _safe_float(market.get("no_bid_dollars")),
            "no_ask": _safe_float(market.get("no_ask_dollars")),
            "last_price": _safe_float(market.get("last_price_dollars")),
            "previous_price": _safe_float(market.get("previous_price_dollars")),
            "volume_24h": _safe_float(market.get("volume_24h_fp")),
            "volume": _safe_float(market.get("volume_fp")),
            "liquidity": _safe_float(market.get("liquidity_dollars")),
            "open_interest": _safe_float(market.get("open_interest_fp")),
            "close_time": market.get("close_time"),
            "result": market.get("result"),
        }


def _safe_float(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0
