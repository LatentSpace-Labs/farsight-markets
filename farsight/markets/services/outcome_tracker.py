"""
OutcomeTracker — the label loop for Phase 0.

Two responsibilities, both run on background timers:

1. Outcome capture: for each emitted signal, record the market price at
   T+1h, T+4h, and T+24h. Runs on OUTCOME_CAPTURE_INTERVAL_SECONDS cadence.
   Queries signal_outcomes for rows whose horizon has elapsed but whose
   price_tNh column is still NULL, fetches the current mid, writes it back,
   and computes realized_edge_Nh signed by the signal's direction.

2. Resolution backfill: periodically scan Gamma for markets that have
   closed/resolved, write a row to `resolutions`, and backfill
   resolved_price + realized_edge_final on every signal_outcome for that
   market.

The tracker is resilient to restart: it reads state entirely from SQLite
each tick, so if the bot was offline when T+4h passed for some signal,
it will still capture the price the next time it wakes up — just a bit
late. That's acceptable for the label loop (KPIs over many signals), and
the `last_updated_at` column makes the delay visible.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

from farsight.markets.clients.polymarket.clob_client import ClobClient
from farsight.markets.clients.polymarket.gamma_client import GammaClient
from farsight.markets.config import settings
from farsight.markets.store import LocalStore

logger = logging.getLogger(__name__)

# Signed directions: +1 means BULLISH (we predicted price up); -1 means BEARISH.
# Structural/neutral signals don't have a direction, so realized edge is undefined.
_DIR_SIGN = {"bullish": 1.0, "bearish": -1.0, "BULLISH": 1.0, "BEARISH": -1.0}


def _realized_edge(direction: str | None, entry: float, later: float) -> float | None:
    sign = _DIR_SIGN.get(direction or "")
    if sign is None:
        return None
    return sign * (later - entry)


class OutcomeTracker:
    """Captures post-signal prices and final resolutions."""

    def __init__(
        self,
        store: LocalStore,
        clob: Optional[ClobClient] = None,
        gamma: Optional[GammaClient] = None,
    ):
        self.store = store
        self.clob = clob or ClobClient()
        self.gamma = gamma or GammaClient()
        self._running = False
        self._tasks: list[asyncio.Task] = []

    # ── Signal emission hook ─────────────────────────────────────────

    def on_signal_emitted(self, signal: dict):
        """Create the outcome row when a signal fires.

        Called synchronously from the signal engine's persistence subscriber.
        Writing the row up front means the capture loop has something to
        find even if the bot crashes five seconds later.
        """
        try:
            self.store.create_signal_outcome({
                "signal_id": str(signal.get("id", "")),
                "market_id": signal.get("market_id"),
                "token_id": signal.get("token_id"),
                "signal_type": signal.get("signal_type"),
                "direction": signal.get("direction"),
                "entry_price": float(signal.get("market_price", 0.0) or 0.0),
                "emitted_at": signal.get("created_at") or datetime.utcnow().isoformat(),
            })
        except Exception as e:
            logger.warning(f"outcome_tracker: failed to record signal outcome: {e}")

    # ── Background loops ─────────────────────────────────────────────

    async def start(self):
        if not settings.OUTCOME_TRACKER_ENABLED:
            logger.info("outcome_tracker: disabled via config")
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._capture_loop(), name="outcome-capture"),
            asyncio.create_task(self._resolution_loop(), name="outcome-resolution"),
        ]

    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()

    async def _capture_loop(self):
        interval = max(30, settings.OUTCOME_CAPTURE_INTERVAL_SECONDS)
        while self._running:
            try:
                await self.capture_pending_prices()
            except Exception as e:
                logger.error(f"outcome_tracker: capture tick failed: {e}")
            await asyncio.sleep(interval)

    async def _resolution_loop(self):
        interval = max(60, settings.RESOLUTION_POLL_INTERVAL_MINUTES * 60)
        # Run once immediately on boot, then on schedule.
        while self._running:
            try:
                await self.poll_resolutions()
            except Exception as e:
                logger.error(f"outcome_tracker: resolution poll failed: {e}")
            await asyncio.sleep(interval)

    # ── Price capture ────────────────────────────────────────────────

    async def capture_pending_prices(self) -> int:
        """One tick: find rows past each horizon and fill in the price.

        Returns number of captures written.
        """
        now_iso = datetime.utcnow().isoformat()
        horizons = [
            (1, "price_t1h", "realized_edge_1h"),
            (4, "price_t4h", "realized_edge_4h"),
            (24, "price_t24h", "realized_edge_24h"),
        ]
        written = 0
        for hours, price_col, edge_col in horizons:
            rows = self.store.get_pending_outcome_captures(hours, now_iso, price_col)
            for row in rows:
                token_id = row.get("token_id")
                if not token_id:
                    continue
                mid = await self._fetch_mid(token_id)
                if mid is None:
                    continue
                updates = {price_col: mid}
                edge = _realized_edge(row.get("direction"), row["entry_price"], mid)
                if edge is not None:
                    updates[edge_col] = edge
                self.store.update_signal_outcome(row["signal_id"], **updates)
                written += 1
        if written:
            logger.info(f"outcome_tracker: captured {written} prices")
        return written

    async def _fetch_mid(self, token_id: str) -> float | None:
        """Fetch current mid from the CLOB book. Falls back to None on failure."""
        try:
            book = await self.clob.get_book(token_id)
            if not book:
                return None
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            if not bids or not asks:
                return None
            best_bid = float(bids[0].get("price", 0))
            best_ask = float(asks[0].get("price", 0))
            if best_bid <= 0 or best_ask <= 0:
                return None
            return (best_bid + best_ask) / 2.0
        except Exception as e:
            logger.debug(f"outcome_tracker: mid fetch failed for {token_id[:16]}: {e}")
            return None

    # ── Resolution backfill ──────────────────────────────────────────

    async def poll_resolutions(self) -> int:
        """Check markets with unresolved signal_outcomes for final settlement.

        Only hits markets we actually have signals on — keeps the poll cheap.
        Returns number of newly-resolved markets written.
        """
        market_ids = self.store.get_unresolved_market_ids_with_signals()
        if not market_ids:
            return 0

        newly_resolved = 0
        for market_id in market_ids:
            raw = await self._fetch_market_raw(market_id)
            if not raw:
                continue
            parsed = self._parse_resolution(raw)
            if parsed is None:
                continue  # Not resolved yet
            self.store.save_resolution(parsed)
            self._backfill_final_edges(parsed)
            newly_resolved += 1
        if newly_resolved:
            logger.info(f"outcome_tracker: recorded {newly_resolved} resolutions")
        return newly_resolved

    async def _fetch_market_raw(self, market_id: str) -> dict | None:
        try:
            return await self.gamma.get_market_by_id(market_id)
        except Exception as e:
            logger.debug(f"outcome_tracker: gamma fetch failed for {market_id}: {e}")
            return None

    @staticmethod
    def _parse_resolution(raw: dict) -> dict | None:
        """Extract resolution state from a Gamma market payload.

        Returns a dict ready for save_resolution, or None if the market
        is still open. Handles the Gamma quirk where outcomePrices is a
        JSON-encoded string (see VALIDATED_ASSUMPTIONS.md).
        """
        resolved_flag = raw.get("resolved") or raw.get("closed")
        if not resolved_flag:
            return None

        # outcomePrices like '["1", "0"]' when resolved; index 0 is YES.
        prices_raw = raw.get("outcomePrices")
        resolved_price = None
        resolved_outcome = "UNKNOWN"
        try:
            if isinstance(prices_raw, str):
                prices = json.loads(prices_raw)
            else:
                prices = prices_raw or []
            if prices:
                yes_price = float(prices[0])
                resolved_price = yes_price
                if yes_price >= 0.99:
                    resolved_outcome = "YES"
                elif yes_price <= 0.01:
                    resolved_outcome = "NO"
                else:
                    resolved_outcome = "INVALID"
        except (ValueError, TypeError, json.JSONDecodeError):
            pass

        return {
            "market_id": raw.get("conditionId") or str(raw.get("id", "")),
            "token_id": None,
            "resolved_outcome": resolved_outcome,
            "resolved_price": resolved_price,
            "resolved_at": raw.get("endDate") or raw.get("closedTime"),
            "observed_at": datetime.utcnow().isoformat(),
            "source": "gamma",
        }

    def _backfill_final_edges(self, resolution: dict):
        """Write resolved_price + realized_edge_final on every outcome row for this market."""
        price = resolution.get("resolved_price")
        if price is None:
            return
        conn = self.store._get_conn()
        rows = conn.execute(
            "SELECT signal_id, direction, entry_price FROM signal_outcomes WHERE market_id = ? AND resolved_price IS NULL",
            (resolution["market_id"],),
        ).fetchall()
        for r in rows:
            edge = _realized_edge(r["direction"], r["entry_price"], price)
            updates = {"resolved_price": price}
            if edge is not None:
                updates["realized_edge_final"] = edge
            self.store.update_signal_outcome(r["signal_id"], **updates)
