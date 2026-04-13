"""Executor — turns Policy-approved Orders into paper Fills.

Handles both opens (size_usd > 0) and closes (size_usd == 0 against an
existing position). Emits `trade.open` / `trade.close` telemetry and
updates the portfolio. Strategies and Policy never touch the store
directly through this path.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional
from uuid import uuid4

from farsight.markets import telemetry
from farsight.markets.store import LocalStore
from farsight.markets.strategies.types import Fill, Leg, Order

logger = logging.getLogger(__name__)


@dataclass
class SessionRef:
    session_id: str


class Executor:
    """Apply an Order → paper Fill. One Fill per open, per close."""

    def __init__(self, store: LocalStore, session: SessionRef):
        self.store = store
        self.session = session

    async def execute(self, order: Order) -> Optional[Fill]:
        if order.size_usd == 0.0 and len(order.legs) == 1 and order.legs[0].side == "close":
            return await self._close(order)
        return await self._open(order)

    async def _open(self, order: Order) -> Optional[Fill]:
        portfolio = self.store.get_portfolio()
        # Single-leg today; multi-leg (baskets) splits size by weight and
        # writes one trade row per leg.
        total_weight = sum(max(0.0, leg.size_weight) for leg in order.legs) or 1.0
        fill_prices: list[float] = []
        trade_ids: list[str] = []

        for leg in order.legs:
            leg_size = order.size_usd * (max(0.0, leg.size_weight) / total_weight)
            # Slippage: half the spread on top of target.
            fill_price = max(0.01, min(0.99, leg.target_price))
            num_shares = leg_size / fill_price if fill_price > 0 else 0

            trade_id = str(uuid4())
            trade = {
                "id": trade_id,
                "signal_id": None,
                "market_id": leg.market_id,
                "market_question": order.signal_ref[:500],
                "token_id": leg.token_id,
                "outcome": leg.outcome_label,
                "direction": leg.side.upper(),
                "entry_price": leg.target_price,
                "fill_price": fill_price,
                "size_usd": leg_size,
                "num_shares": num_shares,
                "slippage_bps": 0,
                "strategy": order.strategy,
                "session_id": self.session.session_id,
            }
            self.store.save_trade(trade)
            self.store.update_portfolio(
                current_balance=portfolio["current_balance"] - leg_size,
                total_trades=portfolio["total_trades"] + 1,
            )
            portfolio = self.store.get_portfolio()
            fill_prices.append(fill_price)
            trade_ids.append(trade_id)

            telemetry.emit(
                "trade.open", strategy=order.strategy,
                trade_id=trade_id, market_id=leg.market_id,
                slug=leg.outcome_label or order.signal_ref[:40],
                direction=leg.side, entry=fill_price,
                size_usd=leg_size, edge=order.edge,
            )

        telemetry.emit(
            "portfolio",
            balance=portfolio["current_balance"],
            total_pnl=portfolio.get("total_pnl", 0),
            open_positions=len(self.store.get_open_trades()),
        )
        return Fill(
            order_ref=order.signal_ref, strategy=order.strategy,
            legs=list(order.legs), fill_prices=fill_prices,
            size_usd=order.size_usd, trade_ids=trade_ids,
        )

    async def _close(self, order: Order) -> Optional[Fill]:
        leg = order.legs[0]
        open_trades = self.store.get_open_trades()
        trade = next((t for t in open_trades if t.get("market_id") == leg.market_id), None)
        if not trade:
            return None

        exit_price = leg.target_price
        num_shares = trade.get("num_shares", 0)
        fill = trade["fill_price"]
        pnl = (exit_price - fill) * num_shares if trade["direction"] == "BUY" \
              else (fill - exit_price) * num_shares
        size = trade["size_usd"]
        return_pct = (pnl / size) * 100 if size > 0 else 0

        self.store.close_trade(trade["id"], exit_price, "CLOSE",
                               round(pnl, 2), round(return_pct, 2))
        portfolio = self.store.get_portfolio()
        self.store.update_portfolio(
            current_balance=portfolio["current_balance"] + size + pnl,
            total_pnl=portfolio["total_pnl"] + pnl,
            winning_trades=portfolio["winning_trades"] + (1 if pnl > 0 else 0),
        )
        telemetry.emit(
            "trade.close", strategy=order.strategy,
            trade_id=trade["id"], market_id=leg.market_id,
            slug=trade.get("market_question", "?")[:40],
            exit=exit_price, pnl=round(pnl, 2), return_pct=round(return_pct, 2),
            reason=order.signal_ref,
        )
        portfolio = self.store.get_portfolio()
        telemetry.emit(
            "portfolio",
            balance=portfolio["current_balance"],
            total_pnl=portfolio["total_pnl"],
            open_positions=len(self.store.get_open_trades()),
        )
        return Fill(
            order_ref=order.signal_ref, strategy=order.strategy,
            legs=list(order.legs), fill_prices=[exit_price],
            size_usd=0.0, trade_ids=[trade["id"]],
        )
