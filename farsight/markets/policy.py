"""Policy — portfolio-aware gate between Signals and Orders.

A Signal says "here's a trade idea with these legs." The Policy decides
whether that idea actually becomes an Order given the current portfolio,
open positions, and risk limits. Everything that's not strategy-specific
(Kelly sizing, position caps, duplicate-market dedup, minimum size floor)
lives here, not in the runner.

Reject reasons are emitted as telemetry so you can see why good signals
don't become trades.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from farsight.markets import telemetry
from farsight.markets.store import LocalStore
from farsight.markets.strategies.types import Order, Signal


@dataclass
class PolicyConfig:
    max_concurrent_positions: int = 20
    kelly_fraction: float = 0.15
    max_position_pct: float = 5.0     # cap per trade as % of balance
    max_position_usd: float = 0.0     # absolute $ cap per trade (0 = disabled)
    min_order_usd: float = 5.0
    min_entry_price: float = 0.01     # don't size Kelly on deep OTM prices


class Policy:
    """Signal → Order (or None + telemetered reason)."""

    def __init__(self, store: LocalStore, config: Optional[PolicyConfig] = None):
        self.store = store
        self.config = config or PolicyConfig()

    def apply(self, signal: Signal) -> Optional[Order]:
        cfg = self.config

        # Close signals: always approved as zero-size orders; the Executor
        # handles them as position closures against existing trades.
        if signal.is_single and signal.side == "close":
            return Order(
                signal_ref=signal.reason[:80],
                strategy=signal.strategy,
                legs=list(signal.legs),
                size_usd=0.0,
                edge=signal.edge,
                confidence=signal.confidence,
            )

        portfolio = self.store.get_portfolio()
        open_trades = self.store.get_open_trades()

        # Position cap
        if len(open_trades) >= cfg.max_concurrent_positions:
            self._reject(signal, "max_concurrent_positions",
                         open=len(open_trades), cap=cfg.max_concurrent_positions)
            return None

        # Duplicate-market dedup (only for single-leg signals — baskets
        # legitimately touch markets we may already hold as singles)
        if signal.is_single:
            if any(t.get("market_id") == signal.market_id for t in open_trades):
                self._reject(signal, "already_open", market_id=signal.market_id)
                return None

        # Guard rails for sizing
        entry = signal.entry_price
        if signal.edge <= 0 or entry <= cfg.min_entry_price:
            self._reject(signal, "unsizable", edge=signal.edge, entry=entry)
            return None

        # Kelly: b = (1-p)/p, size = (p*b - q)/b using model-implied p
        b = (1.0 - entry) / entry
        p = entry + abs(signal.edge)
        q = 1.0 - p
        kelly = (p * b - q) / b
        if kelly <= 0:
            self._reject(signal, "negative_kelly", edge=signal.edge, entry=entry)
            return None

        balance = portfolio["current_balance"]
        fraction = kelly * cfg.kelly_fraction
        size_usd = round(balance * fraction, 2)
        max_size = balance * (cfg.max_position_pct / 100.0)
        if cfg.max_position_usd > 0:
            max_size = min(max_size, cfg.max_position_usd)
        size_usd = min(size_usd, max_size)

        if size_usd < cfg.min_order_usd:
            self._reject(signal, "below_min_order_usd", size_usd=size_usd,
                         floor=cfg.min_order_usd)
            return None

        order = Order(
            signal_ref=signal.reason[:80],
            strategy=signal.strategy,
            legs=list(signal.legs),
            size_usd=size_usd,
            edge=signal.edge,
            confidence=signal.confidence,
        )
        telemetry.emit(
            "policy.accept", strategy=signal.strategy,
            slug=signal.legs[0].outcome_label[:40],
            size_usd=size_usd, edge=signal.edge, kelly=kelly,
        )
        return order

    def _reject(self, signal: Signal, reason: str, **data) -> None:
        data.setdefault("market_id", signal.market_id)
        telemetry.emit(
            "policy.reject", strategy=signal.strategy,
            reason=reason, **data,
        )
