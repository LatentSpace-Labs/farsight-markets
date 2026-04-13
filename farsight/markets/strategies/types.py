"""Unified trading types. One shape for strategy outputs, policy decisions,
and executor results — replaces the split Opportunity/Signal/Action concepts.

A Signal is what a strategy emits: "here is a trade idea, with these legs,
this much edge, this much confidence." It knows nothing about portfolio
state, sizing, or whether it will actually execute — those are Policy's job.

Baskets (cross-event arb, cross-venue arb) use `len(legs) > 1`. Single-market
strategies use one leg.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field

Side = Literal["buy", "sell", "close"]


class Leg(BaseModel):
    """One market instruction within a Signal. Baskets combine multiple."""

    market_id: str
    token_id: str
    side: Side
    target_price: float
    outcome_label: str = ""
    # Optional hint from the strategy about relative sizing across legs.
    # Policy is free to ignore. Defaults to 1.0 (equal weighting).
    size_weight: float = 1.0


class Signal(BaseModel):
    """A strategy's trade idea. Policy decides whether it becomes an Order."""

    strategy: str
    legs: list[Leg]
    edge: float                        # expected return (fraction, e.g. 0.05)
    confidence: float                  # 0..1
    reason: str                        # human-readable
    features: dict = Field(default_factory=dict)  # provenance — what led here

    # Market quality context (single-market convenience; zero for baskets)
    liquidity: float = 0.0
    spread: float = 0.0
    volume_24h: float = 0.0

    risk_flags: list[str] = Field(default_factory=list)
    resolution_date: Optional[datetime] = None
    horizon: str = ""

    emitted_at: datetime = Field(default_factory=datetime.utcnow)

    # ── Convenience accessors for single-leg signals ──────────────────
    @property
    def is_single(self) -> bool:
        return len(self.legs) == 1

    @property
    def market_id(self) -> str:
        return self.legs[0].market_id if self.legs else ""

    @property
    def token_id(self) -> str:
        return self.legs[0].token_id if self.legs else ""

    @property
    def side(self) -> Side:
        return self.legs[0].side if self.legs else "buy"

    @property
    def entry_price(self) -> float:
        return self.legs[0].target_price if self.legs else 0.0

    # ── Back-compat shim for existing Opportunity consumers ───────────
    @classmethod
    def from_opportunity(cls, opp) -> "Signal":
        """Convert a legacy Opportunity so older code can route through
        the Policy/Executor path without being rewritten."""
        leg = Leg(
            market_id=opp.market_id,
            token_id=opp.token_id,
            side=opp.direction or "buy",
            target_price=opp.entry_price,
            outcome_label=opp.outcome or "",
        )
        return cls(
            strategy=opp.strategy or "",
            legs=[leg],
            edge=opp.edge,
            confidence=opp.confidence,
            reason=opp.reasoning or "",
            liquidity=opp.liquidity,
            spread=opp.spread,
            volume_24h=opp.volume_24h,
            risk_flags=list(opp.risk_flags or []),
            resolution_date=opp.resolution_date,
            horizon=opp.horizon or "",
        )


class Order(BaseModel):
    """A Policy-approved instruction ready for the Executor."""

    signal_ref: str                    # reason/slug — for telemetry
    strategy: str
    legs: list[Leg]
    size_usd: float                    # total $ across legs
    edge: float
    confidence: float

    @property
    def is_single(self) -> bool:
        return len(self.legs) == 1


class Fill(BaseModel):
    """An Executor result — what actually got opened."""

    order_ref: str
    strategy: str
    legs: list[Leg]
    fill_prices: list[float]
    size_usd: float
    trade_ids: list[str] = Field(default_factory=list)
    ts: datetime = Field(default_factory=datetime.utcnow)
