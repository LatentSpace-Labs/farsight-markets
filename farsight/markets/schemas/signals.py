"""
Signal and evidence schemas.

Signals are the primary output of the event markets platform.
Each signal has a type, direction, confidence, and evidence bundle.
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class SignalType(str, Enum):
    PROBABILITY_SHOCK = "probability_shock"
    MOMENTUM_CONTINUATION = "momentum_continuation"
    MEAN_REVERSION = "mean_reversion"
    THEMATIC_REPRICING = "thematic_repricing"
    STRUCTURAL_INCONSISTENCY = "structural_inconsistency"
    CROSS_VENUE_DIVERGENCE = "cross_venue_divergence"


class Direction(str, Enum):
    BULLISH = "bullish"   # Probability increasing / YES favored
    BEARISH = "bearish"   # Probability decreasing / NO favored
    NEUTRAL = "neutral"


class SignalStatus(str, Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    INVALIDATED = "invalidated"
    CONFIRMED = "confirmed"
    PERFORMANCE_SCORED = "performance_scored"


class SignalEvidence(BaseModel):
    """A single piece of evidence supporting a signal."""
    source: str                             # "price_delta", "volume_spike", "cross_market", "news"
    description: str                        # Human-readable
    value: float                            # Numeric evidence value
    weight: float = 1.0                     # Contribution to confidence


class SignalSchema(BaseModel):
    """A generated trading signal."""
    id: Optional[UUID] = None
    market_id: Optional[str] = None        # Internal market ID or token_id (string for flexibility)
    event_id: Optional[str] = None
    source: str                             # "polymarket", "kalshi", "composite"
    signal_type: SignalType
    direction: Direction
    confidence: float                       # 0.0 - 1.0
    horizon: str                            # "1h", "4h", "1d", "1w"
    tradability_score: float                # 0.0 - 1.0 (liquidity-adjusted)
    evidence: list[SignalEvidence] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    status: SignalStatus = SignalStatus.ACTIVE

    # Calibration fields
    model_probability: float                # Our estimated probability
    market_price: float                     # Market price at signal generation
    edge: float                             # model_probability - market_price

    # Backfilled on resolution
    actual_outcome: Optional[bool] = None
    outcome_correct: Optional[bool] = None
    peak_edge: Optional[float] = None
    time_to_peak: Optional[timedelta] = None

    # Versioning
    feature_set_version: str
    rule_version: str

    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None


class SignalCard(BaseModel):
    """Signal packaged for UI display. This is what Farsight consumers see."""
    signal_id: Optional[UUID] = None
    headline: str                           # "Fed rate cut probability shocked -8% in 5 min"
    market_question: str
    event_title: Optional[str] = None
    signal_type: SignalType
    direction: Direction
    confidence: float
    probability_move: float
    cross_asset_confirmation: list[str] = Field(default_factory=list)
    related_holdings: list[str] = Field(default_factory=list)
    why_it_matters: Optional[str] = None
    tradability: float
    risk_flags: list[str] = Field(default_factory=list)
    timestamp: datetime
