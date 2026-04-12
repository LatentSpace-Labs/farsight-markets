"""
Canonical schemas for prediction market events, markets, and outcomes.

These are the source-agnostic Pydantic models that all platform components consume.
Polymarket, Kalshi, etc. each have their own raw formats — clients normalize into these.
"""

from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class MarketSource(str, Enum):
    POLYMARKET = "polymarket"
    KALSHI = "kalshi"


class MarketStatus(str, Enum):
    ACTIVE = "active"
    CLOSED = "closed"
    RESOLVED = "resolved"
    ARCHIVED = "archived"


class MarketTier(int, Enum):
    """Ingestion tier. Lower = higher priority."""
    TIER_1 = 1  # Full WS streaming, 1s state updates
    TIER_2 = 2  # WS streaming, 15s state updates
    TIER_3 = 3  # REST polling every 5 min, metadata only
    TIER_4 = 4  # Closed/resolved, snapshot only


class OutcomeSchema(BaseModel):
    """A single outcome within a market (e.g., YES or NO)."""
    id: Optional[UUID] = None
    token_id: str                           # Polymarket: large uint256, Kalshi: ticker
    label: str                              # "Yes", "No", "Trump", "DeSantis", etc.
    current_price: float = 0.0              # 0.00 - 1.00 (probability)
    volume_24h: float = 0.0


class MarketSchema(BaseModel):
    """A single prediction market (one question, 2-N outcomes)."""
    id: Optional[UUID] = None
    event_id: Optional[UUID] = None
    source: MarketSource
    condition_id: str                       # Polymarket condition_id or Kalshi ticker
    question: str                           # "Will the Fed cut rates in June 2026?"
    slug: Optional[str] = None
    status: MarketStatus = MarketStatus.ACTIVE
    tier: MarketTier = MarketTier.TIER_3

    # Market parameters
    min_tick_size: float = 0.01
    min_order_size: float = 5.0             # USD
    maker_fee: float = 0.0
    taker_fee: float = 0.02

    # Neg-risk (multi-outcome events on Polymarket)
    neg_risk: bool = False
    neg_risk_market_id: Optional[str] = None

    # Metadata
    end_date: Optional[datetime] = None
    resolution_source: Optional[str] = None  # "uma_oracle", "kalshi_internal"
    volume_total: float = 0.0
    liquidity: float = 0.0

    outcomes: list[OutcomeSchema] = Field(default_factory=list)

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class EventSchema(BaseModel):
    """A prediction event (groups related markets)."""
    id: Optional[UUID] = None
    source: MarketSource
    slug: str                               # "2026-us-presidential-election"
    title: str                              # "2026 US Presidential Election"
    description: Optional[str] = None
    category: Optional[str] = None          # "politics", "crypto", "weather", "economics"
    status: MarketStatus = MarketStatus.ACTIVE
    end_date: Optional[datetime] = None
    tags: list[str] = Field(default_factory=list)
    volume_total: float = 0.0
    liquidity: float = 0.0

    markets: list[MarketSchema] = Field(default_factory=list)

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class MarketSyncResult(BaseModel):
    """Result of a catalog sync operation."""
    events_discovered: int = 0
    events_updated: int = 0
    markets_discovered: int = 0
    markets_updated: int = 0
    markets_closed: int = 0
    markets_resolved: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = Field(default_factory=list)
