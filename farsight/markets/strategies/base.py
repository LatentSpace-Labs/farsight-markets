"""
Strategy framework — composable pipeline stages for prediction market intelligence.

A strategy is a FLOW defined as a pipeline of stages:

    Source → Enrich → Analyze → Score → Filter → Output

Each stage is a distinct concern that can be reused:
  - By other strategies
  - By Farsight agents as skills/tools
  - By the API as endpoint logic

Example: OpportunityScanner flow:
    PolymarketSource(top_markets)          # Source: fetch top markets by volume
    → MarketEnricher(orderbook, history)   # Enrich: add orderbook + price history
    → FeatureAnalyzer()                    # Analyze: compute 20 features
    → SignalScorer()                       # Score: run signal detectors, compute edge
    → QualityFilter(min_edge, min_liq)     # Filter: remove low-quality opportunities
    → OpportunityOutput()                  # Output: ranked Opportunity objects

Example: CrossEventArb flow:
    PolymarketSource(events)               # Source: fetch multi-outcome events
    → EventEnricher(prices)                # Enrich: sum outcome prices
    → StructuralAnalyzer()                 # Analyze: check price sum deviation
    → ArbScorer()                          # Score: deviation * liquidity * confidence
    → QualityFilter(min_deviation)         # Filter: min 3% mispricing
    → OpportunityOutput()                  # Output: ranked arb opportunities
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


# ── Pipeline Data Models ─────────────────────────────────────────────


class StrategyMode(str, Enum):
    SCAN = "scan"        # Periodic: fetch → analyze → output
    STREAM = "stream"    # Continuous: event bus → react → output
    HYBRID = "hybrid"    # Scan to find candidates, stream to time entries


class ActionType(str, Enum):
    OPEN = "open"
    CLOSE = "close"
    ADD = "add"
    STOP_LOSS = "stop_loss"


@dataclass
class MarketContext:
    """Enriched market data flowing through the pipeline.

    Built up stage by stage: Source sets basic fields,
    Enrich adds orderbook/history, Analyze adds features/signals.
    """
    # Source stage
    market_id: str = ""                     # condition_id
    market_question: str = ""
    event_slug: Optional[str] = None
    token_id: str = ""
    outcome_label: str = "Yes"
    source_platform: str = "polymarket"

    # Price data (from Source or Enrich)
    current_price: float = 0.0
    best_bid: float = 0.0
    best_ask: float = 0.0
    spread: float = 0.0
    volume_24h: float = 0.0
    liquidity: float = 0.0

    # Event-level data (for multi-outcome strategies)
    event_title: str = ""
    sibling_prices: list[float] = field(default_factory=list)
    sibling_labels: list[str] = field(default_factory=list)
    price_sum: float = 0.0

    # Enrichment stage
    price_history: list[dict] = field(default_factory=list)    # [{t, p}, ...]
    orderbook_depth: float = 0.0

    # Analysis stage
    features: dict = field(default_factory=dict)               # Feature vector
    signals: list[dict] = field(default_factory=dict)          # Detected signals

    # Metadata
    end_date: Optional[datetime] = None
    resolution_source: Optional[str] = None
    raw: Optional[dict] = None                                 # Original API response

    # Tags for agent/skill consumption
    theme: Optional[str] = None
    sector: Optional[str] = None
    related_tickers: list[str] = field(default_factory=list)


@dataclass
class Opportunity:
    """A scored, filtered trade opportunity — the strategy's output."""

    # Identity
    market_id: str
    market_question: str
    event_slug: Optional[str] = None
    token_id: str = ""
    outcome: str = "Yes"

    # Strategy context
    strategy: str = ""
    reasoning: str = ""

    # Trade parameters
    direction: str = "buy"
    entry_price: float = 0.0
    model_price: float = 0.0
    edge: float = 0.0
    confidence: float = 0.0
    horizon: str = "1d"

    # Market quality
    liquidity: float = 0.0
    volume_24h: float = 0.0
    spread: float = 0.0

    # Risk
    risk_flags: list[str] = field(default_factory=list)
    resolution_date: Optional[datetime] = None

    # Composite score
    score: float = 0.0

    # Context for agents/skills
    context: Optional[MarketContext] = field(default=None, repr=False)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def compute_score(self):
        liq_factor = min(1.0, self.liquidity / 100_000) if self.liquidity > 0 else 0.1
        spread_penalty = max(0.5, 1.0 - self.spread * 5)
        risk_penalty = max(0.3, 1.0 - len(self.risk_flags) * 0.15)
        self.score = abs(self.edge) * self.confidence * liq_factor * spread_penalty * risk_penalty
        return self.score

    def to_dict(self) -> dict:
        return {
            "market_id": self.market_id,
            "market_question": self.market_question,
            "event_slug": self.event_slug,
            "token_id": self.token_id[:30] + "..." if len(self.token_id) > 30 else self.token_id,
            "outcome": self.outcome,
            "strategy": self.strategy,
            "reasoning": self.reasoning,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "model_price": self.model_price,
            "edge": round(self.edge, 4),
            "confidence": round(self.confidence, 3),
            "liquidity": round(self.liquidity, 2),
            "spread": round(self.spread, 4),
            "risk_flags": self.risk_flags,
            "score": round(self.score, 6),
        }

    def to_skill_context(self) -> dict:
        """Format for agent skill consumption."""
        return {
            "opportunity": self.to_dict(),
            "related_tickers": self.context.related_tickers if self.context else [],
            "theme": self.context.theme if self.context else None,
            "features": self.context.features if self.context else {},
        }


@dataclass
class Action:
    """An instruction for an open position."""
    action_type: ActionType
    trade_id: str
    reason: str = ""
    exit_price: Optional[float] = None
    add_size_usd: Optional[float] = None
    opportunity: Optional[Opportunity] = None


# ── Pipeline Stages ──────────────────────────────────────────────────


class Source(ABC):
    """Fetches raw market data. First stage in the pipeline."""

    @abstractmethod
    async def fetch(self) -> list[MarketContext]:
        """Fetch market candidates to analyze."""
        ...


class Enricher(ABC):
    """Adds data to MarketContext (orderbook, history, theme mapping)."""

    @abstractmethod
    async def enrich(self, ctx: MarketContext) -> MarketContext:
        """Enrich a market context with additional data."""
        ...


class Analyzer(ABC):
    """Computes features, signals, or structural properties."""

    @abstractmethod
    def analyze(self, ctx: MarketContext) -> MarketContext:
        """Analyze and annotate a market context."""
        ...


class Scorer(ABC):
    """Converts analyzed context into scored Opportunities."""

    @abstractmethod
    def score(self, ctx: MarketContext) -> list[Opportunity]:
        """Score a context and produce zero or more opportunities."""
        ...


class Filter:
    """Removes low-quality opportunities based on configurable thresholds."""

    def __init__(
        self,
        min_edge: float = 0.03,
        min_confidence: float = 0.4,
        min_liquidity: float = 5000,
        max_spread: float = 0.10,
        min_score: float = 0.0001,
    ):
        self.min_edge = min_edge
        self.min_confidence = min_confidence
        self.min_liquidity = min_liquidity
        self.max_spread = max_spread
        self.min_score = min_score

    def filter(self, opps: list[Opportunity]) -> list[Opportunity]:
        result = []
        for opp in opps:
            if abs(opp.edge) < self.min_edge:
                continue
            if opp.confidence < self.min_confidence:
                continue
            if opp.liquidity < self.min_liquidity:
                continue
            if opp.spread > self.max_spread:
                continue
            opp.compute_score()
            if opp.score < self.min_score:
                continue
            result.append(opp)
        return sorted(result, key=lambda o: o.score, reverse=True)


# ── Strategy (Pipeline Composition) ──────────────────────────────────


class Strategy(ABC):
    """A composable pipeline: Source → Enrich → Analyze → Score → Filter → Output.

    Subclasses define which stages to use. The run() method executes the pipeline.
    Individual stages can be extracted and reused as agent skills.
    """

    name: str = "base"
    mode: StrategyMode = StrategyMode.SCAN
    scan_interval_seconds: int = 300

    @abstractmethod
    async def scan(self) -> list[Opportunity]:
        """Execute the full pipeline and return scored, filtered opportunities."""
        ...

    async def tick(self):
        """Unified output method: returns a list of Signals.

        Default implementation wraps legacy `scan()`. Strategies that have
        been migrated to the Signal model override this directly.
        """
        from farsight.markets.strategies.types import Signal
        return [Signal.from_opportunity(o) for o in await self.scan()]

    async def on_state_update(self, payload: dict):
        """Handle live state update (stream/hybrid mode)."""
        pass

    async def on_signal(self, payload: dict):
        """Handle generated signal (stream mode)."""
        pass

    async def monitor(self, open_positions: list[dict]) -> list[Action]:
        """Monitor open positions for actions."""
        return []

    # ── Skill extraction helpers ─────────────────────────────────────

    def get_source(self) -> Optional[Source]:
        """Return the source stage for reuse as an agent skill."""
        return None

    def get_analyzer(self) -> Optional[Analyzer]:
        """Return the analyzer stage for reuse as an agent skill."""
        return None
