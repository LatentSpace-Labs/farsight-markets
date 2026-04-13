"""
Event Markets Platform configuration.

Standalone settings class — no external dependencies beyond pydantic-settings.
All thresholds are env-var overridable for tuning without code changes.
"""

import logging
from typing import Optional

from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class MarketsPlatformSettings(BaseSettings):
    """Configuration for the Event Markets Intelligence Platform."""

    model_config = {"env_prefix": "PM_", "env_file": ".env", "extra": "ignore"}

    # ── Polymarket API ───────────────────────────────────────────────
    POLYMARKET_GAMMA_URL: str = "https://gamma-api.polymarket.com"
    POLYMARKET_CLOB_URL: str = "https://clob.polymarket.com"
    POLYMARKET_DATA_URL: str = "https://data-api.polymarket.com"
    POLYMARKET_WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # ── Kalshi API ───────────────────────────────────────────────────
    KALSHI_API_URL: str = "https://trading-api.kalshi.com/trade-api/v2"
    KALSHI_DEMO_URL: str = "https://demo-api.kalshi.co/trade-api/v2"
    KALSHI_WS_URL: str = "wss://trading-api.kalshi.com/trade-api/v2"
    KALSHI_API_KEY: Optional[str] = None

    # ── Goldsky Subgraphs ────────────────────────────────────────────
    GOLDSKY_BASE_URL: str = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs"
    GOLDSKY_POSITIONS_VERSION: str = "0.0.7"
    GOLDSKY_ACTIVITY_VERSION: str = "0.0.4"
    GOLDSKY_PNL_VERSION: str = "0.0.14"

    # ── Catalog ──────────────────────────────────────────────────────
    CATALOG_SYNC_INTERVAL_SECONDS: int = 300  # 5 minutes
    CATALOG_MAX_MARKETS: int = 5000

    # ── Market Tiering ───────────────────────────────────────────────
    TIER1_MIN_VOLUME_USD: float = 500_000
    TIER1_MIN_LIQUIDITY_USD: float = 50_000
    TIER2_MIN_VOLUME_USD: float = 50_000
    TIER3_MIN_VOLUME_USD: float = 0  # All active markets

    # ── Ingestion ────────────────────────────────────────────────────
    WS_RECONNECT_DELAY_INITIAL: float = 1.0  # seconds
    WS_RECONNECT_DELAY_MAX: float = 60.0
    WS_MAX_SUBSCRIPTIONS: int = 500  # Max token IDs per WS connection
    CACHE_TTL_SECONDS: int = 30  # TTL for REST fallback cache
    GAP_FILL_LOOKBACK_MINUTES: int = 30
    RECONCILIATION_INTERVAL_SECONDS: int = 900  # 15 minutes

    # ── State Engine ─────────────────────────────────────────────────
    STATE_SNAPSHOT_INTERVAL_SECONDS: int = 300  # 5 minutes
    ROLLING_WINDOW_MAX_ENTRIES: int = 10_000

    # ── Feature Engine ───────────────────────────────────────────────
    FEATURE_SET_VERSION: str = "v1"

    # ── Signal Engine ────────────────────────────────────────────────
    RULE_VERSION: str = "v1"

    # Signal thresholds
    SIGNAL_PROBABILITY_SHOCK_DELTA: float = 0.05  # 5% move in 5 minutes
    SIGNAL_MOMENTUM_MIN_DRIFT_MINUTES: int = 30
    SIGNAL_MEAN_REVERSION_SIGMA: float = 2.0
    SIGNAL_THEMATIC_MIN_BREADTH: int = 3  # Minimum markets moving together
    SIGNAL_STRUCTURAL_MAX_DEVIATION: float = 0.03  # 3% from sum = 1.0
    SIGNAL_CROSS_VENUE_MIN_SPREAD: float = 0.03  # 3% Polymarket-Kalshi spread

    # Filter chain
    FILTER_COOLDOWN_MINUTES: int = 30
    FILTER_MIN_LIQUIDITY_SCORE: float = 0.3
    FILTER_MIN_CONFIDENCE: float = 0.4
    FILTER_MAX_RESOLUTION_PROXIMITY_DAYS: int = 1
    FILTER_DEDUP_WINDOW_HOURS: int = 4
    FILTER_MAX_SIGNALS_PER_MARKET_PER_HOUR: int = 5
    FILTER_MIN_TRADABILITY: float = 0.2
    FILTER_MIN_CONVERGENCE: int = 2
    FILTER_MAX_ENTRY_PRICE: float = 0.90
    FILTER_MIN_ENTRY_PRICE: float = 0.10
    FILTER_MAX_DAILY_SIGNALS: int = 100
    FILTER_MAX_SIGNALS_PER_THEME_PER_HOUR: int = 10

    # Edge thresholds by horizon
    FILTER_MIN_EDGE_1H: float = 0.03
    FILTER_MIN_EDGE_4H: float = 0.05
    FILTER_MIN_EDGE_1D: float = 0.08
    FILTER_MIN_EDGE_1W: float = 0.12

    # ── Paper Trading ────────────────────────────────────────────────
    PAPER_STARTING_BALANCE: float = 10_000.0
    PAPER_DEFAULT_KELLY_FRACTION: float = 0.15
    PAPER_DEFAULT_MAX_POSITION_PCT: float = 5.0
    PAPER_DEFAULT_MAX_DAILY_LOSS: float = 500.0
    PAPER_DEFAULT_MAX_OPEN_POSITIONS: int = 20

    # ── Session + outcome tracking (Phase 0) ─────────────────────────
    WARMUP_SECONDS: int = 300                    # Suppress signals for first N seconds after boot
    COOLDOWN_WARMSTART_HOURS: int = 2            # Load signals from this window to rebuild cooldowns/dedup
    OUTCOME_CAPTURE_INTERVAL_SECONDS: int = 300  # How often to scan for pending outcome captures
    RESOLUTION_POLL_INTERVAL_MINUTES: int = 60   # How often to poll Gamma for newly-resolved markets
    OUTCOME_TRACKER_ENABLED: bool = True

    # ── Observability ────────────────────────────────────────────────
    SOURCE_LAG_ALERT_SECONDS: int = 120  # Alert if no WS message for 2 min
    END_TO_END_LATENCY_WARN_MS: int = 10_000  # 10 seconds
    SIGNAL_FLOOD_THRESHOLD_PER_HOUR: int = 50  # Per market

    @property
    def min_edge_by_horizon(self) -> dict[str, float]:
        return {
            "1h": self.FILTER_MIN_EDGE_1H,
            "4h": self.FILTER_MIN_EDGE_4H,
            "1d": self.FILTER_MIN_EDGE_1D,
            "1w": self.FILTER_MIN_EDGE_1W,
        }

    @property
    def goldsky_positions_url(self) -> str:
        return f"{self.GOLDSKY_BASE_URL}/positions-subgraph/{self.GOLDSKY_POSITIONS_VERSION}/gn"

    @property
    def goldsky_activity_url(self) -> str:
        return f"{self.GOLDSKY_BASE_URL}/activity-subgraph/{self.GOLDSKY_ACTIVITY_VERSION}/gn"

    @property
    def goldsky_pnl_url(self) -> str:
        return f"{self.GOLDSKY_BASE_URL}/pnl-subgraph/{self.GOLDSKY_PNL_VERSION}/gn"


# Singleton — import this from other modules
settings = MarketsPlatformSettings()
