"""
SessionService — tracks a single bot run from start() to stop().

A "session" bounds one invocation of the runner. Everything emitted during
that run (signals, paper trades) is stamped with the session_id for later
attribution ("which version of the bot produced this signal?").

Design notes:
  - Pure bookkeeping. No domain logic.
  - The session_id is generated once per run and exposed as a property.
  - config_hash is computed from the Settings object so we can tell two
    runs apart even on the same code revision.
"""

import hashlib
import json
import logging
from datetime import datetime
from typing import Optional
from uuid import uuid4

from farsight.markets.config import MarketsPlatformSettings as Settings
from farsight.markets.store import LocalStore

logger = logging.getLogger(__name__)


class SessionService:
    def __init__(self, store: LocalStore):
        self.store = store
        self.session_id: Optional[str] = None
        self.started_at: Optional[datetime] = None
        self._counts = {
            "events_processed": 0,
            "signals_emitted": 0,
            "signals_suppressed": 0,
            "trades_opened": 0,
            "errors": 0,
        }

    def start(self, settings: Settings, strategies: list[str], auto_trade: bool) -> str:
        self.session_id = str(uuid4())
        self.started_at = datetime.utcnow()
        self.store.start_session(
            session_id=self.session_id,
            config_hash=self._config_hash(settings),
            strategies=",".join(strategies),
            auto_trade=auto_trade,
        )
        logger.info(f"Session {self.session_id[:8]} started (strategies={strategies}, auto_trade={auto_trade})")
        return self.session_id

    def end(self):
        if not self.session_id:
            return
        self.store.end_session(self.session_id, counts=self._counts)
        logger.info(f"Session {self.session_id[:8]} ended | {self._counts}")

    def increment(self, key: str, n: int = 1):
        if key in self._counts:
            self._counts[key] += n

    @property
    def counts(self) -> dict:
        return dict(self._counts)

    @staticmethod
    def _config_hash(settings: Settings) -> str:
        """Hash the subset of settings that actually affect bot behavior.

        Kept narrow on purpose: paths, URLs, and infra knobs don't count; only
        detector thresholds, filter gates, and versions do.
        """
        keys = [
            "FEATURE_SET_VERSION", "RULE_VERSION",
            "SIGNAL_PROBABILITY_SHOCK_DELTA", "SIGNAL_MEAN_REVERSION_SIGMA",
            "SIGNAL_THEMATIC_MIN_BREADTH", "SIGNAL_STRUCTURAL_MAX_DEVIATION",
            "SIGNAL_CROSS_VENUE_MIN_SPREAD",
            "FILTER_COOLDOWN_MINUTES", "FILTER_MIN_CONFIDENCE",
            "FILTER_MAX_ENTRY_PRICE", "FILTER_MIN_ENTRY_PRICE",
            "FILTER_MAX_DAILY_SIGNALS", "FILTER_MAX_SIGNALS_PER_MARKET_PER_HOUR",
            "FILTER_MIN_EDGE_1H", "FILTER_MIN_EDGE_4H",
            "FILTER_MIN_EDGE_1D", "FILTER_MIN_EDGE_1W",
            "WARMUP_SECONDS",
        ]
        payload = {k: getattr(settings, k, None) for k in keys}
        return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:16]
