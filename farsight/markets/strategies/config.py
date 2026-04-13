"""Strategy configuration — base types shared across strategies.

Each strategy subclasses `StrategyConfig` with its own `params`. Config
lives in `config/strategies/<name>.yaml`; Pydantic provides type validation
and fallback defaults if the file is missing.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from farsight.markets.strategies.base import StrategyMode


class ScopeConfig(BaseModel):
    """What universe of markets/events the strategy looks at."""

    market_slugs: list[str] = []
    condition_ids: list[str] = []
    event_slugs: list[str] = []
    tag_slugs: list[str] = []
    expand_related_tags: bool = False
    categories: list[str] = []


class ThresholdsConfig(BaseModel):
    min_edge: float = 0.03
    min_confidence: float = 0.40
    min_liquidity: float = 5_000
    min_volume_24h: float = 0
    max_spread: float = 0.10


class RiskConfig(BaseModel):
    max_position_usd: float = 500
    max_positions: int = 10
    stop_loss_pct: float = 0.05
    take_profit_price: float = 0.97
    kelly_fraction: float = 0.25


class SchedulingConfig(BaseModel):
    scan_interval_seconds: int = 900
    mode: StrategyMode = StrategyMode.HYBRID


CONFIG_DIR = Path(__file__).resolve().parents[3] / "config" / "strategies"


class StrategyConfig(BaseModel):
    """Base config shared by every strategy."""

    name: str
    enabled: bool = True
    scope: ScopeConfig = Field(default_factory=ScopeConfig)
    thresholds: ThresholdsConfig = Field(default_factory=ThresholdsConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    scheduling: SchedulingConfig = Field(default_factory=SchedulingConfig)

    @classmethod
    def load(cls, strategy_name: str) -> "StrategyConfig":
        """Load `config/strategies/<name>.yaml`, falling back to defaults."""
        path = CONFIG_DIR / f"{strategy_name}.yaml"
        if not path.is_file():
            return cls()
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return cls.model_validate(data)
