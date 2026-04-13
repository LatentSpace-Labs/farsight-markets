"""
MomentumTracker — streaming strategy that reacts to live market data.

Unlike scan strategies that fetch-analyze-return periodically, this strategy
is WIRED to the event bus and reacts to every state update in real-time.

Flow:
  WebSocket tick arrives
    → StateEngine updates rolling windows
      → FeatureEngine computes features
        → MomentumTracker.on_state_update() fires
          → Checks if momentum is building across multiple timeframes
          → If threshold crossed, emits an Opportunity

Use cases:
  - Detect sustained drift WHILE it's happening (not after a 5-min scan)
  - Catch sudden volume surges in real-time
  - Track RSI crossing 70/30 boundaries live
  - Alert when Bollinger bands squeeze then break

This is the strategy that makes the streaming pipeline valuable.
Without it, we're just scan-polling every 5 minutes.
"""

import logging
import time
from datetime import datetime
from typing import Optional

from farsight.markets.services.feature_engine import compute_features
from farsight.markets.services.state_engine import MarketState, StateEngine
from farsight.markets.strategies.base import (
    Action,
    ActionType,
    Opportunity,
    Strategy,
    StrategyMode,
)

logger = logging.getLogger(__name__)


from typing import Literal as _Literal
from pydantic import BaseModel as _BaseModel, Field as _Field
from farsight.markets.strategies.config import StrategyConfig


class MomentumParams(_BaseModel):
    min_momentum: float = 0.3
    min_volume_ratio: float = 1.5
    cooldown_seconds: int = 300


class MomentumConfig(StrategyConfig):
    name: _Literal["momentum"] = "momentum"
    params: MomentumParams = _Field(default_factory=MomentumParams)


class MomentumTracker(Strategy):
    """Real-time momentum detection via streaming state updates.

    Wired to the event bus — on_state_update fires on every price tick.
    Maintains per-token tracking state to detect momentum building.

    Detects:
    1. Multi-timeframe momentum alignment (5m + 15m + 1h all same direction)
    2. RSI crossings (entering overbought/oversold territory)
    3. Volume surge with price confirmation
    4. Sustained drift acceleration (momentum getting stronger)
    """

    name = "momentum"
    mode = StrategyMode.STREAM
    scan_interval_seconds = 999999  # Not used — stream mode

    def __init__(
        self,
        state_engine: Optional[StateEngine] = None,
        min_momentum: float = 0.3,         # Minimum composite momentum score
        min_volume_ratio: float = 1.5,     # Minimum volume vs average
        cooldown_seconds: int = 300,        # 5 min between signals per token
        config: Optional["MomentumConfig"] = None,
    ):
        if config is not None:
            min_momentum = config.params.min_momentum
            min_volume_ratio = config.params.min_volume_ratio
            cooldown_seconds = config.params.cooldown_seconds
        self.config = config
        self._state_engine = state_engine
        self.min_momentum = min_momentum
        self.min_volume_ratio = min_volume_ratio
        self.cooldown_seconds = cooldown_seconds

        # Per-token tracking
        self._last_signal_time: dict[str, float] = {}
        self._prev_rsi: dict[str, float] = {}

        # Collected opportunities (consumed by runner on next cycle)
        self.pending_opportunities: list[Opportunity] = []

        # Stats
        self.updates_processed = 0
        self.opportunities_found = 0

    async def scan(self) -> list[Opportunity]:
        """Return and clear any opportunities found since last call."""
        opps = list(self.pending_opportunities)
        self.pending_opportunities.clear()
        return opps

    async def on_state_update(self, payload: dict):
        """React to a live state update from the streaming pipeline.

        This fires on EVERY price tick for EVERY tracked token.
        Must be fast — heavy computation per tick kills performance.
        """
        self.updates_processed += 1

        token_id = payload.get("token_id", "")
        if not token_id or not self._state_engine:
            return

        # Cooldown check (per token)
        now = time.time()
        last = self._last_signal_time.get(token_id, 0)
        if now - last < self.cooldown_seconds:
            return

        state = self._state_engine.get_state(token_id)
        if state is None:
            return

        # Need enough data for meaningful analysis
        if state.prices_5m.count() < 10 or state.prices_1h.count() < 20:
            return

        # Compute features (lightweight — ~0.1ms)
        features = compute_features(state)

        # Run detectors
        opp = self._detect_momentum_alignment(token_id, state, features)
        if opp:
            self._emit(token_id, opp, now)
            return

        opp = self._detect_rsi_crossing(token_id, state, features)
        if opp:
            self._emit(token_id, opp, now)
            return

        opp = self._detect_volume_surge(token_id, state, features)
        if opp:
            self._emit(token_id, opp, now)
            return

    def _emit(self, token_id: str, opp: Opportunity, now: float):
        """Record an opportunity and update cooldown."""
        self._last_signal_time[token_id] = now
        self.pending_opportunities.append(opp)
        self.opportunities_found += 1
        logger.info(f"MomentumTracker: {opp.reasoning[:60]}")

    # ── Detectors ────────────────────────────────────────────────────

    def _detect_momentum_alignment(
        self, token_id: str, state: MarketState, features: dict,
    ) -> Optional[Opportunity]:
        """Detect when multiple timeframes agree on direction.

        Strongest signal: 5m, 15m, and 1h all moving the same way
        with increasing magnitude at shorter timeframes (acceleration).
        """
        d5m = features.get("delta_5m")
        d15m = features.get("delta_15m")
        d1h = features.get("delta_1h")
        momentum = features.get("momentum_score")

        if d5m is None or d15m is None or d1h is None or momentum is None:
            return None

        if abs(momentum) < self.min_momentum:
            return None

        # All three timeframes same direction?
        all_positive = d5m > 0.005 and d15m > 0.005 and d1h > 0.005
        all_negative = d5m < -0.005 and d15m < -0.005 and d1h < -0.005

        if not (all_positive or all_negative):
            return None

        # Acceleration: shorter timeframes moving faster than longer
        accelerating = abs(d5m) > abs(d15m / 3)  # 5m delta > proportional 15m

        direction = "buy" if all_positive else "sell"
        edge = abs(momentum) * 0.03
        confidence = min(0.75, 0.4 + abs(momentum) * 0.3)
        if accelerating:
            confidence += 0.1

        return Opportunity(
            market_id=token_id,
            market_question=self._get_question(token_id),
            token_id=token_id,
            strategy=self.name,
            reasoning=(
                f"Multi-timeframe momentum: "
                f"5m={d5m:+.1%}, 15m={d15m:+.1%}, 1h={d1h:+.1%}"
                f"{' (accelerating)' if accelerating else ''}"
            ),
            direction=direction,
            entry_price=state.last_price,
            model_price=state.last_price + (momentum * 0.03),
            edge=edge,
            confidence=confidence,
            liquidity=state.bid_depth + state.ask_depth,
            spread=state.spread,
            risk_flags=["streaming", "momentum"],
        )

    def _detect_rsi_crossing(
        self, token_id: str, state: MarketState, features: dict,
    ) -> Optional[Opportunity]:
        """Detect RSI crossing 70 (overbought) or 30 (oversold) boundaries.

        Only fires on the CROSSING — not when RSI is already extreme.
        """
        rsi = features.get("rsi_1h")
        if rsi is None:
            return None

        prev_rsi = self._prev_rsi.get(token_id)
        self._prev_rsi[token_id] = rsi

        if prev_rsi is None:
            return None

        # Crossing INTO overbought territory
        if prev_rsi < 70 and rsi >= 70:
            return Opportunity(
                market_id=token_id,
                market_question=self._get_question(token_id),
                token_id=token_id,
                strategy=self.name,
                reasoning=f"RSI crossed above 70 ({prev_rsi:.0f} → {rsi:.0f}) — entering overbought",
                direction="sell",
                entry_price=state.last_price,
                model_price=state.last_price - 0.02,
                edge=0.02,
                confidence=0.5,
                liquidity=state.bid_depth + state.ask_depth,
                spread=state.spread,
                risk_flags=["streaming", "rsi_crossing"],
            )

        # Crossing INTO oversold territory
        if prev_rsi > 30 and rsi <= 30:
            return Opportunity(
                market_id=token_id,
                market_question=self._get_question(token_id),
                token_id=token_id,
                strategy=self.name,
                reasoning=f"RSI crossed below 30 ({prev_rsi:.0f} → {rsi:.0f}) — entering oversold",
                direction="buy",
                entry_price=state.last_price,
                model_price=state.last_price + 0.02,
                edge=0.02,
                confidence=0.5,
                liquidity=state.bid_depth + state.ask_depth,
                spread=state.spread,
                risk_flags=["streaming", "rsi_crossing"],
            )

        return None

    def _detect_volume_surge(
        self, token_id: str, state: MarketState, features: dict,
    ) -> Optional[Opportunity]:
        """Detect sudden volume surge with price confirmation.

        Volume > 3x normal AND momentum aligned = informed trading.
        """
        vol_ratio = features.get("volume_ratio")
        momentum = features.get("momentum_score")

        if vol_ratio is None or momentum is None:
            return None

        if vol_ratio < 3.0 or abs(momentum) < 0.2:
            return None

        direction = "buy" if momentum > 0 else "sell"
        edge = abs(momentum) * 0.02 * min(vol_ratio / 5, 1.5)

        return Opportunity(
            market_id=token_id,
            market_question=self._get_question(token_id),
            token_id=token_id,
            strategy=self.name,
            reasoning=f"Volume surge: {vol_ratio:.1f}x normal with {momentum:+.2f} momentum",
            direction=direction,
            entry_price=state.last_price,
            model_price=state.last_price + (momentum * 0.02),
            edge=edge,
            confidence=min(0.7, 0.4 + vol_ratio / 20),
            liquidity=state.bid_depth + state.ask_depth,
            spread=state.spread,
            risk_flags=["streaming", "volume_surge"],
        )

    def _get_question(self, token_id: str) -> str:
        """Get the market question for a token (from runner's mapping)."""
        # This will be set by the runner when it wires the strategy
        return getattr(self, "_token_questions", {}).get(token_id, token_id[:30] + "...")

    async def monitor(self, open_positions: list[dict]) -> list[Action]:
        """Monitor momentum positions — exit when momentum fades."""
        if not self._state_engine:
            return []

        actions = []
        for pos in open_positions:
            if pos.get("strategy") != self.name:
                continue

            token_id = pos.get("token_id", "")
            state = self._state_engine.get_state(token_id)
            if not state:
                continue

            features = compute_features(state)
            momentum = features.get("momentum_score")
            entry = pos.get("entry_price", 0)

            # Exit if momentum reversed
            if momentum is not None:
                was_buy = pos.get("direction", "").upper() == "BUY"
                if (was_buy and momentum < -0.1) or (not was_buy and momentum > 0.1):
                    actions.append(Action(
                        action_type=ActionType.CLOSE,
                        trade_id=pos["id"],
                        reason=f"Momentum reversed: {momentum:+.2f}",
                        exit_price=state.last_price,
                    ))

            # Stop loss: 3% adverse move
            if entry > 0 and abs(state.last_price - entry) > 0.03:
                if (pos.get("direction", "").upper() == "BUY" and state.last_price < entry - 0.03) or \
                   (pos.get("direction", "").upper() == "SELL" and state.last_price > entry + 0.03):
                    actions.append(Action(
                        action_type=ActionType.STOP_LOSS,
                        trade_id=pos["id"],
                        reason=f"Stop loss: price moved 3%+ against position",
                        exit_price=state.last_price,
                    ))

        return actions
