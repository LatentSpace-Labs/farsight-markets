"""
SignalEngine — generates trading signals from feature vectors.

Subscribes to: derived.features
Publishes:     signal.generated

Applies signal detection rules, then passes candidates through a filter chain.
Signals that survive all filters are published and persisted.

Design for testability:
  - Each signal detector is a standalone function
  - Filter chain is a list of functions, each independently testable
  - evaluate() can be called directly without the bus
"""

import hashlib
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Callable, Optional
from uuid import uuid4

from farsight.markets.config import settings
from farsight.markets.engine.event_bus import EventBus
from farsight.markets.schemas.signals import (
    Direction,
    SignalEvidence,
    SignalSchema,
    SignalStatus,
    SignalType,
)


def dedup_hash(market_id: str | None, signal_type: str, direction: str, market_price: float) -> str:
    """Stable fingerprint used to squash duplicate emissions across restarts.

    Rounded to 2 decimal places so that a signal firing twice from the same
    conditions with slightly different prices still collapses.
    """
    payload = f"{market_id or ''}|{signal_type}|{direction}|{round(float(market_price or 0.0), 2)}"
    return hashlib.sha1(payload.encode()).hexdigest()[:16]

logger = logging.getLogger(__name__)


# ── Signal Detectors ─────────────────────────────────────────────────
# Each returns a SignalSchema or None. Pure functions of features dict.


def detect_probability_shock(features: dict, token_id: str, market_id: str | None) -> SignalSchema | None:
    """Detect large rapid probability movements.

    Trigger: |delta_5m| > threshold (default 5%)
    """
    delta = features.get("delta_5m")
    if delta is None:
        return None

    threshold = settings.SIGNAL_PROBABILITY_SHOCK_DELTA
    if abs(delta) < threshold:
        return None

    direction = Direction.BULLISH if delta > 0 else Direction.BEARISH
    magnitude = abs(delta)

    # Confidence based on magnitude and volume confirmation
    liq = features.get("liquidity_score", 0)
    trade_vel = features.get("trade_velocity", 0)
    confidence = min(1.0, (magnitude / 0.15) * 0.6 + liq * 0.2 + min(trade_vel / 5, 1) * 0.2)

    return _build_signal(
        token_id=token_id,
        market_id=market_id,
        signal_type=SignalType.PROBABILITY_SHOCK,
        direction=direction,
        confidence=confidence,
        horizon="1h",
        features=features,
        evidence=[
            SignalEvidence(source="delta_5m", description=f"5m price change: {delta:+.1%}", value=delta, weight=0.6),
            SignalEvidence(source="liquidity", description=f"Liquidity score: {liq:.2f}", value=liq, weight=0.2),
            SignalEvidence(source="trade_velocity", description=f"Trade velocity: {trade_vel:.1f}/min", value=trade_vel, weight=0.2),
        ],
    )


def detect_momentum_continuation(features: dict, token_id: str, market_id: str | None) -> SignalSchema | None:
    """Detect sustained directional drift with increasing velocity.

    Trigger: drift_score > 1.5 AND trade_velocity increasing
    """
    drift = features.get("drift_score")
    accel = features.get("acceleration")
    trade_vel = features.get("trade_velocity", 0)

    if drift is None or abs(drift) < 1.5:
        return None
    if trade_vel < 0.5:  # Need some minimum activity
        return None

    direction = Direction.BULLISH if drift > 0 else Direction.BEARISH
    confidence = min(1.0, abs(drift) / 3.0 * 0.5 + (0.3 if accel and abs(accel) > 0.001 else 0) + 0.2)

    return _build_signal(
        token_id=token_id,
        market_id=market_id,
        signal_type=SignalType.MOMENTUM_CONTINUATION,
        direction=direction,
        confidence=confidence,
        horizon="4h",
        features=features,
        evidence=[
            SignalEvidence(source="drift_score", description=f"Drift: {drift:.2f}σ", value=drift, weight=0.5),
            SignalEvidence(source="acceleration", description=f"Acceleration: {accel}", value=accel or 0, weight=0.3),
            SignalEvidence(source="trade_velocity", description=f"Trade velocity: {trade_vel:.1f}/min", value=trade_vel, weight=0.2),
        ],
    )


def detect_mean_reversion(features: dict, token_id: str, market_id: str | None) -> SignalSchema | None:
    """Detect price extended from VWAP with reversion potential.

    Trigger: |reversion_score| > 2σ (overextended from VWAP)
    """
    rev = features.get("reversion_score")
    if rev is None or abs(rev) < settings.SIGNAL_MEAN_REVERSION_SIGMA:
        return None

    # Reversion direction is OPPOSITE to the deviation
    direction = Direction.BEARISH if rev > 0 else Direction.BULLISH
    confidence = min(1.0, abs(rev) / 4.0 * 0.6 + features.get("liquidity_score", 0) * 0.4)

    return _build_signal(
        token_id=token_id,
        market_id=market_id,
        signal_type=SignalType.MEAN_REVERSION,
        direction=direction,
        confidence=confidence,
        horizon="4h",
        features=features,
        evidence=[
            SignalEvidence(source="reversion_score", description=f"VWAP deviation: {rev:.2f}σ", value=rev, weight=0.6),
            SignalEvidence(source="liquidity_score", description=f"Liquidity: {features.get('liquidity_score', 0):.2f}", value=features.get("liquidity_score", 0), weight=0.4),
        ],
    )


def detect_thematic_repricing(
    features: dict,
    token_id: str,
    market_id: str | None,
    sibling_deltas: list[float] | None = None,
) -> SignalSchema | None:
    """Detect coordinated movement across related markets.

    Trigger: 3+ sibling markets moving in the same direction
    Requires sibling_deltas to be passed in (cross-market context).
    """
    if sibling_deltas is None or len(sibling_deltas) < settings.SIGNAL_THEMATIC_MIN_BREADTH:
        return None

    # Check if enough siblings are moving in same direction
    positive = sum(1 for d in sibling_deltas if d > 0.01)
    negative = sum(1 for d in sibling_deltas if d < -0.01)

    if positive >= settings.SIGNAL_THEMATIC_MIN_BREADTH:
        direction = Direction.BULLISH
        breadth = positive
    elif negative >= settings.SIGNAL_THEMATIC_MIN_BREADTH:
        direction = Direction.BEARISH
        breadth = negative
    else:
        return None

    avg_magnitude = sum(abs(d) for d in sibling_deltas) / len(sibling_deltas)
    confidence = min(1.0, breadth / 5.0 * 0.5 + avg_magnitude / 0.10 * 0.5)

    return _build_signal(
        token_id=token_id,
        market_id=market_id,
        signal_type=SignalType.THEMATIC_REPRICING,
        direction=direction,
        confidence=confidence,
        horizon="1d",
        features=features,
        evidence=[
            SignalEvidence(source="theme_breadth", description=f"{breadth} markets moving together", value=float(breadth), weight=0.5),
            SignalEvidence(source="avg_magnitude", description=f"Avg move: {avg_magnitude:.1%}", value=avg_magnitude, weight=0.5),
        ],
    )


def detect_structural_inconsistency(
    features: dict,
    token_id: str,
    market_id: str | None,
    outcome_prices_sum: float | None = None,
) -> SignalSchema | None:
    """Detect when neg-risk outcome probabilities don't sum to ~1.0.

    Trigger: |sum - 1.0| > threshold (default 3%)
    Requires outcome_prices_sum from the parent event's markets.
    """
    if outcome_prices_sum is None:
        return None

    deviation = abs(outcome_prices_sum - 1.0)
    if deviation < settings.SIGNAL_STRUCTURAL_MAX_DEVIATION:
        return None

    direction = Direction.NEUTRAL  # Structural signals are about mispricing, not direction
    confidence = min(1.0, deviation / 0.10 * 0.7 + features.get("liquidity_score", 0) * 0.3)

    return _build_signal(
        token_id=token_id,
        market_id=market_id,
        signal_type=SignalType.STRUCTURAL_INCONSISTENCY,
        direction=direction,
        confidence=confidence,
        horizon="1d",
        features=features,
        evidence=[
            SignalEvidence(source="outcome_sum", description=f"Outcomes sum to {outcome_prices_sum:.1%} (should be ~100%)", value=outcome_prices_sum, weight=0.7),
            SignalEvidence(source="deviation", description=f"Deviation: {deviation:.1%}", value=deviation, weight=0.3),
        ],
    )


# ── Filter Chain ─────────────────────────────────────────────────────


class SignalFilter:
    """Evaluates a signal against the filter chain. Returns (pass, reason)."""

    def __init__(self):
        self._cooldowns: dict[str, datetime] = {}  # key: "{token_id}:{signal_type}" → last trigger time
        self._daily_count = 0
        self._daily_reset: datetime = datetime.utcnow()
        self._per_market_hourly: dict[str, list[datetime]] = defaultdict(list)
        self._dedup_hashes: set[str] = set()       # content hashes of recent emissions
        self._warmup_until: Optional[datetime] = None  # suppress all signals before this time

    def begin_warmup(self, seconds: int):
        """Suppress signal emissions for `seconds` from now. Called on boot."""
        self._warmup_until = datetime.utcnow() + timedelta(seconds=seconds)

    def is_in_warmup(self) -> bool:
        return self._warmup_until is not None and datetime.utcnow() < self._warmup_until

    def check(self, signal: SignalSchema) -> tuple[bool, str]:
        """Run signal through all filters. Returns (passed, rejection_reason)."""
        # Reset daily counter
        now = datetime.utcnow()
        if (now - self._daily_reset).total_seconds() > 86400:
            self._daily_count = 0
            self._daily_reset = now

        # 0. Warmup suppression (post-restart grace period while windows fill)
        if self.is_in_warmup():
            remaining = (self._warmup_until - now).total_seconds()
            return False, f"warmup: {remaining:.0f}s remaining"

        # 0b. Dedup: same content already emitted within the dedup window?
        sig_hash = dedup_hash(
            signal.market_id, signal.signal_type.value,
            signal.direction.value, signal.market_price,
        )
        if sig_hash in self._dedup_hashes:
            return False, "dedup: identical signal recently emitted"

        features = {}  # Features are in the signal evidence, extract what we need
        for ev in signal.evidence:
            if ev.source == "liquidity_score" or ev.source == "liquidity":
                features["liquidity_score"] = ev.value

        # 1. Confidence gate
        if signal.confidence < settings.FILTER_MIN_CONFIDENCE:
            return False, f"confidence {signal.confidence:.2f} < {settings.FILTER_MIN_CONFIDENCE}"

        # 2. Entry price gate
        if signal.market_price > settings.FILTER_MAX_ENTRY_PRICE:
            return False, f"market_price {signal.market_price:.2f} > {settings.FILTER_MAX_ENTRY_PRICE}"
        if signal.market_price < settings.FILTER_MIN_ENTRY_PRICE:
            return False, f"market_price {signal.market_price:.2f} < {settings.FILTER_MIN_ENTRY_PRICE}"

        # 3. Edge gate by horizon
        min_edges = settings.min_edge_by_horizon
        min_edge = min_edges.get(signal.horizon, 0.05)
        if abs(signal.edge) < min_edge:
            return False, f"edge {abs(signal.edge):.2%} < min {min_edge:.2%} for horizon {signal.horizon}"

        # 4. Cooldown
        cooldown_key = f"{signal.market_id}:{signal.signal_type.value}"
        last_trigger = self._cooldowns.get(cooldown_key)
        if last_trigger:
            elapsed = (now - last_trigger).total_seconds()
            if elapsed < settings.FILTER_COOLDOWN_MINUTES * 60:
                return False, f"cooldown: {settings.FILTER_COOLDOWN_MINUTES - elapsed / 60:.0f}min remaining"

        # 5. Daily circuit breaker
        if self._daily_count >= settings.FILTER_MAX_DAILY_SIGNALS:
            return False, f"daily limit reached: {self._daily_count}"

        # 6. Per-market hourly flood protection
        market_key = str(signal.market_id)
        hour_ago = now - timedelta(hours=1)
        self._per_market_hourly[market_key] = [
            t for t in self._per_market_hourly[market_key] if t > hour_ago
        ]
        if len(self._per_market_hourly[market_key]) >= settings.FILTER_MAX_SIGNALS_PER_MARKET_PER_HOUR:
            return False, f"market hourly limit: {len(self._per_market_hourly[market_key])}"

        return True, "passed"

    def record_emission(self, signal: SignalSchema):
        """Record that a signal was emitted (for cooldown/counter tracking)."""
        now = datetime.utcnow()
        cooldown_key = f"{signal.market_id}:{signal.signal_type.value}"
        self._cooldowns[cooldown_key] = now
        self._daily_count += 1
        market_key = str(signal.market_id)
        self._per_market_hourly[market_key].append(now)
        self._dedup_hashes.add(dedup_hash(
            signal.market_id, signal.signal_type.value,
            signal.direction.value, signal.market_price,
        ))

    def warm_start(self, recent_signals: list[dict]):
        """Rebuild cooldown + dedup state from persisted recent signals.

        Called on boot so that a restart doesn't re-emit signals that were
        already sent minutes earlier. `recent_signals` should be signals
        from the last FILTER_COOLDOWN_MINUTES * 2 (or more) window.
        """
        now = datetime.utcnow()
        cutoff = now - timedelta(minutes=settings.FILTER_COOLDOWN_MINUTES)
        for s in recent_signals:
            created_raw = s.get("created_at")
            if not created_raw:
                continue
            try:
                created = datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
                if created.tzinfo is not None:
                    created = created.replace(tzinfo=None)
            except ValueError:
                continue

            market_id = s.get("market_id") or s.get("token_id")
            stype = s.get("signal_type") or ""
            direction = s.get("direction") or ""
            price = float(s.get("market_price") or 0.0)

            # Dedup set: keep hashes for all signals in window (drives dedup suppression).
            self._dedup_hashes.add(dedup_hash(market_id, stype, direction, price))

            # Cooldowns: only the most recent per (market, type), and only if still active.
            if created >= cutoff:
                key = f"{market_id}:{stype}"
                prev = self._cooldowns.get(key)
                if prev is None or created > prev:
                    self._cooldowns[key] = created

    def reset(self):
        """Reset all filter state. Used in testing and replay."""
        self._cooldowns.clear()
        self._daily_count = 0
        self._daily_reset = datetime.utcnow()
        self._per_market_hourly.clear()
        self._dedup_hashes.clear()
        self._warmup_until = None


# ── Signal Engine ────────────────────────────────────────────────────


# All detector functions to run on every feature update
DETECTORS = [
    detect_probability_shock,
    detect_momentum_continuation,
    detect_mean_reversion,
]
# These require cross-market context, run separately:
# detect_thematic_repricing, detect_structural_inconsistency


class SignalEngine:
    """Generates trading signals from feature vectors.

    Runs all detectors on each feature update, filters candidates,
    and publishes survivors to the event bus.
    """

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        session_id: Optional[str] = None,
        on_emit: Optional[Callable[[dict], None]] = None,
    ):
        self._bus = event_bus
        self._filter = SignalFilter()
        self._signals_generated = 0
        self._signals_suppressed = 0
        self._active_signals: dict[str, SignalSchema] = {}  # id → signal
        self._session_id = session_id
        self._on_emit = on_emit  # e.g. store.save_signal or OutcomeTracker.on_signal_emitted

    def set_session_id(self, session_id: str | None):
        self._session_id = session_id

    def begin_warmup(self, seconds: int):
        self._filter.begin_warmup(seconds)

    def warm_start(self, recent_signals: list[dict]):
        self._filter.warm_start(recent_signals)

    def wire(self, bus: EventBus):
        self._bus = bus
        bus.subscribe("derived.features", self.on_features)

    async def on_features(self, payload: dict):
        """Handle a feature vector — run all detectors and filter."""
        token_id = payload.get("token_id", "")
        market_id = payload.get("market_id")
        features = payload.get("features", {})

        if not token_id or not features:
            return

        for detector in DETECTORS:
            signal = detector(features, token_id, market_id)
            if signal is None:
                continue

            passed, reason = self._filter.check(signal)
            if not passed:
                self._signals_suppressed += 1
                logger.debug(f"Signal suppressed ({signal.signal_type.value}): {reason}")
                continue

            # Signal passed all filters — emit
            self._filter.record_emission(signal)
            self._signals_generated += 1
            self._active_signals[str(signal.id)] = signal

            payload = signal.model_dump(mode="json")
            if self._session_id:
                payload["session_id"] = self._session_id

            if self._bus:
                await self._bus.publish("signal.generated", payload)
                logger.info(
                    f"Signal: {signal.signal_type.value} {signal.direction.value} "
                    f"conf={signal.confidence:.2f} edge={signal.edge:+.2%} "
                    f"token={token_id[:20]}..."
                )

            if self._on_emit is not None:
                try:
                    self._on_emit(payload)
                except Exception as e:
                    logger.warning(f"on_emit hook failed: {e}")

    def evaluate(self, features: dict, token_id: str, market_id: str | None = None) -> list[SignalSchema]:
        """Synchronous evaluation — run detectors without bus. For testing/API."""
        results = []
        for detector in DETECTORS:
            signal = detector(features, token_id, market_id)
            if signal is not None:
                results.append(signal)
        return results

    def get_active_signals(self) -> list[SignalSchema]:
        return list(self._active_signals.values())

    def get_health(self) -> dict:
        return {
            "signals_generated": self._signals_generated,
            "signals_suppressed": self._signals_suppressed,
            "active_signals": len(self._active_signals),
            "filter_daily_count": self._filter._daily_count,
            "in_warmup": self._filter.is_in_warmup(),
            "session_id": self._session_id,
        }

    def reset(self):
        """Reset all state. Used in testing and replay."""
        self._filter.reset()
        self._active_signals.clear()
        self._signals_generated = 0
        self._signals_suppressed = 0


# ── Helpers ──────────────────────────────────────────────────────────


def _build_signal(
    token_id: str,
    market_id: str | None,
    signal_type: SignalType,
    direction: Direction,
    confidence: float,
    horizon: str,
    features: dict,
    evidence: list[SignalEvidence],
) -> SignalSchema:
    """Build a signal with all required fields populated."""
    market_price = features.get("last_price", 0.5)
    # Simple model probability: market_price adjusted by feature direction
    delta_5m = features.get("delta_5m") or 0
    model_prob = market_price + delta_5m  # Naive — will be replaced by ensemble later

    liq = features.get("liquidity_score", 0.5)
    stale = features.get("stale_score", 0)
    tradability = max(0.0, liq * (1 - stale))

    risk_flags = []
    if liq < 0.3:
        risk_flags.append("low_liquidity")
    if stale > 0.5:
        risk_flags.append("stale_data")
    if features.get("manipulation_heuristic", 0) > 0.5:
        risk_flags.append("manipulation_risk")
    res_days = features.get("resolution_proximity_days", 999)
    if res_days < 1:
        risk_flags.append("near_resolution")

    return SignalSchema(
        id=uuid4(),
        market_id=market_id,
        source="polymarket",
        signal_type=signal_type,
        direction=direction,
        confidence=min(1.0, max(0.0, confidence)),
        horizon=horizon,
        tradability_score=tradability,
        evidence=evidence,
        risk_flags=risk_flags,
        status=SignalStatus.ACTIVE,
        model_probability=max(0.01, min(0.99, model_prob)),
        market_price=market_price,
        edge=model_prob - market_price,
        feature_set_version=settings.FEATURE_SET_VERSION,
        rule_version=settings.RULE_VERSION,
    )
