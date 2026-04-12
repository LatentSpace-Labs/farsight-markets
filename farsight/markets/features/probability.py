"""
Probability dynamics features — how is the probability moving.

These capture momentum, acceleration, mean-reversion tendency,
and volatility regime shifts in prediction market probabilities.
"""

from farsight.markets.services.state_engine import MarketState


def delta_1m(state: MarketState) -> float | None:
    """Price change over last 1 minute."""
    return state.prices_1m.delta()


def delta_5m(state: MarketState) -> float | None:
    """Price change over last 5 minutes. Primary shock detector."""
    return state.prices_5m.delta()


def delta_15m(state: MarketState) -> float | None:
    """Price change over last 15 minutes."""
    return state.prices_15m.delta()


def delta_1h(state: MarketState) -> float | None:
    """Price change over last 1 hour."""
    return state.prices_1h.delta()


def delta_4h(state: MarketState) -> float | None:
    """Price change over last 4 hours."""
    return state.prices_4h.delta()


def acceleration(state: MarketState) -> float | None:
    """Second derivative of probability — is the rate of change increasing?

    Computed as delta_5m - delta_15m (normalized to same timeframe).
    Positive = accelerating in current direction.
    """
    d5 = state.prices_5m.delta()
    d15 = state.prices_15m.delta()
    if d5 is None or d15 is None:
        return None
    # Normalize 15m delta to 5m equivalent
    d15_per_5m = d15 / 3.0
    return d5 - d15_per_5m


def drift_score(state: MarketState) -> float | None:
    """Sustained directional movement score.

    Measures whether the 1h window shows a consistent trend (not just noise).
    Range roughly [-1, 1]. High absolute value = strong sustained drift.
    """
    d1h = state.prices_1h.delta()
    std = state.prices_1h.std()
    if d1h is None or std is None or std <= 0:
        return None
    # Drift = delta / volatility (like a signal-to-noise ratio)
    return max(-3.0, min(3.0, d1h / std))


def reversion_score(state: MarketState) -> float | None:
    """Deviation from 1h VWAP in standard deviations.

    Large positive = price is above VWAP (potential reversion down).
    Large negative = price is below VWAP (potential reversion up).
    """
    vwap = state.trades_1h.vwap()
    std = state.prices_1h.std()
    if vwap is None or std is None or std <= 0:
        return None
    return (state.last_price - vwap) / std


def volatility_burst(state: MarketState) -> float | None:
    """Realized volatility ratio: recent vs baseline.

    Compares 5m realized vol to 1h realized vol.
    Value > 2.0 = volatility burst (2x normal).
    """
    vol_5m = state.prices_5m.std()
    vol_1h = state.prices_1h.std()
    if vol_5m is None or vol_1h is None or vol_1h <= 0:
        return None
    return vol_5m / vol_1h


def compute_all(state: MarketState) -> dict[str, float | None]:
    """Compute all probability dynamics features."""
    return {
        "delta_1m": delta_1m(state),
        "delta_5m": delta_5m(state),
        "delta_15m": delta_15m(state),
        "delta_1h": delta_1h(state),
        "delta_4h": delta_4h(state),
        "acceleration": acceleration(state),
        "drift_score": drift_score(state),
        "reversion_score": reversion_score(state),
        "volatility_burst": volatility_burst(state),
    }
