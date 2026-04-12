"""
Technical indicator features — adapted for prediction markets (0-1 price range).

Traditional technicals like RSI, Bollinger Bands, and volume analysis
are reinterpreted for binary outcome markets:
  - RSI 30/70 means overbought/oversold probability
  - Bollinger squeeze means probability is consolidating before a move
  - Volume surge means informed trading is happening

All functions are pure: MarketState in → float out.
"""

import math
from farsight.markets.services.state_engine import MarketState


# ── RSI ──────────────────────────────────────────────────────────────


def rsi(state: MarketState, window_attr: str = "prices_1h") -> float | None:
    """Relative Strength Index adapted for prediction markets.

    Measures momentum by comparing average gains vs average losses
    over the rolling window. Range [0, 100].

    Interpretation for prediction markets:
      RSI > 70: Probability has been rising fast — potential overreaction
      RSI < 30: Probability has been falling fast — potential overreaction
      RSI 40-60: Neutral momentum
    """
    window = getattr(state, window_attr, None)
    if window is None:
        return None

    values = window.values
    if len(values) < 5:
        return None

    gains = []
    losses = []
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        if change > 0:
            gains.append(change)
        elif change < 0:
            losses.append(abs(change))

    if not gains and not losses:
        return 50.0  # No movement

    avg_gain = sum(gains) / len(values) if gains else 0.0001
    avg_loss = sum(losses) / len(values) if losses else 0.0001

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def rsi_1h(state: MarketState) -> float | None:
    """RSI over the 1-hour window."""
    return rsi(state, "prices_1h")


def rsi_4h(state: MarketState) -> float | None:
    """RSI over the 4-hour window."""
    return rsi(state, "prices_4h")


# ── Bollinger Bands ──────────────────────────────────────────────────


def bollinger_position(state: MarketState) -> float | None:
    """Where is the current price relative to 1h Bollinger Bands?

    Returns position as fraction: 0.0 = at lower band, 1.0 = at upper band.
    Values > 1.0 or < 0.0 mean price is outside the bands.

    Interpretation:
      > 0.8: Near upper band — probability may be overextended upward
      < 0.2: Near lower band — probability may be overextended downward
      ~0.5: At the middle — no directional signal
    """
    mean = state.prices_1h.mean()
    std = state.prices_1h.std()
    if mean is None or std is None or std <= 0:
        return None

    upper = mean + 2 * std
    lower = mean - 2 * std
    band_width = upper - lower

    if band_width <= 0:
        return 0.5

    return (state.last_price - lower) / band_width


def bollinger_width(state: MarketState) -> float | None:
    """Bollinger Band width — measures volatility regime.

    Narrow bands (squeeze) = low volatility, breakout likely.
    Wide bands = high volatility, trend in progress.

    Returns width as fraction of mean price.
    """
    mean = state.prices_1h.mean()
    std = state.prices_1h.std()
    if mean is None or std is None or mean <= 0:
        return None

    return (4 * std) / mean  # Full band width (2 std each side) / mean


# ── Volume Analysis ──────────────────────────────────────────────────


def volume_ratio(state: MarketState) -> float | None:
    """Ratio of 5-minute volume to 1-hour average (per 5 minutes).

    > 2.0: Volume surge — 2x normal activity in the last 5 minutes
    > 5.0: Major volume spike — something is happening
    ~1.0: Normal activity

    Volume surges often precede or accompany significant price moves.
    """
    vol_5m = state.volume_5m.sum()
    vol_1h = state.volume_1h.sum()

    if vol_1h <= 0:
        return None

    # Normalize 1h to per-5-minute average
    avg_5m = vol_1h / 12.0  # 12 five-minute periods in an hour

    if avg_5m <= 0:
        return None

    return vol_5m / avg_5m


def volume_price_divergence(state: MarketState) -> float | None:
    """Detects when volume and price disagree.

    Price rising + volume falling = weak rally (bearish divergence)
    Price falling + volume rising = climactic selling (potential bottom)
    Price rising + volume rising = strong rally (bullish confirmation)

    Returns [-1, 1]:
      Positive = volume confirms price direction (healthy)
      Negative = volume diverges from price (suspicious)
    """
    delta = state.prices_1h.delta()
    if delta is None:
        return None

    vol_5m = state.volume_5m.sum()
    vol_1h = state.volume_1h.sum()
    if vol_1h <= 0:
        return None

    vol_ratio = vol_5m / (vol_1h / 12.0) if vol_1h > 0 else 1.0

    # Price going up with rising volume = confirming (+)
    # Price going up with falling volume = diverging (-)
    price_direction = 1.0 if delta > 0 else -1.0 if delta < 0 else 0.0
    volume_direction = 1.0 if vol_ratio > 1.2 else -1.0 if vol_ratio < 0.8 else 0.0

    if price_direction == 0:
        return 0.0

    return max(-1.0, min(1.0, price_direction * volume_direction))


# ── Momentum Oscillators ─────────────────────────────────────────────


def rate_of_change(state: MarketState) -> float | None:
    """Rate of change: current price vs N periods ago, as percentage.

    Uses 1h window. Positive = price higher than 1h ago.
    """
    first = state.prices_1h.first()
    if first is None or first <= 0:
        return None
    return (state.last_price - first) / first


def momentum_score(state: MarketState) -> float | None:
    """Composite momentum score combining multiple timeframes.

    Weights: 5m (30%) + 15m (30%) + 1h (40%)
    Range roughly [-1, 1]. Positive = bullish momentum.
    """
    d5 = state.prices_5m.delta()
    d15 = state.prices_15m.delta()
    d1h = state.prices_1h.delta()

    if d5 is None and d15 is None and d1h is None:
        return None

    # Normalize each delta: 5% move = full signal
    def norm(d, max_d):
        if d is None:
            return 0.0
        return max(-1.0, min(1.0, d / max_d))

    score = (
        norm(d5, 0.03) * 0.3 +
        norm(d15, 0.05) * 0.3 +
        norm(d1h, 0.08) * 0.4
    )
    return score


# ── Price Levels ─────────────────────────────────────────────────────


def distance_from_midpoint(state: MarketState) -> float:
    """Distance from 50% (maximum uncertainty).

    Markets near 50% have the most potential for large moves.
    Markets near 0% or 100% are nearly resolved.

    Returns 0.0 at 50%, 0.5 at 0% or 100%.
    """
    return abs(state.last_price - 0.50)


def price_percentile(state: MarketState) -> float | None:
    """Where current price sits relative to its 4h range.

    0.0 = at 4h low, 1.0 = at 4h high.
    Useful for detecting range breakouts.
    """
    low = state.prices_4h.min()
    high = state.prices_4h.max()
    if low is None or high is None:
        return None

    range_size = high - low
    if range_size <= 0:
        return 0.5

    return (state.last_price - low) / range_size


# ── Compute All ──────────────────────────────────────────────────────


def compute_all(state: MarketState) -> dict[str, float | None]:
    """Compute all technical indicator features."""
    return {
        "rsi_1h": rsi_1h(state),
        "rsi_4h": rsi_4h(state),
        "bollinger_position": bollinger_position(state),
        "bollinger_width": bollinger_width(state),
        "volume_ratio": volume_ratio(state),
        "volume_price_divergence": volume_price_divergence(state),
        "rate_of_change": rate_of_change(state),
        "momentum_score": momentum_score(state),
        "distance_from_midpoint": distance_from_midpoint(state),
        "price_percentile": price_percentile(state),
    }
