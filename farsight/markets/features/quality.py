"""
Market quality features — can we trust this market's data?

These gate whether signals from a market should be acted upon.
Low quality = suppress signals, don't trade.
"""

from datetime import datetime
from farsight.markets.services.state_engine import MarketState


def liquidity_score(state: MarketState) -> float:
    """Composite liquidity score [0, 1]. Higher = more liquid.

    Combines spread, depth, and volume into a single tradability metric.
    """
    # Spread component: tight spread = high score
    # 1% spread → 0.9, 5% spread → 0.5, 20% spread → 0.0
    spread = spread_pct(state)
    spread_score = max(0.0, 1.0 - spread * 5.0)

    # Depth component: more depth = higher score
    total_depth = state.bid_depth + state.ask_depth
    # $100K depth → 1.0, $10K → 0.5, $1K → 0.1
    depth_score = min(1.0, total_depth / 100_000)

    # Volume component: higher volume = higher score
    vol_1h = state.volume_1h.sum()
    # $50K/hour → 1.0, $5K → 0.5
    volume_score = min(1.0, vol_1h / 50_000)

    # Weighted composite
    return 0.4 * spread_score + 0.35 * depth_score + 0.25 * volume_score


def spread_pct(state: MarketState) -> float:
    """Spread as fraction of mid. Duplicated here for quality context."""
    if state.last_price <= 0:
        return 1.0
    return state.spread / state.last_price


def stale_score(state: MarketState) -> float:
    """Staleness indicator [0, 1]. Higher = more stale.

    0.0 = updated in last minute
    0.5 = no update in 5 minutes
    1.0 = no update in 10+ minutes
    """
    secs = state.seconds_since_last_update
    if secs is None:
        return 1.0  # Never updated = fully stale
    # Linear ramp: 0 at 0s, 0.5 at 300s, 1.0 at 600s+
    return min(1.0, secs / 600.0)


def manipulation_heuristic(state: MarketState) -> float:
    """Simple manipulation detection score [0, 1]. Higher = more suspicious.

    Looks for:
    - Very high quote velocity with no trade velocity (spoofing pattern)
    - Extreme depth imbalance (one-sided book stuffing)

    This is a heuristic, not a definitive detector.
    """
    score = 0.0

    # Spoofing: lots of quote updates but no trades
    q_vel = state.prices_5m.count() / 5.0 if state.prices_5m.count() > 0 else 0
    t_vel = state.trades_1h.trade_count() / 60.0
    if q_vel > 10 and t_vel < 0.1:
        score += 0.5  # High quote churn, no trades

    # Extreme depth imbalance (>90% one side)
    total_depth = state.bid_depth + state.ask_depth
    if total_depth > 0:
        imbalance = abs(state.bid_depth - state.ask_depth) / total_depth
        if imbalance > 0.9:
            score += 0.3

    # Very wide spread on a supposedly active market
    if state.spread > 0.20 and state.volume_1h.sum() > 1000:
        score += 0.2

    return min(1.0, score)


def resolution_proximity(state: MarketState, end_date: datetime | None = None) -> float:
    """Days until expected resolution. Lower = riskier.

    Returns float days. None end_date → returns 999 (no known resolution date).
    """
    if end_date is None:
        return 999.0
    delta = (end_date - datetime.utcnow()).total_seconds()
    return max(0.0, delta / 86400.0)  # Convert to days


def compute_all(state: MarketState, end_date: datetime | None = None) -> dict[str, float]:
    """Compute all quality features."""
    return {
        "liquidity_score": liquidity_score(state),
        "stale_score": stale_score(state),
        "manipulation_heuristic": manipulation_heuristic(state),
        "resolution_proximity_days": resolution_proximity(state, end_date),
    }
