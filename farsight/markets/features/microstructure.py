"""
Microstructure features — what's happening in the order book right now.

Computed from MarketState on each state update.
All functions are pure: MarketState in → float out. No side effects.
"""

from farsight.markets.services.state_engine import MarketState


def spread_pct(state: MarketState) -> float:
    """Bid-ask spread as percentage of mid. Higher = less liquid."""
    if state.last_price <= 0:
        return 1.0
    return state.spread / state.last_price if state.last_price > 0 else 1.0


def depth_imbalance(state: MarketState) -> float:
    """(bid_depth - ask_depth) / total_depth. Range [-1, 1].

    Positive = more buy pressure. Negative = more sell pressure.
    """
    total = state.bid_depth + state.ask_depth
    if total <= 0:
        return 0.0
    return (state.bid_depth - state.ask_depth) / total


def trade_imbalance_5m(state: MarketState) -> float:
    """Net buy volume / total volume over 5 minutes. Range [-1, 1].

    Approximated from price direction of trades in the volume window.
    Positive = net buying. Negative = net selling.
    """
    # We track total volume but not per-side volume in the current windows.
    # Use price delta as a proxy: if price rose, net buying dominates.
    delta = state.prices_5m.delta()
    if delta is None:
        return 0.0
    # Normalize: 5% move = full signal
    return max(-1.0, min(1.0, delta / 0.05))


def trade_imbalance_1h(state: MarketState) -> float:
    """Same as trade_imbalance_5m but over 1 hour."""
    delta = state.prices_1h.delta()
    if delta is None:
        return 0.0
    return max(-1.0, min(1.0, delta / 0.10))


def quote_velocity(state: MarketState) -> float:
    """Price updates per minute over last 5 minutes.

    Higher velocity = more active market.
    """
    count = state.prices_5m.count()
    # 5-minute window
    return count / 5.0 if count > 0 else 0.0


def trade_velocity(state: MarketState) -> float:
    """Trades per minute over last hour."""
    count = state.trades_1h.trade_count()
    return count / 60.0 if count > 0 else 0.0


def large_trade_ratio(state: MarketState) -> float:
    """Fraction of volume from trades > $5K in last hour.

    Not directly available from current window structure — returns 0.0.
    Will be populated when we add per-trade size tracking.
    """
    # TODO: requires per-trade size bucketing in VolumeWeightedWindow
    return 0.0


def compute_all(state: MarketState) -> dict[str, float]:
    """Compute all microstructure features for a market state."""
    return {
        "spread_pct": spread_pct(state),
        "depth_imbalance": depth_imbalance(state),
        "trade_imbalance_5m": trade_imbalance_5m(state),
        "trade_imbalance_1h": trade_imbalance_1h(state),
        "quote_velocity": quote_velocity(state),
        "trade_velocity": trade_velocity(state),
        "large_trade_ratio": large_trade_ratio(state),
    }
