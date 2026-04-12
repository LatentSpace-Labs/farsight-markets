"""
Prediction Markets API routes.

Live data from Polymarket (and later Kalshi) — normalized, enriched, and analyzed.
No database required. No auth required (all public market data).

Mounted at /api/markets/ — mirrors Polymarket's flat API style.

Endpoint groups:
  - Discovery: /markets, /events, /categories, /tags
  - Market Data: /book, /price, /trades, /price-history, /stream-test
  - Analysis: /features, /analyze/market, /analyze/event
"""

import json
import logging
import time
from typing import Optional

from fastapi import APIRouter, Query

from farsight.markets.clients.polymarket.gamma_client import GammaClient
from farsight.markets.clients.polymarket.clob_client import ClobClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/prediction-markets")

# Shared client instances (stateless, safe to reuse)
_gamma = GammaClient()
_clob = ClobClient()


@router.get("/health", tags=["Health"])
async def health_check():
    """Test connectivity to all external prediction market APIs.

    Returns latency and status for each API endpoint.
    """
    import time

    results = {}

    # Gamma API
    try:
        start = time.time()
        markets = await _gamma.get_markets(limit=1)
        latency_ms = (time.time() - start) * 1000
        results["gamma_api"] = {
            "status": "ok" if markets else "empty",
            "latency_ms": round(latency_ms),
            "url": _gamma._base_url,
        }
    except Exception as e:
        results["gamma_api"] = {"status": "error", "error": str(e)}

    # CLOB REST
    if markets:
        clob_ids = markets[0].get("clobTokenIds", "[]")
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except json.JSONDecodeError:
                clob_ids = []
        if clob_ids:
            try:
                start = time.time()
                price = await _clob.get_price(str(clob_ids[0]))
                latency_ms = (time.time() - start) * 1000
                results["clob_rest"] = {
                    "status": "ok",
                    "latency_ms": round(latency_ms),
                    "sample_price": price,
                    "url": _clob._base_url,
                }
            except Exception as e:
                results["clob_rest"] = {"status": "error", "error": str(e)}

    results["websocket_url"] = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    return results


@router.get("/markets", tags=["Discovery"])
async def explore_markets(
    limit: int = Query(20, ge=1, le=100, description="Number of markets to return"),
    order: str = Query("volume_24hr", description="Sort by: volume_24hr, volume, liquidity, competitive, end_date"),
    tag_id: Optional[int] = Query(None, description="Filter by tag ID (get IDs from /tags)"),
):
    """Fetch top active markets from Polymarket Gamma API (live, no DB).

    Returns normalized MarketSchema objects. Use `order` param for server-side sorting.
    Use this to discover markets, find token_ids, and validate data quality.
    """
    raw = await _gamma.get_markets(
        active=True,
        closed=False,
        limit=limit,
        order=order,
        ascending=False,
        tag_id=tag_id,
        exclude_tag_id=102127,  # Filter out "Up or Down" 5-min crypto spam
    )

    markets = []
    for m in raw:
        try:
            normalized = GammaClient.normalize_market(m)
            # Enrich with extra fields available from Gamma but not in our schema
            market_dict = normalized.model_dump(mode="json")
            market_dict["best_bid"] = m.get("bestBid")
            market_dict["best_ask"] = m.get("bestAsk")
            market_dict["last_trade_price"] = m.get("lastTradePrice")
            market_dict["spread"] = m.get("spread")
            market_dict["volume_24h"] = m.get("volume24hr")
            market_dict["one_day_price_change"] = m.get("oneDayPriceChange")
            market_dict["competitive"] = m.get("competitive")
            market_dict["accepting_orders"] = m.get("acceptingOrders")
            markets.append(market_dict)
        except Exception as e:
            logger.warning(f"Failed to normalize market: {e}")

    return {
        "count": len(markets),
        "source": "polymarket_gamma_api",
        "live": True,
        "order": order,
        "markets": markets,
    }


@router.get("/events", tags=["Discovery"])
async def explore_events(
    limit: int = Query(15, ge=1, le=50, description="Number of events to return"),
    order: str = Query("volume_24hr", description="Sort by: volume_24hr, volume, liquidity, competitive, end_date"),
    tag_id: Optional[int] = Query(None, description="Filter by tag ID (get IDs from /tags)"),
):
    """Fetch active events from Polymarket Gamma API (live, no DB).

    Events group related markets (e.g., "2026 US Election" contains
    "Will Trump win?", "Will DeSantis win?", etc.)
    """
    raw = await _gamma.get_events(
        active=True,
        closed=False,
        limit=limit,
        order=order,
        ascending=False,
        tag_id=tag_id,
        exclude_tag_id=102127,
    )

    events = []
    for e in raw:
        try:
            normalized = GammaClient.normalize_event(e)
            event_dict = normalized.model_dump(mode="json")
            event_dict["volume_24h"] = e.get("volume24hr")
            event_dict["comment_count"] = e.get("commentCount")
            event_dict["neg_risk"] = e.get("negRisk")
            event_dict["competitive"] = e.get("competitive")
            events.append(event_dict)
        except Exception as e_err:
            logger.warning(f"Failed to normalize event: {e_err}")

    return {
        "count": len(events),
        "source": "polymarket_gamma_api",
        "live": True,
        "order": order,
        "events": events,
    }


@router.get("/markets/{slug}", tags=["Discovery"])
async def explore_market_detail(slug: str):
    """Fetch detailed market info by slug (live, no DB).

    Returns the normalized market with all outcomes, token_ids, and
    enriched fields (bestBid, bestAsk, spread, price changes).
    Use the token_ids to query /book and /price endpoints.
    """
    raw = await _gamma.get_market_by_slug(slug)
    if not raw:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Market not found: {slug}")

    normalized = GammaClient.normalize_market(raw)
    market_dict = normalized.model_dump(mode="json")

    # Enrich with live trading data from Gamma response
    market_dict["best_bid"] = raw.get("bestBid")
    market_dict["best_ask"] = raw.get("bestAsk")
    market_dict["last_trade_price"] = raw.get("lastTradePrice")
    market_dict["spread"] = raw.get("spread")
    market_dict["volume_24h"] = raw.get("volume24hr")
    market_dict["volume_1w"] = raw.get("volume1wk")
    market_dict["volume_1m"] = raw.get("volume1mo")
    market_dict["one_hour_price_change"] = raw.get("oneHourPriceChange")
    market_dict["one_day_price_change"] = raw.get("oneDayPriceChange")
    market_dict["one_week_price_change"] = raw.get("oneWeekPriceChange")
    market_dict["competitive"] = raw.get("competitive")
    market_dict["accepting_orders"] = raw.get("acceptingOrders")
    market_dict["group_item_title"] = raw.get("groupItemTitle")
    market_dict["question_id"] = raw.get("questionID")

    return {
        "source": "polymarket_gamma_api",
        "live": True,
        "market": market_dict,
    }


@router.get("/book", tags=["Market Data"])
async def get_orderbook(
    token_id: str = Query(..., description="Outcome token ID (from /markets/{slug} → outcomes[].token_id)"),
):
    """Fetch L2 orderbook for a specific outcome token (live from CLOB API)."""
    book = await _clob.get_orderbook(token_id)
    if not book:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Could not fetch orderbook")

    return {
        "source": "polymarket_clob_api",
        "live": True,
        "token_id": token_id,
        "best_bid": book.best_bid,
        "best_ask": book.best_ask,
        "mid": book.mid,
        "spread": book.spread,
        "total_bid_depth_usd": book.total_bid_depth,
        "total_ask_depth_usd": book.total_ask_depth,
        "bid_levels": len(book.bids),
        "ask_levels": len(book.asks),
        "bids": [{"price": b.price, "size": b.size} for b in book.bids[:10]],
        "asks": [{"price": a.price, "size": a.size} for a in book.asks[:10]],
    }


@router.get("/price", tags=["Market Data"])
async def get_price(
    token_id: str = Query(..., description="Outcome token ID"),
):
    """Fetch current price, midpoint, and spread for a token (live from CLOB API)."""
    price = await _clob.get_price(token_id)
    midpoint = await _clob.get_midpoint(token_id)
    spread = await _clob.get_spread(token_id)

    return {
        "source": "polymarket_clob_api",
        "live": True,
        "token_id": token_id,
        "price": price,
        "midpoint": midpoint,
        "spread": spread,
    }


@router.get("/trades", tags=["Market Data"])
async def get_trades(
    condition_id: str = Query(..., description="Market condition_id (from /markets → markets[].condition_id)"),
    limit: int = Query(20, ge=1, le=100),
):
    """Fetch recent trades for a market (live from CLOB API).

    Note: requires CLOB API auth. Returns empty if unauthenticated.
    """
    trades = await _clob.get_trades(condition_id, limit=limit)

    return {
        "source": "polymarket_clob_api",
        "live": True,
        "condition_id": condition_id,
        "count": len(trades),
        "trades": [
            {
                "timestamp": t.timestamp.isoformat(),
                "price": t.price,
                "size_usd": t.size_usd,
                "side": t.side.value,
                "taker_address": t.taker_address,
                "is_whale": t.size_usd > 5000,
            }
            for t in trades
        ],
    }


@router.get("/price-history", tags=["Market Data"])
async def get_price_history(
    token_id: str = Query(..., description="Outcome token ID"),
    interval: str = Query("1h", description="Interval: 1m, 5m, 1h, 1d"),
    fidelity: int = Query(60, ge=10, le=500, description="Number of data points"),
):
    """Fetch historical price series for a token (live from CLOB API)."""
    history = await _clob.get_price_history(token_id, interval=interval, fidelity=fidelity)

    return {
        "source": "polymarket_clob_api",
        "live": True,
        "token_id": token_id,
        "interval": interval,
        "data_points": len(history),
        "history": history,
    }


@router.get("/stream-test", tags=["Streaming"])
async def test_websocket_stream(
    seconds: int = Query(10, ge=3, le=60, description="Seconds to stream"),
    max_markets: int = Query(3, ge=1, le=10, description="Number of markets to subscribe to"),
):
    """Stream live WebSocket data for N seconds and report what was received.

    Connects to Polymarket's CLOB WebSocket, subscribes to the top markets
    by volume, and collects all events. Returns a summary of what was received.

    Use this to verify WebSocket connectivity and see live data flowing.
    """
    import asyncio
    from farsight.markets.clients.polymarket.ws_client import PolymarketWsClient
    from farsight.markets.engine.checkpoint import MemoryCheckpointStore
    from farsight.markets.engine.event_bus import EventBus

    # Discover top markets to subscribe to (by 24h volume, excluding crypto spam)
    raw_markets = await _gamma.get_markets(
        active=True, closed=False, limit=max_markets * 2,
        order="volume_24hr", ascending=False,
    )

    # Collect token IDs
    token_ids = set()
    market_info = []
    for m in raw_markets[:max_markets]:
        clob_ids = m.get("clobTokenIds", "[]")
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except json.JSONDecodeError:
                continue
        question = m.get("question", "?")[:60]
        for tid in clob_ids:
            token_ids.add(str(tid))
        market_info.append({"question": question, "tokens": len(clob_ids)})

    if not token_ids:
        return {"error": "No token IDs found to subscribe to"}

    # Set up event bus with collectors
    bus = EventBus()
    checkpoint = MemoryCheckpointStore()
    collected = {"price_ticks": [], "trade_prints": [], "orderbook_updates": []}

    async def on_tick(payload):
        collected["price_ticks"].append({
            "token_id": payload.get("token_id", "")[:20] + "...",
            "bid": payload.get("bid"),
            "ask": payload.get("ask"),
            "mid": payload.get("mid"),
            "spread": payload.get("spread"),
            "timestamp": payload.get("timestamp"),
        })

    async def on_trade(payload):
        collected["trade_prints"].append({
            "token_id": payload.get("token_id", "")[:20] + "...",
            "price": payload.get("price"),
            "size_usd": payload.get("size_usd"),
            "timestamp": payload.get("timestamp"),
        })

    async def on_book(payload):
        collected["orderbook_updates"].append({
            "token_id": payload.get("token_id", "")[:20] + "...",
            "bid_levels": len(payload.get("bids", [])),
            "ask_levels": len(payload.get("asks", [])),
        })

    bus.subscribe("raw.price_tick", on_tick)
    bus.subscribe("raw.trade_print", on_trade)
    bus.subscribe("raw.orderbook", on_book)

    ws = PolymarketWsClient(bus, checkpoint)
    await ws.update_subscriptions(token_ids)

    # Run WebSocket for N seconds
    ws_task = asyncio.create_task(ws.connect())
    await asyncio.sleep(seconds)
    await ws.stop()
    ws_task.cancel()
    try:
        await ws_task
    except asyncio.CancelledError:
        pass

    health = ws.get_health()
    return {
        "stream_duration_seconds": seconds,
        "subscribed_tokens": len(token_ids),
        "markets_watched": market_info,
        "websocket_health": health,
        "events_received": {
            "price_ticks": len(collected["price_ticks"]),
            "trade_prints": len(collected["trade_prints"]),
            "orderbook_updates": len(collected["orderbook_updates"]),
        },
        "sample_price_ticks": collected["price_ticks"][:5],
        "sample_trade_prints": collected["trade_prints"][:5],
        "sample_orderbook_updates": collected["orderbook_updates"][:3],
        "raw_ws_samples": health.get("raw_samples", [])[:5],
    }


@router.get("/tags", tags=["Discovery"])
async def explore_tags():
    """Fetch all available Polymarket tags (live).

    Tags are the primary categorization system. Use the tag `id` as the
    `tag_id` query parameter on /markets and /events to filter by category.
    """
    tags = await _gamma.get_tags()
    return {
        "source": "polymarket_gamma_api",
        "live": True,
        "count": len(tags),
        "tags": [
            {
                "id": t.get("id"),
                "label": t.get("label"),
                "slug": t.get("slug"),
            }
            for t in tags
        ],
    }


@router.get("/features", tags=["Analysis"])
async def get_features(
    token_id: str = Query(..., description="Outcome token ID"),
):
    """Compute live features for a specific outcome token.

    Backfills rolling windows from price history (last 4 hours),
    overlays the current orderbook, and runs the full feature engine.
    This gives you the same feature vector the streaming engine would produce.
    """
    from farsight.markets.services.state_engine import MarketState
    from farsight.markets.services.feature_engine import compute_features
    from datetime import datetime, timedelta, timezone

    # Fetch real orderbook
    book = await _clob.get_orderbook(token_id)
    if not book:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Could not fetch orderbook for token")

    # Build state and backfill from price history
    state = MarketState(token_id)

    history = await _clob.get_price_history(token_id, interval="1m", fidelity=300)
    backfill_count = 0
    for point in history:
        try:
            ts_val = point.get("t")
            price_val = point.get("p")
            if ts_val is None or price_val is None:
                continue
            ts = datetime.fromtimestamp(int(ts_val), tz=timezone.utc).replace(tzinfo=None)
            price = float(price_val)
            if price > 0:
                state.update_price(ts, mid=price, bid=price - book.spread / 2, ask=price + book.spread / 2)
                backfill_count += 1
        except (ValueError, TypeError):
            continue

    # Overlay current orderbook
    now = datetime.utcnow()
    state.update_price(now, mid=book.mid, bid=book.best_bid, ask=book.best_ask)
    state.update_book(book.total_bid_depth, book.total_ask_depth, book.best_bid, book.best_ask)

    features = compute_features(state)

    return {
        "source": "computed_from_live_orderbook_and_history",
        "token_id": token_id,
        "backfill_points": backfill_count,
        "state": {
            "mid": book.mid,
            "best_bid": book.best_bid,
            "best_ask": book.best_ask,
            "spread": book.spread,
            "bid_depth_usd": book.total_bid_depth,
            "ask_depth_usd": book.total_ask_depth,
        },
        "features": features,
    }


@router.get("/analyze/token", tags=["Analysis"])
async def analyze_token(
    token_id: str = Query(..., description="Outcome token ID"),
):
    """Run signal detectors on live data for a specific token.

    Backfills history, computes features, runs all signal detectors.
    Shows what signals WOULD fire right now (ignores filter chain).
    """
    from farsight.markets.services.state_engine import MarketState
    from farsight.markets.services.feature_engine import compute_features
    from farsight.markets.services.signal_engine import SignalEngine
    from datetime import datetime, timezone

    book = await _clob.get_orderbook(token_id)
    if not book:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Could not fetch orderbook for token")

    # Build state with backfilled history
    state = MarketState(token_id)

    history = await _clob.get_price_history(token_id, interval="1m", fidelity=300)
    for point in history:
        try:
            ts_val = point.get("t")
            price_val = point.get("p")
            if ts_val is None or price_val is None:
                continue
            ts = datetime.fromtimestamp(int(ts_val), tz=timezone.utc).replace(tzinfo=None)
            price = float(price_val)
            if price > 0:
                state.update_price(ts, mid=price, bid=price - book.spread / 2, ask=price + book.spread / 2)
        except (ValueError, TypeError):
            continue

    now = datetime.utcnow()
    state.update_price(now, mid=book.mid, bid=book.best_bid, ask=book.best_ask)
    state.update_book(book.total_bid_depth, book.total_ask_depth, book.best_bid, book.best_ask)

    features = compute_features(state)

    # Add last_price to features for signal builder
    features["last_price"] = book.mid

    engine = SignalEngine()
    raw_signals = engine.evaluate(features, token_id)

    return {
        "source": "computed_from_live_data_with_history",
        "token_id": token_id,
        "price_history_points": len(history),
        "features_summary": {
            "microstructure": {
                "spread_pct": features.get("spread_pct"),
                "depth_imbalance": features.get("depth_imbalance"),
                "trade_imbalance_5m": features.get("trade_imbalance_5m"),
                "quote_velocity": features.get("quote_velocity"),
                "trade_velocity": features.get("trade_velocity"),
            },
            "probability_dynamics": {
                "delta_1m": features.get("delta_1m"),
                "delta_5m": features.get("delta_5m"),
                "delta_15m": features.get("delta_15m"),
                "delta_1h": features.get("delta_1h"),
                "acceleration": features.get("acceleration"),
                "drift_score": features.get("drift_score"),
                "reversion_score": features.get("reversion_score"),
                "volatility_burst": features.get("volatility_burst"),
            },
            "technicals": {
                "rsi_1h": features.get("rsi_1h"),
                "rsi_4h": features.get("rsi_4h"),
                "bollinger_position": features.get("bollinger_position"),
                "bollinger_width": features.get("bollinger_width"),
                "volume_ratio": features.get("volume_ratio"),
                "momentum_score": features.get("momentum_score"),
                "price_percentile": features.get("price_percentile"),
            },
            "quality": {
                "liquidity_score": features.get("liquidity_score"),
                "stale_score": features.get("stale_score"),
                "manipulation_heuristic": features.get("manipulation_heuristic"),
            },
        },
        "signals_detected": len(raw_signals),
        "signals": [
            {
                "type": s.signal_type.value,
                "direction": s.direction.value,
                "confidence": s.confidence,
                "horizon": s.horizon,
                "edge": s.edge,
                "tradability": s.tradability_score,
                "risk_flags": s.risk_flags,
                "evidence": [{"source": e.source, "description": e.description, "value": e.value} for e in s.evidence],
            }
            for s in raw_signals
        ],
        "note": "Raw signal candidates before filter chain. In production, signals must also pass cooldown, edge, and entry price filters.",
    }


@router.get("/analyze/market/{slug}", tags=["Analysis"])
async def analyze_market(slug: str):
    """Run full signal analysis on a market — both outcomes with trade thesis.

    Fetches market metadata, backfills price history for both YES and NO tokens,
    computes features, runs signal detectors, and presents a unified view
    with actionable trade ideas.
    """
    from farsight.markets.services.state_engine import MarketState
    from farsight.markets.services.feature_engine import compute_features
    from farsight.markets.services.signal_engine import SignalEngine
    from datetime import datetime, timezone

    # Fetch market detail
    raw = await _gamma.get_market_by_slug(slug)
    if not raw:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Market not found: {slug}")

    market = GammaClient.normalize_market(raw)
    if len(market.outcomes) < 2:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="Market has fewer than 2 outcomes")

    # Analyze each outcome
    outcomes_analysis = []
    all_signals = []
    engine = SignalEngine()

    for outcome in market.outcomes:
        # Build state with backfilled history
        state = MarketState(outcome.token_id)

        history = await _clob.get_price_history(outcome.token_id, interval="1m", fidelity=300)
        for point in history:
            try:
                ts_val = point.get("t")
                price_val = point.get("p")
                if ts_val is None or price_val is None:
                    continue
                ts = datetime.fromtimestamp(int(ts_val), tz=timezone.utc).replace(tzinfo=None)
                price = float(price_val)
                if price > 0:
                    state.update_price(ts, mid=price)
            except (ValueError, TypeError):
                continue

        # Get live orderbook
        book = await _clob.get_orderbook(outcome.token_id)
        if book:
            now = datetime.utcnow()
            state.update_price(now, mid=book.mid, bid=book.best_bid, ask=book.best_ask)
            state.update_book(book.total_bid_depth, book.total_ask_depth, book.best_bid, book.best_ask)

        features = compute_features(state)
        features["last_price"] = outcome.current_price

        signals = engine.evaluate(features, outcome.token_id, str(market.condition_id))

        outcomes_analysis.append({
            "label": outcome.label,
            "token_id": outcome.token_id[:30] + "...",
            "price": outcome.current_price,
            "best_bid": book.best_bid if book else None,
            "best_ask": book.best_ask if book else None,
            "spread": book.spread if book else None,
            "bid_depth_usd": round(book.total_bid_depth, 2) if book else None,
            "ask_depth_usd": round(book.total_ask_depth, 2) if book else None,
            "history_points": len(history),
            "features": {
                "delta_1m": features.get("delta_1m"),
                "delta_5m": features.get("delta_5m"),
                "delta_15m": features.get("delta_15m"),
                "delta_1h": features.get("delta_1h"),
                "depth_imbalance": features.get("depth_imbalance"),
                "liquidity_score": features.get("liquidity_score"),
                "drift_score": features.get("drift_score"),
                "reversion_score": features.get("reversion_score"),
                "volatility_burst": features.get("volatility_burst"),
                "trade_velocity": features.get("trade_velocity"),
            },
            "signals": [
                {
                    "type": s.signal_type.value,
                    "direction": s.direction.value,
                    "confidence": round(s.confidence, 3),
                    "edge": round(s.edge, 4),
                    "risk_flags": s.risk_flags,
                }
                for s in signals
            ],
        })
        all_signals.extend(signals)

    # Build trade thesis
    yes_data = outcomes_analysis[0] if outcomes_analysis else {}
    no_data = outcomes_analysis[1] if len(outcomes_analysis) > 1 else {}
    yes_price = yes_data.get("price", 0.5)
    no_price = no_data.get("price", 0.5)

    trade_thesis = None
    if all_signals:
        best = max(all_signals, key=lambda s: s.confidence)
        if best.direction.value == "bullish":
            trade_thesis = {
                "action": f"BUY {yes_data.get('label', 'YES')}",
                "entry_price": yes_price,
                "max_payout_per_share": round(1.0 - yes_price, 2),
                "max_return_pct": round((1.0 - yes_price) / yes_price * 100, 1) if yes_price > 0 else 0,
                "signal": best.signal_type.value,
                "confidence": round(best.confidence, 3),
                "reasoning": best.evidence[0].description if best.evidence else "",
            }
        elif best.direction.value == "bearish":
            trade_thesis = {
                "action": f"BUY {no_data.get('label', 'NO')}",
                "entry_price": no_price,
                "max_payout_per_share": round(1.0 - no_price, 2),
                "max_return_pct": round((1.0 - no_price) / no_price * 100, 1) if no_price > 0 else 0,
                "signal": best.signal_type.value,
                "confidence": round(best.confidence, 3),
                "reasoning": best.evidence[0].description if best.evidence else "",
            }

    return {
        "source": "computed_from_live_data",
        "market": {
            "question": market.question,
            "slug": market.slug,
            "condition_id": market.condition_id,
            "status": market.status.value,
            "volume_total": market.volume_total,
            "liquidity": market.liquidity,
            "end_date": market.end_date.isoformat() if market.end_date else None,
        },
        "outcomes": outcomes_analysis,
        "total_signals": len(all_signals),
        "trade_thesis": trade_thesis,
    }


@router.get("/analyze/event/{slug}", tags=["Analysis"])
async def analyze_event(slug: str):
    """Run full signal analysis on an EVENT — all child markets with cross-market signals.

    An event like "Next Prime Minister of Hungary" contains multiple markets
    (Magyar, Orbán, Kapitány, etc.). This endpoint:
    1. Fetches all child markets and their orderbooks
    2. Computes features for each outcome
    3. Checks structural consistency (should prices sum to ~100%?)
    4. Detects thematic repricing (multiple outcomes moving together)
    5. Generates trade thesis based on strongest signal
    """
    from farsight.markets.services.state_engine import MarketState
    from farsight.markets.services.feature_engine import compute_features
    from farsight.markets.services.signal_engine import (
        SignalEngine,
        detect_probability_shock,
        detect_momentum_continuation,
        detect_mean_reversion,
        detect_structural_inconsistency,
        detect_thematic_repricing,
    )
    from datetime import datetime, timezone

    # Fetch event with all child markets
    raw_event = await _gamma.get_event_by_slug(slug)
    if not raw_event:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Event not found: {slug}")

    event = GammaClient.normalize_event(raw_event)

    # Analyze each child market
    markets_analysis = []
    all_signals = []
    all_deltas = []  # For thematic repricing detection
    prices_sum = 0.0
    engine = SignalEngine()

    for market in event.markets:
        if not market.outcomes:
            continue

        # Use the first outcome (typically YES / the named candidate)
        primary_outcome = market.outcomes[0]
        prices_sum += primary_outcome.current_price

        # Build state with backfilled history
        state = MarketState(primary_outcome.token_id)

        history = await _clob.get_price_history(primary_outcome.token_id, interval="1m", fidelity=300)
        for point in history:
            try:
                ts_val = point.get("t")
                price_val = point.get("p")
                if ts_val is None or price_val is None:
                    continue
                ts = datetime.fromtimestamp(int(ts_val), tz=timezone.utc).replace(tzinfo=None)
                price = float(price_val)
                if price > 0:
                    state.update_price(ts, mid=price)
            except (ValueError, TypeError):
                continue

        # Get live orderbook
        book = await _clob.get_orderbook(primary_outcome.token_id)
        if book:
            now = datetime.utcnow()
            state.update_price(now, mid=book.mid, bid=book.best_bid, ask=book.best_ask)
            state.update_book(book.total_bid_depth, book.total_ask_depth, book.best_bid, book.best_ask)

        features = compute_features(state)
        features["last_price"] = primary_outcome.current_price

        # Run single-market detectors
        signals = engine.evaluate(features, primary_outcome.token_id, market.condition_id)

        delta_5m = features.get("delta_5m")
        if delta_5m is not None:
            all_deltas.append(delta_5m)

        market_data = {
            "question": market.question,
            "slug": market.slug,
            "condition_id": market.condition_id,
            "outcome": primary_outcome.label,
            "price": primary_outcome.current_price,
            "price_pct": f"{primary_outcome.current_price:.0%}",
            "best_bid": book.best_bid if book else None,
            "best_ask": book.best_ask if book else None,
            "spread": round(book.spread, 4) if book else None,
            "depth_usd": round(book.total_bid_depth + book.total_ask_depth, 2) if book else None,
            "volume_total": market.volume_total,
            "history_points": len(history),
            "features": {
                "delta_5m": features.get("delta_5m"),
                "delta_1h": features.get("delta_1h"),
                "depth_imbalance": features.get("depth_imbalance"),
                "liquidity_score": features.get("liquidity_score"),
                "drift_score": features.get("drift_score"),
                "reversion_score": features.get("reversion_score"),
            },
            "signals": [
                {
                    "type": s.signal_type.value,
                    "direction": s.direction.value,
                    "confidence": round(s.confidence, 3),
                }
                for s in signals
            ],
        }
        markets_analysis.append(market_data)
        all_signals.extend(signals)

    # ── Cross-market signals ─────────────────────────────────────────

    # Structural inconsistency: do prices sum to ~100%?
    structural_signal = None
    if prices_sum > 0:
        deviation = abs(prices_sum - 1.0)
        structural_signal = {
            "prices_sum": round(prices_sum, 4),
            "prices_sum_pct": f"{prices_sum:.1%}",
            "deviation": round(deviation, 4),
            "deviation_pct": f"{deviation:.1%}",
            "mispriced": deviation > 0.03,
            "interpretation": (
                f"Outcomes sum to {prices_sum:.1%} — "
                + ("this is within normal range."
                   if deviation <= 0.03
                   else f"there is {deviation:.1%} overpricing across outcomes. "
                        "Some candidates may be overvalued relative to the field.")
            ),
        }

        # Run the actual structural inconsistency detector
        if markets_analysis:
            primary_features = {"liquidity_score": 0.5, "last_price": markets_analysis[0].get("price", 0.5)}
            struct_sig = detect_structural_inconsistency(
                primary_features,
                markets_analysis[0].get("condition_id", ""),
                None,
                outcome_prices_sum=prices_sum,
            )
            if struct_sig:
                all_signals.append(struct_sig)

    # Thematic repricing: are multiple outcomes moving together?
    thematic_signal = None
    if len(all_deltas) >= 3:
        positive = sum(1 for d in all_deltas if d > 0.01)
        negative = sum(1 for d in all_deltas if d < -0.01)
        if positive >= 3 or negative >= 3:
            direction = "bullish" if positive >= 3 else "bearish"
            thematic_signal = {
                "moving_positive": positive,
                "moving_negative": negative,
                "direction": direction,
                "interpretation": f"{max(positive, negative)} of {len(all_deltas)} markets moving {'up' if direction == 'bullish' else 'down'} — broad repricing underway.",
            }

    # ── Trade thesis ─────────────────────────────────────────────────
    trade_ideas = []

    # Structural arb: if prices sum > 103%, some outcomes are overpriced
    if structural_signal and structural_signal["mispriced"]:
        # Find the candidate with the worst liquidity/depth ratio — likely most overpriced
        trade_ideas.append({
            "type": "structural_arbitrage",
            "description": f"Outcomes sum to {prices_sum:.1%}. The market is overpriced by {abs(prices_sum - 1.0):.1%}. "
                           f"Consider selling (going NO on) the least liquid outcome.",
            "confidence": "medium",
        })

    # Momentum/shock on individual candidates
    for m in markets_analysis:
        for s in m.get("signals", []):
            action = "BUY YES" if s["direction"] == "bullish" else "BUY NO"
            trade_ideas.append({
                "type": s["type"],
                "market": m["outcome"],
                "action": f"{action} on {m['outcome']}",
                "price": m["price"],
                "confidence": s["confidence"],
                "description": f"{s['type']} signal ({s['direction']}) on {m['outcome']} at {m['price']:.0%}",
            })

    return {
        "source": "computed_from_live_data",
        "event": {
            "title": event.title,
            "slug": event.slug,
            "category": event.category,
            "status": event.status.value,
            "volume_total": event.volume_total,
            "liquidity": event.liquidity,
            "end_date": event.end_date.isoformat() if event.end_date else None,
            "num_markets": len(event.markets),
        },
        "markets": markets_analysis,
        "cross_market_analysis": {
            "structural_consistency": structural_signal,
            "thematic_repricing": thematic_signal,
        },
        "total_signals": len(all_signals),
        "trade_ideas": trade_ideas,
    }


@router.get("/categories", tags=["Discovery"])
async def explore_categories():
    """Discover available market categories by scanning current active events.

    Useful for understanding what types of markets exist on Polymarket.
    """
    raw_events = await _gamma.get_events(active=True, closed=False, limit=100, order="volume_24hr")

    category_counts: dict[str, int] = {}
    for e in raw_events:
        normalized = GammaClient.normalize_event(e)
        cat = normalized.category or "uncategorized"
        market_count = len(normalized.markets)
        category_counts[cat] = category_counts.get(cat, 0) + market_count

    # Sort by market count
    sorted_cats = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)

    return {
        "source": "polymarket_gamma_api",
        "live": True,
        "total_events_scanned": len(raw_events),
        "categories": [
            {"category": cat, "market_count": count}
            for cat, count in sorted_cats
        ],
    }
