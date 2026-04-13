"""
Farsight Prediction Markets CLI

A developer tool for exploring, analyzing, and streaming prediction market data.
Wraps the Polymarket Gamma + CLOB APIs with normalization and signal detection.

Usage:
    python -m farsight.markets <command> [options]

Commands:
    run                     Start the signal generation pipeline (interactive)
    status                  Show local store stats
    health                  Test API connectivity
    markets                 Browse top markets
    events                  Browse top events
    market <slug>           Market detail with outcomes
    event <slug>            Event detail with price consistency check
    book <token_id>         L2 orderbook for a token
    price <token_id>        Current price + spread
    features <token_id>     Compute feature vector from live data
    analyze <slug>          Full market or event analysis with signals
    stream                  Stream live WebSocket data
    tags                    List all Polymarket tags
    serve                   Start the API server on :8001
"""

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone

# Fix Windows console encoding for Unicode characters (e.g., Péter Magyar)
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

# ---- Branding ----

ORANGE = "\033[38;5;208m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
WHITE = "\033[97m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"

LOGO = (
    f"\n"
    f"  {ORANGE}    ______              _       __    __\n"
    f"   / ____/___ ________ (_)___ _/ /_  / /_\n"
    f"  / /_  / __ `/ ___/ ___/ / __ `/ __ \\/ __/\n"
    f" / __/ / /_/ / /  (__  ) / /_/ / / / / /_\n"
    f"/_/    \\__,_/_/  /____/_/\\__, /_/ /_/\\__/\n"
    f"                        /____/{RESET}\n"
    f"  {DIM}Prediction Markets{RESET} {DIM}v0.1{RESET}\n"
)

BANNER_SHORT = f"{ORANGE}Oracle{RESET} {DIM}Prediction Markets{RESET}"


def _ok(msg):
    return f"  {GREEN}OK{RESET}  {msg}"


def _fail(msg):
    return f"  {RED}FAIL{RESET}  {msg}"


def _warn(msg):
    return f"  {YELLOW}WARN{RESET}  {msg}"


def _header(title):
    print(f"\n  {BOLD}{title}{RESET}")
    print(f"  {DIM}{'=' * len(title)}{RESET}\n")


def _subheader(title):
    print(f"\n  {DIM}{title}{RESET}")
    print(f"  {DIM}{'-' * len(title)}{RESET}")


def _fmt_pct(p):
    if p is None:
        return f"{DIM}--{RESET}"
    return f"{float(p):.0%}"


def _fmt_usd(v):
    if v is None or v == 0:
        return f"{DIM}$0{RESET}"
    v = float(v)
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v / 1_000:.0f}K"
    return f"${v:,.0f}"


def _fmt_delta(d):
    if d is None:
        return f"{DIM}--{RESET}"
    color = GREEN if d > 0 else RED if d < 0 else ""
    return f"{color}{d:+.1%}{RESET}"


# ---- Commands ----


async def cmd_health(args):
    from farsight.markets.clients.polymarket.gamma_client import GammaClient
    from farsight.markets.clients.polymarket.clob_client import ClobClient

    _header("API Connectivity")
    results = []

    gamma = GammaClient()
    clob = ClobClient()
    markets = []

    try:
        start = time.time()
        markets = await gamma.get_markets(limit=1)
        ms = (time.time() - start) * 1000
        results.append(_ok(f"Gamma API         {DIM}{ms:.0f}ms{RESET}"))
    except Exception as e:
        results.append(_fail(f"Gamma API         {e}"))

    if markets:
        clob_ids = markets[0].get("clobTokenIds", "[]")
        if isinstance(clob_ids, str):
            clob_ids = json.loads(clob_ids)
        if clob_ids:
            try:
                start = time.time()
                book = await clob.get_orderbook(str(clob_ids[0]))
                ms = (time.time() - start) * 1000
                mid = f"mid={book.mid:.4f}" if book else "empty"
                results.append(_ok(f"CLOB Orderbook    {DIM}{ms:.0f}ms  {mid}{RESET}"))
            except Exception as e:
                results.append(_fail(f"CLOB Orderbook    {e}"))

            try:
                start = time.time()
                history = await clob.get_price_history(str(clob_ids[0]), interval="1h", fidelity=10)
                ms = (time.time() - start) * 1000
                results.append(_ok(f"CLOB History      {DIM}{ms:.0f}ms  {len(history)} points{RESET}"))
            except Exception as e:
                results.append(_fail(f"CLOB History      {e}"))

    results.append(f"  {DIM}--{RESET}   WebSocket         wss://ws-subscriptions-clob.polymarket.com/ws/market")
    results.append(f"  {DIM}--{RESET}   {DIM}(use 'stream' command to test){RESET}")

    for r in results:
        print(r)

    await gamma.close()
    await clob.close()
    print()


async def cmd_markets(args):
    from farsight.markets.clients.polymarket.gamma_client import GammaClient

    gamma = GammaClient()
    try:
        raw = await gamma.get_markets(
            active=True, closed=False, limit=args.limit,
            order=args.order, ascending=False,
        )
        _header(f"Top {len(raw)} Markets (by {args.order})")

        for i, m in enumerate(raw, 1):
            q = m.get("question", "?")[:65]
            vol = float(m.get("volumeNum") or m.get("volume") or 0)
            prices = m.get("outcomePrices", "[]")
            if isinstance(prices, str):
                prices = json.loads(prices)
            yes = float(prices[0]) if prices else 0
            slug = m.get("slug", "")
            change = m.get("oneDayPriceChange")
            change_str = _fmt_delta(change) if change else ""

            print(f"  {BOLD}{i:3}.{RESET} {q}")
            print(f"       {CYAN}{yes:.0%}{RESET} {change_str}  {DIM}vol {_fmt_usd(vol)}{RESET}  {DIM}{slug}{RESET}")
            print()
    finally:
        await gamma.close()


async def cmd_events(args):
    from farsight.markets.clients.polymarket.gamma_client import GammaClient

    gamma = GammaClient()
    try:
        raw = await gamma.get_events(
            active=True, closed=False, limit=args.limit,
            order=args.order, ascending=False,
        )
        _header(f"Top {len(raw)} Events")

        for i, e in enumerate(raw, 1):
            title = e.get("title", "?")[:65]
            markets = e.get("markets", [])
            slug = e.get("slug", "")

            print(f"  {BOLD}{i:3}.{RESET} {title}")
            print(f"       {DIM}{len(markets)} markets  |  {slug}{RESET}")
            for m in markets[:3]:
                prices = m.get("outcomePrices", "[]")
                if isinstance(prices, str):
                    prices = json.loads(prices)
                yes = float(prices[0]) if prices else 0
                q = m.get("question", "?")[:55]
                print(f"       {CYAN}{yes:>5.0%}{RESET}  {q}")
            if len(markets) > 3:
                print(f"       {DIM}... +{len(markets) - 3} more{RESET}")
            print()
    finally:
        await gamma.close()


async def cmd_market_detail(args):
    from farsight.markets.clients.polymarket.gamma_client import GammaClient

    gamma = GammaClient()
    try:
        raw = await gamma.get_market_by_slug(args.slug)
        if not raw:
            print(_fail(f"Market not found: {args.slug}"))
            return

        market = GammaClient.normalize_market(raw)
        _header(market.question)

        print(f"  Status       {market.status.value}")
        print(f"  Volume       {_fmt_usd(market.volume_total)}")
        print(f"  Liquidity    {_fmt_usd(market.liquidity)}")
        print(f"  End date     {market.end_date or 'None'}")
        print(f"  Condition    {DIM}{market.condition_id[:50]}...{RESET}")
        print(f"  Neg-risk     {market.neg_risk}")

        _subheader("Outcomes")
        for o in market.outcomes:
            print(f"  {o.label:15} {CYAN}{_fmt_pct(o.current_price):>5}{RESET}  {DIM}token: {o.token_id[:35]}...{RESET}")
        print()
    finally:
        await gamma.close()


async def cmd_event_detail(args):
    from farsight.markets.clients.polymarket.gamma_client import GammaClient

    gamma = GammaClient()
    try:
        raw = await gamma.get_event_by_slug(args.slug)
        if not raw:
            print(_fail(f"Event not found: {args.slug}"))
            return

        event = GammaClient.normalize_event(raw)
        _header(event.title)

        print(f"  Category     {event.category or 'uncategorized'}")
        print(f"  Status       {event.status.value}")
        print(f"  Volume       {_fmt_usd(event.volume_total)}")
        print(f"  Markets      {len(event.markets)}")

        price_sum = 0
        _subheader("Outcomes")
        for m in event.markets:
            if m.outcomes:
                price = m.outcomes[0].current_price
                price_sum += price
                label = m.outcomes[0].label
                vol = _fmt_usd(m.volume_total)
                print(f"  {CYAN}{_fmt_pct(price):>5}{RESET}  {label:20} {DIM}vol {vol}{RESET}")

        _subheader("Structural Consistency")
        if price_sum > 1.03:
            print(f"  {RED}Prices sum to {price_sum:.1%} -- OVERPRICED by {price_sum - 1:.1%}{RESET}")
        elif price_sum < 0.97:
            print(f"  {YELLOW}Prices sum to {price_sum:.1%} -- UNDERPRICED by {1 - price_sum:.1%}{RESET}")
        else:
            print(f"  {GREEN}Prices sum to {price_sum:.1%} -- within normal range{RESET}")
        print()
    finally:
        await gamma.close()


async def cmd_book(args):
    from farsight.markets.clients.polymarket.clob_client import ClobClient

    clob = ClobClient()
    try:
        book = await clob.get_orderbook(args.token_id)
        if not book:
            print(_fail("Could not fetch orderbook"))
            return

        _header("Orderbook")
        print(f"  Bid {CYAN}{book.best_bid:.4f}{RESET}   Ask {CYAN}{book.best_ask:.4f}{RESET}   "
              f"Spread {CYAN}{book.spread:.4f}{RESET}   Mid {CYAN}{book.mid:.4f}{RESET}")
        print(f"  Bid depth {_fmt_usd(book.total_bid_depth)}   Ask depth {_fmt_usd(book.total_ask_depth)}")

        _subheader("Top 5 Levels")
        print(f"  {'BIDS':>25}   {'ASKS':<25}")
        for i in range(min(5, max(len(book.bids), len(book.asks)))):
            b = ""
            a = ""
            if i < len(book.bids):
                bk = book.bids[i]
                b = f"{_fmt_usd(bk.size * bk.price):>8} @ {bk.price:.4f}"
            if i < len(book.asks):
                ak = book.asks[i]
                a = f"{ak.price:.4f} @ {_fmt_usd(ak.size * ak.price)}"
            print(f"  {b:>25}   {a:<25}")
        print()
    finally:
        await clob.close()


async def cmd_price(args):
    from farsight.markets.clients.polymarket.clob_client import ClobClient

    clob = ClobClient()
    try:
        price = await clob.get_price(args.token_id)
        spread = await clob.get_spread(args.token_id)

        _header("Price")
        print(f"  Price      {CYAN}{price}{RESET}")
        if spread:
            print(f"  Bid        {spread.get('bid', '?')}")
            print(f"  Ask        {spread.get('ask', '?')}")
            print(f"  Spread     {spread.get('spread', '?')}")
        print()
    finally:
        await clob.close()


async def cmd_features(args):
    from farsight.markets.clients.polymarket.clob_client import ClobClient
    from farsight.markets.services.state_engine import MarketState
    from farsight.markets.services.feature_engine import compute_features

    clob = ClobClient()
    try:
        book = await clob.get_orderbook(args.token_id)
        if not book:
            print(_fail("Could not fetch orderbook"))
            return

        state = MarketState(args.token_id)
        history = await clob.get_price_history(args.token_id, interval="1m", fidelity=300)
        for pt in history:
            try:
                ts = datetime.fromtimestamp(int(pt["t"]), tz=timezone.utc).replace(tzinfo=None)
                price = float(pt["p"])
                if price > 0:
                    state.update_price(ts, mid=price)
            except (ValueError, TypeError, KeyError):
                continue

        now = datetime.utcnow()
        state.update_price(now, mid=book.mid, bid=book.best_bid, ask=book.best_ask)
        state.update_book(book.total_bid_depth, book.total_ask_depth, book.best_bid, book.best_ask)
        features = compute_features(state)

        _header(f"Features ({len(history)} history points + live book)")

        groups = {
            "Microstructure": ["spread_pct", "depth_imbalance", "trade_imbalance_5m",
                               "trade_imbalance_1h", "quote_velocity", "trade_velocity", "large_trade_ratio"],
            "Probability Dynamics": ["delta_1m", "delta_5m", "delta_15m", "delta_1h", "delta_4h",
                                     "acceleration", "drift_score", "reversion_score", "volatility_burst"],
            "Quality": ["liquidity_score", "stale_score", "manipulation_heuristic", "resolution_proximity_days"],
        }

        for group_name, keys in groups.items():
            _subheader(group_name)
            for k in keys:
                v = features.get(k)
                if v is None:
                    print(f"  {k:35} {DIM}--{RESET}")
                elif isinstance(v, float):
                    print(f"  {k:35} {CYAN}{v:>10.4f}{RESET}")
                else:
                    print(f"  {k:35} {v}")
        print()
    finally:
        await clob.close()


async def cmd_analyze(args):
    """Analyze a market or event slug -- auto-detects which one."""
    from farsight.markets.clients.polymarket.gamma_client import GammaClient

    gamma = GammaClient()
    try:
        # Try as event first (more common for multi-outcome slugs)
        raw = await gamma.get_event_by_slug(args.slug)
        if raw:
            await gamma.close()
            await _analyze_event(args.slug)
            return

        # Try as market
        raw = await gamma.get_market_by_slug(args.slug)
        if raw:
            await gamma.close()
            await _analyze_market(args.slug)
            return

        print(_fail(f"Not found as event or market: {args.slug}"))
    finally:
        await gamma.close()


async def _analyze_market(slug):
    from farsight.markets.clients.polymarket.gamma_client import GammaClient
    from farsight.markets.clients.polymarket.clob_client import ClobClient
    from farsight.markets.services.state_engine import MarketState
    from farsight.markets.services.feature_engine import compute_features
    from farsight.markets.services.signal_engine import SignalEngine

    gamma = GammaClient()
    clob = ClobClient()
    try:
        raw = await gamma.get_market_by_slug(slug)
        market = GammaClient.normalize_market(raw)
        _header(f"Market Analysis: {market.question}")

        engine = SignalEngine()
        all_signals = []

        for outcome in market.outcomes:
            state = MarketState(outcome.token_id)
            history = await clob.get_price_history(outcome.token_id, interval="1m", fidelity=300)
            for pt in history:
                try:
                    ts = datetime.fromtimestamp(int(pt["t"]), tz=timezone.utc).replace(tzinfo=None)
                    price = float(pt["p"])
                    if price > 0:
                        state.update_price(ts, mid=price)
                except (ValueError, TypeError, KeyError):
                    continue

            book = await clob.get_orderbook(outcome.token_id)
            if book:
                now = datetime.utcnow()
                state.update_price(now, mid=book.mid, bid=book.best_bid, ask=book.best_ask)
                state.update_book(book.total_bid_depth, book.total_ask_depth, book.best_bid, book.best_ask)

            features = compute_features(state)
            features["last_price"] = outcome.current_price
            signals = engine.evaluate(features, outcome.token_id)

            bid = f"{book.best_bid:.4f}" if book else "--"
            ask = f"{book.best_ask:.4f}" if book else "--"
            depth = _fmt_usd((book.total_bid_depth + book.total_ask_depth)) if book else "--"
            d5m = _fmt_delta(features.get("delta_5m"))

            print(f"  {BOLD}{outcome.label:10}{RESET} {CYAN}{_fmt_pct(outcome.current_price):>5}{RESET}  "
                  f"bid={bid} ask={ask}  depth={depth}  d5m={d5m}  "
                  f"{DIM}{len(history)} pts{RESET}")

            for s in signals:
                print(f"           {YELLOW}>> {s.signal_type.value}{RESET} "
                      f"{s.direction.value} conf={s.confidence:.2f} edge={s.edge:+.2%}")
            all_signals.extend(signals)

        if not all_signals:
            print(f"\n  {DIM}No signals detected. Market is quiet.{RESET}")
        print()
    finally:
        await gamma.close()
        await clob.close()


async def _analyze_event(slug):
    from farsight.markets.clients.polymarket.gamma_client import GammaClient
    from farsight.markets.clients.polymarket.clob_client import ClobClient
    from farsight.markets.services.state_engine import MarketState
    from farsight.markets.services.feature_engine import compute_features
    from farsight.markets.services.signal_engine import SignalEngine

    gamma = GammaClient()
    clob = ClobClient()
    try:
        raw = await gamma.get_event_by_slug(slug)
        event = GammaClient.normalize_event(raw)
        _header(f"Event Analysis: {event.title}")

        print(f"  Category     {event.category or 'uncategorized'}")
        print(f"  Markets      {len(event.markets)}")
        print(f"  Volume       {_fmt_usd(event.volume_total)}")

        engine = SignalEngine()
        price_sum = 0.0
        all_signals = []
        active_outcomes = []
        skipped = 0

        # Sort markets by price descending (show leaders first)
        sorted_markets = sorted(
            [m for m in event.markets if m.outcomes],
            key=lambda m: m.outcomes[0].current_price,
            reverse=True,
        )

        _subheader("Outcomes")
        for market in sorted_markets:
            primary = market.outcomes[0]
            price_sum += primary.current_price

            # Skip near-zero outcomes with no orderbook (noise)
            if primary.current_price < 0.005:
                skipped += 1
                continue

            state = MarketState(primary.token_id)
            history = await clob.get_price_history(primary.token_id, interval="1m", fidelity=300)
            for pt in history:
                try:
                    ts = datetime.fromtimestamp(int(pt["t"]), tz=timezone.utc).replace(tzinfo=None)
                    price = float(pt["p"])
                    if price > 0:
                        state.update_price(ts, mid=price)
                except (ValueError, TypeError, KeyError):
                    continue

            book = await clob.get_orderbook(primary.token_id)
            if book and book.mid > 0:
                now = datetime.utcnow()
                state.update_price(now, mid=book.mid, bid=book.best_bid, ask=book.best_ask)
                state.update_book(book.total_bid_depth, book.total_ask_depth, book.best_bid, book.best_ask)

            features = compute_features(state)
            features["last_price"] = primary.current_price
            signals = engine.evaluate(features, primary.token_id)

            d5m = _fmt_delta(features.get("delta_5m"))
            depth = _fmt_usd((book.total_bid_depth + book.total_ask_depth)) if book and book.mid > 0 else f"{DIM}--{RESET}"
            vol = _fmt_usd(market.volume_total)

            print(f"  {CYAN}{_fmt_pct(primary.current_price):>5}{RESET}  "
                  f"{primary.label:25} d5m={d5m}  depth={depth}  vol={vol}")

            for s in signals:
                print(f"        {YELLOW}>> {s.signal_type.value}{RESET} "
                      f"{s.direction.value} conf={s.confidence:.2f}")
            all_signals.extend(signals)
            active_outcomes.append(primary.label)

        if skipped:
            print(f"  {DIM}... +{skipped} outcomes below 1% (skipped){RESET}")

        _subheader("Structural Consistency")
        deviation = abs(price_sum - 1.0)
        if price_sum > 1.03:
            print(f"  {RED}Prices sum to {price_sum:.1%} -- OVERPRICED by {deviation:.1%}{RESET}")
            print(f"  {DIM}Structural arbitrage: some outcomes are overvalued relative to the field.{RESET}")
        elif price_sum < 0.97:
            print(f"  {YELLOW}Prices sum to {price_sum:.1%} -- UNDERPRICED by {deviation:.1%}{RESET}")
        else:
            print(f"  {GREEN}Prices sum to {price_sum:.1%} -- consistent{RESET}")

        if all_signals:
            _subheader(f"Signals ({len(all_signals)})")
            for s in all_signals:
                print(f"  {YELLOW}{s.signal_type.value:30}{RESET} "
                      f"{s.direction.value:8} conf={s.confidence:.2f} edge={s.edge:+.2%}")
        else:
            print(f"\n  {DIM}No signals detected. Market is quiet.{RESET}")
        print()
    finally:
        await gamma.close()
        await clob.close()


async def cmd_stream(args):
    from farsight.markets.clients.polymarket.gamma_client import GammaClient
    from farsight.markets.clients.polymarket.ws_client import PolymarketWsClient
    from farsight.markets.engine.checkpoint import MemoryCheckpointStore
    from farsight.markets.engine.event_bus import EventBus

    gamma = GammaClient()
    try:
        raw = await gamma.get_markets(active=True, limit=args.markets, order="volume")
    finally:
        await gamma.close()

    token_ids = set()
    names = {}
    for m in raw:
        clob_ids = m.get("clobTokenIds", "[]")
        if isinstance(clob_ids, str):
            clob_ids = json.loads(clob_ids)
        q = m.get("question", "?")[:45]
        for tid in clob_ids:
            token_ids.add(str(tid))
            names[str(tid)] = q

    bus = EventBus()
    counts = {"ticks": 0, "trades": 0, "books": 0}

    async def on_tick(p):
        counts["ticks"] += 1
        name = names.get(p.get("token_id", ""), "?")
        mid = p.get("mid", 0)
        bid = p.get("bid", 0)
        ask = p.get("ask", 0)
        if mid > 0:
            print(f"  {CYAN}TICK{RESET}   mid={mid:.4f}  bid={bid:.4f}  ask={ask:.4f}  {DIM}{name}{RESET}")

    async def on_trade(p):
        counts["trades"] += 1
        name = names.get(p.get("token_id", ""), "?")
        size = p.get("size_usd", 0)
        price = p.get("price", 0)
        side = p.get("side", "")
        color = GREEN if side == "buy" else RED
        whale = f"  {YELLOW}WHALE{RESET}" if size > 5000 else ""
        print(f"  {color}TRADE{RESET}  ${size:>8,.2f} @ {price:.4f} {side:4}{whale}  {DIM}{name}{RESET}")

    async def on_book(p):
        counts["books"] += 1

    bus.subscribe("raw.price_tick", on_tick)
    bus.subscribe("raw.trade_print", on_trade)
    bus.subscribe("raw.orderbook", on_book)

    ws = PolymarketWsClient(bus, MemoryCheckpointStore())
    await ws.update_subscriptions(token_ids)

    _header(f"Live Stream ({args.seconds}s)")
    print(f"  Watching {len(token_ids)} tokens from top {len(raw)} markets\n")

    task = asyncio.create_task(ws.connect())
    await asyncio.sleep(args.seconds)
    await ws.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    health = ws.get_health()
    print(f"\n  {DIM}Stream complete: {counts['ticks']} ticks, {counts['trades']} trades, {counts['books']} books{RESET}")
    if health.get("reconnect_count", 0) > 0:
        print(_warn(f"Reconnected {health['reconnect_count']} times"))
    print()


async def cmd_tags(args):
    from farsight.markets.clients.polymarket.gamma_client import GammaClient

    gamma = GammaClient()
    try:
        tags = await gamma.get_tags()
        _header(f"Polymarket Tags ({len(tags)})")
        for t in sorted(tags, key=lambda x: x.get("label", "")):
            print(f"  {DIM}{t.get('id', '?'):>5}{RESET}  {t.get('label', '?')}")
        print()
    finally:
        await gamma.close()


def _cmd_status():
    """Show local store stats (sync, no async needed)."""
    from farsight.markets.store import LocalStore

    store = LocalStore()
    stats = store.get_stats()
    portfolio = store.get_portfolio()
    signals = store.get_recent_signals(limit=5)
    open_trades = store.get_open_trades()
    pnl = portfolio["total_pnl"]
    pnl_color = GREEN if pnl >= 0 else RED

    _header("Local Store Status")
    print(f"  Database:        {DIM}{stats['db_path']}{RESET}")
    print(f"  Signals:         {stats['total_signals']} total")
    print(f"  Trades:          {stats['total_trades']} total, {stats['open_trades']} open")
    print(f"  Subscriptions:   {stats['subscriptions']}")
    print(f"  Portfolio:       ${portfolio['current_balance']:,.2f}  "
          f"PnL: {pnl_color}${pnl:+,.2f}{RESET}")

    if signals:
        _subheader("Recent Signals")
        for s in signals:
            dir_color = GREEN if s["direction"] == "bullish" else RED
            print(f"  {s['created_at'][:19]}  {s['signal_type']:25} "
                  f"{dir_color}{s['direction']:8}{RESET} conf={s['confidence']:.0%}")

    if open_trades:
        _subheader("Open Trades")
        for t in open_trades[:5]:
            q = (t.get("market_question") or "?")[:40]
            print(f"  {t['outcome']:4} ${t['size_usd']:>7,.2f} @ {t['entry_price']:.4f}  {DIM}{q}{RESET}")

    store.close()
    print()


def _cmd_kpi():
    """Show KPIs computed from signal_outcomes. Phase 0 feedback loop."""
    from farsight.markets.store import LocalStore

    store = LocalStore()
    summary = store.kpi_summary()

    _header("Signal Quality KPIs")
    print(f"  {'Horizon':<10} {'N':>6}  {'Hit rate':>10}  {'Avg edge':>12}")
    print(f"  {DIM}{'-' * 44}{RESET}")
    for horizon in ["1h", "4h", "24h", "final"]:
        row = summary.get(horizon, {})
        n = row.get("n", 0)
        hit = row.get("hit_rate", 0.0)
        avg = row.get("avg_edge", 0.0)
        edge_color = GREEN if avg > 0 else (RED if avg < 0 else DIM)
        if n == 0:
            print(f"  {horizon:<10} {n:>6}  {DIM}{'—':>10}{RESET}  {DIM}{'—':>12}{RESET}")
        else:
            print(f"  {horizon:<10} {n:>6}  {hit:>9.1%}  {edge_color}{avg:>+11.4f}{RESET}")

    by_type = summary.get("by_type") or []
    if by_type:
        _subheader("By signal type (1h horizon)")
        print(f"  {'Type':<28} {'N':>5}  {'Avg 1h':>10}  {'Avg final':>12}")
        print(f"  {DIM}{'-' * 60}{RESET}")
        for r in by_type:
            n = r.get("n") or 0
            e1 = r.get("avg_edge_1h") or 0.0
            ef = r.get("avg_edge_final")
            ef_str = f"{ef:>+11.4f}" if ef is not None else f"{DIM}{'—':>12}{RESET}"
            print(f"  {r['signal_type']:<28} {n:>5}  {e1:>+9.4f}  {ef_str}")
    else:
        print(f"\n  {DIM}No scored signals yet. Run the bot long enough for T+1h captures to land.{RESET}")

    store.close()
    print()


def _cmd_sessions():
    """List recent bot sessions."""
    from farsight.markets.store import LocalStore

    store = LocalStore()
    sessions = store.get_recent_sessions(limit=20)

    _header("Recent Sessions")
    if not sessions:
        print(f"  {DIM}No sessions recorded yet.{RESET}\n")
        store.close()
        return

    print(f"  {'Started':<20} {'Dur':>6}  {'Sig':>4}  {'Supp':>5}  {'Trd':>4}  {'Strategies':<30} {'Config':<10}")
    print(f"  {DIM}{'-' * 90}{RESET}")
    for s in sessions:
        started = (s.get("started_at") or "")[:19]
        ended = s.get("ended_at")
        if ended:
            try:
                dur_s = (datetime.fromisoformat(ended) - datetime.fromisoformat(s["started_at"])).total_seconds()
                dur = f"{dur_s / 60:.0f}m"
            except Exception:
                dur = "?"
        else:
            dur = f"{DIM}live{RESET}"
        print(f"  {started:<20} {dur:>6}  {s.get('signals_emitted', 0):>4}  "
              f"{s.get('signals_suppressed', 0):>5}  {s.get('trades_opened', 0):>4}  "
              f"{(s.get('strategies') or '')[:30]:<30} {DIM}{(s.get('config_hash') or '')[:8]}{RESET}")
    store.close()
    print()


def cmd_serve(args):
    """Start the prediction markets API server."""
    import uvicorn
    _header(f"Starting API Server on :{args.port}")
    print(f"  Swagger UI:  http://localhost:{args.port}/docs")
    print(f"  ReDoc:       http://localhost:{args.port}/redoc\n")
    uvicorn.run("farsight.markets.app:app", host="0.0.0.0", port=args.port, reload=args.reload)


# ---- Main ----


def main():
    # Parse args first to decide whether to show logo
    # (runner prints its own branded logo)
    if len(sys.argv) < 2 or sys.argv[1] not in ("run",):
        print(LOGO)

    parser = argparse.ArgumentParser(
        prog="farsight-markets",
        description="Prediction Markets Intelligence CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
{DIM}Examples:{RESET}
  python -m farsight.markets health
  python -m farsight.markets markets --limit 10
  python -m farsight.markets event next-prime-minister-of-hungary
  python -m farsight.markets analyze next-prime-minister-of-hungary
  python -m farsight.markets stream --seconds 30
  python -m farsight.markets serve --port 8001
""",
    )
    sub = parser.add_subparsers(dest="command", metavar="command")

    # Pipeline
    p = sub.add_parser("run", help="Start the intelligence bot (interactive)")
    p.add_argument("--strategies", default="scanner,arb,resolution,cross_venue,momentum", help="Strategies: scanner,arb,resolution,cross_venue,momentum")
    p.add_argument("--auto-trade", action="store_true", help="Enable auto paper trading")
    p.add_argument("--max-trades", type=int, default=3, help="Max trades per scan cycle")
    p.add_argument("--stream-markets", type=int, default=20, help="Markets to stream via WebSocket")

    sub.add_parser("status", help="Show local store stats and portfolio")

    sub.add_parser("kpi", help="Show signal-quality KPIs (hit rate, realized edge) from signal_outcomes")
    sub.add_parser("sessions", help="List recent bot sessions")

    # Exploration
    sub.add_parser("health", help="Test API connectivity")

    p = sub.add_parser("markets", help="Browse top markets")
    p.add_argument("--limit", type=int, default=15)
    p.add_argument("--order", default="volume_24hr", help="volume_24hr | volume | liquidity | competitive | end_date")

    p = sub.add_parser("events", help="Browse top events")
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--order", default="volume_24hr")

    p = sub.add_parser("market", help="Market detail by slug")
    p.add_argument("slug")

    p = sub.add_parser("event", help="Event detail with consistency check")
    p.add_argument("slug")

    p = sub.add_parser("book", help="L2 orderbook for a token")
    p.add_argument("token_id")

    p = sub.add_parser("price", help="Current price + spread")
    p.add_argument("token_id")

    p = sub.add_parser("features", help="Compute feature vector from live data")
    p.add_argument("token_id")

    p = sub.add_parser("analyze", help="Full analysis with signals (market or event slug)")
    p.add_argument("slug")

    p = sub.add_parser("stream", help="Stream live WebSocket data")
    p.add_argument("--seconds", type=int, default=15)
    p.add_argument("--markets", type=int, default=3, help="Number of top markets to watch")

    sub.add_parser("tags", help="List all Polymarket tags")

    p = sub.add_parser("serve", help="Start the API server")
    p.add_argument("--port", type=int, default=8001)
    p.add_argument("--reload", action="store_true", default=True)

    p = sub.add_parser("tail", help="Tail the live telemetry event log for the current session")
    p.add_argument("--session", help="Session id (defaults to latest)")
    p.add_argument("--kind", help="Comma-separated event kinds to include (e.g. stage.drop,signal)")
    p.add_argument("--strategy", help="Filter to one strategy")
    p.add_argument("--follow", action="store_true", help="Skip history and only show new events")

    p = sub.add_parser("dashboard", help="Live TUI dashboard (rich-based)")
    p.add_argument("--session", help="Session id (defaults to latest)")

    p = sub.add_parser("opps", help="Page through all opportunities emitted this session")
    p.add_argument("--session", help="Session id (defaults to latest)")
    p.add_argument("--strategy", help="Filter to one strategy")

    p = sub.add_parser("trades", help="Page through all trades (open+close) this session")
    p.add_argument("--session", help="Session id (defaults to latest)")
    p.add_argument("--strategy", help="Filter to one strategy")

    p = sub.add_parser("scans", help="Page through all scan cycles + their funnels")
    p.add_argument("--session", help="Session id (defaults to latest)")
    p.add_argument("--strategy", help="Filter to one strategy")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    # Sync commands (no asyncio)
    if args.command == "serve":
        cmd_serve(args)
        return
    if args.command == "run":
        # Suppress all logging before imports — runner manages its own output
        import logging as _log
        _log.disable(_log.INFO)  # Globally disable INFO and below

        from farsight.markets.runner import run_pipeline

        _log.disable(_log.NOTSET)  # Re-enable after imports

        strats = [s.strip() for s in args.strategies.split(",")]
        asyncio.run(run_pipeline(
            strategies=strats,
            auto_trade=args.auto_trade,
            max_trades=args.max_trades,
            stream_markets=args.stream_markets,
        ))
        return
    if args.command == "status":
        _cmd_status()
        return
    if args.command == "kpi":
        _cmd_kpi()
        return
    if args.command == "sessions":
        _cmd_sessions()
        return
    if args.command == "tail":
        from farsight.markets.telemetry_tail import run_tail
        run_tail(session=args.session,
                 kinds=(args.kind.split(",") if args.kind else None),
                 strategy=args.strategy,
                 from_start=not args.follow)
        return
    if args.command == "dashboard":
        from farsight.markets.telemetry_dashboard import run_dashboard
        run_dashboard(session=args.session)
        return
    if args.command == "opps":
        from farsight.markets.telemetry_pager import cmd_opps
        cmd_opps(session=args.session, strategy=args.strategy)
        return
    if args.command == "trades":
        from farsight.markets.telemetry_pager import cmd_trades
        cmd_trades(session=args.session, strategy=args.strategy)
        return
    if args.command == "scans":
        from farsight.markets.telemetry_pager import cmd_scans
        cmd_scans(session=args.session, strategy=args.strategy)
        return

    cmds = {
        "health": cmd_health,
        "markets": cmd_markets,
        "events": cmd_events,
        "market": cmd_market_detail,
        "event": cmd_event_detail,
        "book": cmd_book,
        "price": cmd_price,
        "features": cmd_features,
        "analyze": cmd_analyze,
        "stream": cmd_stream,
        "tags": cmd_tags,
    }
    asyncio.run(cmds[args.command](args))


if __name__ == "__main__":
    main()
