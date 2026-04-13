"""`farsight dashboard` — live TUI built on rich.

Reads the session JSONL file (same source as `tail`) + the SQLite store to
render panels: portfolio, strategies, last scan funnel, tracked markets,
recent signals, event feed.

Pure consumer — never writes. Multiple dashboards can follow the same
session simultaneously.
"""

from __future__ import annotations

import json
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from farsight.markets.store import LocalStore
from farsight.markets.telemetry import LATEST_POINTER, resolve_session_file

REFRESH_HZ = 2


class DashboardState:
    """Everything the panels need, updated as events stream in."""

    def __init__(self):
        self.session_id: Optional[str] = None
        self.started_at: Optional[float] = None
        self.strategies_requested: list[str] = []
        self.auto_trade: bool = False

        # Per-strategy last scan breakdown.
        # structure: {strategy: [(stage, label, count)]}
        self.last_scan: dict[str, list[tuple[str, str, int]]] = defaultdict(list)
        self.last_scan_emitted: dict[str, int] = {}
        self.last_scan_elapsed_ms: dict[str, int] = {}

        # Rolling recent data.
        # Buffers are generous so the dashboard preserves history within a
        # session; the bottom Event Feed still shows only the last 15 for
        # readability. Use `farsight opps/trades/scans` to page through
        # the full JSONL when you want to scroll.
        self.opportunities: deque = deque(maxlen=200)
        self.signals: deque = deque(maxlen=200)
        self.trades: deque = deque(maxlen=200)
        self.events: deque = deque(maxlen=200)

        # Live portfolio snapshot.
        self.portfolio = {"balance": 0.0, "total_pnl": 0.0, "open_positions": 0}
        self.next_scan_in: Optional[int] = None
        self.last_heartbeat_ts: Optional[str] = None
        # Latest mark per open trade_id: {"mark", "mtm_value", "unrealized_pnl"}
        self.marks: dict[str, dict] = {}

        self._lock = threading.Lock()
        self._current_scan: dict[str, list[tuple[str, str, int]]] = defaultdict(list)

    def load_trades(self, store: LocalStore) -> None:
        """Seed the Trades panel with currently-open positions from SQLite
        so carry-overs from prior sessions are visible immediately."""
        with self._lock:
            for t in store.get_open_trades():
                self.trades.append({
                    "ts": t.get("opened_at", ""),
                    "kind": "trade.open",
                    "trade_id": t.get("id"),
                    "strategy": t.get("strategy") or "—",
                    "slug": (t.get("market_question") or "?")[:60],
                    "outcome": t.get("outcome") or "—",
                    "direction": t.get("direction", ""),
                    "entry": t.get("fill_price"),
                    "exit": None,
                    "size_usd": t.get("size_usd"),
                    "pnl": None,
                    "reason": "(carried over)",
                })

    def ingest(self, ev: dict) -> None:
        kind = ev.get("kind")
        strategy = ev.get("strategy")
        data = ev.get("data") or {}
        with self._lock:
            self.events.append(ev)

            if kind == "session.start":
                self.session_id = ev.get("session")
                self.started_at = time.time()
                self.strategies_requested = list(data.get("strategies") or [])
                self.auto_trade = bool(data.get("auto_trade"))

            elif kind == "scan.start" and strategy:
                self._current_scan[strategy] = []

            elif kind == "stage.enter" and strategy:
                stage = data.get("stage", "?")
                in_count = data.get("input_count") or data.get("fetched") or 0
                self._current_scan[strategy].append((stage, f"in/{stage}", in_count))

            elif kind == "stage.drop" and strategy:
                stage = data.get("stage", "?")
                reason = data.get("reason", "?")[:24]
                count = data.get("count", 0)
                self._current_scan[strategy].append((stage, f"  -{reason}", -count))

            elif kind == "stage.keep" and strategy:
                stage = data.get("stage", "?")
                self._current_scan[strategy].append((stage, f"keep/{stage}", data.get("count", 0)))

            elif kind == "scan.end" and strategy:
                # Promote the current-scan breakdown to "last scan".
                self.last_scan[strategy] = self._current_scan.pop(strategy, [])
                self.last_scan_emitted[strategy] = data.get("emitted", 0)
                self.last_scan_elapsed_ms[strategy] = data.get("elapsed_ms", 0)

            elif kind == "opportunity":
                self.opportunities.append({
                    "ts": ev.get("ts", ""),
                    "strategy": strategy,
                    "slug": data.get("slug", "?"),
                    "price": data.get("price", 0),
                    "fair_value": data.get("fair_value", 0),
                    "edge": data.get("edge", 0),
                    "liquidity": data.get("liquidity", 0),
                    "horizon": data.get("horizon", ""),
                })

            elif kind == "signal":
                self.signals.append({
                    "ts": ev.get("ts", ""),
                    "type": data.get("signal_type") or data.get("type") or "?",
                    "edge": data.get("edge"),
                })

            elif kind == "position.mark":
                tid = data.get("trade_id")
                if tid:
                    self.marks[tid] = {
                        "mark": data.get("mark"),
                        "mtm_value": data.get("mtm_value"),
                        "unrealized_pnl": data.get("unrealized_pnl"),
                    }

            elif kind in ("trade.open", "trade.close"):
                self.trades.append({
                    "ts": ev.get("ts", ""),
                    "kind": kind,
                    "trade_id": data.get("trade_id"),
                    "strategy": strategy or "—",
                    "slug": data.get("slug", "?"),
                    "outcome": data.get("outcome") or "—",
                    "direction": data.get("direction", ""),
                    "entry": data.get("entry"),
                    "exit": data.get("exit"),
                    "size_usd": data.get("size_usd"),
                    "pnl": data.get("pnl"),
                    "reason": data.get("reason", ""),
                })

            elif kind == "portfolio":
                self.portfolio = {
                    "balance": data.get("balance", 0),
                    "total_pnl": data.get("total_pnl", 0),
                    "open_positions": data.get("open_positions", 0),
                }

            elif kind == "heartbeat":
                self.next_scan_in = data.get("next_scan_in")
                self.last_heartbeat_ts = ev.get("ts")
                if "balance" in data:
                    self.portfolio["balance"] = data["balance"]
                if "total_pnl" in data:
                    self.portfolio["total_pnl"] = data["total_pnl"]
                if "open_positions" in data:
                    self.portfolio["open_positions"] = data["open_positions"]


# ── Reader ───────────────────────────────────────────────────────────


def _current_session_id() -> Optional[str]:
    if LATEST_POINTER.is_file():
        try:
            return LATEST_POINTER.read_text(encoding="utf-8").strip() or None
        except OSError:
            return None
    return None


def _reader_thread(
    initial_path: Path,
    state_box: dict,
    stop: threading.Event,
) -> None:
    # SQLite connections are bound to the creating thread, so the reader
    # owns its own LocalStore. The main thread keeps its own for rendering.
    store = LocalStore()
    """Read the session JSONL. If the latest-session pointer changes
    (runner restart), reopen the new file and swap in a fresh DashboardState
    so stale scan/opp data clears.
    """
    path = initial_path
    fh = path.open("r", encoding="utf-8")
    fh.seek(0, 0)
    current_session = path.stem
    last_pointer_check = 0.0

    while not stop.is_set():
        line = fh.readline()
        if line:
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            state_box["state"].ingest(ev)
            continue

        time.sleep(0.1)

        now = time.time()
        if now - last_pointer_check < 2.0:
            continue
        last_pointer_check = now
        new_session = _current_session_id()
        if new_session and new_session != current_session:
            new_path = path.with_name(f"{new_session}.jsonl")
            if new_path.is_file():
                fh.close()
                state_box["state"] = DashboardState()
                state_box["state"].load_trades(store)
                path = new_path
                current_session = new_session
                fh = path.open("r", encoding="utf-8")
                fh.seek(0, 0)


# ── Panels ───────────────────────────────────────────────────────────


def _fmt_duration(secs: float) -> str:
    s = int(secs)
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _header_panel(state: DashboardState, store: LocalStore) -> Panel:
    portfolio = store.get_portfolio()
    uptime = _fmt_duration(time.time() - state.started_at) if state.started_at else "—"
    cash = state.portfolio["balance"] or portfolio.get("current_balance", 0)
    total_pnl = state.portfolio["total_pnl"] or portfolio.get("total_pnl", 0)
    open_trades = store.get_open_trades()
    open_n = len(open_trades) or state.portfolio["open_positions"]
    # Exposure = cost basis sitting in open positions.
    # MtM value = live mark × num_shares, from position.mark telemetry.
    # Equity = cash + MtM (falls back to exposure when marks aren't in yet).
    exposure = sum(t.get("size_usd", 0) for t in open_trades)
    mtm_value = sum(
        (state.marks.get(t["id"]) or {}).get("mtm_value")
        or t.get("size_usd", 0)           # fallback before first mark lands
        for t in open_trades
    )
    unrealized = sum(
        (state.marks.get(t["id"]) or {}).get("unrealized_pnl") or 0
        for t in open_trades
    )
    equity = cash + mtm_value
    total_trades = portfolio.get("total_trades", 0)
    wins = portfolio.get("winning_trades", 0)
    win_rate = f"{wins/total_trades:.0%}" if total_trades else "—"

    mode = "[bold green]AUTO-TRADE[/]" if state.auto_trade else "[bold cyan]MONITOR[/]"
    pnl_style = "green" if total_pnl >= 0 else "red"
    next_scan = f"next scan [bold]{state.next_scan_in}s[/]  " if state.next_scan_in is not None else ""
    unreal_style = "green" if unrealized >= 0 else "red"
    line = Text.from_markup(
        f"session [bold]{state.session_id or '—'}[/]  "
        f"uptime [bold]{uptime}[/]  "
        f"{mode}  {next_scan}"
        f"│  equity [bold]${equity:,.2f}[/]  "
        f"cash [bold]${cash:,.2f}[/]  "
        f"mtm [dim]${mtm_value:,.2f}[/]  "
        f"unreal [{unreal_style}]${unrealized:+,.2f}[/]  "
        f"realized [{pnl_style}]${total_pnl:+,.2f}[/]  "
        f"open [bold]{open_n}[/]  "
        f"win% [bold]{win_rate}[/] ({wins}/{total_trades})"
    )
    return Panel(line, title="Farsight · Live", border_style="orange3")


def _strategies_panel(state: DashboardState) -> Panel:
    table = Table.grid(padding=(0, 1))
    table.add_column(style="bold")
    table.add_column()
    table.add_column(style="dim")
    if not state.strategies_requested:
        table.add_row("—", "", "")
    for s in state.strategies_requested:
        emitted = state.last_scan_emitted.get(s)
        elapsed = state.last_scan_elapsed_ms.get(s)
        status = "●" if s in state.last_scan else "○"
        detail = f"emitted={emitted}  {elapsed}ms" if emitted is not None else "idle"
        table.add_row(f"{status} {s}", "", detail)
    return Panel(table, title="Strategies", border_style="cyan")


def _last_scan_panel(state: DashboardState) -> Panel:
    if not state.last_scan:
        return Panel(Text("waiting for first scan…", style="dim"),
                     title="Last Scan", border_style="blue")
    blocks = []
    for strategy, rows in state.last_scan.items():
        t = Table.grid(padding=(0, 1))
        t.add_column(style="dim", width=22)
        t.add_column(justify="right")
        for _stage, label, count in rows:
            style = "yellow" if count < 0 else ("green" if label.startswith("keep") else "")
            t.add_row(label, Text(f"{count:+d}" if count < 0 else str(count), style=style))
        blocks.append(Panel(t, title=f"[bold]{strategy}[/]", border_style="dim"))
    return Panel(Group(*blocks), title="Last Scan", border_style="blue")


def _opportunities_panel(state: DashboardState) -> Panel:
    t = Table(show_header=True, header_style="bold", padding=(0, 1), expand=True)
    t.add_column("strategy", style="dim", width=10, no_wrap=True)
    t.add_column("slug", overflow="ellipsis", ratio=1, no_wrap=True)
    t.add_column("price", justify="right", width=5)
    t.add_column("fv", justify="right", width=5)
    t.add_column("edge", justify="right", width=7)
    t.add_column("liq", justify="right", style="dim", width=10)
    for o in list(state.opportunities)[-25:]:
        edge = o["edge"]
        edge_style = "bold magenta" if edge > 0 else "red"
        t.add_row(
            o["strategy"] or "—",
            o["slug"],
            f"{o['price']:.2f}",
            f"{o['fair_value']:.2f}",
            Text(f"{edge:+.1%}", style=edge_style),
            f"${o['liquidity']:,.0f}",
        )
    return Panel(t, title="Recent Opportunities", border_style="magenta")


def _trades_panel(state: DashboardState) -> Panel:
    t = Table(show_header=True, header_style="bold", padding=(0, 1), expand=True)
    t.add_column("time", style="dim", width=8, no_wrap=True)
    t.add_column("kind", width=5, no_wrap=True)
    t.add_column("strategy", style="cyan", width=10, no_wrap=True)
    t.add_column("market", overflow="ellipsis", ratio=1, no_wrap=True)
    t.add_column("side", style="magenta", width=12, no_wrap=True)
    t.add_column("entry", justify="right", width=6)
    t.add_column("mark", justify="right", width=6)
    t.add_column("pnl / $", justify="right", width=10)
    for tr in list(state.trades)[-25:]:
        is_open = tr["kind"] == "trade.open"
        kind_cell = Text("OPEN" if is_open else "CLOSE",
                         style="bold green" if is_open else "yellow")
        entry = tr.get("entry")
        entry_cell = f"{entry:.3f}" if isinstance(entry, (int, float)) else "—"

        # mark + pnl column — live for opens, realized for closes
        mark_cell = "—"
        if is_open:
            mark_info = state.marks.get(tr.get("trade_id")) if hasattr(state, "marks") else None
            if mark_info and mark_info.get("mark") is not None:
                mark_cell = f"{mark_info['mark']:.3f}"
                upnl = mark_info.get("unrealized_pnl")
                if upnl is not None:
                    right = f"${upnl:+.2f}"
                    right_style = "green" if upnl >= 0 else "red"
                else:
                    right = f"${tr.get('size_usd', 0):.0f}"
                    right_style = "dim"
            else:
                right = f"${tr.get('size_usd', 0):.0f}"
                right_style = "dim"
        else:
            mark_cell = f"{tr.get('exit', 0):.3f}" if isinstance(tr.get("exit"), (int, float)) else "—"
            pnl = tr.get("pnl") or 0
            right = f"${pnl:+.2f}"
            right_style = "green" if pnl >= 0 else "red"

        strategy_cell = (tr.get("strategy") or "—")[:12]
        outcome = tr.get("outcome") or "—"
        t.add_row(
            tr["ts"][11:19], kind_cell, strategy_cell, tr["slug"],
            outcome[:12],
            entry_cell, mark_cell,
            Text(right, style=right_style),
        )
    return Panel(t, title="Trades", border_style="green")


def _signals_panel(state: DashboardState) -> Panel:
    t = Table(show_header=True, header_style="bold", padding=(0, 1))
    t.add_column("time", style="dim", width=8)
    t.add_column("type")
    t.add_column("edge", justify="right")
    for s in list(state.signals)[-20:]:
        edge = s.get("edge")
        edge_txt = f"{edge:+.2%}" if isinstance(edge, (int, float)) else "—"
        t.add_row(s["ts"][11:19], s["type"], edge_txt)
    return Panel(t, title="Recent Signals", border_style="cyan")


def _feed_panel(state: DashboardState) -> Panel:
    from farsight.markets.telemetry_tail import _format as fmt_line
    lines = [fmt_line(ev) for ev in list(state.events)[-15:]]
    return Panel(Group(*lines) if lines else Text("…", style="dim"),
                 title="Event Feed", border_style="green")


def _render(state: DashboardState, store: LocalStore) -> Layout:
    layout = Layout()
    # Proportional split keeps every panel visible on small terminals.
    layout.split(
        Layout(name="header", size=3),
        Layout(name="body", ratio=3),
        Layout(name="feed", ratio=1, minimum_size=8),
    )
    layout["body"].split_row(Layout(name="left"), Layout(name="right"))
    layout["left"].split(
        Layout(name="strategies", size=5),
        Layout(name="scan", ratio=1),
    )
    # Right column: trades is the primary panel (most important live info),
    # opportunities second, signals last (often empty).
    layout["right"].split(
        Layout(name="trades", ratio=3),
        Layout(name="opps", ratio=2),
        Layout(name="signals", ratio=1, minimum_size=5),
    )

    layout["header"].update(_header_panel(state, store))
    layout["strategies"].update(_strategies_panel(state))
    layout["scan"].update(_last_scan_panel(state))
    layout["trades"].update(_trades_panel(state))
    layout["opps"].update(_opportunities_panel(state))
    layout["signals"].update(_signals_panel(state))
    layout["feed"].update(_feed_panel(state))
    return layout


def run_dashboard(session: Optional[str] = None) -> None:
    console = Console()
    path = resolve_session_file(session)
    if not path:
        console.print("[dim]Waiting for runner to start a session…  (Ctrl-C to abort)[/dim]")
        try:
            while path is None:
                time.sleep(1.0)
                path = resolve_session_file(session)
        except KeyboardInterrupt:
            return
    console.print(f"[dim]Following {path}[/dim]  (Ctrl-C to exit)")
    state_box = {"state": DashboardState()}
    store = LocalStore()
    state_box["state"].load_trades(store)

    stop = threading.Event()
    t = threading.Thread(target=_reader_thread, args=(path, state_box, stop), daemon=True)
    t.start()
    try:
        with Live(_render(state_box["state"], store),
                  refresh_per_second=REFRESH_HZ, screen=True) as live:
            while True:
                time.sleep(1 / REFRESH_HZ)
                live.update(_render(state_box["state"], store))
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
