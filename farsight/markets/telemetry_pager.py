"""Scrollable history views over a session JSONL.

`tail` streams the live feed; these commands drill into full history with
a pager so you can scroll through every opportunity, trade, or scan cycle
emitted during the run.

Readers only — never writes. Multiple pagers can follow the same session.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Optional

from rich.console import Console
from rich.table import Table
from rich.text import Text

from farsight.markets.telemetry import resolve_session_file

console = Console()


def _iter_events(path: Path, kinds: Optional[set[str]] = None,
                 strategy: Optional[str] = None) -> Iterable[dict]:
    """Yield filtered events from an entire session file."""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            if kinds and ev.get("kind") not in kinds:
                continue
            if strategy and ev.get("strategy") != strategy:
                continue
            yield ev


def _resolve(session: Optional[str]) -> Optional[Path]:
    path = resolve_session_file(session)
    if not path:
        console.print("[red]No telemetry session found.[/red]")
    return path


def cmd_opps(session: Optional[str] = None, strategy: Optional[str] = None) -> None:
    """Page through every opportunity emitted this session."""
    path = _resolve(session)
    if not path:
        return
    table = Table(title=f"Opportunities — {path.stem}", show_header=True,
                  header_style="bold magenta")
    table.add_column("time", style="dim", width=12)
    table.add_column("strategy", width=10)
    table.add_column("slug", overflow="fold", max_width=50)
    table.add_column("p", justify="right")
    table.add_column("fv", justify="right")
    table.add_column("edge", justify="right")
    table.add_column("conf", justify="right")
    table.add_column("liq", justify="right", style="dim")
    n = 0
    for ev in _iter_events(path, kinds={"opportunity"}, strategy=strategy):
        d = ev.get("data") or {}
        edge = d.get("edge", 0)
        table.add_row(
            ev.get("ts", "")[11:23],
            ev.get("strategy") or "—",
            d.get("slug", "?") or "?",
            f"{d.get('price', 0):.2f}",
            f"{d.get('fair_value', 0):.2f}",
            Text(f"{edge:+.1%}", style="magenta" if edge > 0 else "red"),
            f"{d.get('confidence', 0):.0%}",
            f"${d.get('liquidity', 0):,.0f}",
        )
        n += 1
    with console.pager(styles=True):
        console.print(table)
    console.print(f"[dim]{n} opportunities in {path.name}[/dim]")


def cmd_trades(session: Optional[str] = None, strategy: Optional[str] = None) -> None:
    """Page through every trade (open + close) this session."""
    path = _resolve(session)
    if not path:
        return
    table = Table(title=f"Trades — {path.stem}", show_header=True,
                  header_style="bold green")
    table.add_column("time", style="dim", width=12)
    table.add_column("kind", width=6)
    table.add_column("strategy", width=10)
    table.add_column("slug", overflow="fold", max_width=50)
    table.add_column("dir", width=4)
    table.add_column("price", justify="right")
    table.add_column("size/pnl", justify="right")
    opens = closes = 0
    for ev in _iter_events(path, kinds={"trade.open", "trade.close"}, strategy=strategy):
        d = ev.get("data") or {}
        is_open = ev["kind"] == "trade.open"
        kind_cell = Text("OPEN" if is_open else "CLOSE",
                         style="bold green" if is_open else "yellow")
        if is_open:
            price = d.get("entry")
            right = f"${d.get('size_usd', 0):.0f}"
            right_style = "dim"
            opens += 1
        else:
            price = d.get("exit")
            pnl = d.get("pnl") or 0
            right = f"${pnl:+.2f}"
            right_style = "green" if pnl >= 0 else "red"
            closes += 1
        table.add_row(
            ev.get("ts", "")[11:23],
            kind_cell,
            ev.get("strategy") or "—",
            d.get("slug", "?") or "?",
            (d.get("direction") or "").upper(),
            f"{price:.3f}" if isinstance(price, (int, float)) else "—",
            Text(right, style=right_style),
        )
    with console.pager(styles=True):
        console.print(table)
    console.print(f"[dim]{opens} opens, {closes} closes in {path.name}[/dim]")


def cmd_scans(session: Optional[str] = None, strategy: Optional[str] = None) -> None:
    """Page through every scan cycle with its funnel."""
    path = _resolve(session)
    if not path:
        return
    # Group stage events by scan. scan.start resets; scan.end finalises.
    scans: list[dict] = []
    current: Optional[dict] = None
    for ev in _iter_events(path, strategy=strategy):
        kind = ev.get("kind")
        if kind == "scan.start":
            current = {"ts": ev.get("ts", ""), "strategy": ev.get("strategy", "—"),
                       "rows": [], "emitted": None, "elapsed_ms": None}
        elif current is None:
            continue
        elif kind == "stage.enter":
            d = ev.get("data") or {}
            stage = d.get("stage", "?")
            in_count = d.get("input_count") or d.get("fetched") or 0
            current["rows"].append(("enter", stage, in_count, None))
        elif kind == "stage.drop":
            d = ev.get("data") or {}
            current["rows"].append(("drop", d.get("stage", "?"), d.get("count", 0),
                                    d.get("reason")))
        elif kind == "stage.keep":
            d = ev.get("data") or {}
            current["rows"].append(("keep", d.get("stage", "?"), d.get("count", 0), None))
        elif kind == "scan.end":
            d = ev.get("data") or {}
            current["emitted"] = d.get("emitted")
            current["elapsed_ms"] = d.get("elapsed_ms")
            scans.append(current)
            current = None

    with console.pager(styles=True):
        for sc in scans:
            console.rule(
                f"[bold]{sc['strategy']}[/]  {sc['ts'][11:23]}  "
                f"emitted=[bold]{sc['emitted']}[/] "
                f"[dim]{sc['elapsed_ms']}ms[/]",
                style="blue",
            )
            table = Table.grid(padding=(0, 2))
            table.add_column()
            table.add_column(justify="right")
            table.add_column(style="dim")
            for step, stage, n, reason in sc["rows"]:
                sym = {"enter": "▸ in", "drop": "  ×", "keep": "✓ out"}[step]
                style = "yellow" if step == "drop" else ("green" if step == "keep" else "")
                table.add_row(
                    f"{sym}  {stage}",
                    Text(f"{n:+d}" if step == "drop" else str(n), style=style),
                    reason or "",
                )
            console.print(table)
    console.print(f"[dim]{len(scans)} scans in {path.name}[/dim]")
