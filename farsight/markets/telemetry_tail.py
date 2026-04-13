"""`farsight tail` — pretty-print the live telemetry JSONL stream."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterable, Optional

from rich.console import Console
from rich.text import Text

from farsight.markets.telemetry import LATEST_POINTER, resolve_session_file

console = Console()

KIND_COLORS = {
    "session.start": "bold green",
    "session.end":   "bold red",
    "scan.start":    "dim",
    "scan.end":      "cyan",
    "stage.enter":   "blue",
    "stage.keep":    "green",
    "stage.drop":    "yellow",
    "opportunity":   "bold magenta",
    "signal":        "bold cyan",
    "trade.open":    "bold green",
    "trade.close":   "bold yellow",
    "trade.executed":"green",
    "trade.print":   "dim",
    "portfolio":     "cyan",
    "tick":          "dim",
    "heartbeat":     "dim",
    "error":         "bold red",
}


def _follow(path: Path, from_start: bool = True) -> Iterable[str]:
    """Yield lines from `path`. Starts at beginning by default so users see
    the session's history immediately; pass `from_start=False` to skip ahead
    to EOF and only see new events.

    Additionally watches the latest-session pointer: if the runner restarts
    and writes a new session, switches over to it automatically. Announces
    the switch with a dimmed header line.
    """
    fh = path.open("r", encoding="utf-8")
    if not from_start:
        fh.seek(0, 2)
    current_session = path.stem
    last_pointer_check = 0.0
    last_inode = path.stat().st_ino if hasattr(path.stat(), "st_ino") else 0
    while True:
        line = fh.readline()
        if line:
            yield line
            continue
        time.sleep(0.2)

        now = time.time()
        if now - last_pointer_check >= 2.0:
            last_pointer_check = now
            try:
                if LATEST_POINTER.is_file():
                    latest = LATEST_POINTER.read_text(encoding="utf-8").strip()
                    if latest and latest != current_session:
                        new_path = path.with_name(f"{latest}.jsonl")
                        if new_path.is_file():
                            fh.close()
                            path = new_path
                            current_session = latest
                            fh = path.open("r", encoding="utf-8")
                            # Resync rotation-detection so the next stat()
                            # doesn't treat this as a rotation and re-open
                            # from position 0 (which caused duplicate lines).
                            try:
                                last_inode = getattr(path.stat(), "st_ino", 0)
                            except OSError:
                                pass
                            yield f'__SESSION_SWITCH__ {latest}\n'
                            continue
            except OSError:
                pass

        try:
            st = path.stat()
        except FileNotFoundError:
            continue
        inode = getattr(st, "st_ino", 0)
        if inode and inode != last_inode:
            fh.close()
            fh = path.open("r", encoding="utf-8")
            last_inode = inode


def _format(event: dict) -> Text:
    ts = event.get("ts", "")[11:23]  # HH:MM:SS.mmm
    kind = event.get("kind", "?")
    strategy = event.get("strategy", "—")
    data = event.get("data", {}) or {}
    color = KIND_COLORS.get(kind, "white")

    head = Text()
    head.append(f"{ts}  ", style="dim")
    head.append(f"{strategy:<12}", style="bold")
    head.append(f"{kind:<14}", style=color)

    if kind == "stage.drop":
        head.append(f"{data.get('stage','?'):<9} ", style="blue")
        head.append(f"reason={data.get('reason','?')} ", style="yellow")
        head.append(f"×{data.get('count','?')}", style="bold yellow")
        samples = data.get("samples") or []
        if samples:
            head.append(f"  ({', '.join(samples[:3])})", style="dim")
    elif kind == "stage.enter":
        head.append(f"{data.get('stage','?'):<9} ", style="blue")
        head.append(f"in={data.get('input_count', data.get('fetched', '?'))}", style="")
        params = data.get("params") or {}
        if params:
            head.append(f"  {params}", style="dim")
    elif kind == "stage.keep":
        head.append(f"{data.get('stage','?'):<9} ", style="blue")
        head.append(f"keep={data.get('count','?')}", style="green")
    elif kind == "opportunity":
        head.append(f"{(data.get('slug') or '?')[:40]:<40} ", style="")
        head.append(f"p={data.get('price',0):.2f} fv={data.get('fair_value',0):.2f} ", style="")
        head.append(f"edge={data.get('edge',0):+.1%} ", style="bold magenta")
        head.append(f"liq=${data.get('liquidity',0):,.0f}", style="dim")
    elif kind == "signal":
        head.append(f"{data.get('signal_type') or data.get('type') or '?'}", style="")
        if "edge" in data:
            head.append(f"  edge={data['edge']:+.2%}", style="dim")
    elif kind == "trade.open":
        head.append(f"{data.get('direction','?').upper()} ", style="bold")
        head.append(f"{(data.get('slug') or '?')[:35]:<35} ", style="")
        head.append(f"@ {data.get('entry',0):.4f} ", style="")
        head.append(f"${data.get('size_usd',0):.2f}", style="green")
    elif kind == "trade.close":
        pnl = data.get("pnl", 0)
        style = "bold green" if pnl >= 0 else "bold red"
        head.append(f"{(data.get('slug') or '?')[:35]:<35} ", style="")
        head.append(f"pnl=${pnl:+.2f} ", style=style)
        head.append(f"({data.get('reason','')})", style="dim")
    elif kind == "portfolio":
        head.append(f"bal=${data.get('balance',0):,.2f}  ", style="")
        head.append(f"pnl=${data.get('total_pnl',0):+,.2f}  ", style="")
        head.append(f"open={data.get('open_positions',0)}", style="dim")
    elif kind == "scan.end":
        head.append(f"emitted={data.get('emitted',0)}  ", style="")
        head.append(f"{data.get('elapsed_ms',0)}ms", style="dim")
    elif kind == "tick":
        slug = data.get("slug") or "?"
        mid = data.get("mid", data.get("price"))
        head.append(f"{slug[:35]:<35} ", style="")
        head.append(f"mid={mid}" if mid is not None else "", style="dim")
    elif kind == "trade.print":
        slug = data.get("slug") or "?"
        price = data.get("price")
        size = data.get("size_usd")
        side = data.get("side", "")
        head.append(f"{slug[:30]:<30} ", style="")
        head.append(f"{side:<4} ", style="")
        if price is not None:
            head.append(f"@ {price} ", style="")
        if size is not None:
            head.append(f"${size:,.0f}", style="dim")
    elif kind == "heartbeat":
        head.append(f"next_scan_in={data.get('next_scan_in','?')}s  ", style="")
        head.append(f"open={data.get('open_positions',0)}  ", style="dim")
        head.append(f"pnl=${data.get('total_pnl',0):+.2f}", style="dim")
    else:
        if data:
            head.append(f" {data}", style="dim")
    return head


def _wait_for_session(session: Optional[str], console: Console) -> Optional[Path]:
    """Block until a session file exists. Useful for compound launches
    where tail/dashboard start before the runner has written the JSONL."""
    path = resolve_session_file(session)
    if path:
        return path
    console.print("[dim]Waiting for runner to start a session…  (Ctrl-C to abort)[/dim]")
    try:
        while True:
            time.sleep(1.0)
            path = resolve_session_file(session)
            if path:
                console.print(f"[dim]Session detected: {path.stem}[/dim]")
                return path
    except KeyboardInterrupt:
        return None


def run_tail(
    session: Optional[str] = None,
    kinds: Optional[list[str]] = None,
    strategy: Optional[str] = None,
    from_start: bool = True,
) -> None:
    path = _wait_for_session(session, console)
    if not path:
        return
    kind_filter = set(k.strip() for k in kinds) if kinds else None
    console.print(f"[dim]Tailing {path}[/dim]")
    console.print(f"[dim]Filters: kinds={kind_filter or 'all'}  strategy={strategy or 'all'}"
                  f"  from_start={from_start}[/dim]")
    console.print()
    try:
        for line in _follow(path, from_start=from_start):
            if line.startswith("__SESSION_SWITCH__"):
                new_sid = line.strip().split(maxsplit=1)[1]
                console.rule(f"[yellow]switched to session {new_sid}[/yellow]", style="yellow")
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            if kind_filter and ev.get("kind") not in kind_filter:
                continue
            if strategy and ev.get("strategy") != strategy:
                continue
            console.print(_format(ev))
    except KeyboardInterrupt:
        console.print("\n[dim]tail stopped[/dim]")
