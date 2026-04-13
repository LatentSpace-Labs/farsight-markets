"""Telemetry sink — one append-only JSONL file per session.

Every pipeline action emits a line through `get_sink().emit(kind, ...)`.
Readers (`cli tail`, `cli dashboard`) tail the file; they don't share process
memory with the writer. Keeps the runner simple and the UI decoupled.

Event envelope:
    {"ts": ISO8601, "session": short_id, "seq": int,
     "kind": "stage.drop" | ..., "strategy": "resolution" | None, "data": {...}}

Rotation: when the active file crosses TELEMETRY_MAX_BYTES, it's renamed to
`<session>.N.jsonl` and a fresh file is opened. Readers following by inode
will need to reopen — our readers re-stat by path, so they pick up the new file.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

TELEMETRY_DIR = Path.home() / ".farsight" / "telemetry"
TELEMETRY_MAX_BYTES = 50 * 1024 * 1024  # 50 MB before rotation
LATEST_POINTER = TELEMETRY_DIR / "latest"


class TelemetrySink:
    """Thread-safe JSONL writer. One instance per session."""

    def __init__(self, session_id: str):
        self.session_id = session_id[:8]
        self._seq = 0
        self._lock = threading.Lock()
        TELEMETRY_DIR.mkdir(parents=True, exist_ok=True)
        self._path = TELEMETRY_DIR / f"{self.session_id}.jsonl"
        self._fh = self._path.open("a", encoding="utf-8", buffering=1)  # line-buffered
        # Point the "latest" file so readers without --session find us.
        try:
            LATEST_POINTER.write_text(self.session_id, encoding="utf-8")
        except OSError as e:
            logger.debug(f"Couldn't write latest pointer: {e}")

    @property
    def path(self) -> Path:
        return self._path

    def emit(
        self,
        kind: str,
        *,
        strategy: Optional[str] = None,
        **data: Any,
    ) -> None:
        """Write one telemetry event. Never raises — telemetry must not
        take down the runner."""
        try:
            with self._lock:
                self._seq += 1
                event = {
                    "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                    "session": self.session_id,
                    "seq": self._seq,
                    "kind": kind,
                }
                if strategy is not None:
                    event["strategy"] = strategy
                if data:
                    event["data"] = data
                self._fh.write(json.dumps(event, default=str) + "\n")
                self._rotate_if_needed()
        except Exception as e:
            logger.debug(f"Telemetry emit failed ({kind}): {e}")

    def _rotate_if_needed(self) -> None:
        try:
            size = os.fstat(self._fh.fileno()).st_size
        except OSError:
            return
        if size < TELEMETRY_MAX_BYTES:
            return
        self._fh.close()
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self._path.rename(self._path.with_suffix(f".{ts}.jsonl"))
        self._fh = self._path.open("a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        with self._lock:
            if not self._fh.closed:
                self._fh.close()


# ── Global access ────────────────────────────────────────────────────
# One active sink per process. Strategies call `emit(...)` without knowing
# whether telemetry is configured — no-op when sink is unset.

_active_sink: Optional[TelemetrySink] = None


def set_sink(sink: Optional[TelemetrySink]) -> None:
    global _active_sink
    _active_sink = sink


def get_sink() -> Optional[TelemetrySink]:
    return _active_sink


def emit(kind: str, *, strategy: Optional[str] = None, **data: Any) -> None:
    """Module-level emit — no-op when no sink is active."""
    sink = _active_sink
    if sink is not None:
        sink.emit(kind, strategy=strategy, **data)


def resolve_session_file(session: Optional[str] = None) -> Optional[Path]:
    """Find the JSONL file for a session id, or the latest session if None."""
    if session:
        p = TELEMETRY_DIR / f"{session[:8]}.jsonl"
        return p if p.is_file() else None
    if LATEST_POINTER.is_file():
        sid = LATEST_POINTER.read_text(encoding="utf-8").strip()
        p = TELEMETRY_DIR / f"{sid}.jsonl"
        if p.is_file():
            return p
    # Fallback: newest .jsonl in the dir.
    if TELEMETRY_DIR.is_dir():
        candidates = sorted(
            (p for p in TELEMETRY_DIR.glob("*.jsonl") if "." not in p.stem[8:]),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            return candidates[0]
    return None
