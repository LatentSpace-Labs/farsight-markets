"""
Local SQLite store for standalone prediction markets console.

Auto-creates tables on first use. No Alembic, no PostgreSQL dependency.
File location: ~/.farsight/markets.db (configurable via PM_DB_PATH env var)

Tables:
  signals         — persisted signal history
  paper_trades    — simulated trade log
  paper_portfolio — portfolio state (single row)
  subscriptions   — watched markets/categories
  alert_rules     — user-defined alert conditions
  config          — key-value settings
  sessions        — one row per bot run (Phase 0)
  resolutions     — final resolved price per market (Phase 0)
  signal_outcomes — price captures + realized edge per signal (Phase 0)
"""

import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = os.path.join(Path.home(), ".farsight", "markets.db")


class LocalStore:
    """SQLite-backed local store for the standalone markets console."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.environ.get("PM_DB_PATH", DEFAULT_DB_PATH)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._ensure_tables()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def _ensure_tables(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY,
                market_id TEXT,
                event_id TEXT,
                token_id TEXT,
                signal_type TEXT NOT NULL,
                direction TEXT NOT NULL,
                confidence REAL NOT NULL,
                horizon TEXT,
                tradability REAL,
                model_probability REAL,
                market_price REAL,
                edge REAL,
                evidence TEXT,
                risk_flags TEXT,
                status TEXT DEFAULT 'active',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS paper_portfolio (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                starting_balance REAL NOT NULL DEFAULT 10000.0,
                current_balance REAL NOT NULL DEFAULT 10000.0,
                total_pnl REAL NOT NULL DEFAULT 0.0,
                total_trades INTEGER NOT NULL DEFAULT 0,
                winning_trades INTEGER NOT NULL DEFAULT 0,
                max_position_pct REAL NOT NULL DEFAULT 5.0,
                max_daily_loss REAL NOT NULL DEFAULT 500.0,
                kelly_fraction REAL NOT NULL DEFAULT 0.15,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS paper_trades (
                id TEXT PRIMARY KEY,
                signal_id TEXT,
                market_id TEXT,
                market_question TEXT,
                token_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_price REAL NOT NULL,
                fill_price REAL NOT NULL,
                size_usd REAL NOT NULL,
                num_shares REAL NOT NULL,
                slippage_bps REAL DEFAULT 0,
                exit_price REAL,
                exit_reason TEXT,
                pnl_usd REAL,
                return_pct REAL,
                is_open INTEGER DEFAULT 1,
                opened_at TEXT NOT NULL,
                closed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                id TEXT PRIMARY KEY,
                target_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                target_label TEXT NOT NULL,
                filters TEXT,
                enabled INTEGER DEFAULT 1,
                auto_trade INTEGER DEFAULT 0,
                trade_size_usd REAL DEFAULT 50.0,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS alert_rules (
                id TEXT PRIMARY KEY,
                subscription_id TEXT,
                name TEXT NOT NULL,
                condition_type TEXT NOT NULL,
                condition_params TEXT NOT NULL,
                cooldown_minutes INTEGER DEFAULT 60,
                action_type TEXT,
                last_triggered_at TEXT,
                trigger_count INTEGER DEFAULT 0,
                enabled INTEGER DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                data TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            -- Phase 0: session lifecycle. One row per bot run.
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                config_hash TEXT,
                strategies TEXT,
                auto_trade INTEGER DEFAULT 0,
                events_processed INTEGER DEFAULT 0,
                signals_emitted INTEGER DEFAULT 0,
                signals_suppressed INTEGER DEFAULT 0,
                trades_opened INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                notes TEXT
            );

            -- Phase 0: ground-truth resolutions. Populated by nightly poll.
            CREATE TABLE IF NOT EXISTS resolutions (
                market_id TEXT PRIMARY KEY,
                token_id TEXT,
                resolved_outcome TEXT,          -- 'YES' | 'NO' | 'INVALID' | 'UNKNOWN'
                resolved_price REAL,            -- 1.0, 0.0, or split
                resolved_at TEXT,               -- when the market closed
                observed_at TEXT NOT NULL,      -- when we recorded it
                source TEXT DEFAULT 'gamma'
            );

            -- Phase 0: signal outcome join. One row per emitted signal.
            -- Price captures and realized-edge columns are filled over time.
            CREATE TABLE IF NOT EXISTS signal_outcomes (
                signal_id TEXT PRIMARY KEY,
                market_id TEXT,
                token_id TEXT,
                signal_type TEXT,
                direction TEXT,
                entry_price REAL NOT NULL,
                emitted_at TEXT NOT NULL,
                price_t1h REAL,
                price_t4h REAL,
                price_t24h REAL,
                resolved_price REAL,
                realized_edge_1h REAL,
                realized_edge_4h REAL,
                realized_edge_24h REAL,
                realized_edge_final REAL,
                last_updated_at TEXT
            );

            CREATE INDEX IF NOT EXISTS ix_signals_type ON signals(signal_type);
            CREATE INDEX IF NOT EXISTS ix_signals_created ON signals(created_at);
            CREATE INDEX IF NOT EXISTS ix_trades_open ON paper_trades(is_open);
            CREATE INDEX IF NOT EXISTS ix_event_log_channel ON event_log(channel, id);
            CREATE INDEX IF NOT EXISTS ix_outcomes_emitted ON signal_outcomes(emitted_at);
            CREATE INDEX IF NOT EXISTS ix_outcomes_pending_t1h  ON signal_outcomes(emitted_at) WHERE price_t1h  IS NULL;
            CREATE INDEX IF NOT EXISTS ix_outcomes_pending_t4h  ON signal_outcomes(emitted_at) WHERE price_t4h  IS NULL;
            CREATE INDEX IF NOT EXISTS ix_outcomes_pending_t24h ON signal_outcomes(emitted_at) WHERE price_t24h IS NULL;
            CREATE INDEX IF NOT EXISTS ix_sessions_started ON sessions(started_at);
        """)
        # Additive column migrations for existing deployments.
        self._add_column_if_missing("signals", "session_id", "TEXT")
        self._add_column_if_missing("paper_trades", "session_id", "TEXT")
        self._add_column_if_missing("paper_trades", "strategy", "TEXT")
        conn.commit()
        logger.debug(f"Local store ready: {self.db_path}")

    def _add_column_if_missing(self, table: str, column: str, coltype: str):
        """Additive schema migration — SQLite has no native IF NOT EXISTS on ALTER."""
        conn = self._get_conn()
        existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    # ── Signals ──────────────────────────────────────────────────────

    def save_signal(self, signal: dict):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO signals
            (id, market_id, event_id, token_id, signal_type, direction, confidence,
             horizon, tradability, model_probability, market_price, edge,
             evidence, risk_flags, status, created_at, session_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            signal.get("id", ""),
            signal.get("market_id"),
            signal.get("event_id"),
            signal.get("token_id"),
            signal.get("signal_type", ""),
            signal.get("direction", ""),
            signal.get("confidence", 0),
            signal.get("horizon"),
            signal.get("tradability_score"),
            signal.get("model_probability"),
            signal.get("market_price"),
            signal.get("edge"),
            json.dumps(signal.get("evidence", [])),
            json.dumps(signal.get("risk_flags", [])),
            signal.get("status", "active"),
            signal.get("created_at", datetime.utcnow().isoformat()),
            signal.get("session_id"),
        ))
        conn.commit()

    def get_signals_since(self, since_iso: str) -> list[dict]:
        """Recent signals — used for warm-start cooldown/dedup reconstruction."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM signals WHERE created_at >= ? ORDER BY created_at DESC",
            (since_iso,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_recent_signals(self, limit: int = 20) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM signals ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_signal_counts(self) -> dict:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT signal_type, COUNT(*) as cnt FROM signals GROUP BY signal_type"
        ).fetchall()
        return {r["signal_type"]: r["cnt"] for r in rows}

    # ── Paper Portfolio ──────────────────────────────────────────────

    def get_portfolio(self) -> dict:
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM paper_portfolio WHERE id = 1").fetchone()
        if row:
            return dict(row)
        # Initialize
        conn.execute("""
            INSERT INTO paper_portfolio (id, starting_balance, current_balance, created_at)
            VALUES (1, 10000.0, 10000.0, ?)
        """, (datetime.utcnow().isoformat(),))
        conn.commit()
        return dict(conn.execute("SELECT * FROM paper_portfolio WHERE id = 1").fetchone())

    def update_portfolio(self, **kwargs):
        conn = self._get_conn()
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values())
        conn.execute(f"UPDATE paper_portfolio SET {sets} WHERE id = 1", vals)
        conn.commit()

    def reset_portfolio(self):
        conn = self._get_conn()
        conn.execute("DELETE FROM paper_trades")
        conn.execute("""
            UPDATE paper_portfolio SET
                current_balance = starting_balance, total_pnl = 0,
                total_trades = 0, winning_trades = 0
            WHERE id = 1
        """)
        conn.commit()

    # ── Paper Trades ─────────────────────────────────────────────────

    def save_trade(self, trade: dict):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO paper_trades
            (id, signal_id, market_id, market_question, token_id, outcome,
             direction, entry_price, fill_price, size_usd, num_shares,
             slippage_bps, is_open, opened_at, session_id, strategy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
        """, (
            trade["id"], trade.get("signal_id"), trade.get("market_id"),
            trade.get("market_question"), trade["token_id"], trade["outcome"],
            trade["direction"], trade["entry_price"], trade["fill_price"],
            trade["size_usd"], trade["num_shares"], trade.get("slippage_bps", 0),
            trade.get("opened_at", datetime.utcnow().isoformat()),
            trade.get("session_id"),
            trade.get("strategy"),
        ))
        conn.commit()

    def close_trade(self, trade_id: str, exit_price: float, exit_reason: str, pnl: float, return_pct: float):
        conn = self._get_conn()
        conn.execute("""
            UPDATE paper_trades SET
                exit_price = ?, exit_reason = ?, pnl_usd = ?, return_pct = ?,
                is_open = 0, closed_at = ?
            WHERE id = ?
        """, (exit_price, exit_reason, pnl, return_pct, datetime.utcnow().isoformat(), trade_id))
        conn.commit()

    def get_open_trades(self) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM paper_trades WHERE is_open = 1 ORDER BY opened_at DESC").fetchall()
        return [dict(r) for r in rows]

    def get_trade_history(self, limit: int = 50) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM paper_trades ORDER BY opened_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    # ── Subscriptions ────────────────────────────────────────────────

    def save_subscription(self, sub: dict):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO subscriptions
            (id, target_type, target_id, target_label, filters, enabled,
             auto_trade, trade_size_usd, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sub["id"], sub["target_type"], sub["target_id"], sub["target_label"],
            json.dumps(sub.get("filters")), sub.get("enabled", 1),
            sub.get("auto_trade", 0), sub.get("trade_size_usd", 50.0),
            sub.get("created_at", datetime.utcnow().isoformat()),
        ))
        conn.commit()

    def get_subscriptions(self, enabled_only: bool = True) -> list[dict]:
        conn = self._get_conn()
        query = "SELECT * FROM subscriptions"
        if enabled_only:
            query += " WHERE enabled = 1"
        rows = conn.execute(query).fetchall()
        return [dict(r) for r in rows]

    def delete_subscription(self, sub_id: str):
        conn = self._get_conn()
        conn.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
        conn.commit()

    # ── Config ───────────────────────────────────────────────────────

    def get_config(self, key: str, default: str = "") -> str:
        conn = self._get_conn()
        row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_config(self, key: str, value: str):
        conn = self._get_conn()
        conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, value))
        conn.commit()

    # ── Event Log (for multi-console tailing) ──────────────────────

    def log_event(self, channel: str, data: str):
        """Write an event to the log. Channels: stream, signal, feature, trade, system."""
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO event_log (channel, data, created_at) VALUES (?, ?, ?)",
            (channel, data, datetime.utcnow().isoformat()),
        )
        conn.commit()
        # Keep log bounded — trim old entries beyond 10K
        conn.execute("DELETE FROM event_log WHERE id < (SELECT MAX(id) - 10000 FROM event_log)")
        conn.commit()

    def tail_events(self, channel: str | None = None, after_id: int = 0, limit: int = 50) -> list[dict]:
        """Read events after a given ID. For tailing from another process."""
        conn = self._get_conn()
        if channel:
            rows = conn.execute(
                "SELECT id, channel, data, created_at FROM event_log WHERE channel = ? AND id > ? ORDER BY id ASC LIMIT ?",
                (channel, after_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, channel, data, created_at FROM event_log WHERE id > ? ORDER BY id ASC LIMIT ?",
                (after_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Stats ────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        conn = self._get_conn()
        signal_count = conn.execute("SELECT COUNT(*) as c FROM signals").fetchone()["c"]
        trade_count = conn.execute("SELECT COUNT(*) as c FROM paper_trades").fetchone()["c"]
        open_count = conn.execute("SELECT COUNT(*) as c FROM paper_trades WHERE is_open = 1").fetchone()["c"]
        sub_count = conn.execute("SELECT COUNT(*) as c FROM subscriptions WHERE enabled = 1").fetchone()["c"]
        portfolio = self.get_portfolio()

        return {
            "db_path": self.db_path,
            "total_signals": signal_count,
            "total_trades": trade_count,
            "open_trades": open_count,
            "subscriptions": sub_count,
            "portfolio_balance": portfolio["current_balance"],
            "portfolio_pnl": portfolio["total_pnl"],
        }

    # ── Sessions (Phase 0) ───────────────────────────────────────────

    def start_session(self, session_id: str, config_hash: str, strategies: str, auto_trade: bool) -> dict:
        conn = self._get_conn()
        now = datetime.utcnow().isoformat()
        conn.execute("""
            INSERT INTO sessions (id, started_at, config_hash, strategies, auto_trade)
            VALUES (?, ?, ?, ?, ?)
        """, (session_id, now, config_hash, strategies, 1 if auto_trade else 0))
        conn.commit()
        return {"id": session_id, "started_at": now}

    def end_session(self, session_id: str, counts: dict | None = None):
        conn = self._get_conn()
        counts = counts or {}
        conn.execute("""
            UPDATE sessions SET
                ended_at = ?,
                events_processed = COALESCE(?, events_processed),
                signals_emitted = COALESCE(?, signals_emitted),
                signals_suppressed = COALESCE(?, signals_suppressed),
                trades_opened = COALESCE(?, trades_opened),
                errors = COALESCE(?, errors)
            WHERE id = ?
        """, (
            datetime.utcnow().isoformat(),
            counts.get("events_processed"),
            counts.get("signals_emitted"),
            counts.get("signals_suppressed"),
            counts.get("trades_opened"),
            counts.get("errors"),
            session_id,
        ))
        conn.commit()

    def get_recent_sessions(self, limit: int = 20) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM sessions ORDER BY started_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Resolutions (Phase 0) ────────────────────────────────────────

    def save_resolution(self, res: dict):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO resolutions
            (market_id, token_id, resolved_outcome, resolved_price, resolved_at, observed_at, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            res["market_id"], res.get("token_id"),
            res.get("resolved_outcome"), res.get("resolved_price"),
            res.get("resolved_at"),
            res.get("observed_at", datetime.utcnow().isoformat()),
            res.get("source", "gamma"),
        ))
        conn.commit()

    def get_resolution(self, market_id: str) -> Optional[dict]:
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM resolutions WHERE market_id = ?", (market_id,)).fetchone()
        return dict(row) if row else None

    def get_unresolved_market_ids_with_signals(self) -> list[str]:
        """Markets that have at least one outcome row without resolved_price."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT DISTINCT o.market_id FROM signal_outcomes o
            LEFT JOIN resolutions r ON r.market_id = o.market_id
            WHERE o.resolved_price IS NULL AND o.market_id IS NOT NULL
        """).fetchall()
        return [r["market_id"] for r in rows if r["market_id"]]

    # ── Signal outcomes (Phase 0) ────────────────────────────────────

    def create_signal_outcome(self, outcome: dict):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO signal_outcomes
            (signal_id, market_id, token_id, signal_type, direction,
             entry_price, emitted_at, last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            outcome["signal_id"], outcome.get("market_id"), outcome.get("token_id"),
            outcome.get("signal_type"), outcome.get("direction"),
            outcome["entry_price"], outcome["emitted_at"],
            datetime.utcnow().isoformat(),
        ))
        conn.commit()

    def update_signal_outcome(self, signal_id: str, **fields):
        if not fields:
            return
        fields["last_updated_at"] = datetime.utcnow().isoformat()
        sets = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [signal_id]
        conn = self._get_conn()
        conn.execute(f"UPDATE signal_outcomes SET {sets} WHERE signal_id = ?", vals)
        conn.commit()

    def get_pending_outcome_captures(self, horizon_hours: int, now_iso: str, column: str) -> list[dict]:
        """Return outcome rows whose emitted_at + horizon_hours <= now and `column` is still NULL.

        Horizon hours must be in {1, 4, 24}; column is the corresponding price_tNh column.
        """
        assert column in {"price_t1h", "price_t4h", "price_t24h"}
        conn = self._get_conn()
        rows = conn.execute(f"""
            SELECT * FROM signal_outcomes
            WHERE {column} IS NULL
              AND datetime(emitted_at, '+{horizon_hours} hours') <= ?
            ORDER BY emitted_at ASC
            LIMIT 500
        """, (now_iso,)).fetchall()
        return [dict(r) for r in rows]

    def get_signal_outcomes(self, limit: int = 200) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM signal_outcomes ORDER BY emitted_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def kpi_summary(self) -> dict:
        """Aggregate KPIs across all outcomes with realized-edge data."""
        conn = self._get_conn()
        summary = {}
        for horizon, col in [("1h", "realized_edge_1h"), ("4h", "realized_edge_4h"),
                             ("24h", "realized_edge_24h"), ("final", "realized_edge_final")]:
            row = conn.execute(f"""
                SELECT
                    COUNT({col}) AS n,
                    AVG({col}) AS avg_edge,
                    SUM(CASE WHEN {col} > 0 THEN 1 ELSE 0 END) AS wins
                FROM signal_outcomes
                WHERE {col} IS NOT NULL
            """).fetchone()
            n = row["n"] or 0
            summary[horizon] = {
                "n": n,
                "avg_edge": row["avg_edge"] or 0.0,
                "hit_rate": (row["wins"] / n) if n else 0.0,
            }
        by_type = conn.execute("""
            SELECT signal_type,
                   COUNT(realized_edge_1h) AS n,
                   AVG(realized_edge_1h) AS avg_edge_1h,
                   AVG(realized_edge_final) AS avg_edge_final
            FROM signal_outcomes
            WHERE realized_edge_1h IS NOT NULL
            GROUP BY signal_type
        """).fetchall()
        summary["by_type"] = [dict(r) for r in by_type]
        return summary
