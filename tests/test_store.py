"""Tests for the local SQLite store."""

import os
import pytest
import tempfile
from datetime import datetime
from uuid import uuid4

from farsight.markets.store import LocalStore


@pytest.fixture
def store():
    """Create a temporary store that auto-cleans."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    s = LocalStore(db_path=path)
    yield s
    s.close()
    os.unlink(path)


class TestSignals:
    def test_save_and_retrieve(self, store):
        store.save_signal({
            "id": "sig_1",
            "signal_type": "probability_shock",
            "direction": "bullish",
            "confidence": 0.82,
            "edge": 0.10,
            "market_price": 0.50,
            "created_at": datetime.utcnow().isoformat(),
        })
        signals = store.get_recent_signals(limit=10)
        assert len(signals) == 1
        assert signals[0]["signal_type"] == "probability_shock"
        assert signals[0]["confidence"] == 0.82

    def test_signal_counts(self, store):
        for i in range(3):
            store.save_signal({"id": f"s{i}", "signal_type": "probability_shock",
                               "direction": "bullish", "confidence": 0.5,
                               "created_at": datetime.utcnow().isoformat()})
        store.save_signal({"id": "s99", "signal_type": "momentum_continuation",
                           "direction": "bearish", "confidence": 0.6,
                           "created_at": datetime.utcnow().isoformat()})

        counts = store.get_signal_counts()
        assert counts["probability_shock"] == 3
        assert counts["momentum_continuation"] == 1


class TestPortfolio:
    def test_auto_create(self, store):
        portfolio = store.get_portfolio()
        assert portfolio["current_balance"] == 10000.0
        assert portfolio["total_pnl"] == 0.0

    def test_update(self, store):
        store.get_portfolio()  # Initialize
        store.update_portfolio(current_balance=9500.0, total_pnl=-500.0)
        p = store.get_portfolio()
        assert p["current_balance"] == 9500.0
        assert p["total_pnl"] == -500.0

    def test_reset(self, store):
        store.get_portfolio()
        store.update_portfolio(current_balance=5000.0, total_pnl=-5000.0, total_trades=10)
        store.save_trade({
            "id": "t1", "token_id": "tok", "outcome": "Yes", "direction": "BUY",
            "entry_price": 0.5, "fill_price": 0.51, "size_usd": 100, "num_shares": 200,
        })
        store.reset_portfolio()
        p = store.get_portfolio()
        assert p["current_balance"] == p["starting_balance"]
        assert p["total_trades"] == 0
        assert store.get_open_trades() == []


class TestTrades:
    def test_save_and_retrieve(self, store):
        store.save_trade({
            "id": str(uuid4()), "token_id": "tok1", "outcome": "Yes",
            "direction": "BUY", "entry_price": 0.50, "fill_price": 0.51,
            "size_usd": 100, "num_shares": 200, "market_id": "m1",
        })
        trades = store.get_open_trades()
        assert len(trades) == 1
        assert trades[0]["is_open"] == 1

    def test_close_trade(self, store):
        tid = str(uuid4())
        store.save_trade({
            "id": tid, "token_id": "tok1", "outcome": "Yes",
            "direction": "BUY", "entry_price": 0.50, "fill_price": 0.51,
            "size_usd": 100, "num_shares": 200,
        })
        store.close_trade(tid, exit_price=0.60, exit_reason="manual", pnl=18.0, return_pct=18.0)

        open_trades = store.get_open_trades()
        assert len(open_trades) == 0

        history = store.get_trade_history()
        assert len(history) == 1
        assert history[0]["pnl_usd"] == 18.0


class TestSubscriptions:
    def test_save_and_retrieve(self, store):
        store.save_subscription({
            "id": "sub_1", "target_type": "category", "target_id": "politics",
            "target_label": "Politics Markets",
        })
        subs = store.get_subscriptions()
        assert len(subs) == 1
        assert subs[0]["target_type"] == "category"

    def test_delete(self, store):
        store.save_subscription({
            "id": "sub_1", "target_type": "category", "target_id": "politics",
            "target_label": "Politics",
        })
        store.delete_subscription("sub_1")
        assert store.get_subscriptions() == []


class TestConfig:
    def test_get_set(self, store):
        store.set_config("theme", "dark")
        assert store.get_config("theme") == "dark"

    def test_default(self, store):
        assert store.get_config("missing", "fallback") == "fallback"


class TestStats:
    def test_stats(self, store):
        stats = store.get_stats()
        assert stats["total_signals"] == 0
        assert stats["total_trades"] == 0
        assert stats["portfolio_balance"] == 10000.0
        assert "db_path" in stats
