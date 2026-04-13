"""Phase 0 tests: sessions, resolutions, signal_outcomes, warm-start, outcome capture.

The label loop is the foundation for every downstream KPI, so these tests
focus on correctness of the tables and the (store, filter, tracker) glue
rather than on integration with live APIs.
"""

import os
import tempfile
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from farsight.markets.config import settings as global_settings
from farsight.markets.schemas.signals import Direction, SignalEvidence, SignalSchema, SignalType
from farsight.markets.services.outcome_tracker import OutcomeTracker, _realized_edge
from farsight.markets.services.session_service import SessionService
from farsight.markets.services.signal_engine import SignalEngine, SignalFilter, dedup_hash
from farsight.markets.store import LocalStore


@pytest.fixture
def store():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    s = LocalStore(db_path=path)
    yield s
    s.close()
    os.unlink(path)


def _make_signal(
    market_id="mkt_A",
    signal_type=SignalType.PROBABILITY_SHOCK,
    direction=Direction.BULLISH,
    price=0.45,
    emitted_at=None,
) -> dict:
    sig = SignalSchema(
        id=uuid4(),
        market_id=market_id,
        token_id=f"tok_{market_id}",
        source="polymarket",
        signal_type=signal_type,
        direction=direction,
        confidence=0.8,
        horizon="1h",
        tradability_score=0.7,
        evidence=[SignalEvidence(source="delta_5m", description="+8%", value=0.08, weight=1.0)],
        model_probability=price + 0.05,
        market_price=price,
        edge=0.05,
        feature_set_version="v1",
        rule_version="v1",
        created_at=emitted_at or datetime.utcnow(),
    )
    return sig.model_dump(mode="json")


# ── Schema / store tables ─────────────────────────────────────────────


class TestSessionsTable:
    def test_start_and_end_session(self, store):
        sid = str(uuid4())
        store.start_session(sid, config_hash="abc123", strategies="scanner,arb", auto_trade=True)
        rows = store.get_recent_sessions()
        assert len(rows) == 1
        assert rows[0]["id"] == sid
        assert rows[0]["auto_trade"] == 1
        assert rows[0]["ended_at"] is None

        store.end_session(sid, counts={"signals_emitted": 5, "trades_opened": 2})
        rows = store.get_recent_sessions()
        assert rows[0]["ended_at"] is not None
        assert rows[0]["signals_emitted"] == 5
        assert rows[0]["trades_opened"] == 2

    def test_session_id_stamped_on_signal(self, store):
        sid = str(uuid4())
        payload = _make_signal()
        payload["session_id"] = sid
        store.save_signal(payload)

        recent = store.get_recent_signals()
        assert recent[0]["session_id"] == sid


class TestResolutionsTable:
    def test_save_and_get(self, store):
        store.save_resolution({
            "market_id": "mkt_1",
            "resolved_outcome": "YES",
            "resolved_price": 1.0,
            "resolved_at": datetime.utcnow().isoformat(),
        })
        r = store.get_resolution("mkt_1")
        assert r is not None
        assert r["resolved_outcome"] == "YES"
        assert r["resolved_price"] == 1.0

    def test_upsert_updates_existing(self, store):
        store.save_resolution({"market_id": "m1", "resolved_outcome": "UNKNOWN", "resolved_price": None})
        store.save_resolution({"market_id": "m1", "resolved_outcome": "YES", "resolved_price": 1.0})
        r = store.get_resolution("m1")
        assert r["resolved_outcome"] == "YES"


class TestSignalOutcomesTable:
    def test_create_and_update(self, store):
        emitted = datetime.utcnow().isoformat()
        store.create_signal_outcome({
            "signal_id": "s1", "market_id": "m1", "token_id": "tok_1",
            "signal_type": "probability_shock", "direction": "bullish",
            "entry_price": 0.40, "emitted_at": emitted,
        })
        outcomes = store.get_signal_outcomes()
        assert len(outcomes) == 1
        assert outcomes[0]["entry_price"] == 0.40
        assert outcomes[0]["price_t1h"] is None

        store.update_signal_outcome("s1", price_t1h=0.46, realized_edge_1h=0.06)
        outcomes = store.get_signal_outcomes()
        assert outcomes[0]["price_t1h"] == 0.46
        assert outcomes[0]["realized_edge_1h"] == pytest.approx(0.06)

    def test_pending_captures_query_respects_horizon(self, store):
        # Emitted 2h ago → t1h is due, t4h/t24h are not.
        emitted = (datetime.utcnow() - timedelta(hours=2)).isoformat()
        store.create_signal_outcome({
            "signal_id": "s1", "market_id": "m1", "token_id": "tok_1",
            "signal_type": "probability_shock", "direction": "bullish",
            "entry_price": 0.40, "emitted_at": emitted,
        })
        now = datetime.utcnow().isoformat()
        assert len(store.get_pending_outcome_captures(1, now, "price_t1h")) == 1
        assert len(store.get_pending_outcome_captures(4, now, "price_t4h")) == 0
        assert len(store.get_pending_outcome_captures(24, now, "price_t24h")) == 0

    def test_unresolved_markets_excludes_already_resolved(self, store):
        emitted = datetime.utcnow().isoformat()
        store.create_signal_outcome({
            "signal_id": "s1", "market_id": "m1", "token_id": "t",
            "signal_type": "x", "direction": "bullish",
            "entry_price": 0.5, "emitted_at": emitted,
        })
        store.create_signal_outcome({
            "signal_id": "s2", "market_id": "m2", "token_id": "t",
            "signal_type": "x", "direction": "bullish",
            "entry_price": 0.5, "emitted_at": emitted,
        })
        store.update_signal_outcome("s2", resolved_price=1.0, realized_edge_final=0.5)

        ids = store.get_unresolved_market_ids_with_signals()
        assert "m1" in ids
        assert "m2" not in ids

    def test_kpi_summary_ignores_unlabeled_rows(self, store):
        emitted = datetime.utcnow().isoformat()
        for i, edge in enumerate([0.05, -0.02, 0.08]):
            store.create_signal_outcome({
                "signal_id": f"s{i}", "market_id": f"m{i}", "token_id": "t",
                "signal_type": "probability_shock", "direction": "bullish",
                "entry_price": 0.5, "emitted_at": emitted,
            })
            store.update_signal_outcome(f"s{i}", price_t1h=0.5 + edge, realized_edge_1h=edge)

        summary = store.kpi_summary()
        assert summary["1h"]["n"] == 3
        assert summary["1h"]["hit_rate"] == pytest.approx(2 / 3)
        assert summary["1h"]["avg_edge"] == pytest.approx((0.05 - 0.02 + 0.08) / 3)
        # 4h/24h/final still NULL → should report 0
        assert summary["4h"]["n"] == 0


# ── SignalFilter: warmup, dedup, warm-start ───────────────────────────


class TestWarmupAndDedup:
    def test_warmup_suppresses_everything(self):
        f = SignalFilter()
        f.begin_warmup(600)
        sig = SignalSchema.model_validate(_make_signal())
        passed, reason = f.check(sig)
        assert not passed
        assert "warmup" in reason

    def test_dedup_suppresses_repeat_content(self):
        f = SignalFilter()
        sig = SignalSchema.model_validate(_make_signal())
        # First one passes; record it.
        passed, _ = f.check(sig)
        assert passed
        f.record_emission(sig)

        # Identical content → dedup catches it.
        sig2 = SignalSchema.model_validate(_make_signal())
        passed, reason = f.check(sig2)
        assert not passed
        assert "dedup" in reason

    def test_warm_start_rebuilds_cooldown_from_signals(self):
        """A recent persisted signal should suppress re-emission after restart."""
        f = SignalFilter()
        recent_emit = datetime.utcnow() - timedelta(minutes=5)
        persisted = [{
            "id": "s_old", "market_id": "mkt_A",
            "signal_type": SignalType.PROBABILITY_SHOCK.value,
            "direction": Direction.BULLISH.value,
            "market_price": 0.45,
            "created_at": recent_emit.isoformat(),
        }]
        f.warm_start(persisted)

        # Same market/type content → blocked by dedup (content hash match).
        sig = SignalSchema.model_validate(_make_signal())
        passed, reason = f.check(sig)
        assert not passed
        assert "dedup" in reason

    def test_warm_start_ignores_old_signals_for_cooldown(self):
        """Cooldown doesn't apply outside the cooldown window, only dedup does."""
        f = SignalFilter()
        ancient = datetime.utcnow() - timedelta(hours=10)
        f.warm_start([{
            "id": "old", "market_id": "mkt_A",
            "signal_type": SignalType.PROBABILITY_SHOCK.value,
            "direction": Direction.BULLISH.value,
            "market_price": 0.99,   # different price → different dedup hash
            "created_at": ancient.isoformat(),
        }])
        # Different price (not dedup-equal) and beyond cooldown window → pass
        sig = SignalSchema.model_validate(_make_signal(price=0.45))
        passed, _ = f.check(sig)
        assert passed

    def test_dedup_hash_is_stable(self):
        h1 = dedup_hash("m", "probability_shock", "bullish", 0.456)
        h2 = dedup_hash("m", "probability_shock", "bullish", 0.461)  # rounds to same 2dp
        h3 = dedup_hash("m", "probability_shock", "bullish", 0.500)
        assert h1 == h2
        assert h1 != h3


# ── SessionService ────────────────────────────────────────────────────


class TestSessionService:
    def test_lifecycle(self, store):
        svc = SessionService(store)
        sid = svc.start(global_settings, ["scanner"], auto_trade=False)
        assert sid
        svc.increment("signals_emitted", 3)
        svc.increment("trades_opened")
        svc.end()

        rows = store.get_recent_sessions()
        assert rows[0]["id"] == sid
        assert rows[0]["signals_emitted"] == 3
        assert rows[0]["trades_opened"] == 1
        assert rows[0]["ended_at"] is not None

    def test_config_hash_changes_with_threshold(self, store):
        svc = SessionService(store)
        s1 = SimpleNamespace(**{k: getattr(global_settings, k) for k in dir(global_settings) if not k.startswith("_")})
        h1 = SessionService._config_hash(global_settings)

        # Mutate a threshold → hash must change
        s1.FILTER_MIN_CONFIDENCE = 0.99
        h2 = SessionService._config_hash(s1)
        assert h1 != h2


# ── OutcomeTracker ────────────────────────────────────────────────────


class TestOutcomeTracker:
    def test_on_signal_emitted_creates_row(self, store):
        tracker = OutcomeTracker(store)
        payload = _make_signal()
        tracker.on_signal_emitted(payload)

        rows = store.get_signal_outcomes()
        assert len(rows) == 1
        assert rows[0]["entry_price"] == pytest.approx(0.45)
        assert rows[0]["market_id"] == payload["market_id"]

    def test_realized_edge_direction_signing(self):
        # Bullish call on a price move up → positive edge
        assert _realized_edge("bullish", 0.40, 0.48) == pytest.approx(0.08)
        # Bearish call on a price move up → negative edge
        assert _realized_edge("bearish", 0.40, 0.48) == pytest.approx(-0.08)
        # Neutral / unknown → None
        assert _realized_edge("neutral", 0.40, 0.48) is None
        assert _realized_edge(None, 0.40, 0.48) is None

    @pytest.mark.asyncio
    async def test_capture_pending_prices_fills_t1h(self, store, monkeypatch):
        tracker = OutcomeTracker(store)

        # Seed one row whose t1h capture is due.
        emitted = (datetime.utcnow() - timedelta(hours=2)).isoformat()
        store.create_signal_outcome({
            "signal_id": "s1", "market_id": "m1", "token_id": "tok",
            "signal_type": "probability_shock", "direction": "bullish",
            "entry_price": 0.40, "emitted_at": emitted,
        })

        async def fake_mid(self, token_id):
            return 0.48
        monkeypatch.setattr(OutcomeTracker, "_fetch_mid", fake_mid)

        n = await tracker.capture_pending_prices()
        assert n == 1
        rows = store.get_signal_outcomes()
        assert rows[0]["price_t1h"] == pytest.approx(0.48)
        assert rows[0]["realized_edge_1h"] == pytest.approx(0.08)

    def test_parse_resolution_handles_json_encoded_prices(self):
        raw = {
            "conditionId": "0xabc",
            "id": 123,
            "resolved": True,
            "outcomePrices": '["1", "0"]',   # The Gamma quirk
            "endDate": "2026-01-01T00:00:00Z",
        }
        parsed = OutcomeTracker._parse_resolution(raw)
        assert parsed["resolved_outcome"] == "YES"
        assert parsed["resolved_price"] == 1.0
        assert parsed["market_id"] == "0xabc"

    def test_parse_resolution_returns_none_if_open(self):
        raw = {"conditionId": "x", "resolved": False, "closed": False}
        assert OutcomeTracker._parse_resolution(raw) is None

    def test_backfill_final_edges_writes_signed_edge(self, store):
        tracker = OutcomeTracker(store)
        emitted = datetime.utcnow().isoformat()
        store.create_signal_outcome({
            "signal_id": "s_up", "market_id": "m1", "token_id": "t",
            "signal_type": "probability_shock", "direction": "bullish",
            "entry_price": 0.40, "emitted_at": emitted,
        })
        store.create_signal_outcome({
            "signal_id": "s_dn", "market_id": "m1", "token_id": "t",
            "signal_type": "probability_shock", "direction": "bearish",
            "entry_price": 0.40, "emitted_at": emitted,
        })
        tracker._backfill_final_edges({"market_id": "m1", "resolved_price": 1.0})

        rows = {r["signal_id"]: r for r in store.get_signal_outcomes()}
        assert rows["s_up"]["realized_edge_final"] == pytest.approx(0.60)
        assert rows["s_dn"]["realized_edge_final"] == pytest.approx(-0.60)
        assert rows["s_up"]["resolved_price"] == 1.0
