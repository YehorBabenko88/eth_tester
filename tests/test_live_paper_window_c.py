"""Bounded-live paper clock regression: no data-quality promotion."""
from datetime import datetime, timedelta, timezone

import pytest

from eth_tester.live_paper_window import (
    bounded_live_window, live_source_readiness, inspect_latest_live_bar,
)


UTC = timezone.utc
BAR = datetime(2026, 11, 25, 19, 42, tzinfo=UTC)


def test_live_one_minute_paper_gate_real_time_and_90_second_boundary():
    assert bounded_live_window(BAR, BAR + timedelta(seconds=59))["status"] == "pending"
    assert bounded_live_window(BAR, BAR + timedelta(minutes=1))["status"] == "eligible_window"
    assert bounded_live_window(BAR, BAR + timedelta(seconds=150))["status"] == "eligible_window"
    state = bounded_live_window(BAR, BAR + timedelta(seconds=150, microseconds=1))
    assert state == {
        "status": "expired", "reason": "decision_window_elapsed",
        "bar_start": BAR, "bar_end": BAR + timedelta(minutes=1),
    }


def test_live_paper_no_retrospective_reentry_next_day_archive():
    state = bounded_live_window(BAR, BAR + timedelta(days=1))
    assert state["status"] == "expired"
    assert live_source_readiness(
        window=state, source_witness=None,
        sampled_at=BAR + timedelta(days=1))["status"] == "expired"


@pytest.mark.parametrize("bad", [
    BAR.replace(second=2),
    BAR.replace(microsecond=1),
])
def test_live_paper_only_exact_one_minute_utc_start(bad):
    with pytest.raises(ValueError, match="unaligned"):
        bounded_live_window(bad, bad + timedelta(minutes=1))


def test_live_paper_tz_aware_only_and_no_deadline_extension():
    with pytest.raises(ValueError, match="timezone"):
        bounded_live_window(BAR.replace(tzinfo=None), BAR)
    with pytest.raises(ValueError, match="deadline"):
        bounded_live_window(BAR, BAR, deadline_seconds=91)


def test_live_paper_witness_cannot_masquerade_as_v2_ml():
    window = bounded_live_window(BAR, BAR + timedelta(minutes=1, seconds=25))
    witness = {
        "contract": "eth_feature_snapshot_v2",
        "quality": "complete", "paper_only": True,
        "ml_eligible": False, "complete_trade_ids_proven": False,
    }
    state = live_source_readiness(
        window=window, source_witness=witness,
        sampled_at=BAR + timedelta(minutes=1, seconds=25))
    assert state["status"] == "pending"
    assert state["reason"] == "invalid_live_paper_evidence_contract"


def test_live_paper_even_plausible_unverified_claim_not_accepted():
    now = BAR + timedelta(minutes=1, seconds=20)
    witness = {
        "contract": "eth_paper_live_evidence_v1",
        "quality": "live_observed_bounded", "paper_only": True,
        "ml_eligible": False, "complete_trade_ids_proven": False,
        "source_received_at": BAR + timedelta(minutes=1, seconds=2),
        "source_checked_at": BAR + timedelta(minutes=1, seconds=4),
        "computed_at": BAR + timedelta(minutes=1, seconds=8),
    }
    result = live_source_readiness(
        window=bounded_live_window(BAR, now),
        source_witness=witness, sampled_at=now)
    assert result == {"status": "pending", "reason": "db_reverification_required",
                      "paper_only": True, "ml_eligible": False}
    witness["source_received_at"] = now + timedelta(seconds=1)
    assert live_source_readiness(
        window=bounded_live_window(BAR, now),
        source_witness=witness, sampled_at=now)["reason"] == "source_clock_missing_or_future"


def test_live_window_readonly_db_missing_latest_bar(monkeypatch):
    class Conn:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def execute(self, query, parameters=None):
            class Cursor:
                def fetchone(self):
                    return {"ts": BAR} if "clock_timestamp()" in query else None
            return Cursor()
    monkeypatch.setattr("eth_tester.database.connection", lambda _: Conn())
    settings = type("S", (), {"symbol": "ETHUSDT"})()
    state = inspect_latest_live_bar(settings)
    assert state["status"] == "pending"
    assert state["reason"] == "closed_1m_candle_unavailable"


def test_live_window_never_writes_a_snapshot(monkeypatch):
    class Conn:
        def __init__(self):
            self.statements = []
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def execute(self, query, parameters=None):
            self.statements.append(query)
            class Cursor:
                def __init__(self, ts):
                    self.ts = ts
                def fetchone(self):
                    if "clock_timestamp()" in self.ts:
                        return {"ts": BAR + timedelta(minutes=1, seconds=10)}
                    return {"ts": BAR}
            return Cursor(query)
    conn = Conn()
    monkeypatch.setattr("eth_tester.database.connection", lambda _: conn)
    settings = type("S", (), {"symbol": "ETHUSDT"})()
    result = inspect_latest_live_bar(settings)
    assert result["reason"] == "b_authoritative_source_witness_missing"
    assert result["ml_eligible"] is False
    assert all(stmt.lstrip().upper().startswith("SELECT") for stmt in conn.statements)


def _mock_fast_publisher(monkeypatch, *, now, bar, has_book=True,
                         has_oi=True, has_rest=True, sessions=1, b_reject=False):
    """Source selectors are mocked; B alone supplies authoritative publication."""
    from contextlib import nullcontext
    class DB:
        statements = []
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def transaction(self):
            return nullcontext()
        def execute(self, query, args=None):
            self.statements.append(query)
            if "pg_advisory_xact_lock" in query:
                return type("Cursor", (), {"fetchone": lambda self: None})()
            if "clock_timestamp() AS ts" in query:
                row = {"ts": now}
            elif "FROM candles" in query:
                row = {"ts": bar}
            elif "FROM ingestion_coverage" in query:
                row = {"checked_at": now} if has_rest else None
            elif "FROM paper_live_evidence" in query:
                row = None
            elif "FROM live_book_receipts" in query:
                row = {"id": 17} if has_book else None
            elif "FROM live_oi_receipts" in query:
                row = {"id": 23} if has_oi else None
            elif "FROM live_trade_receipts" in query:
                rows = [{"session_id": "real-session", "prints": 1}]
                if sessions == 0:
                    rows = []
                elif sessions == 2:
                    rows.append({"session_id": "other", "prints": 1})
                return type("Cursor", (), {"fetchall": lambda self: rows})()
            else:
                raise AssertionError("Unexpected SQL: " + query)
            return type("Cursor", (), {
                "fetchone": lambda self: row})()
    db = DB()
    recorded = []
    monkeypatch.setattr("eth_tester.database.connection", lambda _: db)
    monkeypatch.setattr("eth_tester.book_analysis.replay_orderbook",
                        lambda settings, **kwargs: 3)
    def issue(conn, symbol, start, session, book_id, oi_id):
        recorded.append((symbol, start, session, book_id, oi_id))
        if b_reject:
            raise ValueError("Source receipt incomplete")
        return "a"*64
    monkeypatch.setattr(
        "eth_tester.storage.live_evidence.publish_bounded_paper_evidence",
        issue)
    def confirm(conn, *, symbol, bar_ts, evidence_key, decision_at):
        assert evidence_key == "a"*64
        assert decision_at == now
        return {"snapshot_key": evidence_key, "source_checksum": "b"*64,
                "computed_at": now}
    monkeypatch.setattr(
        "eth_tester.storage.live_evidence.require_bounded_paper_evidence",
        confirm)
    return type("Settings", (), {
        "symbol": "ETHUSDT", "category": "linear"})(), db, recorded


def test_round7_fast_c_publisher_selects_real_b_ids_not_caller_flags(monkeypatch):
    from eth_tester.live_paper_window import publish_latest_bounded_live
    now = BAR + timedelta(minutes=1, seconds=10)
    settings, db, recorded = _mock_fast_publisher(
        monkeypatch, now=now, bar=BAR)
    result = publish_latest_bounded_live(settings)
    assert result["status"] == "published"
    assert result["snapshot_key"] == "a"*64
    assert result["source_checksum"] == "b"*64
    assert result["ml_eligible"] is False
    assert result["paper_only"] is True
    assert result["publication_latency_ms"] == 10000
    assert result["rest_close_receipt_latency_ms"] == 10000
    assert recorded == [("ETHUSDT", BAR, "real-session", 17, 23)]
    assert all("INSERT INTO feature_snapshots" not in query
               for query in db.statements)


@pytest.mark.parametrize("condition", ("rest", "book", "oi", "sessions", "expired"))
def test_round7_fast_c_requires_all_real_sources_before_b_issuance(
        monkeypatch, condition):
    from eth_tester.live_paper_window import publish_latest_bounded_live
    now = BAR+timedelta(minutes=1, seconds=20)
    settings, _, recorded = _mock_fast_publisher(
        monkeypatch, now=now if condition != "expired" else
            BAR+timedelta(minutes=3),
        bar=BAR, has_rest=condition != "rest",
        has_book=condition != "book",
        has_oi=condition != "oi",
        sessions=2 if condition == "sessions" else 1)
    result = publish_latest_bounded_live(settings)
    assert result["status"] in ("pending", "expired")
    assert result["ml_eligible"] is False
    assert recorded == []


def test_round7_b_writer_rejection_is_not_positive_publication(monkeypatch):
    from eth_tester.live_paper_window import publish_latest_bounded_live
    now = BAR+timedelta(minutes=1, seconds=11)
    settings, _, issued = _mock_fast_publisher(
        monkeypatch, now=now, bar=BAR, b_reject=True)
    result = publish_latest_bounded_live(settings)
    assert result["status"] == "pending"
    assert result["reason"] == "b_source_validation_rejected"
    assert len(issued) == 1
    assert "snapshot_key" not in result


def test_round7_stale_fast_heartbeat_cannot_claim_current_publication(monkeypatch):
    from eth_tester.live_paper_window import read_live_paper_status
    class DB:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def execute(self, *_):
            return type("Cursor", (), {"fetchone": lambda _: {
                "status": "ok",
                "detail": {"status": "published", "reason": "old_sample"},
                "updated_at": BAR,
                "checked_at": BAR+timedelta(seconds=26),
            }})()
    monkeypatch.setattr("eth_tester.database.connection", lambda _: DB())
    settings = type("S", (), {"symbol": "ETHUSDT"})()
    result = read_live_paper_status(settings)
    assert result["status"] == "pending"
    assert result["reason"] == "fast_lane_heartbeat_stale"
