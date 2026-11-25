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
