"""Agent A Round 22: bounded, fail-closed source recovery regression cases.

No exchange connection and no real-order or production PostgreSQL access.
"""
import asyncio
from datetime import date, datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest

from eth_tester import archives, live
from eth_tester.bybit import SharedRateBudget
from eth_tester.data_quality import DataIntegrityError


UTC = timezone.utc


class _One:
    def __init__(self, row=None):
        self.row = row

    def fetchone(self):
        return self.row


class _SourceDB:
    """Capture the observable DB contract without impersonating PostgreSQL ACID."""

    def __init__(self, status="connected", gap=None):
        self.status = status
        self.gap = gap
        self.queries = []
        self.health_rows = []
        self.gap_inserts = 0
        self.now = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)

    def execute(self, query, args=None):
        self.queries.append(query)
        if "SELECT ts,status FROM feed_health" in query:
            return _One({"ts": self.now - timedelta(minutes=2), "status": self.status})
        if "SELECT id FROM data_gaps" in query:
            return _One({"id": self.gap} if self.gap is not None else None)
        if "SELECT clock_timestamp() AS ts" in query:
            return _One({"ts": self.now})
        if "INSERT INTO feed_health" in query:
            self.status = "disconnected"
            self.health_rows.append(json.loads(args[1]))
        if "INSERT INTO data_gaps" in query:
            self.gap_inserts += 1
            self.gap = 49
            return _One({"id": self.gap})
        if "SELECT max(recv_ts)" in query:
            return _One({"ts": None})
        return _One()


def test_unclean_restart_persists_one_gap_and_never_promotes_source():
    db = _SourceDB()
    previous = db.now - timedelta(minutes=2)
    assert live.reconcile_unclean_ws_start(db, "ETHUSDT", last_trade=previous) == 49
    assert "pg_advisory_xact_lock" in db.queries[0]
    assert db.status == "disconnected"
    assert db.gap_inserts == 1
    assert db.health_rows[0]["source_quality"] == "gap"
    assert db.health_rows[0]["ml_eligible"] is False
    assert live.reconcile_unclean_ws_start(db, "ETHUSDT", last_trade=previous) == 49
    assert db.gap_inserts == 1
    assert len(db.health_rows) == 1


def test_disconnect_status_and_gap_share_source_fence():
    db = _SourceDB(status="disconnected")
    stamp = db.now - timedelta(seconds=10)
    pending = live.persist_ws_disconnect(
        db, "ETHUSDT", "session-r22", "socket_closed",
        connected_at=stamp, last_trade=stamp)
    assert pending == 49
    assert "pg_advisory_xact_lock" in db.queries[0]
    assert db.gap_inserts == 1
    assert db.health_rows[0]["ml_eligible"] is False
    assert db.health_rows[0]["source_quality"] == "gap"
    again = live.persist_ws_disconnect(
        db, "ETHUSDT", "session-r22", "repeat_disconnect", pending_id=pending)
    assert again == pending
    assert db.gap_inserts == 1


def test_catchup_plan_prioritizes_rehydrate_and_retry_without_duplicates():
    start = date(2026, 1, 1)
    end = start + timedelta(days=120)
    saved = start + timedelta(days=6)
    work = archives.plan_archive_work(
        start, end, start, [saved, start + timedelta(days=40)],
        start, [start + timedelta(days=70)], rehydrate=[saved, start])
    days = [day for _, day in work]
    assert len(days) == len(set(days))
    assert work[0] == ("rehydrate", saved)
    assert work[1] == ("rehydrate", start)
    assert ("retry", start + timedelta(days=40)) in work
    assert ("fresh", saved) not in work
    assert all(start <= day < end for day in days)
    assert len([1 for mode, _ in work if mode == "fresh"]) <= 24
    assert len([1 for mode, _ in work if mode == "rehydrate"]) <= 4


def test_catchup_plan_excludes_future_and_rejects_invalid_range():
    start = date(2026, 1, 1)
    end = start + timedelta(days=2)
    planned = archives.plan_archive_work(
        start, end, start + timedelta(days=100),
        [end + timedelta(days=1)], end + timedelta(days=50),
        [end + timedelta(days=2)], rehydrate=[end])
    assert planned
    assert all(start <= day < end for _, day in planned)
    assert len({day for _, day in planned}) == len(planned)
    with pytest.raises(ValueError, match="Invalid or unbounded"):
        archives.plan_archive_work(start, start - timedelta(days=1), start, [], start, [])


@pytest.mark.parametrize("http_status", [200, 404])
def test_completed_archive_changed_or_missing_fails_before_copy(
        monkeypatch, tmp_path, http_status):
    """A completed immutable source cannot be silently re-certified/replaced."""
    original = "a" * 64
    sql_seen = []

    class DB:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, args=None):
            sql_seen.append(sql)
            if "SELECT state,sha256 FROM ingestion_job" in sql:
                return _One({"state": "complete", "sha256": original})
            if "SELECT 1 FROM archive_source_price_bins" in sql:
                return _One(None)
            if "SELECT 1 FROM archive_stage_receipts" in sql:
                return _One(None)
            raise AssertionError("No post-download mutation/COPY permitted")

    class Response:
        status_code = http_status

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def raise_for_status(self):
            pass

        async def aiter_bytes(self):
            yield b"not-the-original-immutable-gzip-source"

    class Client:
        def stream(self, *args, **kwargs):
            return Response()

    monkeypatch.setattr(archives, "connection", lambda _settings: DB())
    monkeypatch.setattr(archives, "disk_usage", lambda _path: SimpleNamespace(free=10**12))
    monkeypatch.setattr(archives.tempfile, "gettempdir", lambda: str(tmp_path))
    settings = SimpleNamespace(symbol="ETHUSDT", category="linear", minimum_free_gb=0)
    with pytest.raises(DataIntegrityError, match=(
            "SHA256 changed" if http_status == 200 else "unavailable")):
        asyncio.run(archives.download_archive(settings, date(2026, 11, 20), Client()))
    assert len(sql_seen) == 3
    assert all("UPDATE" not in sql and "INSERT" not in sql
               and "COPY" not in sql for sql in sql_seen)


def test_truncated_shared_rest_budget_quarantines_instead_of_reset(tmp_path):
    path = tmp_path / "shared-rest-budget.json"
    path.touch()
    budget = SharedRateBudget("https://api.bybit.com", 2, path=path)
    delay = budget._update()
    assert delay >= 60
    assert budget._update() > 0
    assert json.loads(path.read_text())["next"] > 0


def test_impossibly_future_rest_budget_quarantines(tmp_path):
    path = tmp_path / "shared-rest-budget.json"
    path.write_text(json.dumps({"next": 9_000_000_000_000}))
    budget = SharedRateBudget("https://api.bybit.com", 2, path=path)
    delay = budget._update()
    assert delay >= 60
    assert budget._update() > 0
