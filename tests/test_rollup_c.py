"""SQL contract tests with a strict cursor; real PostgreSQL integration remains required."""
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from eth_tester import rollups


class Result:
    def __init__(self, rows=(), rowcount=0):
        self.rows = list(rows)
        self.rowcount = rowcount

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, old, new, done=False, bad_witness=None):
        self.old, self.new, self.done = old, new, done
        self.bad_witness = bad_witness
        self.deleted = False
        self.sql = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def execute(self, query, args=None):
        params = args if args is not None else ()
        assert query.count("%s") == len(params), (query, params)
        self.sql.append((query, params))
        # Model the immutable original archive SHA contract; missing receipts
        # must still fail closed before any DELETE/INSERT.
        if "SELECT state,sha256 FROM ingestion_job" in query:
            return Result([] if self.bad_witness == "job" else
                          [{"state": "complete", "sha256": "a" * 64}])
        if "FROM archive_stage_receipts" in query:
            return Result([] if self.bad_witness == "stage" else
                          [{"original_rows": 2, "distinct_trade_ids": 2,
                            "price_bins": 1, "base_qty": Decimal("3"),
                            "quote_notional": Decimal("300"),
                            "exchange_complete": False}])
        if "SELECT count(*) AS price_bins" in query:
            return Result([{"price_bins": 1, "prints": 2,
                            "base_qty": Decimal("3"),
                            "quote_notional": Decimal("301") if
                            self.bad_witness == "bins" else Decimal("300")}])
        if "WITH expected AS" in query:
            return Result([{"differs": False}])
        if "SELECT 1 FROM aggregation_job" in query:
            return Result([{"exists": 1}] if self.done else [])
        if "SELECT DISTINCT bucket FROM footprint_daily" in query:
            return Result([{"bucket": x} for x in
                           (self.new if self.deleted else self.old)])
        if "DELETE FROM footprint_daily" in query:
            self.deleted = True
        if "INSERT INTO footprint_daily" in query:
            return Result(rowcount=len(self.new))
        return Result()


def run(monkeypatch, old, new, tf="W", done=False):
    fake = FakeConnection(old, new, done)
    monkeypatch.setattr(rollups, "connection", lambda _settings: fake)
    count = rollups.rollup_day(SimpleNamespace(symbol="ETHUSDT",
                                                category="linear"),
                               date(2026, 11, 25), tf, "a" * 64)
    return count, fake.sql


def test_changed_archive_rebuilds_all_contributing_days(monkeypatch):
    week = datetime(2026, 11, 23, tzinfo=timezone.utc)
    count, queries = run(monkeypatch, [week], [week], "W")
    assert count == 1
    sql = "\n".join(q for q, _ in queries)
    assert "sum(buy_qty)" in sql
    assert "sum(sell_qty)" in sql
    assert "DELETE FROM footprint" in sql
    assert "DELETE FROM features" in sql
    assert "INSERT INTO aggregation_job" in sql
    assert "604800" in sql


def test_deleted_archive_buckets_cannot_leave_stale_poc(monkeypatch):
    week = datetime(2026, 11, 23, tzinfo=timezone.utc)
    count, queries = run(monkeypatch, [week], [], "W")
    assert count == 0
    deletes = [(q, params) for q, params in queries
               if "DELETE FROM features" in q]
    assert len(deletes) == 1
    assert deletes[0][1][-1] == [week]


def test_unchanged_digest_performs_no_writes(monkeypatch):
    count, queries = run(monkeypatch, [], [], done=True)
    assert count == 0
    assert not any("DELETE FROM" in q or "INSERT INTO" in q
                   for q, _ in queries)


def test_month_interval_uses_calendar_duration(monkeypatch):
    month = datetime(2026, 11, 1, tzinfo=timezone.utc)
    _, queries = run(monkeypatch, [], [month], "M")
    assert any("interval '1 month'" in q for q, _ in queries)


@pytest.mark.parametrize("bad_witness,error", [
    ("job", "archive_job_sha_missing_or_changed"),
    ("stage", "sha_pinned_archive_stage_receipt_missing"),
    ("bins", "archive_source_price_bins_manifest_mismatch"),
])
def test_missing_or_mismatched_sha_witness_never_mutates_rollup(
        monkeypatch, bad_witness, error):
    fake = FakeConnection([], [], bad_witness=bad_witness)
    monkeypatch.setattr(rollups, "connection", lambda _settings: fake)
    settings = SimpleNamespace(symbol="ETHUSDT", category="linear")
    with pytest.raises(ValueError, match=error):
        rollups.rollup_day(settings, date(2026, 11, 25), "1", "a" * 64)
    assert not any("DELETE FROM" in query or "INSERT INTO" in query
                   for query, _ in fake.sql)
