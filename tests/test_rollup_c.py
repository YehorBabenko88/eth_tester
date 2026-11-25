"""SQL contract tests with a strict cursor; real PostgreSQL integration remains required."""
from datetime import date, datetime, timezone
from types import SimpleNamespace

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
    def __init__(self, old, new, done=False):
        self.old, self.new, self.done = old, new, done
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
    count = rollups.rollup_day(SimpleNamespace(symbol="ETHUSDT"),
                               date(2026, 11, 25), tf, "sha256")
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
