"""Round 9 Storage B pure deterministic, non-destructive fault simulations."""
import os
from types import SimpleNamespace

import pytest

from eth_tester.storage.maintenance import (
    assess_pressure, cleanup_owned_temp, local_temp_preflight,
)


def test_capacity_fail_closed_on_remote_unknown_and_cluster_wal_unknown():
    actual = assess_pressure(
        local_free_bytes=1000, minimum_local_free_bytes=200,
        db_bytes=1000, max_db_bytes=3000,
        remote_pg_disk_free_bytes=None, min_remote_pg_free_bytes=500,
        wal_on_disk_bytes=None, max_wal_on_disk_bytes=10000,
    )
    assert actual["status"] == "unknown"
    assert actual["checks"]["postgres_free"] == "unknown"
    assert actual["checks"]["cluster_wal_on_disk"] == "unknown"
    assert actual["may_delete_raw_data"] is False


@pytest.mark.parametrize("observed,threshold,expected", [
    (999, 1000, "blocked"), (1000, 1000, "ok"),
    (1001, 1000, "ok"), (0, 0, "ok"),
])
def test_local_space_boundaries(observed, threshold, expected):
    assert assess_pressure(
        local_free_bytes=observed, minimum_local_free_bytes=threshold,
        db_bytes=1,
    )["status"] == expected


def test_db_highwater_and_recovery_block_without_destructive_response():
    report = assess_pressure(
        local_free_bytes=2**30, minimum_local_free_bytes=1,
        db_bytes=101, max_db_bytes=100, database_in_recovery=True,
    )
    assert report["status"] == "blocked"
    assert report["checks"]["database_role"] == "blocked"
    assert report["may_delete_raw_data"] is False


@pytest.mark.parametrize("bad", [-1, True, 1.5, "100"])
def test_invalid_threshold_rejected(bad):
    with pytest.raises(ValueError):
        assess_pressure(local_free_bytes=100, minimum_local_free_bytes=bad,
                        db_bytes=100)


def test_owned_temp_fail_closed_without_exact_marker(tmp_path):
    owned = tmp_path / "owned"
    owned.mkdir()
    target = owned / ("eth-tester-stage-" + "a"*32 + ".part")
    target.write_text("partial")
    with pytest.raises(RuntimeError, match="marker|atomic owner"):
        cleanup_owned_temp(owned)
    assert target.exists()
    (owned / ".eth-tester-owned-temp").write_text("foreign\n")
    with pytest.raises(RuntimeError, match="marker|atomic owner"):
        cleanup_owned_temp(owned)
    assert target.exists()


def test_cleanup_only_old_marked_regular_files_never_symlinks_or_backups(tmp_path):
    owned = tmp_path / "owned"
    owned.mkdir()
    (owned / ".eth-tester-owned-temp").write_text("eth_tester:temporary:v1\n")
    old = owned / ("eth-tester-stage-" + "a"*32 + ".part")
    recent = owned / ("eth-tester-stage-" + "b"*32 + ".part")
    stranger = owned / "eth-tester-database-backup.dump"
    external = tmp_path / "outside"
    external.write_text("external-owned")
    for p in (old, recent, stranger):
        p.write_text("data")
    os.utime(old, (10, 10))
    os.utime(recent, (9990, 9990))
    os.utime(stranger, (10, 10))
    link = owned / ("eth-tester-stage-" + "c"*32 + ".part")
    try:
        link.symlink_to(external)
    except (OSError, NotImplementedError):
        link = None
    if os.name != "posix":
        with pytest.raises(RuntimeError, match="atomic owner"):
            cleanup_owned_temp(owned, now=10000, older_than_seconds=3600)
        assert old.exists() and recent.exists() and stranger.exists()
        assert external.read_text() == "external-owned"
        return
    report = cleanup_owned_temp(owned, now=10000, older_than_seconds=3600)
    assert report["deleted_owned_temp_files"] == 1
    assert report["raw_market_data_deleted"] == 0
    assert not old.exists() and recent.exists() and stranger.exists()
    assert external.read_text() == "external-owned"
    if link is not None:
        assert link.is_symlink()


def test_cleanup_bounded_batches_and_idempotent_replay(tmp_path):
    (tmp_path / ".eth-tester-owned-temp").write_text("eth_tester:temporary:v1\n")
    for i in range(4):
        p = tmp_path / (f"eth-tester-stage-{i:032x}.part")
        p.write_text("staging")
        os.utime(p, (10, 10))
    if os.name != "posix":
        with pytest.raises(RuntimeError, match="atomic owner"):
            cleanup_owned_temp(tmp_path, now=10000,
                               older_than_seconds=3600, max_files=2)
        assert sum(p.suffix == ".part" for p in tmp_path.iterdir()) == 4
        return
    first = cleanup_owned_temp(tmp_path, now=10000,
                               older_than_seconds=3600, max_files=2)
    assert first["deleted_owned_temp_files"] == 2 and first["bounded"]
    second = cleanup_owned_temp(tmp_path, now=10000,
                                older_than_seconds=3600, max_files=2)
    assert second["deleted_owned_temp_files"] == 2
    assert cleanup_owned_temp(tmp_path, now=10000)["deleted_owned_temp_files"] == 0


def test_local_preflight_labels_local_not_pg_host(tmp_path):
    assert local_temp_preflight(tmp_path, minimum_free_gb=0)["filesystem_scope"] == "local_process_only"
    with pytest.raises(ValueError):
        local_temp_preflight(tmp_path, minimum_free_gb=-1)


def test_maintenance_retention_source_guards(monkeypatch):
    """Test the real retention module via a fake psycopg import in CI."""
    from eth_tester import retention
    monkeypatch.delenv("ETH_TESTER_OWNED_TEMP_DIR", raising=False)
    class Conn:
        def __init__(self):
            self.statements = []
        def execute(self, statement, params=None):
            query = str(statement)
            self.statements.append((query, params))
            if "pg_try_advisory_xact_lock" in query:
                return SimpleNamespace(fetchone=lambda: {"acquired": True})
            if "WITH doomed" in query:
                return SimpleNamespace(rowcount=1)
            return SimpleNamespace(rowcount=0)
    from contextlib import contextmanager
    conn = Conn()
    @contextmanager
    def fake_connection(_settings):
        yield conn
    monkeypatch.setattr(retention, "connection", fake_connection)
    monkeypatch.setattr(retention, "check_ownership", lambda _: True)
    result = retention.cleanup_diagnostics(object(), max_rows_per_table=100)
    assert result["feed_health_deleted"] == 1
    assert result["raw_market_data_deleted"] == 0
    assert any("pg_try_advisory_xact_lock" in sql for sql, _ in conn.statements)
    assert any("set_config" in sql for sql, _ in conn.statements)


def test_maintenance_rejects_competing_cleanup_lease(monkeypatch):
    from contextlib import contextmanager
    from eth_tester import retention
    class Conn:
        def execute(self, *a, **k):
            return SimpleNamespace(fetchone=lambda: {"acquired": False})
    @contextmanager
    def fake_connection(*_args):
        yield Conn()
    monkeypatch.setattr(retention, "connection", fake_connection)
    monkeypatch.setattr(retention, "check_ownership", lambda _: True)
    with pytest.raises(RuntimeError, match="Another"):
        retention.cleanup_diagnostics(object())


@pytest.mark.parametrize("bad_limit", [0, 10001, True, -1])
def test_maintenance_refuses_unbounded_or_invalid_delete(monkeypatch, bad_limit):
    from eth_tester import retention
    monkeypatch.setattr(retention, "connection", lambda _: (_ for _ in ()).throw(
        AssertionError("Invalid request must not touch DB")))
    with pytest.raises(ValueError):
        retention.cleanup_diagnostics(object(), max_rows_per_table=bad_limit)


def test_maintenance_readonly_preflight_does_not_certify_unknown_pg_disk(monkeypatch, tmp_path):
    from eth_tester import retention
    monkeypatch.setattr(retention, "storage_report", lambda *a, **k: {
        "bytes": 100, "cluster_wal_on_disk_bytes": None,
        "database_in_recovery": False,
        "local_disk": {"free": 20*1024**3},
    })
    report = retention.storage_operational_preflight(
        SimpleNamespace(minimum_free_gb=1), local_temp_path=tmp_path,
        max_database_bytes=200, max_cluster_wal_bytes=100,
        min_remote_pg_free_bytes=1000,
    )
    assert report["status"] == "unknown"
    assert report["checks"]["postgres_free"] == "unknown"
    assert report["checks"]["cluster_wal_on_disk"] == "unknown"
    assert report["may_delete_raw_data"] is False


def test_recovery_requires_true_worker_shutdown_before_sql():
    from eth_tester.storage.recovery import recover_abandoned_ingestion
    class Deny:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("Must reject before any SQL")
    with pytest.raises(RuntimeError, match="Stop all"):
        recover_abandoned_ingestion(Deny(), workers_stopped="true")
    with pytest.raises(ValueError, match="Recovery"):
        recover_abandoned_ingestion(Deny(), workers_stopped=True, max_jobs=5001)


def test_recovery_uses_bounded_skip_locked_and_leaves_raw_rows(monkeypatch):
    from eth_tester.storage import recovery
    monkeypatch.setattr(recovery, "check_ownership", lambda _: True)
    class Conn:
        def __init__(self):
            self.sql = []
        def execute(self, statement, params=None):
            self.sql.append((statement, params))
            if "pg_try_advisory" in statement:
                return SimpleNamespace(fetchone=lambda: {"locked": True})
            return SimpleNamespace(rowcount=2)
    conn = Conn()
    result = recovery.recover_abandoned_ingestion(
        conn, workers_stopped=True, max_jobs=4,
    )
    assert result == {
        "requeued": 2, "raw_rows_deleted": 0,
        "bounded": False, "workers_stopped_required": True,
    }
    updates = [sql for sql, _ in conn.sql if "UPDATE ingestion_job" in sql]
    assert len(updates) == 1
    assert "LIMIT %s FOR UPDATE SKIP LOCKED" in updates[0]
    assert "state='pending'" in updates[0]
    assert "DELETE" not in updates[0]


def test_recovery_refuses_concurrent_lease(monkeypatch):
    from eth_tester.storage import recovery
    monkeypatch.setattr(recovery, "check_ownership", lambda _: True)
    class Conn:
        def execute(self, statement, params=None):
            if "pg_try_advisory" in statement:
                return SimpleNamespace(fetchone=lambda: {"locked": False})
            return SimpleNamespace(rowcount=0)
    with pytest.raises(RuntimeError, match="Another"):
        recovery.recover_abandoned_ingestion(Conn(), workers_stopped=True)


def test_research_review_is_exempt_from_bounded_diagnostic_deletion():
    from eth_tester import retention
    class Conn:
        def __init__(self):
            self.commands = []
        def execute(self, command, params):
            self.commands.append(str(command))
            return SimpleNamespace(rowcount=0)
    conn = Conn()
    retention._purge_bounded(conn, "bot_events", "ts", 180, 100)
    assert "event <> 'paper_trade_review_v1'" in conn.commands[0]
    assert "data->>'retention'" in conn.commands[0]
    assert "FOR UPDATE SKIP LOCKED" in conn.commands[0]
    retention._purge_bounded(conn, "feed_health", "ts", 90, 100)
    assert "paper_trade_review_v1" not in conn.commands[1]


def test_unsupported_table_is_never_deletable():
    from eth_tester import retention
    class Deny:
        def execute(self, *_args):
            raise AssertionError("Unsupported retention cannot reach SQL")
    with pytest.raises(ValueError, match="Only ETH Tester"):
        retention._purge_bounded(Deny(), "paper_trades", "opened_at", 180, 100)
