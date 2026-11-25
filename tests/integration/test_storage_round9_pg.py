"""B round-9 disposable PostgreSQL fault tests; no existing DB is modified.

Requires ETH_TEST_PG_DSN. Synthetic data here is NOT ETHUSDT completeness
certification or production acceptance; no exchange orders are placed.
"""
from datetime import date, datetime, timezone
import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.retention import cleanup_diagnostics, storage_operational_preflight
from eth_tester.storage.recovery import recover_abandoned_ingestion

pytestmark = pytest.mark.postgres


@pytest.fixture
def owned_storage_lab():
    admin_dsn = os.getenv("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("ETH_TEST_PG_DSN required for disposable PostgreSQL")
    dbname = "eth_tester_b9_" + uuid4().hex[:12]
    settings = Settings(_env_file=None,
                        database_url=make_conninfo(admin_dsn, dbname=dbname))
    try:
        init_database(settings)
        yield settings
    finally:
        # Only drop the fresh, uniquely named database allocated by THIS test.
        with psycopg.connect(make_conninfo(admin_dsn, dbname="postgres"),
                             autocommit=True) as admin:
            if admin.execute("SELECT 1 FROM pg_database WHERE datname=%s",
                             (dbname,)).fetchone():
                admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(dbname)))


def test_serialized_cleanup_skips_competing_lease_preserves_raw(owned_storage_lab,
                                                               monkeypatch):
    monkeypatch.delenv("ETH_TESTER_OWNED_TEMP_DIR", raising=False)
    old = datetime(2020, 1, 1, tzinfo=timezone.utc)
    with connection(owned_storage_lab) as conn:
        conn.execute("""INSERT INTO feed_health(symbol,topic,ts,status)
            VALUES('ETHUSDT','b9-cleanup',%s,'error')""", (old,))
        conn.execute("""INSERT INTO bot_events(ts,severity,event)
            VALUES(%s,'info','b9-cleanup')""", (old,))
        conn.execute("""INSERT INTO bot_events(ts,severity,event,data)
            VALUES(%s,'info','paper_trade_review_v1',%s::jsonb)""",
            (old, '{"retention":"preserve","test":"b9"}'))
        conn.execute("""INSERT INTO trades(symbol,ts,trade_id,price,qty,side,
            notional,source,raw)
            VALUES('ETHUSDT',%s,%s,3000,0.01,'Buy',30,
                   'archive','{}'::jsonb)""",
            (old, 'B9-PRESERVE-'+uuid4().hex[:12]),
        )
    with connection(owned_storage_lab) as blocker:
        blocker.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            ("eth_tester:storage:diagnostic_cleanup",),
        )
        with pytest.raises(RuntimeError, match="Another"):
            cleanup_diagnostics(owned_storage_lab, health_days=90,
                                event_days=180, max_rows_per_table=1)
        assert blocker.execute("""SELECT count(*) AS n FROM feed_health
            WHERE topic='b9-cleanup'""").fetchone()["n"] == 1
    result = cleanup_diagnostics(owned_storage_lab, max_rows_per_table=1)
    assert result["status"] == "ok" and result["serialized"] is True
    assert result["feed_health_deleted"] == 1
    assert result["raw_market_data_deleted"] == 0
    with connection(owned_storage_lab) as conn:
        assert conn.execute("""SELECT count(*) AS n FROM trades
            WHERE trade_id LIKE 'B9-PRESERVE-%'""").fetchone()["n"] == 1
        assert conn.execute("""SELECT count(*) AS n FROM bot_events
            WHERE event='paper_trade_review_v1'""").fetchone()["n"] == 1


def test_recovery_bounded_requeues_only_old_running_jobs(owned_storage_lab):
    days = [date(2026, 11, 19), date(2026, 11, 20), date(2026, 11, 21)]
    with connection(owned_storage_lab) as conn:
        for day in days:
            conn.execute("""INSERT INTO ingestion_job(
                source,symbol,day,state,attempts,updated_at)
                VALUES('b9-recover','ETHUSDT',%s,'running',1,
                       now()-interval '4 hours')""", (day,))
        with pytest.raises(RuntimeError, match="Stop all"):
            recover_abandoned_ingestion(conn, workers_stopped=False)
        first = recover_abandoned_ingestion(conn, workers_stopped=True,
                                             stale_minutes=5, max_jobs=1)
        assert first["requeued"] == 1 and first["bounded"] is True
        assert first["raw_rows_deleted"] == 0
        second = recover_abandoned_ingestion(conn, workers_stopped=True,
                                              stale_minutes=5, max_jobs=1)
        assert second["requeued"] == 1
        third = recover_abandoned_ingestion(conn, workers_stopped=True,
                                             stale_minutes=5, max_jobs=1)
        assert third["requeued"] == 1
        empty = recover_abandoned_ingestion(conn, workers_stopped=True,
                                             stale_minutes=5, max_jobs=1)
        assert empty["requeued"] == 0
    with connection(owned_storage_lab) as conn:
        assert conn.execute("""SELECT count(*) AS n FROM ingestion_job
            WHERE source='b9-recover' AND state='pending'""").fetchone()["n"] == 3


def test_readonly_capacity_preflight_never_confuses_wal_or_remote_disk(
        owned_storage_lab):
    report = storage_operational_preflight(
        owned_storage_lab, max_database_bytes=1,
        max_cluster_wal_bytes=1, min_remote_pg_free_bytes=1,
    )
    assert report["status"] == "blocked"  # Actual DB exceeds 1 byte.
    assert report["checks"]["database_bytes"] == "blocked"
    assert report["checks"]["postgres_free"] == "unknown"
    assert report["paper_only"] is True
    assert report["may_delete_raw_data"] is False
