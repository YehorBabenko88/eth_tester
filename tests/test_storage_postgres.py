"""Optional real PostgreSQL integration suite.

Set ETH_TEST_PG_DSN to an administrative PostgreSQL connection URL for a
disposable PostgreSQL instance. The fixture creates and cleans ONLY a uniquely
named ETH Tester test database. Without that variable all tests SKIP.
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import os
from pathlib import Path
import shutil
from types import SimpleNamespace
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.database import init_database, connection
from eth_tester.storage import (
    backup_database, backup_sha256, deletion_manifest,
    drop_owned_database, restore_database, verify_backup,
)
from eth_tester.storage.bulk import copy_trades
from eth_tester.storage.partitions import ensure_month_partition


@pytest.fixture(scope="module")
def postgres_lab():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("Real PostgreSQL suite requires ETH_TEST_PG_DSN")
    name = f"eth_tester_ci_{uuid4().hex[:12]}"
    dsn = make_conninfo(admin_dsn, dbname=name)
    settings = SimpleNamespace(database_url=dsn, initial_equity_usd=Decimal("10000"))
    try:
        init_database(settings)
        yield settings
    finally:
        # Only delete the fresh, randomly named test DB we created.
        with psycopg.connect(make_conninfo(admin_dsn, dbname="postgres"),
                             autocommit=True) as admin:
            present = admin.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s", (name,)
            ).fetchone()
            if present:
                admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


def test_migrations_and_reinitialization_preserve_rows(postgres_lab):
    with connection(postgres_lab) as conn:
        versions = conn.execute(
            "SELECT version FROM eth_schema_migrations ORDER BY version"
        ).fetchall()
        assert [r["version"] for r in versions] == [
            "0001_partitioned_archive.sql", "0002_quality_and_provenance.sql"
        ]
        conn.execute(
            """INSERT INTO funding(symbol,ts,rate) VALUES(%s,%s,%s)
               ON CONFLICT DO NOTHING""",
            ("ETHUSDT", datetime(2024, 1, 1, tzinfo=timezone.utc), Decimal("0.0001")),
        )
    init_database(postgres_lab)
    with connection(postgres_lab) as conn:
        assert conn.execute("SELECT count(*) AS n FROM funding").fetchone()["n"] == 1
        assert conn.execute("SELECT owner_tag FROM eth_storage_identity").fetchone()[
            "owner_tag"
        ] == "eth_tester"
        assert conn.execute(
            """SELECT count(*) AS n FROM pg_inherits
               WHERE inhparent=to_regclass('trade_archive_batches')"""
        ).fetchone()["n"] >= 2


def sample_trade(identifier, *, price="3000", minute=0):
    ts = datetime(2026, 11, 20, tzinfo=timezone.utc) + timedelta(minutes=minute)
    p = Decimal(price)
    quantity = Decimal("0.02")
    return ("ETHUSDT", ts, identifier, p, quantity, "Buy", p * quantity,
            "archive", {"trade_id": identifier})


def test_copy_replay_and_conflict_rejection(postgres_lab):
    with connection(postgres_lab) as conn:
        records = [sample_trade("BPG-001"), sample_trade("BPG-002", minute=1)]
        first = copy_trades(conn, records)
        assert first["inserted_rows"] == 2
        repeated = copy_trades(conn, records)
        assert repeated["inserted_rows"] == 0
        assert repeated["deduplicated_rows"] == 2
        assert copy_trades(conn, [])["input_rows"] == 0
        assert copy_trades(conn, [])["input_rows"] == 0
    with connection(postgres_lab) as conn:
        with pytest.raises(ValueError, match="Conflicting"):
            with conn.transaction():
                copy_trades(conn, [sample_trade("BPG-001", price="3100")])
        assert conn.execute(
            "SELECT count(*) AS n FROM trades WHERE trade_id LIKE 'BPG-%'"
        ).fetchone()["n"] == 2
        with pytest.raises(ValueError, match="Conflicting"):
            with conn.transaction():
                copy_trades(conn, [sample_trade("BPG-003"),
                                   sample_trade("BPG-003", price="3100")])
        assert conn.execute(
            "SELECT count(*) AS n FROM trades WHERE trade_id='BPG-003'"
        ).fetchone()["n"] == 0


def test_append_only_feature_snapshots(postgres_lab):
    timestamp = datetime(2026, 11, 20, tzinfo=timezone.utc)
    with connection(postgres_lab) as conn:
        conn.execute(
            """INSERT INTO feature_snapshots(
                snapshot_key,symbol,tf,bar_ts,available_at,observed_at,
                source,quality,is_causal,values_json)
               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
            ("test-snap-001", "ETHUSDT", "1", timestamp,
             timestamp + timedelta(minutes=1), timestamp + timedelta(minutes=2),
             "test", "complete", True, '{"delta":"8"}'),
        )
        with pytest.raises(psycopg.Error, match="append-only"):
            with conn.transaction():
                conn.execute(
                    "UPDATE feature_snapshots SET quality='bad' WHERE snapshot_key=%s",
                    ("test-snap-001",),
                )
        assert conn.execute(
            "SELECT quality FROM feature_snapshots WHERE snapshot_key=%s",
            ("test-snap-001",),
        ).fetchone()["quality"] == "complete"


def test_monthly_partitions(postgres_lab):
    with connection(postgres_lab) as conn:
        child = ensure_month_partition(conn, "eth_storage_events", 2026, 12)
        assert child == "eth_storage_events_2026_12"
        assert conn.execute(
            """SELECT count(*) AS n FROM pg_inherits
               WHERE inhrelid=to_regclass(%s)
                 AND inhparent=to_regclass('eth_storage_events')""",
            (child,),
        ).fetchone()["n"] == 1


def test_verified_backup_restore_and_scoped_delete(postgres_lab, tmp_path):
    if not shutil.which("pg_dump") or not shutil.which("pg_restore"):
        pytest.skip("pg_dump and pg_restore are mandatory for backup integration")
    archive = Path(tmp_path) / "owned.dump"
    backup_database(postgres_lab.database_url, archive)
    checksum = backup_sha256(archive)
    assert verify_backup(archive, expected_sha256=checksum)
    with pytest.raises(RuntimeError, match="checksum"):
        verify_backup(archive, expected_sha256="0" * 64)
    source_name = postgres_lab.database_url
    restored_name = f"eth_tester_ci_restored_{uuid4().hex[:12]}"
    restored = make_conninfo(source_name, dbname=restored_name)
    admin = make_conninfo(source_name, dbname="postgres")
    try:
        restore_database(restored, archive, expected_sha256=checksum)
        with psycopg.connect(restored) as conn:
            assert conn.execute(
                "SELECT count(*) FROM trades WHERE trade_id LIKE 'BPG-%'"
            ).fetchone()[0] == 2
        manifest = deletion_manifest(restored, archive)
        with pytest.raises(ValueError):
            drop_owned_database(
                restored, confirmation="wrong", backup_path=archive,
                backup_sha=checksum, expected_database_oid=manifest["database_oid"],
            )
        assert drop_owned_database(
            restored, confirmation=manifest["confirmation_phrase"],
            backup_path=archive, backup_sha=checksum,
            expected_database_oid=manifest["database_oid"],
        ) == restored_name
    finally:
        # In case an assertion above fails, never leave the test DB behind.
        with psycopg.connect(admin, autocommit=True) as conn:
            exists = conn.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s", (restored_name,)
            ).fetchone()
            if exists:
                conn.execute(
                    sql.SQL("DROP DATABASE {}").format(sql.Identifier(restored_name))
                )


@pytest.mark.skipif(
    os.environ.get("ETH_TEST_STORAGE_STRESS") != "1",
    reason="Set ETH_TEST_STORAGE_STRESS=1 for 10k-trade COPY stress",
)
def test_bulk_copy_ten_thousand(postgres_lab):
    with connection(postgres_lab) as conn:
        result = copy_trades(
            conn, (sample_trade(f"STRESS-{i:07d}", minute=i % 1440)
                   for i in range(10_000))
        )
        assert result["inserted_rows"] == 10_000
        assert copy_trades(
            conn, (sample_trade(f"STRESS-{i:07d}", minute=i % 1440)
                   for i in range(10_000))
        )["deduplicated_rows"] == 10_000
