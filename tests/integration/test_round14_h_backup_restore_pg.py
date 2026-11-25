"""QA H R14: real backup/restore acceptance on UUID disposable PostgreSQL only.

Without an explicit ETH_TEST_PG_DSN this module SKIPS. No production database
is inspected, removed or reconfigured; no exchange operations exist here.
"""
import hashlib
import os
from uuid import uuid4

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.storage import (
    backup_database, backup_sha256, restore_database, verify_backup,
)

pytestmark = pytest.mark.postgres


@pytest.fixture
def h14_disposable_pg():
    admin_dsn = os.getenv("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("Explicit disposable ETH_TEST_PG_DSN required")
    token = uuid4().hex[:12]
    source_name = f"eth_tester_qa_h14_src_{token}"
    target_name = f"eth_tester_qa_h14_dst_{token}"
    source_dsn = make_conninfo(admin_dsn, dbname=source_name)
    target_dsn = make_conninfo(admin_dsn, dbname=target_name)
    owned = []
    try:
        with psycopg.connect(
            make_conninfo(admin_dsn, dbname="postgres"), autocommit=True
        ) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(source_name)))
            owned.append(source_name)
        settings = Settings(_env_file=None, database_url=source_dsn)
        init_database(settings)
        yield settings, source_dsn, target_dsn, target_name, owned
    finally:
        with psycopg.connect(
            make_conninfo(admin_dsn, dbname="postgres"), autocommit=True
        ) as admin:
            for name in reversed(owned):
                assert name in (source_name, target_name)
                admin.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                        sql.Identifier(name))
                )


def _require_trusted_native_postgres_cli():
    """Configured real PG must use explicit B-approved native binaries.

    Never recover by selecting pg_dump/pg_restore from PATH. Unsupported
    Windows archive access intentionally remains fail-closed.
    """
    if os.name != "posix":
        pytest.skip("B secure native PostgreSQL backup currently POSIX-only")
    from eth_tester.storage.secure_backup import trusted_tool
    for binary in ("pg_dump", "pg_restore"):
        try:
            trusted_tool(binary)
        except (OSError, ValueError, RuntimeError) as exc:
            pytest.fail(
                f"No B-trusted {binary}: {type(exc).__name__}. "
                "Configure ETH_TESTER_PG_BIN_DIR to owned absolute bin dir; "
                "PATH search is forbidden", pytrace=False,
            )


def test_native_backup_sha_and_full_restored_research_record(h14_disposable_pg, tmp_path):
    settings, source_dsn, target_dsn, target_name, owned = h14_disposable_pg
    _require_trusted_native_postgres_cli()
    tmp_path.chmod(0o700)
    marker = "QA_H14_NOT_REAL_TRADING_" + uuid4().hex
    with connection(settings) as conn:
        conn.execute(
            """INSERT INTO bot_events(severity,event,data)
               VALUES('info','paper_trade_review_v1',%s::jsonb)""",
            ('{"retention":"preserve","qa_only":true,"marker":"' + marker + '"}',),
        )
    archive = tmp_path / "qa-owned.backup"
    backup_database(source_dsn, archive)
    digest = backup_sha256(archive)
    assert digest == hashlib.sha256(archive.read_bytes()).hexdigest()
    assert verify_backup(
        archive, expected_sha256=digest,
        expected_database=conninfo_to_dict(source_dsn)["dbname"],
    ) is True
    # Include even a partially created target in cleanup ownership ledger.
    owned.append(target_name)
    restore_database(target_dsn, archive, expected_sha256=digest)
    restored = Settings(_env_file=None, database_url=target_dsn)
    with connection(restored) as conn:
        count = conn.execute(
            """SELECT count(*) AS n FROM bot_events
               WHERE event='paper_trade_review_v1'
                 AND data->>'marker'=%s
                 AND data->>'retention'='preserve'""",
            (marker,),
        ).fetchone()["n"]
    assert count == 1


def test_corrupted_archive_cannot_create_restored_database(h14_disposable_pg, tmp_path):
    _settings, _source_dsn, target_dsn, target_name, _owned = h14_disposable_pg
    _require_trusted_native_postgres_cli()
    tmp_path.chmod(0o700)
    corrupt = tmp_path / "not-a-backup"
    corrupt.write_bytes(b"corrupt and untrusted")
    digest = hashlib.sha256(b"different authentic archive").hexdigest()
    with pytest.raises((RuntimeError, ValueError)):
        restore_database(target_dsn, corrupt, expected_sha256=digest)
    with psycopg.connect(
        make_conninfo(os.environ["ETH_TEST_PG_DSN"], dbname="postgres"),
        autocommit=True,
    ) as admin:
        assert admin.execute(
            "SELECT 1 FROM pg_database WHERE datname=%s", (target_name,)
        ).fetchone() is None
