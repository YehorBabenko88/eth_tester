"""Control G real-PG startup test: disposable local CI database and read-only role.

Never uses a pre-existing ETH Tester database or makes network exchange requests.
Requires ETH_TEST_PG_DSN to local CI admin database named 'postgres'.
"""
import asyncio
import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import pytest

from eth_tester.config import Settings
from eth_tester.database import init_database, verify_existing_schema
from eth_tester import supervisor


pytestmark = pytest.mark.postgres


@pytest.fixture
def control_pg():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("Disposable PostgreSQL CI DSN is not configured")
    parts = conninfo_to_dict(admin_dsn)
    if (parts.get("host") not in ("127.0.0.1", "localhost")
            or parts.get("dbname") != "postgres"):
        pytest.skip("Refusing non-local or non-CI PostgreSQL database")
    nonce = uuid4().hex[:12]
    name = "eth_tester_control_" + nonce
    role = "eth_control_read_" + nonce
    password = uuid4().hex
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        admin_url = make_conninfo(admin_dsn, dbname=name)
        init_database(Settings(_env_file=None, database_url=admin_url))
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER "
                        "NOCREATEDB NOCREATEROLE NOINHERIT").format(
                            sql.Identifier(role), sql.Literal(password)))
            admin.execute(sql.SQL("ALTER ROLE {} SET default_transaction_read_only=on")
                          .format(sql.Identifier(role)))
            admin.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}")
                          .format(sql.Identifier(name), sql.Identifier(role)))
        with psycopg.connect(admin_url) as admin:
            admin.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}")
                          .format(sql.Identifier(role)))
            admin.execute(sql.SQL("GRANT SELECT ON public.eth_storage_identity, "
                                  "public.eth_schema_migrations TO {}")
                          .format(sql.Identifier(role)))
        service = Settings(_env_file=None, database_url=make_conninfo(
            admin_dsn, dbname=name, user=role, password=password))
        yield service, Settings(_env_file=None, database_url=admin_url)
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)")
                          .format(sql.Identifier(name)))
            admin.execute(sql.SQL("DROP ROLE IF EXISTS {}")
                          .format(sql.Identifier(role)))


def test_readonly_role_verifies_owner_migrations_and_tables(control_pg):
    service, _admin = control_pg
    proof = verify_existing_schema(service)
    assert proof["verified"] is True
    assert proof["database"].startswith("eth_tester_control_")
    assert proof["schema_version"].startswith("000")
    with psycopg.connect(service.database_url) as conn:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            conn.execute("CREATE TABLE forbidden_runtime_ddl (id integer)")


def test_checksum_tamper_blocks_supervisor_before_network_workers(control_pg, monkeypatch, tmp_path):
    service, admin_settings = control_pg
    started = []

    async def reject_spawn(*args, **kwargs):
        started.append(args)
        raise AssertionError("worker spawned on stale schema")

    with psycopg.connect(admin_settings.database_url) as admin:
        admin.execute(
            "UPDATE public.eth_schema_migrations SET checksum=%s "
            "WHERE version=(SELECT min(version) FROM public.eth_schema_migrations)",
            ("0" * 64,))
    monkeypatch.setattr(supervisor.asyncio, "create_subprocess_exec", reject_spawn)
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        asyncio.run(supervisor.Supervisor(path=tmp_path).run(service))
    assert started == []
    assert not (tmp_path / "supervisor.json").exists()
