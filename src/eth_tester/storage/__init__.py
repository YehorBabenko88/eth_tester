"""Guarded PostgreSQL backup, restore, and database lifecycle for ETH Tester.

Never manipulate a shared PostgreSQL data directory, service or other project.
Do not expose database passwords in process command lines.
"""
import hashlib
import os
from pathlib import Path
import subprocess

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo


def validate_db_name(url):
    name = conninfo_to_dict(url).get("dbname", "")
    if name != "eth_tester" and not name.startswith("eth_tester_"):
        raise ValueError("Dedicated ETH Tester database required")
    return name


def check_ownership(conn):
    row = conn.execute(
        "SELECT owner_tag FROM public.eth_storage_identity WHERE singleton=true"
    ).fetchone()
    value = (row.get("owner_tag") if isinstance(row, dict) else row[0]) if row else None
    if value != "eth_tester":
        raise RuntimeError("Invalid or missing ETH Tester ownership marker")


def _pg_cli_conninfo(url):
    """Move password into child environment, not visible in process argv."""
    params = conninfo_to_dict(url)
    password = params.pop("password", None)
    env = os.environ.copy()
    if password is not None:
        env["PGPASSWORD"] = password
    return make_conninfo(**params), env


def backup_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as reader:
        while chunk := reader.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def verify_backup(path, *, expected_sha256=None, expected_database=None):
    """TOC sanity check ONLY; not a successful full restore attestation."""
    from .secure_backup import verified_archive
    with verified_archive(path, expected_sha256=expected_sha256,
                          expected_database=expected_database):
        return True


def backup_database(url, destination):
    """Build a verified archive in an owner-controlled directory, publish last."""
    from .secure_backup import dump_privately
    name = validate_db_name(url)
    with psycopg.connect(url, connect_timeout=10) as conn:
        check_ownership(conn)
    cli_dsn, env = _pg_cli_conninfo(url)
    return dump_privately(url, destination, cli_dsn=cli_dsn,
                          env=env, expected_database=name)


def restore_database(url, backup_path, *, expected_sha256=None):
    """Restore a pinned verified archive only into a NEW dedicated database.

    The same private descriptor is passed to pg_restore both for TOC and data.
    A failed restoration remains for explicit investigation and is NOT claimed
    to be recovered. This operation does not drop any existing database.
    """
    from .secure_backup import verified_archive
    name = validate_db_name(url)
    with verified_archive(backup_path, expected_sha256=expected_sha256) as (
            binary, fd, pinned_source, checksum):
        admin_url = make_conninfo(url, dbname="postgres")
        with psycopg.connect(admin_url, autocommit=True, connect_timeout=10) as admin:
            if admin.execute(
                    "SELECT 1 FROM pg_database WHERE datname=%s", (name,)
            ).fetchone():
                raise FileExistsError(f"Database {name} already exists")
            admin.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))
            )
        cli_dsn, env = _pg_cli_conninfo(url)
        subprocess.run(
            [binary, "--no-owner", "--no-privileges", "--no-password",
             "--exit-on-error", "--single-transaction", "--dbname",
             cli_dsn, pinned_source],
            env=env, pass_fds=(fd,), check=True, timeout=7200,
        )
        with psycopg.connect(url, connect_timeout=10) as conn:
            check_ownership(conn)
    from ..config import Settings
    from ..database import verify_existing_schema
    verify_existing_schema(Settings(_env_file=None, database_url=url))


def deletion_manifest(url, backup_path):
    """Read-only deletion preflight. A manifest is NOT deletion approval."""
    name = validate_db_name(url)
    verify_backup(backup_path, expected_database=name)
    with psycopg.connect(url, connect_timeout=10) as conn:
        check_ownership(conn)
        oid, size = conn.execute(
            """SELECT oid::text, pg_database_size(oid)
               FROM pg_database WHERE datname=current_database()"""
        ).fetchone()
    return {
        "database": name, "database_oid": oid, "database_bytes": size,
        "backup_sha256": backup_sha256(backup_path),
        "backup_bytes": Path(backup_path).stat().st_size,
        "confirmation_phrase": f"DELETE ETH TESTER {name}",
    }


def drop_owned_database(
    url, *, confirmation, backup_path=None, backup_sha=None,
    expected_database_oid=None,
):
    """Explicitly scoped destructive operation; no auto-uninstall path uses this."""
    name = validate_db_name(url)
    if confirmation != f"DELETE ETH TESTER {name}":
        raise ValueError("Exact irreversible database deletion phrase required")
    if not backup_path or not backup_sha or not expected_database_oid:
        raise ValueError("Verified backup SHA and matching database OID are mandatory")
    verify_backup(backup_path, expected_sha256=backup_sha, expected_database=name)
    with psycopg.connect(url, connect_timeout=10) as conn:
        check_ownership(conn)
        database_oid = conn.execute(
            "SELECT oid::text FROM pg_database WHERE datname=current_database()"
        ).fetchone()[0]
        if database_oid != str(expected_database_oid):
            raise RuntimeError("Database OID has changed since preflight")
    admin_url = make_conninfo(url, dbname="postgres")
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=10) as admin:
        active = admin.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE datname=%s AND pid<>pg_backend_pid()",
            (name,),
        ).fetchone()[0]
        if active:
            raise RuntimeError(f"Active sessions prevent database deletion: {active}")
        oid = admin.execute(
            "SELECT oid::text FROM pg_database WHERE datname=%s", (name,)
        ).fetchone()
        if not oid or oid[0] != str(expected_database_oid):
            raise RuntimeError("Database identity changed before deletion")
        admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
    return name
