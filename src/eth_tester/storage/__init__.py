"""Guarded PostgreSQL backup, restore, and database lifecycle for ETH Tester.

Never manipulate a shared PostgreSQL data directory, service or other project.
Do not expose database passwords in process command lines.
"""
import hashlib
import os
from pathlib import Path
import shutil
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
    if not shutil.which("pg_restore"):
        raise RuntimeError("pg_restore unavailable")
    try:
        source = Path(path).resolve(strict=True)
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise RuntimeError('PostgreSQL backup file missing') from exc
    if source.stat().st_size <= 0:
        raise RuntimeError("Empty PostgreSQL backup")
    if expected_sha256 is not None and backup_sha256(source) != expected_sha256:
        raise RuntimeError("PostgreSQL backup checksum mismatch")
    result = subprocess.run(
        ["pg_restore", "--list", str(source)],
        capture_output=True, text=True, check=True, timeout=120,
    )
    if "eth_storage_identity" not in result.stdout:
        raise RuntimeError("ETH Tester ownership marker absent from backup")
    if expected_database is not None:
        names = [line.split("dbname:", 1)[1].strip() for line in result.stdout.splitlines()
                 if line.lstrip().startswith(";") and "dbname:" in line]
        if names != [expected_database]:
            raise RuntimeError("Backup source database does not match target")
    return True


def backup_database(url, destination):
    validate_db_name(url)
    target = Path(destination).resolve()
    if target.exists():
        raise FileExistsError(target)
    if not shutil.which("pg_dump"):
        raise RuntimeError("pg_dump unavailable")
    with psycopg.connect(url, connect_timeout=10) as conn:
        check_ownership(conn)
    target.parent.mkdir(parents=True, exist_ok=True)
    cli_dsn, env = _pg_cli_conninfo(url)
    try:
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as out:
            subprocess.run(
                ["pg_dump", "--format=custom", "--no-password",
                 "--dbname", cli_dsn],
                env=env, stdout=out, check=True, timeout=7200,
            )
        verify_backup(target, expected_database=validate_db_name(url))
        return target
    except BaseException:
        target.unlink(missing_ok=True)
        raise


def restore_database(url, backup_path, *, expected_sha256=None):
    """Restore only into a database which does not already exist."""
    name = validate_db_name(url)
    verify_backup(backup_path, expected_sha256=expected_sha256)
    admin_url = make_conninfo(url, dbname="postgres")
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=10) as admin:
        if admin.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,)).fetchone():
            raise FileExistsError(f"Database {name} already exists")
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    cli_dsn, env = _pg_cli_conninfo(url)
    # A failed restore remains intact for explicit operator inspection.
    subprocess.run(
        ["pg_restore", "--no-owner", "--no-privileges", "--no-password",
         "--exit-on-error", "--single-transaction", "--dbname", cli_dsn,
         str(Path(backup_path).resolve(strict=True))],
        env=env, check=True, timeout=7200,
    )
    with psycopg.connect(url, connect_timeout=10) as conn:
        check_ownership(conn)


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
