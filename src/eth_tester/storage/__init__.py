"""Guarded PostgreSQL backup, restore, and deletion for ETH Tester."""
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
        "SELECT owner_tag FROM eth_storage_identity WHERE singleton=true"
    ).fetchone()
    if not row or row[0] != "eth_tester":
        raise RuntimeError("Invalid or missing ETH Tester ownership marker")


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
    try:
        with target.open("xb") as out:
            subprocess.run(["pg_dump", "--format=custom", "--no-password",
                            "--dbname", url], stdout=out, check=True, timeout=7200)
        if target.stat().st_size == 0:
            raise RuntimeError("Empty backup")
        return target
    except BaseException:
        target.unlink(missing_ok=True)
        raise


def verify_backup(path):
    if not shutil.which("pg_restore"):
        raise RuntimeError("pg_restore unavailable")
    result = subprocess.run(["pg_restore", "--list", str(Path(path).resolve())],
                            capture_output=True, text=True, check=True, timeout=120)
    if "eth_storage_identity" not in result.stdout:
        raise RuntimeError("Ownership marker absent from backup")
    return True


def restore_database(url, backup_path):
    name = validate_db_name(url)
    verify_backup(backup_path)
    admin_url = make_conninfo(url, dbname="postgres")
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=10) as admin:
        if admin.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,)).fetchone():
            raise FileExistsError(f"Database {name} already exists")
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    # A failed restore remains intact for explicit inspection and recovery.
    subprocess.run(["pg_restore", "--no-owner", "--no-privileges", "--no-password",
                    "--exit-on-error", "--single-transaction", "--dbname", url,
                    str(Path(backup_path).resolve())], check=True, timeout=7200)
    with psycopg.connect(url, connect_timeout=10) as conn:
        check_ownership(conn)


def drop_owned_database(url, *, confirmation, backup_verified=False):
    name = validate_db_name(url)
    if confirmation != f"DELETE ETH TESTER {name}" or not backup_verified:
        raise ValueError("Exact confirmation and independently verified backup required")
    with psycopg.connect(url, connect_timeout=10) as conn:
        check_ownership(conn)
    admin_url = make_conninfo(url, dbname="postgres")
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=10) as admin:
        active = admin.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE datname=%s AND pid<>pg_backend_pid()",
            (name,),
        ).fetchone()[0]
        if active:
            raise RuntimeError(f"Active sessions prevent deletion: {active}")
        admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
    return name
