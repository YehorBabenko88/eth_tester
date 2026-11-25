"""Versioned, additive PostgreSQL schema maintenance for ETH Tester.

Never rewrite an existing market-data table during automatic initialization.
"""
from importlib.resources import files
import hashlib

MIGRATIONS = ("0001_partitioned_archive.sql", "0002_quality_and_provenance.sql", "0003_retention_indexes.sql")


def apply_migrations(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS eth_schema_migrations (
             version text PRIMARY KEY,
             checksum text NOT NULL,
             applied_at timestamptz NOT NULL DEFAULT now()
           )"""
    )
    conn.execute("SELECT pg_advisory_xact_lock(hashtext('eth_tester:schema:migrations'))")
    applied = {
        row["version"] if isinstance(row, dict) else row[0]
        for row in conn.execute("SELECT version FROM eth_schema_migrations")
    }
    unknown = applied - set(MIGRATIONS)
    if unknown:
        raise RuntimeError(f"Database schema newer than this code: {sorted(unknown)}")
    root = files("eth_tester").joinpath("migrations")
    for version in MIGRATIONS:
        statement = root.joinpath(version).read_text(encoding="utf-8")
        digest = hashlib.sha256(statement.encode("utf-8")).hexdigest()
        row = conn.execute(
            "SELECT checksum FROM eth_schema_migrations WHERE version=%s", (version,)
        ).fetchone()
        if row:
            current = row["checksum"] if isinstance(row, dict) else row[0]
            if current != digest:
                raise RuntimeError(f"Migration checksum changed: {version}")
            continue
        conn.execute(statement)
        conn.execute(
            "INSERT INTO eth_schema_migrations(version,checksum) VALUES(%s,%s)",
            (version, digest),
        )
