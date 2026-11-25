from contextlib import contextmanager
import re
from importlib.resources import files
import psycopg
from psycopg.rows import dict_row
from psycopg.conninfo import conninfo_to_dict,make_conninfo
from psycopg import sql
from .config import Settings
from .migrations import apply_migrations
from .storage.partitions import ensure_current_partitions

@contextmanager
def connection(settings: Settings):
    with psycopg.connect(settings.database_url, row_factory=dict_row, connect_timeout=10) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        conn.execute("SET search_path TO public")
        yield conn


def _assert_safe_existing_database(conn):
    """Do not silently adopt an unrelated database merely based on its name."""
    tables = {
        row["tablename"] for row in conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='public'"
        )
    }
    if not tables:
        return
    if "eth_storage_identity" in tables:
        owner = conn.execute(
            "SELECT owner_tag FROM public.eth_storage_identity WHERE singleton=true"
        ).fetchone()
        if owner and owner["owner_tag"] == "eth_tester":
            return
        raise RuntimeError("Invalid ETH Tester database ownership marker")

    baseline = {"trades", "ingestion_cursor", "paper_account"}
    if not baseline.issubset(tables):
        raise RuntimeError("Refusing to adopt unknown nonempty PostgreSQL database")
    resources = files("eth_tester")
    sources = [resources.joinpath("schema.sql")]
    for name in ("0001_partitioned_archive.sql", "0002_quality_and_provenance.sql", "0003_retention_indexes.sql", "0004_trade_identity_guard.sql", "0005_paper_settlement_ledger.sql"):
        sources.append(resources.joinpath("migrations").joinpath(name))
    known = set()
    for source in sources:
        known.update(
            re.findall(
                r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z][a-z0-9_]*)",
                source.read_text(encoding="utf-8"), flags=re.IGNORECASE,
            )
        )
    unknown = tables - known
    if unknown:
        raise RuntimeError(f"Unowned tables prevent migration: {sorted(unknown)}")

def init_database(settings: Settings):
    params = conninfo_to_dict(settings.database_url)
    name = params.get("dbname", "")
    if not (name == "eth_tester" or name.startswith("eth_tester_")):
        raise ValueError("Dedicated eth_tester database required")
    admin_url = make_conninfo(settings.database_url, dbname="postgres")
    lock_key = f"eth_tester:init:{name}"
    # Cross-process database creation + schema bootstrap serialization.
    # PostgreSQL CREATE DATABASE cannot run inside a transaction, so hold
    # a session-level advisory lock on the postgres administration database.
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=10) as admin:
        admin.execute("SELECT pg_advisory_lock(hashtext(%s))", (lock_key,))
        try:
            present = admin.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s", (name,)
            ).fetchone()
            if not present:
                admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            with connection(settings) as conn:
                _assert_safe_existing_database(conn)
                conn.execute(files("eth_tester").joinpath("schema.sql").read_text())
                apply_migrations(conn)
                ensure_current_partitions(conn)
                conn.execute(
                    """INSERT INTO paper_account(id,equity,initial_equity)
                       VALUES(1,%s,%s) ON CONFLICT DO NOTHING""",
                    (settings.initial_equity_usd, settings.initial_equity_usd),
                )
        finally:
            admin.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_key,))


def cursor_ts(conn, source, symbol, interval):
    row = conn.execute("SELECT cursor_ts FROM ingestion_cursor WHERE source=%s AND symbol=%s AND instrument='linear' AND interval=%s",
                       (source,symbol,interval)).fetchone()
    return row["cursor_ts"] if row else None

def save_cursor(conn, source, symbol, interval, stamp, status="complete"):
    conn.execute("""INSERT INTO ingestion_cursor(source,symbol,interval,cursor_ts,last_success,status)
      VALUES(%s,%s,%s,%s,now(),%s) ON CONFLICT(source,symbol,instrument,interval)
      DO UPDATE SET cursor_ts=EXCLUDED.cursor_ts,last_success=EXCLUDED.last_success,status=EXCLUDED.status""",
      (source,symbol,interval,stamp,status))


def verify_existing_schema(settings: Settings):
    """Check owner, version and mandatory tables without DDL at service startup."""
    from hashlib import sha256

    from .migrations import MIGRATIONS
    from .storage import check_ownership

    name = conninfo_to_dict(settings.database_url).get("dbname", "")
    if name != "eth_tester" and not name.startswith("eth_tester_"):
        raise ValueError("Dedicated eth_tester database required")
    with connection(settings) as conn:
        check_ownership(conn)
        found = conn.execute(
            "SELECT to_regclass('public.eth_schema_migrations') AS reg"
        ).fetchone()["reg"]
        if found is None:
            raise RuntimeError("ETH Tester migrations have not been initialized")
        rows = conn.execute(
            "SELECT version,checksum FROM public.eth_schema_migrations ORDER BY version"
        ).fetchall()
        versions = [row["version"] for row in rows]
        if versions != sorted(MIGRATIONS):
            raise RuntimeError("Unrecognized or incomplete ETH Tester schema")
        root = files("eth_tester").joinpath("migrations")
        for row in rows:
            contents = root.joinpath(row["version"]).read_text(encoding="utf-8")
            digest = sha256(contents.encode("utf-8")).hexdigest()
            if row["checksum"] != digest:
                raise RuntimeError("ETH Tester schema migration checksum mismatch")
        required = ("trades", "feature_snapshots", "ingestion_coverage",
                    "paper_position_state", "paper_account")
        for table in required:
            found = conn.execute(
                "SELECT to_regclass(%s) AS reg", ("public." + table,)
            ).fetchone()["reg"]
            if found is None:
                raise RuntimeError("ETH Tester table missing: " + table)
    return {"database": name, "schema_version": versions[-1], "verified": True}
