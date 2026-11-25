"""Bounded diagnostics retention and non-destructive PostgreSQL health metrics."""
import os
from shutil import disk_usage

import psycopg
from psycopg import sql

from .database import connection
from .storage import check_ownership


def storage_report(settings, *, exact_counts=False, local_disk_path=None):
    """DB size and WAL telemetry. Remote-PG free bytes are NOT local free bytes."""
    with connection(settings) as conn:
        check_ownership(conn)
        row = conn.execute(
            """SELECT pg_database_size(current_database()) AS bytes,
                      pg_size_pretty(pg_database_size(current_database())) AS pretty_size,
                      CASE WHEN pg_is_in_recovery()
                           THEN pg_last_wal_replay_lsn()::text
                           ELSE pg_current_wal_lsn()::text END AS current_lsn,
                      pg_is_in_recovery() AS database_in_recovery"""
        ).fetchone()
        result = dict(row)
        if exact_counts:
            counts = conn.execute(
                """SELECT (SELECT COUNT(*) FROM trades) trades,
                          (SELECT COUNT(*) FROM candles) candles,
                          (SELECT COUNT(*) FROM book_events) book_events"""
            ).fetchone()
            result.update(counts)
        else:
            estimated = conn.execute(
                """SELECT relname, GREATEST(0, reltuples)::bigint AS n
                   FROM pg_class WHERE oid IN (
                     to_regclass('public.trades'), to_regclass('public.candles'),
                     to_regclass('public.book_events'))"""
            ).fetchall()
            result.update({name: 0 for name in ("trades", "candles", "book_events")})
            result.update({r["relname"]: r["n"] for r in estimated})
        result["row_counts_estimated"] = not exact_counts
        wal = conn.execute(
            "SELECT wal_bytes::numeric AS cluster_wal_bytes_generated, stats_reset AS cluster_wal_stats_reset "
            "FROM pg_stat_wal"
        ).fetchone()
        result.update(wal)
        result["cluster_wal_on_disk_bytes"] = None
        try:
            with conn.transaction():
                item = conn.execute(
                    "SELECT coalesce(sum(size),0)::bigint AS bytes FROM pg_ls_waldir()"
                ).fetchone()
                result["wal_on_disk_bytes"] = item["bytes"]
                result["cluster_wal_on_disk_bytes"] = item["bytes"]
        except psycopg.errors.InsufficientPrivilege:
            pass
        if local_disk_path is not None:
            result["local_disk"] = disk_guard(local_disk_path, minimum_free_gb=0)
        return result


def disk_guard(path, minimum_free_gb=20):
    if minimum_free_gb < 0:
        raise ValueError("minimum_free_gb must be nonnegative")
    usage = disk_usage(path)
    minimum = int(minimum_free_gb * 1024**3)
    if usage.free < minimum:
        raise RuntimeError(f"Insufficient disk space: {usage.free} < {minimum}")
    return {"total": usage.total, "used": usage.used, "free": usage.free}


def _purge_bounded(conn, table, column, days, limit):
    """Delete only short-lived diagnostics; never trade/research/source truth.

    Connection state is an admission dependency. A 90-day-old connected WS
    session can still be the latest valid session if no reconnect occurred.
    Preserve ALL connection history until a separate durable incident ledger
    with independent source-proof retention exists. Never auto-resolve gaps.
    """
    if (table, column) not in {
        ("feed_health", "ts"), ("bot_events", "ts"),
        ("eth_storage_events", "created_at"),
    }:
        raise ValueError("Only ETH Tester diagnostic tables are eligible")
    if table == "bot_events":
        protected = sql.SQL(
            " AND event <> 'paper_trade_review_v1' "
            "AND coalesce(data->>'retention','') <> 'preserve'")
    elif table == "feed_health":
        protected = sql.SQL(" AND topic <> 'connection'")
    else:
        protected = sql.SQL("")
    command = sql.SQL(
        "WITH doomed AS (SELECT tableoid,ctid FROM {} WHERE {} < "
        "(now() - (%s * interval '1 day')){} ORDER BY {} LIMIT %s "
        "FOR UPDATE SKIP LOCKED) "
        "DELETE FROM {} AS target USING doomed "
        "WHERE target.tableoid = doomed.tableoid AND target.ctid = doomed.ctid"
    ).format(sql.Identifier(table), sql.Identifier(column), protected,
             sql.Identifier(column), sql.Identifier(table))
    return conn.execute(command, (days, limit)).rowcount


def cleanup_diagnostics(settings, *, health_days=90, event_days=180,
                        max_rows_per_table=10000):
    """One transaction of bounded diagnostics cleanup; NOT market-data cleanup.

    A completed bounded batch is not proof that all eligible old rows are gone:
    SKIP LOCKED deliberately leaves blocked rows for a later cycle.
    """
    if (type(health_days) is not int or type(event_days) is not int or
            type(max_rows_per_table) is not int or
            health_days < 1 or event_days < 1 or
            not 1 <= max_rows_per_table <= 10000):
        raise ValueError("Retention days must be positive; max 10000 per table")
    with connection(settings) as conn:
        # Timeouts MUST precede ownership read and advisory lock acquisition.
        conn.execute("SELECT set_config('lock_timeout',%s,true)", ("2500ms",))
        conn.execute("SELECT set_config('statement_timeout',%s,true)", ("20000ms",))
        check_ownership(conn)
        acquired = conn.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS acquired",
            ("eth_tester:storage:diagnostic_cleanup",),
        ).fetchone()
        if acquired is None or acquired["acquired"] is not True:
            raise RuntimeError("Another ETH Tester diagnostic cleanup is active")
        health = _purge_bounded(conn, "feed_health", "ts", health_days,
                                max_rows_per_table)
        events = _purge_bounded(conn, "bot_events", "ts", event_days,
                                max_rows_per_table)
        storage = _purge_bounded(conn, "eth_storage_events", "created_at",
                                 event_days, max_rows_per_table)
    # Filesystem sweep is optional and OUTSIDE the already committed SQL txn.
    from .storage.maintenance import cleanup_owned_temp
    owned_root = os.environ.get("ETH_TESTER_OWNED_TEMP_DIR", "")
    if owned_root:
        temp = cleanup_owned_temp(owned_root)  # fail closed on Windows
    else:
        temp = {"status": "not_configured", "deleted_owned_temp_files": 0,
                "raw_market_data_deleted": 0, "backups_deleted": 0}
    return {"status": "ok", "status_scope": "batch_only",
            "serialized": True, "feed_health_deleted": health,
            "bot_events_deleted": events, "storage_events_deleted": storage,
            "raw_market_data_deleted": 0, "owned_temp": temp,
            "cleanup_complete": False,
            "batch_limited": any(x == max_rows_per_table for x in
                                 (health, events, storage))}


def storage_operational_preflight(settings, *, local_temp_path=None,
                                  max_database_bytes=None,
                                  max_cluster_wal_bytes=None,
                                  min_remote_pg_free_bytes=None):
    """Read-only pressure report; unknown PostgreSQL disk cannot be ready."""
    from .storage.maintenance import assess_pressure
    state = storage_report(settings, local_disk_path=local_temp_path)
    disk = state.get("local_disk")
    minimum_gb = getattr(settings, "minimum_free_gb", 20)
    if (isinstance(minimum_gb, bool) or
            not isinstance(minimum_gb, (float, int)) or minimum_gb < 0):
        raise ValueError("Invalid configured local minimum free GB")
    pressure = assess_pressure(
        local_free_bytes=disk["free"] if disk else None,
        minimum_local_free_bytes=int(minimum_gb*1024**3) if local_temp_path else None,
        db_bytes=state["bytes"], max_db_bytes=max_database_bytes,
        remote_pg_disk_free_bytes=None,
        min_remote_pg_free_bytes=min_remote_pg_free_bytes,
        wal_on_disk_bytes=state.get("cluster_wal_on_disk_bytes"),
        max_wal_on_disk_bytes=max_cluster_wal_bytes,
        database_in_recovery=state.get("database_in_recovery",False),
    )
    status = pressure["status"]
    if status == "ok" and pressure["checks"]["postgres_free"] in (
            "unknown", "not_configured"):
        status = "unknown"
    return {"status": status, "checks": pressure["checks"],
            "database_bytes": state["bytes"],
            "cluster_wal_on_disk_bytes": state.get("cluster_wal_on_disk_bytes"),
            "local_disk_scope": "not_measured" if disk is None else "process_host_only",
            "paper_only": True, "may_delete_raw_data": False}
