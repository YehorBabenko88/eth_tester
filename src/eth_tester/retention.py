"""Bounded diagnostic retention and non-destructive PostgreSQL health metrics."""
from shutil import disk_usage

import psycopg
from psycopg import sql

from .database import connection
from .storage import check_ownership


def storage_report(settings, *, exact_counts=False, local_disk_path=None):
    """Report DB bytes and WAL usage without full scans on every heartbeat.

    pg_stat_wal.wal_bytes is cluster-wide cumulative bytes since stats_reset.
    pg_ls_waldir is cluster-wide, privileged WAL-on-disk size; may be None.
    The optional disk path is explicitly local to this Python process, and
    cannot be assumed to match a remote PostgreSQL server's volume.
    """
    with connection(settings) as conn:
        check_ownership(conn)
        row = conn.execute(
            """SELECT pg_database_size(current_database()) AS bytes,
                      pg_size_pretty(pg_database_size(current_database())) AS pretty_size,
                      CASE WHEN pg_is_in_recovery()
                           THEN pg_last_wal_replay_lsn()::text
                           ELSE pg_current_wal_lsn()::text END AS current_lsn"""
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
    if (table, column) not in {
        ("feed_health", "ts"), ("bot_events", "ts"),
        ("eth_storage_events", "created_at"),
    }:
        raise ValueError("Only ETH Tester diagnostic tables are eligible")
    command = sql.SQL(
        "WITH doomed AS (SELECT tableoid,ctid FROM {} WHERE {} < "
        "(now() - (%s * interval '1 day')) ORDER BY {} LIMIT %s) "
        "DELETE FROM {} AS target USING doomed "
        "WHERE target.tableoid = doomed.tableoid AND target.ctid = doomed.ctid"
    ).format(
        sql.Identifier(table), sql.Identifier(column),
        sql.Identifier(column), sql.Identifier(table),
    )
    return conn.execute(command, (days, limit)).rowcount


def cleanup_diagnostics(settings, *, health_days=90, event_days=180,
                        max_rows_per_table=10000):
    """One bounded, transactional cleanup batch, diagnostics only."""
    if health_days < 1 or event_days < 1 or max_rows_per_table < 1:
        raise ValueError("Retention days and batch size must be positive")
    with connection(settings) as conn:
        check_ownership(conn)
        health = _purge_bounded(conn, "feed_health", "ts",
                                health_days, max_rows_per_table)
        events = _purge_bounded(conn, "bot_events", "ts",
                                event_days, max_rows_per_table)
        storage = _purge_bounded(conn, "eth_storage_events", "created_at",
                                 event_days, max_rows_per_table)
    return {"feed_health_deleted": health, "bot_events_deleted": events,
            "storage_events_deleted": storage, "raw_market_data_deleted": 0,
            "batch_limited": any(x == max_rows_per_table for x in
                                 (health, events, storage))}
