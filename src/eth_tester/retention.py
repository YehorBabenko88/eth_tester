"""Safe retention and operational storage health: never prune raw market history."""
from shutil import disk_usage

from psycopg import sql

from .database import connection


def storage_report(settings):
    with connection(settings) as conn:
        size = conn.execute(
            "SELECT pg_database_size(current_database()) AS bytes"
        ).fetchone()
        rows = conn.execute(
            """SELECT (SELECT COUNT(*) FROM trades) trades,
                      (SELECT COUNT(*) FROM candles) candles,
                      (SELECT COUNT(*) FROM book_events) book_events"""
        ).fetchone()
        wal = conn.execute(
            """SELECT pg_current_wal_lsn()::text AS current_lsn,
                      pg_size_pretty(pg_database_size(current_database())) AS pretty_size"""
        ).fetchone()
        return {"bytes": size["bytes"], **rows, **wal}


def disk_guard(path, minimum_free_gb=20):
    usage = disk_usage(path)
    minimum = int(minimum_free_gb * 1024**3)
    if usage.free < minimum:
        raise RuntimeError(f"Insufficient disk space: {usage.free} < {minimum}")
    return {"total": usage.total, "used": usage.used, "free": usage.free}


def cleanup_diagnostics(settings, *, health_days=90, event_days=180):
    if health_days < 1 or event_days < 1:
        raise ValueError("Retention days must be positive")
    with connection(settings) as conn:
        health = conn.execute(
            "DELETE FROM feed_health WHERE ts < now() - (%s * interval '1 day')",
            (health_days,),
        ).rowcount
        logs = conn.execute(
            "DELETE FROM bot_events WHERE ts < now() - (%s * interval '1 day')",
            (event_days,),
        ).rowcount
        storage_logs = conn.execute(
            "DELETE FROM eth_storage_events WHERE created_at < now() - (%s * interval '1 day')",
            (event_days,),
        ).rowcount
    return {"feed_health_deleted": health, "bot_events_deleted": logs,
            "storage_events_deleted": storage_logs, "raw_market_data_deleted": 0}
