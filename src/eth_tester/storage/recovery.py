"""Conservative bounded post-crash reconciliation; PostgreSQL WAL owns rollback."""
from . import check_ownership


def recover_abandoned_ingestion(conn, *, workers_stopped=False, stale_minutes=180,
                                max_jobs=500):
    """Requeue only old jobs after INDEPENDENT confirmation all workers stopped.

    Never mark incomplete downloads complete. Jobs/rows recover transactionally.
    """
    if workers_stopped is not True:
        raise RuntimeError("Stop all ETH Tester ingestion workers before reconciliation")
    if (type(stale_minutes) is not int or stale_minutes < 5 or
            type(max_jobs) is not int or not 1 <= max_jobs <= 5000):
        raise ValueError("Recovery requires >=5 stale minutes and max 5000 jobs")
    # Apply timeouts BEFORE any database ownership SELECT.
    conn.execute("SELECT set_config('lock_timeout',%s,true)", ("2500ms",))
    conn.execute("SELECT set_config('statement_timeout',%s,true)", ("20000ms",))
    check_ownership(conn)
    acquired = conn.execute(
        "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS locked",
        ("eth_tester:storage:recover",),
    ).fetchone()
    locked = next(iter(acquired.values())) if isinstance(acquired, dict) else (
        acquired[0] if acquired else False)
    if not locked:
        raise RuntimeError("Another storage recovery process is active")
    updated = conn.execute(
        """WITH old_jobs AS MATERIALIZED (
               SELECT ctid FROM ingestion_job
               WHERE state='running'
                 AND updated_at < now() - (%s * interval '1 minute')
               ORDER BY updated_at LIMIT %s FOR UPDATE SKIP LOCKED
           )
           UPDATE ingestion_job AS job
           SET state='pending',
               error=concat_ws(' | ', nullif(job.error,''), 'interrupted; retry required'),
               updated_at=now()
           FROM old_jobs WHERE job.ctid=old_jobs.ctid""",
        (stale_minutes, max_jobs),
    ).rowcount
    return {"requeued": updated, "raw_rows_deleted": 0,
            "bounded": updated == max_jobs, "workers_stopped_required": True}
