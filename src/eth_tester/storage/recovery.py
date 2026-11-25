"""Conservative post-crash reconciliation; PostgreSQL WAL owns transaction recovery."""
from . import check_ownership


def recover_abandoned_ingestion(conn, *, workers_stopped=False, stale_minutes=180):
    """Requeue stale work *only* after the workers are confirmed stopped.

    Does not erase data or mark incomplete downloads complete. In-progress
    transactions were already committed or rolled back by PostgreSQL WAL.
    The next archive worker must use the same idempotent COPY transaction.
    """
    if not workers_stopped:
        raise RuntimeError("Stop all ETH Tester ingestion workers before reconciliation")
    if stale_minutes < 5:
        raise ValueError("stale_minutes must be >= 5")
    check_ownership(conn)
    acquired = conn.execute(
        "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
        ("eth_tester:storage:recover",),
    ).fetchone()
    locked = next(iter(acquired.values())) if isinstance(acquired, dict) else acquired[0]
    if not locked:
        raise RuntimeError("Another storage recovery process is active")
    updated = conn.execute(
        """UPDATE ingestion_job
           SET state='pending',
               error=concat_ws(' | ', nullif(error,''), 'interrupted; retry required'),
               updated_at=now()
           WHERE state='running'
             AND updated_at < now() - (%s * interval '1 minute')""",
        (stale_minutes,),
    ).rowcount
    return {"requeued": updated, "raw_rows_deleted": 0}
