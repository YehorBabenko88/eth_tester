"""Bounded monthly partitions for storage metadata and event streams."""
from datetime import date, datetime, timezone

from psycopg import sql

PARENTS = {"trade_archive_batches": "date", "eth_storage_events": "timestamp"}


def month_bounds(year, month):
    if year < 2000 or year > 2200 or month < 1 or month > 12:
        raise ValueError("Out-of-range monthly partition boundary")
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, end


def ensure_month_partition(conn, parent, year, month):
    """Create one safe partition; refuse unknown parent names.

    If a default partition already contains overlapping rows, PostgreSQL
    refuses the attach rather than moving or losing any existing records.
    """
    if parent not in PARENTS:
        raise ValueError(f"Unknown ETH Tester partition parent: {parent}")
    start, end = month_bounds(year, month)
    child = f"{parent}_{year:04d}_{month:02d}"
    if PARENTS[parent] == "timestamp":
        lower = datetime(start.year, start.month, 1, tzinfo=timezone.utc)
        upper = datetime(end.year, end.month, 1, tzinfo=timezone.utc)
    else:
        lower, upper = start, end
    conn.execute(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} PARTITION OF {} "
            "FOR VALUES FROM ({}) TO ({})"
        ).format(
            sql.Identifier(child), sql.Identifier(parent),
            sql.Literal(lower), sql.Literal(upper),
        )
    )
    linked = conn.execute(
        """SELECT 1 FROM pg_inherits WHERE inhrelid=to_regclass(%s)
           AND inhparent=to_regclass(%s)""",
        (child, parent),
    ).fetchone()
    if not linked:
        raise RuntimeError(f"Existing {child} is not attached to {parent}")
    return child


def ensure_current_partitions(conn, *, today=None):
    now = today or datetime.now(timezone.utc).date()
    first = date(now.year, now.month, 1)
    _, second = month_bounds(first.year, first.month)
    children = []
    for parent in PARENTS:
        for start in (first, second):
            children.append(ensure_month_partition(conn, parent, start.year, start.month))
    return children
