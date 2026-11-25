"""Persistence adapter for Storage B's additive market-quality tables (migration 0002)."""
import hashlib
import json
from datetime import datetime, timezone

UTC = timezone.utc
COVERAGE_STATES = frozenset({"verified", "partial", "gap", "unavailable", "pending"})


def utc_millis(value):
    return datetime.fromtimestamp(value / 1000, UTC)


def digest_rows(rows):
    """Canonical per-window SHA256; does not imply exchange historical completeness."""
    payload = json.dumps(rows, separators=(",", ":"), sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def write_coverage(conn, *, source, symbol, interval, start_ms, end_exclusive_ms,
                   status, observed_rows, expected_rows=None, checksum=None,
                   quality="unknown", evidence=None, instrument="linear"):
    if status not in COVERAGE_STATES:
        raise ValueError(f"Invalid coverage state: {status}")
    if end_exclusive_ms <= start_ms or observed_rows < 0:
        raise ValueError("Invalid coverage time range or row count")
    if expected_rows is not None and expected_rows < 0:
        raise ValueError("Negative expected count")
    if not symbol or not source:
        raise ValueError("Coverage source/symbol required")
    conn.execute(
        """INSERT INTO ingestion_coverage
           (source,symbol,instrument,interval,start_ts,end_ts,status,
            observed_rows,expected_rows,checksum,quality,evidence,checked_at)
           VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,
                  clock_timestamp())
           ON CONFLICT(source,symbol,instrument,interval,start_ts,end_ts)
           DO UPDATE SET
             status=EXCLUDED.status,
             observed_rows=EXCLUDED.observed_rows,
             expected_rows=EXCLUDED.expected_rows,
             checksum=EXCLUDED.checksum,
             quality=EXCLUDED.quality,
             evidence=EXCLUDED.evidence,
             checked_at=clock_timestamp()""",
        (source, symbol, instrument, interval, utc_millis(start_ms),
         utc_millis(end_exclusive_ms), status, observed_rows, expected_rows,
         checksum, quality, json.dumps(evidence or {}, sort_keys=True)),
    )
