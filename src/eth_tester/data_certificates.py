"""Fail-closed Data coverage contract for C/F immutable feature snapshot producers.

No trade-print-completeness claims are derived from a volume-only archive check.
Coverage receipts are validated against the actual time of observation.
"""
from datetime import datetime, timezone
import hashlib
import json
import re

UTC = timezone.utc


def _aware(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Timezone-aware certificate time required")
    return value.astimezone(UTC)


def certified_candle_coverage(conn, symbol, tf, start, end, decision_at,
                              *, kind="trade", instrument="linear"):
    """Return a time-bounded source certificate or None if any coverage is unsafe.

    Intervals are half-open, exact coverage ranges are contiguous; no retrospectively
    calculated evidence may be used as a feature available at an earlier decision.
    """
    start, end, decision_at = (_aware(v) for v in (start, end, decision_at))
    if end <= start or decision_at < end:
        return None
    rows = conn.execute(
        """SELECT start_ts,end_ts,status,checksum,quality,checked_at
           FROM ingestion_coverage
           WHERE source=%s AND symbol=%s AND instrument=%s AND interval=%s
             AND end_ts > %s AND start_ts < %s
           ORDER BY start_ts,end_ts""",
        (f"kline:{kind}", symbol, instrument, tf, start, end)).fetchall()
    covered = start
    receipts = []
    digest_parts = []
    for row in rows:
        lo, hi = _aware(row["start_ts"]), _aware(row["end_ts"])
        checked = _aware(row["checked_at"])
        if (lo > covered or row["status"] != "verified"
                or row["quality"] != "exchange_closed_grid_checked"
                or not isinstance(row["checksum"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", row["checksum"])
                or checked > decision_at or checked < hi):
            return None
        if hi <= covered:
            continue
        if lo < covered and lo != start:
            # Overlapping windows may disagree; avoid deciding a winner.
            return None
        covered = hi
        receipts.append(checked)
        digest_parts.append((lo.isoformat(), hi.isoformat(), row["checksum"]))
        if covered >= end:
            break
    if covered < end or not receipts:
        return None
    gap = conn.execute(
        """SELECT 1 FROM data_gaps WHERE symbol=%s AND source=%s
           AND resolved_at IS NULL AND start_ts < %s
           AND (end_ts IS NULL OR end_ts > %s) LIMIT 1""",
        (symbol, f"kline:{kind}:{tf}", end, start)).fetchone()
    if gap:
        return None
    observed_at = max(receipts)
    event_start_ms = int(start.timestamp() * 1000)
    event_end_ms = int(end.timestamp() * 1000)
    receipt_ms = int(observed_at.timestamp() * 1000)
    source_sha256 = hashlib.sha256(
        json.dumps(digest_parts, separators=(",", ":")).encode("utf8")).hexdigest()
    return {
        "source": f"kline:{kind}", "symbol": symbol, "instrument": instrument,
        "tf": tf, "start": start.isoformat(), "end": end.isoformat(),
        "source_sha256": source_sha256, "observed_at": observed_at.isoformat(),
        "available_at": max(observed_at, end).isoformat(),
        "source_receipt_count": len(digest_parts),
        "source_event_start_ms": event_start_ms,
        "source_event_end_exclusive_ms": event_end_ms,
        "source_receipt_ms": receipt_ms,
        "source_checked_at_ms": receipt_ms,
        "feature_available_ms": receipt_ms,
        "coverage_segments": [
            {"start": lo, "end": hi, "checksum": checksum}
            for lo, hi, checksum in digest_parts
        ],
        "certifies_closed_candles_only": True,
        "certifies_trade_prints": False, "no_historical_backdating": True,
    }
