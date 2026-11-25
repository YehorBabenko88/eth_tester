"""Fail-closed writer for immutable point-in-time feature snapshots."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re

from ..ml import FEATURE_NAMES

_HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def _millis(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Timezone-aware timestamp required")
    return int(value.astimezone(timezone.utc).timestamp() * 1000)


def _decimal(value):
    if isinstance(value, bool):
        raise ValueError("Boolean is not a feature value")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Invalid feature numeric value") from exc
    if not result.is_finite():
        raise ValueError("Non-finite feature numeric value")
    return str(result)


def _check_coverage(conn, *, symbol, instrument, source, interval, when_ms, checksum):
    if not isinstance(checksum, str) or not _HEX64.fullmatch(checksum):
        raise ValueError("Source requires lowercase SHA256")
    row = conn.execute(
        """SELECT 1 FROM ingestion_coverage
           WHERE source=%s AND symbol=%s AND instrument=%s AND interval=%s
             AND status='verified' AND checksum=%s
             AND start_ts<=to_timestamp(%s / 1000.0)
             AND end_ts>to_timestamp(%s / 1000.0)
           LIMIT 1""",
        (source, symbol, instrument, interval, checksum, when_ms, when_ms),
    ).fetchone()
    if row is None:
        raise ValueError("Unverified source coverage: " + source)


def write_feature_snapshot(
    conn, *, symbol, tf, bar_ts, observed_at, values, feature_available_ms,
    source, source_checksum, instrument="linear", feature_sources=None,
    certified=False,
):
    """Write a snapshot. Certified requires verified coverage for every feature.

    Server computed_at is never backdated by the caller. Provisional snapshots
    are never eligible for point-in-time ML extraction.
    """
    if not isinstance(symbol, str) or not symbol or not isinstance(tf, str) or not tf:
        raise ValueError("Symbol and timeframe required")
    if not isinstance(source, str) or not source.strip():
        raise ValueError("Snapshot source required")
    if not isinstance(source_checksum, str) or not _HEX64.fullmatch(source_checksum):
        raise ValueError("Snapshot source requires lowercase SHA256")
    if not isinstance(values, dict) or not isinstance(feature_available_ms, dict):
        raise ValueError("ML feature dictionaries required")
    if set(values) != set(FEATURE_NAMES) or set(feature_available_ms) != set(FEATURE_NAMES):
        raise ValueError("Complete ML feature schema required")
    if certified and (not isinstance(feature_sources, dict)
                      or set(feature_sources) != set(FEATURE_NAMES)):
        raise ValueError("Certified snapshots require per-feature source coverage")
    if feature_sources is not None and (
        not isinstance(feature_sources, dict) or set(feature_sources) != set(FEATURE_NAMES)
    ):
        raise ValueError("Feature source evidence must cover every feature")

    bar_ms, observed_ms = _millis(bar_ts), _millis(observed_at)
    times = {}
    for name in FEATURE_NAMES:
        value = feature_available_ms[name]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError("Invalid feature availability: " + name)
        if value > observed_ms:
            raise ValueError("Future/unobserved feature: " + name)
        times[name] = value
    available_ms = max(bar_ms, *times.values())
    if observed_ms < available_ms:
        raise ValueError("Observation precedes feature availability")
    normalized = {name: _decimal(values[name]) for name in FEATURE_NAMES}
    if certified:
        for name in FEATURE_NAMES:
            proof = feature_sources[name]
            if not isinstance(proof, dict) or not all(
                isinstance(proof.get(field), str) and proof[field]
                for field in ("source", "interval", "checksum")
            ):
                raise ValueError("Missing source provenance: " + name)
            _check_coverage(
                conn, symbol=symbol, instrument=instrument,
                source=proof["source"], interval=proof["interval"],
                when_ms=times[name], checksum=proof["checksum"],
            )
    evidence = {
        "contract": "eth_feature_snapshot_v1",
        "feature_available_ms": times,
        "feature_sources": feature_sources or {},
        "instrument": instrument,
        "certified_by": "verified_ingestion_coverage" if certified else "none",
    }
    payload = {
        "symbol": symbol, "tf": tf, "bar_ms": bar_ms,
        "available_ms": available_ms, "observed_ms": observed_ms,
        "source": source, "source_checksum": source_checksum,
        "values": normalized, "provenance": evidence,
        "certified": bool(certified),
    }
    stable = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    key = sha256(stable.encode("utf-8")).hexdigest()
    result = conn.execute(
        """INSERT INTO feature_snapshots(
             snapshot_key,symbol,tf,bar_ts,available_at,observed_at,
             computed_at,source,source_checksum,quality,is_causal,
             values_json,provenance)
           SELECT %s,%s,%s,to_timestamp(%s / 1000.0),
                  to_timestamp(%s / 1000.0),to_timestamp(%s / 1000.0),
                  clock_timestamp(),%s,%s,%s,%s,%s::jsonb,%s::jsonb
           WHERE to_timestamp(%s / 1000.0)<=clock_timestamp()
           ON CONFLICT(snapshot_key) DO NOTHING RETURNING snapshot_key""",
        (key, symbol, tf, bar_ms, available_ms, observed_ms,
         source, source_checksum,
         "complete" if certified else "partial_unverified", certified,
         json.dumps(normalized, sort_keys=True),
         json.dumps(evidence, sort_keys=True), observed_ms),
    ).fetchone()
    if result is None:
        existing = conn.execute(
            "SELECT snapshot_key FROM feature_snapshots WHERE snapshot_key=%s",
            (key,),
        ).fetchone()
        if existing is None:
            raise ValueError("Cannot backdate observation past database clock")
    return key
