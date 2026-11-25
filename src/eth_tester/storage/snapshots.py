"""Single fail-closed publication gateway for immutable point-in-time ML features.

A closed archive/event timestamp is NOT the date at which a historical feature
was observed, checked or available to a live decision. Certification requires
independent DB-persisted proof for all nine upstream feature sources.
"""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re

from ..ml import FEATURE_NAMES

_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

def _millis(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Timezone-aware timestamp required")
    return int((value.astimezone(timezone.utc) - _EPOCH).total_seconds() * 1000)


def _ceil_millis(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Timezone-aware source receipt required")
    delta = value.astimezone(timezone.utc) - _EPOCH
    return (delta.days * 86400000000 + delta.seconds * 1000000
            + delta.microseconds + 999) // 1000


def _decimal(value):
    if isinstance(value, bool):
        raise ValueError("Boolean is not a feature value")
    try:
        item = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Invalid feature numeric value") from exc
    if not item.is_finite():
        raise ValueError("Non-finite feature numeric value")
    return str(item)


def _sha(value):
    return sha256(json.dumps(value, sort_keys=True, default=str,
                             separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _proof(conn, *, name, symbol, instrument, claimed, available_ms):
    """Verify event-time coverage and DB-witnessed receipt, never a caller flag."""
    if not isinstance(claimed, dict):
        raise ValueError("Missing source provenance: " + name)
    required = ("source", "interval", "checksum", "source_event_ms",
                "source_receipt_ms", "source_checked_at_ms",
                "coverage_status", "evidence_ref")
    if any(k not in claimed for k in required):
        raise ValueError("Missing source provenance: " + name)
    source, interval, digest = (claimed[k] for k in ("source", "interval", "checksum"))
    if (not isinstance(source, str) or not source or
            not isinstance(interval, str) or
            not isinstance(digest, str) or not _HEX64.fullmatch(digest)):
        raise ValueError("Invalid source identifier/checksum: " + name)
    if (not isinstance(claimed["evidence_ref"], str)
            or not claimed["evidence_ref"]):
        raise ValueError("Missing immutable evidence reference: " + name)
    times = [claimed[k] for k in ("source_event_ms", "source_receipt_ms",
                                  "source_checked_at_ms")]
    if any(type(t) is not int or t <= 0 for t in times):
        raise ValueError("Invalid source event/receipt/check time: " + name)
    event_ms, receipt_ms, checked_ms = times
    if claimed["coverage_status"] != "verified" or not (
        event_ms <= receipt_ms <= checked_ms <= available_ms
    ):
        raise ValueError("Unverified or retrospective feature proof: " + name)
    row = conn.execute(
        """SELECT start_ts,end_ts,checked_at,evidence,quality,checksum
           FROM ingestion_coverage
           WHERE source=%s AND symbol=%s AND instrument=%s AND interval=%s
             AND status='verified' AND checksum=%s
             AND start_ts<=to_timestamp(%s / 1000.0)
             AND end_ts>to_timestamp(%s / 1000.0)
           ORDER BY checked_at DESC LIMIT 1""",
        (source, symbol, instrument, interval, digest, event_ms, event_ms),
    ).fetchone()
    if row is None:
        raise ValueError("Unverified source event coverage: " + name)
    # Accept the common integer-millisecond representation of the DB witness,
    # but persist the conservative ceiling so no sub-ms check appears earlier.
    if _millis(row["checked_at"]) != checked_ms:
        raise ValueError("Source checked time does not match DB witness: " + name)
    checked_ms = _ceil_millis(row["checked_at"])
    evidence = row["evidence"]
    if isinstance(evidence, str):
        evidence = json.loads(evidence)
    if (not isinstance(evidence, dict)
            or type(evidence.get("source_receipt_ms")) is not int
            or evidence["source_receipt_ms"] != receipt_ms):
        raise ValueError("Source receipt not independently persisted: " + name)
    if row["quality"] in ("unknown", "partial", "partial_unverified", "archive_import_failed"):
        raise ValueError("Unqualified source proof: " + name)
    # An archive-only volume match is necessary, but cannot certify print
    # completeness. This never upgrades an A-owned partial coverage row.
    if source == "trades_archive" and not all(
        evidence.get(key) is True for key in (
            "gzip_crc_checked", "trade_identity_checked", "volume_verified")
    ):
        raise ValueError("Incomplete archive integrity proof: " + name)
    canonical = {
        "source": source, "instrument": instrument, "interval": interval,
        "checksum": digest, "source_event_ms": event_ms,
        "source_receipt_ms": receipt_ms, "source_checked_at_ms": checked_ms,
        "coverage_status": "verified",
        "coverage_window_start": row["start_ts"].isoformat(),
        "coverage_window_end": row["end_ts"].isoformat(),
        "quality": row["quality"],
        "evidence_ref": claimed["evidence_ref"],
        "coverage_evidence_sha256": _sha({
            "checked_at": row["checked_at"].isoformat(), "checksum": row["checksum"],
            "evidence": evidence, "quality": row["quality"],
        }),
    }
    return canonical


def write_feature_snapshot(
    conn, *, symbol, tf, bar_ts, observed_at=None, values,
    feature_available_ms, source, source_checksum, instrument="linear",
    feature_sources=None, certified=False,
):
    """Append one versioned snapshot, returning its immutable key.

    The true available/observed/computed timestamps ALL come from the DB
    insertion clock, not historical bars or a caller's observed_at. The
    caller's observed_at is accepted solely as an input sanity upper-bound;
    it does not establish provenance.
    """
    if (not isinstance(symbol, str) or not symbol or
            not isinstance(tf, str) or not tf or
            not isinstance(instrument, str) or not instrument):
        raise ValueError("Symbol, timeframe and instrument required")
    if not isinstance(source, str) or not source:
        raise ValueError("Snapshot source required")
    if not isinstance(source_checksum, str) or not _HEX64.fullmatch(source_checksum):
        raise ValueError("Snapshot source requires lowercase SHA256")
    if (not isinstance(values, dict) or not isinstance(feature_available_ms, dict)
            or set(values) != set(FEATURE_NAMES)
            or set(feature_available_ms) != set(FEATURE_NAMES)):
        raise ValueError("Complete nine-feature ML schema required")
    bar_ms = _millis(bar_ts)
    if bar_ms <= 0:
        raise ValueError("Bar time must be positive UTC epoch")
    if observed_at is not None:
        observed_ms = _millis(observed_at)
        if observed_ms < bar_ms:
            raise ValueError("Claimed observation precedes source bar")
    times = {}
    for name in FEATURE_NAMES:
        moment = feature_available_ms[name]
        if type(moment) is not int or moment <= 0 or moment < bar_ms:
            raise ValueError("Invalid feature availability: " + name)
        times[name] = moment
    normalized = {name: _decimal(values[name]) for name in FEATURE_NAMES}
    if certified and (not isinstance(feature_sources, dict)
                      or set(feature_sources) != set(FEATURE_NAMES)):
        raise ValueError("Certified snapshots require nine per-feature sources")
    proofs = {}
    if certified:
        for name in FEATURE_NAMES:
            proofs[name] = _proof(
                conn, name=name, symbol=symbol, instrument=instrument,
                claimed=feature_sources[name], available_ms=times[name],
            )
            # A millisecond-truncated source check must never precede its real
            # sub-millisecond DB timestamp; conservatively round availability UP.
            times[name] = max(times[name], proofs[name]["source_checked_at_ms"])
    else:
        if feature_sources is not None and not isinstance(feature_sources, dict):
            raise ValueError("Feature sources must be an object")
    checked = max((p["source_checked_at_ms"] for p in proofs.values()), default=0)
    receipt = max((p["source_receipt_ms"] for p in proofs.values()), default=0)
    if certified and max(times.values()) < checked:
        raise ValueError("Source verification not available before feature")
    provenance = {
        "contract": "eth_feature_snapshot_v2",
        "certified_by": "independent_source_proof" if certified else "none",
        "feature_sources": proofs,
        "feature_available_ms": times,
        "coverage_status": "verified" if certified else "unverified",
        "source_coverage_verified": bool(certified),
        "source_checked_at_ms": checked if certified else None,
        "source_receipt_ms": receipt if certified else None,
        "source_checksum": source_checksum,
        "instrument": instrument,
        "complete_trade_ids_proven": False,
        "publication_clock": "postgresql_clock_timestamp",
    }
    canonical = {
        "symbol": symbol, "tf": tf, "bar_ms": bar_ms,
        "source": source, "checksum": source_checksum,
        "values": normalized, "provenance": provenance,
    }
    key = _sha(canonical)
    ready_ms = max(bar_ms, *times.values(), checked)
    result = conn.execute(
        """WITH published AS MATERIALIZED (SELECT clock_timestamp() AS stamp)
           INSERT INTO feature_snapshots(
              snapshot_key,symbol,tf,bar_ts,available_at,observed_at,
              computed_at,source,source_checksum,quality,is_causal,
              values_json,provenance)
           SELECT %s,%s,%s,to_timestamp(%s / 1000.0),
                  published.stamp,published.stamp,published.stamp,
                  %s,%s,%s,%s,%s::jsonb,%s::jsonb
           FROM published
           WHERE published.stamp >= to_timestamp(%s / 1000.0)
           ON CONFLICT(snapshot_key) DO NOTHING RETURNING snapshot_key""",
        (key, symbol, tf, bar_ms, source, source_checksum,
         "complete" if certified else "partial_unverified", certified,
         json.dumps(normalized, sort_keys=True),
         json.dumps(provenance, sort_keys=True), ready_ms),
    ).fetchone()
    if result is None:
        existing = conn.execute(
            "SELECT snapshot_key FROM feature_snapshots WHERE snapshot_key=%s",
            (key,),
        ).fetchone()
        if existing is None:
            raise ValueError("Feature information not yet available at DB publication")
    return key
