"""D-owned parameter identity and exact as-of paper snapshot binding.

A signal's diagnostic evidence cannot certify market data by itself.  The
database row is independently checked under the paper settlement transaction.
"""
from datetime import datetime, timedelta
import re

from ..strategies import Parameters, strategy_identity
from ..ml import FEATURE_NAMES
from .execution import utc

_HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def rule_identity():
    params = Parameters()
    metadata = params.identity()
    return strategy_identity(params), metadata


def require_new_strategy_identity(reason):
    """Strict v3:SHA12, rules version and complete immutable parameter payload."""
    if not isinstance(reason, dict):
        raise ValueError("New paper entry requires strategy reason mapping")
    params = Parameters()
    version, identity = rule_identity()
    if (reason.get("strategy_version") != version
            or reason.get("rules_version") != params.version
            or reason.get("parameter_sha256") != identity["parameter_sha256"]
            or reason.get("parameters") != identity["parameters"]):
        raise ValueError("Unverified strategy version/parameter identity")
    return dict(reason)


def _snapshot_time(value):
    if not isinstance(value, str):
        raise ValueError("Snapshot clock must be an ISO timestamp")
    try:
        stamp = datetime.fromisoformat(value)
        return utc(stamp)
    except (ValueError, TypeError) as exc:
        raise ValueError("Invalid snapshot observation clock") from exc


def _ms(value):
    return int(utc(value).timestamp() * 1000)


def bind_snapshot_reference(reason, *, decision_at, bar_ts):
    """Pin D's already-validated snapshot reference to the ORIGINAL decision.

    This does not create certified evidence, construct a future label or look
    up a newer snapshot. A subsequent paper transaction verifies the same row.
    """
    context = require_new_strategy_identity(reason)
    decision, start = utc(decision_at), utc(bar_ts)
    end = start + timedelta(minutes=1)
    flow = context.get("orderflow")
    if not isinstance(flow, dict) or flow.get("admissible") is not True:
        raise ValueError("Verified orderflow and snapshot required")
    snapshot = flow.get("immutable_feature_snapshot")
    if not isinstance(snapshot, dict):
        raise ValueError("Missing immutable snapshot identity")
    key, checksum = snapshot.get("snapshot_key"), snapshot.get("source_checksum")
    if (snapshot.get("contract") != "eth_feature_snapshot_v2"
            or type(snapshot.get("source_checked_at_ms")) is not int
            or type(snapshot.get("source_receipt_ms")) is not int
            or snapshot["source_receipt_ms"] > snapshot["source_checked_at_ms"]):
        raise ValueError("Missing canonical B snapshot receipt proof")
    if (not isinstance(key, str) or _HEX64.fullmatch(key) is None
            or not isinstance(checksum, str)
            or _HEX64.fullmatch(checksum) is None):
        raise ValueError("Invalid snapshot key/checksum")
    available = _snapshot_time(snapshot.get("available_at"))
    computed = _snapshot_time(snapshot.get("computed_at"))
    if not end <= available <= computed <= decision:
        raise ValueError("Snapshot observed after decision or before closed bar")
    if "ml_label" in context or "ml_outcome" in context:
        raise ValueError("Future structural labels forbidden at paper admission")
    expected = {
        "ml_snapshot_key": key,
        "ml_source_checksum": checksum,
        "ml_signal_decision_ms": _ms(decision),
        "ml_snapshot_decision_ms": _ms(decision),
        "ml_bar_end_ms": _ms(end),
    }
    for name, value in expected.items():
        if name in context and context[name] != value:
            raise ValueError("Conflicting immutable snapshot/decision binding")
    context.update(expected)
    return context


def verify_persisted_snapshot(conn, *, symbol, reason, decision_at):
    """Recheck the exact immutable B row under the paper entry transaction.

    Synthetic or incomplete proof cannot become certified through this API.
    No SQL UPDATE of an immutable row is ever performed.
    """
    decision = utc(decision_at)
    key, checksum = reason.get("ml_snapshot_key"), reason.get("ml_source_checksum")
    if (not isinstance(key, str) or _HEX64.fullmatch(key) is None
            or not isinstance(checksum, str)
            or _HEX64.fullmatch(checksum) is None):
        raise ValueError("Missing immutable snapshot key/checksum")
    if reason.get("ml_signal_decision_ms") != _ms(decision):
        raise ValueError("Paper decision timestamp does not match snapshot binding")
    row = conn.execute("""SELECT snapshot_key,symbol,tf,bar_ts,available_at,
           observed_at,computed_at,source_checksum,quality,is_causal,provenance
           FROM feature_snapshots WHERE snapshot_key=%s FOR SHARE""", (key,)).fetchone()
    if row is None:
        raise ValueError("Bound snapshot missing from immutable ledger")
    provenance = row["provenance"]
    if (row["snapshot_key"] != key or row["symbol"] != symbol
            or row["tf"] != "1" or row["source_checksum"] != checksum
            or row["quality"] != "complete" or row["is_causal"] is not True
            or not isinstance(provenance, dict)
            or provenance.get("contract") != "eth_feature_snapshot_v2"
            or provenance.get("source_coverage_verified") is not True
            or provenance.get("certified_by") != "independent_source_proof"):
        raise ValueError("Snapshot has no canonical certified provenance")
    # A metadata flag alone is never sufficient. Every feature requires a
    # pinned, source-specific verified ingestion proof from B's v2 writer.
    sources = provenance.get("feature_sources")
    times = provenance.get("feature_available_ms")
    if (not isinstance(sources, dict) or set(sources) != set(FEATURE_NAMES)
            or not isinstance(times, dict) or set(times) != set(FEATURE_NAMES)
            or provenance.get("coverage_status") != "verified"):
        raise ValueError("Missing independent per-feature source proofs")
    available_ms = _ms(row["available_at"])
    checked_times, receipt_times = [], []
    for name in FEATURE_NAMES:
        proof = sources[name]
        at_ms = times[name]
        if (not isinstance(proof, dict) or type(at_ms) is not int
                or not isinstance(proof.get("source"), str) or not proof["source"]
                or not isinstance(proof.get("instrument"), str)
                or not proof["instrument"]
                or not isinstance(proof.get("interval"), str)
                or not isinstance(proof.get("evidence_ref"), str)
                or not proof["evidence_ref"]
                or proof.get("coverage_status") != "verified"
                or not isinstance(proof.get("checksum"), str)
                or _HEX64.fullmatch(proof["checksum"]) is None):
            raise ValueError("Missing independent per-feature source proofs")
        event_ms = proof.get("source_event_ms")
        checked_ms = proof.get("source_checked_at_ms")
        receipt_ms = proof.get("source_receipt_ms")
        if (any(type(t) is not int for t in (event_ms,checked_ms,receipt_ms))
                or not 0 < event_ms <= receipt_ms <= checked_ms <= at_ms <= available_ms):
            raise ValueError("Backdated feature source receipt")
        checked_times.append(checked_ms)
        receipt_times.append(receipt_ms)
    if (type(provenance.get("source_checked_at_ms")) is not int
            or type(provenance.get("source_receipt_ms")) is not int
            or provenance["source_checked_at_ms"] != max(checked_times)
            or provenance["source_receipt_ms"] != max(receipt_times)
            or provenance["source_receipt_ms"] > provenance["source_checked_at_ms"]):
        raise ValueError("Snapshot aggregate source receipts do not match proofs")
    bar = utc(row["bar_ts"])
    available, received, computed = (utc(row[name]) for name in
                                     ("available_at", "observed_at", "computed_at"))
    if (not bar + timedelta(minutes=1) <= available <= received <= computed <= decision
            or _ms(bar + timedelta(minutes=1)) != reason.get("ml_bar_end_ms")):
        raise ValueError("Snapshot materialized after paper decision")
    flow = reason.get("orderflow", {})
    proof = flow.get("immutable_feature_snapshot") if isinstance(flow, dict) else None
    if (not isinstance(proof, dict) or proof.get("snapshot_key") != key
            or proof.get("source_checksum") != checksum
            or proof.get("contract") != "eth_feature_snapshot_v2"
            or proof.get("source_checked_at_ms") != provenance["source_checked_at_ms"]
            or proof.get("source_receipt_ms") != provenance["source_receipt_ms"]
            or _snapshot_time(proof.get("available_at")) != available
            or _snapshot_time(proof.get("computed_at")) != computed):
        raise ValueError("Nested snapshot identity changed")
    return row
