"""B-v2 immutable feature and append-only label validation for shadow ML.

These checks intentionally reject incomplete proof; DB source coverage and
publication timestamps must be independently produced by Storage B.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
import math
import re

from . import FEATURE_NAMES

_HEX = re.compile(r"[0-9a-f]{64}\Z")
CONTRACT = "eth_feature_snapshot_v2"
CERTIFIER = "independent_source_proof"


def milliseconds(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("invalid ISO timestamp") from exc
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("naive evidence timestamp")
        return int(value.astimezone(timezone.utc).timestamp() * 1000)
    if type(value) is int and value > 0:
        return value
    raise ValueError("invalid evidence timestamp")


def _sha(value):
    if not isinstance(value, str) or not _HEX.fullmatch(value):
        raise ValueError("invalid SHA256 evidence")
    return value


def validate_snapshot(row, reason):
    """Require exactly the D/E-bound snapshot, real receipt clocks and 9 proofs."""
    if not isinstance(reason, Mapping):
        raise ValueError("missing persisted signal reason")
    key = _sha(row["snapshot_key"])
    digest = _sha(row["source_checksum"])
    nested = reason.get("orderflow", {})
    if not isinstance(nested, Mapping):
        raise ValueError("missing D confirmation")
    nested = nested.get("immutable_feature_snapshot")
    if (not isinstance(nested, Mapping)
            or nested.get("snapshot_key") != key
            or nested.get("source_checksum") != digest
            or reason.get("ml_snapshot_key") != key
            or reason.get("ml_source_checksum") != digest):
        raise ValueError("D/E snapshot identity mismatch")
    decision = milliseconds(row["decision_at"])
    # E must persist actual decision clock, not delayed exchange event time.
    asserted = reason.get("ml_signal_decision_ms", reason.get("decision_ms"))
    if milliseconds(asserted) != decision:
        raise ValueError("persisted signal decision mismatch")
    bar, available, observed, computed, opened = (
        milliseconds(row[name]) for name in
        ("bar_ts", "available_at", "observed_at", "computed_at", "opened_at")
    )
    if not 0 < bar <= available <= observed <= computed <= decision <= opened:
        raise ValueError("historical/late snapshot at decision")
    if row["snapshot_quality"] != "complete" or row["is_causal"] is not True:
        raise ValueError("uncertified snapshot")
    proof = row["provenance"]
    if (not isinstance(proof, Mapping)
            or proof.get("contract") != CONTRACT
            or proof.get("certified_by") != CERTIFIER
            or proof.get("source_coverage_verified") is not True
            or proof.get("coverage_status") != "verified"):
        raise ValueError("noncanonical source certification")
    receipt = milliseconds(proof.get("source_receipt_ms"))
    checked = milliseconds(proof.get("source_checked_at_ms"))
    if not receipt <= checked <= available:
        raise ValueError("backdated coverage check")
    available_by_feature = proof.get("feature_available_ms")
    sources = proof.get("feature_sources")
    values = row["values_json"]
    if any(not isinstance(x, Mapping) or set(x) != set(FEATURE_NAMES)
           for x in (available_by_feature, sources, values)):
        raise ValueError("incomplete nine-feature schema")
    normalized, times = [], []
    for name in FEATURE_NAMES:
        item = sources[name]
        if (not isinstance(item, Mapping)
                or not isinstance(item.get("source"), str)
                or not item["source"].strip()
                or not isinstance(item.get("interval"), str)
                or item.get("instrument") not in ("linear", "inverse", "spot")
                or item.get("coverage_status") != "verified"
                or not isinstance(item.get("evidence_ref"), str)
                or not item["evidence_ref"].strip()):
            raise ValueError("missing independent source: " + name)
        _sha(item.get("checksum"))
        _sha(item.get("coverage_evidence_sha256"))
        event = milliseconds(item.get("source_event_ms"))
        received = milliseconds(item.get("source_receipt_ms"))
        verified = milliseconds(item.get("source_checked_at_ms"))
        when = milliseconds(available_by_feature[name])
        if not event <= received <= verified <= when <= available:
            raise ValueError("feature hindsight: " + name)
        if isinstance(values[name], bool):
            raise ValueError("boolean feature value")
        value = float(values[name])
        if not math.isfinite(value):
            raise ValueError("nonfinite feature value")
        times.append(when)
        normalized.append(value)
    return tuple(normalized), tuple(times), decision


def validate_label(record, reason, cutoff, closed, trade_id):
    """Only B's append-only paper_ml_labels can supply future structural outcomes."""
    if type(trade_id) is not int or trade_id <= 0 or type(cutoff) is not int:
        raise ValueError("invalid label key/cutoff")
    label = record.get("label_ledger")
    if not isinstance(label, Mapping) or label.get("paper_trade_id") != trade_id:
        raise ValueError("missing post-horizon ledger")
    if (label.get("snapshot_key") != reason.get("ml_snapshot_key")
            or label.get("source_checksum") != reason.get("ml_source_checksum")):
        raise ValueError("label mismatches captured snapshot")
    outcome = label.get("outcome")
    if (label.get("method") != "fixed_horizon_barrier_v1"
            or outcome not in ("breakout", "bounce", "uncertain")):
        raise ValueError("uncertified structural label")
    _sha(label.get("coverage_sha256"))
    if (not isinstance(label.get("label_source"), str)
            or not label["label_source"].strip()
            or not isinstance(label.get("label_interval"), str)
            or label.get("label_instrument") not in ("linear", "inverse", "spot")):
        raise ValueError("missing horizon source identity")
    horizon = milliseconds(label.get("horizon_end_at"))
    issued = milliseconds(label.get("issued_at"))
    from_at = milliseconds(label.get("covered_from_at"))
    received = milliseconds(label.get("source_receipt_at"))
    checked = milliseconds(label.get("coverage_checked_at"))
    if not 0 < from_at <= horizon or not closed <= horizon <= received <= checked <= issued <= cutoff:
        raise ValueError("future, backdated or incomplete horizon")
    evidence = label.get("source_evidence")
    if not isinstance(evidence, Mapping):
        raise ValueError("missing structural evidence")
    _sha(evidence.get("barrier_evidence_sha256"))
    if (not isinstance(evidence, Mapping)
            or evidence.get("complete_closed_bar_grid") is not True
            or type(evidence.get("expected_bars")) is not int
            or evidence["expected_bars"] <= 0
            or evidence.get("verified_bars") != evidence["expected_bars"]):
        raise ValueError("missing complete independent closed-bar grid")
    return outcome, horizon
