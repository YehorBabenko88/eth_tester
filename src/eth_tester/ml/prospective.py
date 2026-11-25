"""Fail-closed, read-only assessment of prospective closed-minute *research*.

This is a verifier for a future B/E-owned original-observation and future-outcome
ledger, NOT that ledger or a source attestation. It deliberately cannot create
training Examples, calibrated OOS scores, finance PnL, approval, or trades.

An E/D diagnostic hash and caller-supplied SHA-looking receipt do NOT prove
exchange origin, immutable issuance, successful ingestion or complete coverage.
The resulting structurally checked counts remain explicitly ML-INELIGIBLE.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import re

_MINUTE_MS = 60_000
_MAX_MINUTES = 10_080
_RECEIPT = re.compile(r"[0-9a-f]{64}\Z")
_REASON_NO_SIGNAL = frozenset((
    "no_causal_pattern", "all_candidates_unconfirmed_or_mtf_conflicted",
    "conflicting_shadow_directions", "candidate_cap_exceeded_no_trade",
))
_LABELS = ("breakout", "bounce", "uncertain")
_DECISIONS = ("candidate", "no_signal", "unavailable")


def _utc_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _integer(value):
    return type(value) is int and value > 0


def _digest(payload):
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                            allow_nan=False).encode("utf-8")
    if len(serialized) > 8_388_608:
        raise ValueError("oversized research diagnostic evidence")
    return sha256(serialized).hexdigest()


def _sha(value):
    return type(value) is str and _RECEIPT.fullmatch(value) is not None


def _observation(row, *, symbol, as_of_ms):
    """Check the D diagnostic envelope and B-style proposed receipt shape.

    B's actual durable record and receipt authenticity must be proved by B;
    passing this check is only INTERNAL SELF-CONSISTENCY, never certification.
    """
    if type(row) is not dict or row.get("symbol") != symbol or row.get("tf") != "1":
        raise ValueError("invalid_minute_observation")
    bar = row.get("bar_ts_ms")
    observed = row.get("observed_at_ms")
    received = row.get("source_receipt_ms")
    if not all(_integer(v) for v in (bar, observed, received)) or bar % _MINUTE_MS:
        raise ValueError("invalid_minute_observation")
    closed = bar + _MINUTE_MS
    if not (closed <= received <= observed <= closed + 90_000 and observed <= as_of_ms):
        raise ValueError("late_or_backdated_minute_observation")
    if not _sha(row.get("source_receipt_sha256")) or not isinstance(
            row.get("source_receipt_key"), str) or not 1 <= len(row["source_receipt_key"]) <= 128:
        raise ValueError("missing_original_source_receipt")
    shadow = row.get("shadow")
    if not isinstance(shadow, dict) or shadow.get("contract") != "eth_strategy_ensemble_v1":
        raise ValueError("missing_original_shadow")
    if (shadow.get("mode") != "shadow_only" or shadow.get("approved") is not False
            or shadow.get("paper_entry_authorized") is not False
            or shadow.get("ml_eligible") is not False
            or shadow.get("promoted") is not False
            or shadow.get("independent_of_incumbent") is not True
            or shadow.get("observation_receipt_authoritative") is not False
            or shadow.get("expected_net_lower_bound") is not None
            or shadow.get("source_identity_authoritative") is True):
        raise ValueError("shadow_cannot_authorize_paper_or_ml")
    if not _sha(shadow.get("diagnostic_sha256")):
        raise ValueError("unverified_original_input_identity")
    if shadow.get("research_observed_at") != _utc_iso(observed):
        raise ValueError("late_or_backdated_minute_observation")
    unsigned = dict(shadow)
    signature = unsigned.pop("diagnostic_sha256")
    if signature != _digest(unsigned):
        raise ValueError("changed_shadow_diagnostic_payload")
    status = shadow.get("status")
    if status == "shadow_candidates":
        if not shadow.get("selected") or not shadow.get("candidates"):
            raise ValueError("invalid_shadow_candidate")
        kind = "candidate"
    elif status == "no_trade" and shadow.get("reason") in _REASON_NO_SIGNAL:
        if shadow.get("selected") is not None:
            raise ValueError("invalid_no_signal_control")
        kind = "no_signal"
    elif status == "no_trade":
        # D's genuine fail-closed _empty() has NO valid past-window digest,
        # bar identity or decision_cutoff_at. A B-original observed bar is
        # therefore counted as 'unavailable', NEVER as a no_signal control.
        if shadow.get("selected") is not None or shadow.get("candidates") != []:
            raise ValueError("invalid_unavailable_shadow")
        kind = "unavailable"
    else:
        raise ValueError("invalid_shadow_status")
    if kind != "unavailable":
        if shadow.get("source_identity_authoritative") is not False:
            raise ValueError("unverified_original_input_identity")
        if not _sha(shadow.get("input_sha256")):
            raise ValueError("unverified_original_input_identity")
        if (shadow.get("bar_ts") != _utc_iso(bar)
                or shadow.get("input_window_closed_at") != _utc_iso(closed)
                or shadow.get("decision_cutoff_at") != _utc_iso(observed)):
            raise ValueError("late_or_backdated_minute_observation")
    elif (shadow.get("source_identity_authoritative") not in (None, False)
          or shadow.get("bar_ts") not in (None, _utc_iso(bar))
          or shadow.get("input_window_closed_at") not in (None, _utc_iso(closed))
          or shadow.get("decision_cutoff_at") not in (None, _utc_iso(observed))):
        raise ValueError("invalid_unavailable_shadow")
    return kind, (bar, observed, row["source_receipt_key"],
                  row["source_receipt_sha256"], signature, kind)


def _outcome(row, *, symbol, bar, horizon_ms, as_of_ms):
    if not isinstance(row, dict) or row.get("symbol") != symbol or row.get("tf") != "1":
        raise ValueError("invalid_forward_outcome")
    if type(row.get("bar_ts_ms")) is not int or row["bar_ts_ms"] != bar:
        raise ValueError("wrong_forward_bar_identity")
    maturity = bar + _MINUTE_MS + horizon_ms
    received = row.get("source_receipt_ms")
    asserted_horizon = row.get("horizon_end_ms")
    observed = row.get("observed_at_ms")
    if (type(asserted_horizon) is not int or asserted_horizon != maturity
            or not _integer(received) or not _integer(observed)
            or not (maturity < received <= observed <= as_of_ms)):
        raise ValueError("unmatured_or_backdated_forward_outcome")
    label = row.get("label")
    if label not in _LABELS:
        raise ValueError("unverified_forward_label")
    if (not _sha(row.get("source_receipt_sha256"))
            or not isinstance(row.get("source_receipt_key"), str)
            or not 1 <= len(row["source_receipt_key"]) <= 128):
        raise ValueError("missing_future_source_receipt")
    return label, (bar, maturity, received, row["source_receipt_key"],
                   row["source_receipt_sha256"], label)


def audit_closed_minute_research(observations, outcomes, *, symbol,
                                 start_ms, end_ms, as_of_ms,
                                 horizon_ms=300_000):
    """Count EVERY expected closed 1m bar, no-signal controls and outcome gaps.

    This is a bounded **research completeness diagnostic**, not a training
    dataset. Future labels are counted only as source-SHAPE-consistent, never
    as independently attested exchange observations. Refuse revenue, reward,
    win-rate, CI, or promotion claims regardless of a caller's proof flags.
    """
    if not isinstance(symbol, str) or not re.fullmatch(r"[A-Z0-9]{3,24}", symbol):
        raise ValueError("invalid research symbol")
    if (not all(type(x) is int for x in (start_ms, end_ms, as_of_ms, horizon_ms))
            or start_ms <= 0 or as_of_ms <= 0 or end_ms <= start_ms
            or start_ms % _MINUTE_MS or end_ms % _MINUTE_MS
            or horizon_ms < _MINUTE_MS or horizon_ms > 86_400_000
            or horizon_ms % _MINUTE_MS or end_ms > as_of_ms):
        raise ValueError("invalid prospective research window")
    expected = (end_ms - start_ms) // _MINUTE_MS
    if expected > _MAX_MINUTES:
        raise ValueError("research window exceeds safe bound")
    if not isinstance(observations, (list, tuple)) or len(observations) > _MAX_MINUTES:
        raise ValueError("unsafe observation population")
    if not isinstance(outcomes, (list, tuple)) or len(outcomes) > _MAX_MINUTES:
        raise ValueError("unsafe outcome population")

    valid = {}
    issues = {"invalid_original_observation": 0, "invalid_future_outcome": 0,
              "orphan_future_outcome": 0, "missing_future_outcome": 0}
    raw_observation_keys = set()
    for row in observations:
        if not isinstance(row, dict):
            issues["invalid_original_observation"] += 1
            continue
        bar = row.get("bar_ts_ms")
        if type(bar) is not int or not start_ms <= bar < end_ms or bar % _MINUTE_MS:
            issues["invalid_original_observation"] += 1
            continue
        if bar in raw_observation_keys:
            raise ValueError("duplicate original minute receipt")
        raw_observation_keys.add(bar)
        try:
            valid[bar] = _observation(row, symbol=symbol, as_of_ms=as_of_ms)
        except (ValueError, OverflowError, TypeError):
            issues["invalid_original_observation"] += 1

    matched = {}
    all_outcome_keys = set()
    for row in outcomes:
        if not isinstance(row, dict):
            issues["invalid_future_outcome"] += 1
            continue
        bar = row.get("bar_ts_ms")
        if type(bar) is not int or not start_ms <= bar < end_ms or bar % _MINUTE_MS:
            issues["invalid_future_outcome"] += 1
            continue
        if bar in all_outcome_keys:
            raise ValueError("duplicate forward outcome receipt")
        all_outcome_keys.add(bar)
        if bar not in valid:
            issues["orphan_future_outcome"] += 1
            continue
        # Matching receipt IDs/digests do not certify independent source:
        # they must not be reused as original decision-window evidence.
        try:
            label, fingerprint = _outcome(row, symbol=symbol, bar=bar,
                                          horizon_ms=horizon_ms, as_of_ms=as_of_ms)
            original = valid[bar][1]
            if (row["source_receipt_key"] == original[2]
                    or row["source_receipt_sha256"] == original[3]):
                raise ValueError("future witness reused decision receipt")
            matched[bar] = label, fingerprint
        except (ValueError, TypeError, OverflowError):
            issues["invalid_future_outcome"] += 1

    n = {key: 0 for key in _DECISIONS}
    cross = {kind: {label: 0 for label in _LABELS} for kind in _DECISIONS}
    signed_rows = []
    matured_expected = 0
    for bar in range(start_ms, end_ms, _MINUTE_MS):
        observed = valid.get(bar)
        mature = bar + _MINUTE_MS + horizon_ms < as_of_ms
        if mature:
            matured_expected += 1
        if observed is None:
            signed_rows.append((bar, "missing", None, None))
            continue
        kind, original = observed
        n[kind] += 1
        future = matched.get(bar)
        if future is None and mature:
            issues["missing_future_outcome"] += 1
        if future is not None:
            cross[kind][future[0]] += 1
        signed_rows.append((bar, kind, original,
                            future[1] if future is not None else None))

    missing = expected - len(valid)
    complete = (missing == 0 and n["unavailable"] == 0
                and all(value == 0 for value in issues.values())
                and matured_expected == expected and len(matched) == expected)
    return {
        "contract": "eth_closed_minute_research_audit_v1",
        "status": ("structurally_complete_not_source_certified" if complete
                   else "blocked_incomplete_prospective_evidence"),
        "symbol": symbol, "tf": "1", "start_ms": start_ms, "end_ms": end_ms,
        "as_of_ms": as_of_ms, "horizon_ms": horizon_ms,
        "expected_closed_minutes": expected,
        "original_observations": len(valid),
        "missing_observations": missing,
        "no_signal_minutes": n["no_signal"],
        "candidate_minutes": n["candidate"],
        "unavailable_minutes": n["unavailable"],
        "matured_expected_minutes": matured_expected,
        "future_outcomes_structurally_checked": len(matched),
        "label_counts_by_original_decision": cross,
        "rejection_counts": issues,
        "diagnostic_fingerprint_sha256": _digest(signed_rows),
        "fingerprint_is_independent_source_receipt": False,
        "original_source_attested_by_F": False,
        "future_source_attested_by_F": False,
        "economics_verified": False,
        "returns_computed": False,
        "selection_bias_excluded": False,
        "paper_only": True, "approved": False, "go": False,
        "ml_eligible": False, "model_promotion_eligible": False,
    }
