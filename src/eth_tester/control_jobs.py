"""Control G: bounded, fail-closed scheduling facade for F-owned paper research.

This module NEVER generates labels, certifies market coverage, activates a model,
or sends exchange orders. The source-proof worker and monotonic ML cursor belong
exclusively to Learning F / Storage B.
"""

import json
import logging
import re

from .database import connection

LOGGER = logging.getLogger(__name__)
JOBS = frozenset(("label-horizons", "train-causal-shadow"))
_MAX_COUNT = 10**12
# Exact, documented source-validation categories only. F's arbitrary exception
# text, query content, paths, tokens and user data must NEVER enter telemetry.
_SAFE_REJECTION_KEYS = frozenset((
    "not_independently_certified_historical_snapshot",
    "missing_or_ambiguous_historical_candle_witness",
    "historical_receipt_not_independently_witnessed",
    "late_or_backdated_horizon_source",
    "unresolved_candle_gap",
    "incomplete_historically_verified_closed_bar_grid",
    "future_bar_gap_or_wrong_source",
    "no precommitted immutable label policy",
    "bounded-live paper evidence is ML ineligible",
    "paper_trade_closed_after_label_horizon",
    "full_future_candle_horizon_not_elapsed",
    "source_not_immutable",
    "scan_limit_reached",
))
_COUNT_FIELDS = frozenset((
    "usable", "required", "processed", "inserted", "skipped", "rejected",
    "labelled", "eligible", "candidate_id", "created", "scanned",
    "next_after_id", "previous_eligible_count", "as_of_ms",
))


def _label_horizons(settings):
    """Import the F-owned implementation only when it really exists."""
    try:
        from .ml.label_worker import run_label_horizons
    except ModuleNotFoundError as exc:
        if exc.name != "eth_tester.ml.label_worker":
            raise
        return {"status": "blocked_missing_verified_label_worker"}
    return run_label_horizons(settings, limit=100)


def _causal_shadow(settings):
    """No fallback to legacy PnL labels or non-causal training."""
    from . import learning

    worker = getattr(learning, "run_causal_shadow_cycle", None)
    if worker is None:
        return {"status": "blocked_missing_causal_shadow_cycle"}
    return worker(settings, min_new=100, min_rows=180, embargo_ms=60000)


def _sanitize(result):
    """Keep F counters; reduce free-form rejection evidence to safe categories."""
    if not isinstance(result, dict):
        raise RuntimeError("Research worker did not return explicit status")
    status = result.get("status")
    if not isinstance(status, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,95}", status) is None:
        raise RuntimeError("Research worker returned invalid status")
    if result.get("approved") is True:
        raise RuntimeError("Automatic model approval is prohibited")
    detail = {"paper_only": True, "ml_certified_by_control": False,
              "status": status}
    for field in _COUNT_FIELDS:
        value = result.get(field)
        if type(value) is int and 0 <= value < _MAX_COUNT:
            detail[field] = value
    rejected = result.get("rejections")
    if rejected is not None:
        if not isinstance(rejected, dict) or len(rejected) > 256:
            raise RuntimeError("Research worker returned invalid rejection map")
        counts = {}
        for key, count in rejected.items():
            if type(count) is not int or count < 0 or count >= _MAX_COUNT:
                raise RuntimeError("Research worker returned invalid rejection count")
            name = key if type(key) is str and key in _SAFE_REJECTION_KEYS else "other"
            counts[name] = counts.get(name, 0) + count
        detail["rejection_counts"] = dict(sorted(counts.items()))
        detail["rejected_total"] = sum(counts.values())
    return status, detail


def _heartbeat(settings, command, status, detail):
    with connection(settings) as conn:
        conn.execute(
            """INSERT INTO bot_heartbeat(component,status,detail)
               VALUES(%s,%s,%s::jsonb)
               ON CONFLICT(component) DO UPDATE SET
               status=EXCLUDED.status,detail=EXCLUDED.detail,updated_at=now()""",
            ("control_" + command.replace("-", "_"), status,
             json.dumps(detail, sort_keys=True)),
        )


def run_control_job(settings, command):
    """One bounded attempt. Supervisor owns intervals and process backoff.

    A missing F producer is a reported blocked state, not a successful label
    or a fast subprocess crash-loop. Unexpected exceptions remain visible.
    """
    if command not in JOBS:
        raise ValueError("Unsupported research worker")
    try:
        result = (_label_horizons(settings) if command == "label-horizons"
                  else _causal_shadow(settings))
        status, detail = _sanitize(result)
    except Exception as exc:
        LOGGER.exception("Paper research job %s failed", command)
        _heartbeat(settings, command, "error", {
            "paper_only": True, "ml_certified_by_control": False,
            "status": "error", "error_type": type(exc).__name__,
        })
        raise
    _heartbeat(settings, command, status, detail)
    return detail
