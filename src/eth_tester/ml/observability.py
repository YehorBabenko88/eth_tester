"""Read-only, untrusted paper research telemetry from E's diagnostic minutes.

This is NOT a B-authenticated original source ledger. It cannot produce Examples,
forward labels, certified OOS denominators, net returns or model promotion.
The PostgreSQL clock is read once, and the closed UTC minute grid is explicit.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

_VERSION = "eth_observed_minute_shadow_v1"
_MINUTE = timedelta(minutes=1)
_MAX_MINUTES = 1440
_NO_SIGNAL_REASONS = frozenset((
    "no_causal_pattern", "all_candidates_unconfirmed_or_mtf_conflicted",
    "conflicting_shadow_directions", "candidate_cap_exceeded_no_trade",
))


def _utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("untrusted research timestamp")
    return value.astimezone(timezone.utc)


def _classify(row, *, symbol):
    if row["symbol"] != symbol or row["tf"] != "1" or row["version"] != _VERSION:
        return "malformed"
    reason = row["reason"]
    if not isinstance(reason, dict):
        return "malformed"
    expected = {
        "contract": _VERSION,
        "independent_of_incumbent": True,
        "paper_only": True,
        "paper_entry_authorized": False,
        "ml_eligible": False,
        "research_cohort_complete": False,
        "prospective_source_certified": False,
        "source_identity_authoritative": False,
        "no_historical_backfill": True,
    }
    if any(reason.get(key) is not value if isinstance(value, bool)
           else reason.get(key) != value for key, value in expected.items()):
        return "malformed"
    if reason.get("bar_ts") != _utc(row["bar_ts"]).isoformat():
        return "malformed"
    observed = reason.get("observed_at")
    try:
        observed = datetime.fromisoformat(observed.replace("Z", "+00:00"))
        observed = _utc(observed)
    except (AttributeError, ValueError, TypeError):
        return "malformed"
    closed = _utc(row["bar_ts"]) + _MINUTE
    if not closed <= observed <= closed + timedelta(seconds=90):
        return "malformed"
    decision = row["decision"]
    shadow = reason.get("shadow")
    if decision == "research_unavailable":
        if isinstance(reason.get("unavailable_reason"), str) and reason["unavailable_reason"]:
            return "unavailable"
        return "malformed"
    if reason.get("unavailable_reason") is not None or not isinstance(shadow, dict):
        return "malformed"
    if (shadow.get("contract") != "eth_strategy_ensemble_v1"
            or shadow.get("mode") != "shadow_only"
            or shadow.get("approved") is not False
            or shadow.get("paper_entry_authorized") is not False
            or shadow.get("ml_eligible") is not False
            or shadow.get("promoted") is not False):
        return "malformed"
    if decision == "research_no_trade" and shadow.get("status") == "no_trade":
        if shadow.get("selected") is None and shadow.get("reason") in _NO_SIGNAL_REASONS:
            return "no_signal"
    if decision == "research_candidates_unverified" and shadow.get("status") == "shadow_candidates":
        if shadow.get("selected") and shadow.get("candidates"):
            return "candidate"
    return "malformed"


def observe_research_window(conn, *, symbol="ETHUSDT", minutes=1440):
    """One bounded SELECT-only snapshot, never labels or simulated settlement."""
    if not isinstance(symbol, str) or not symbol.isascii() or not symbol.isalnum() or len(symbol) > 24:
        raise ValueError("invalid symbol")
    if type(minutes) is not int or not 1 <= minutes <= _MAX_MINUTES:
        raise ValueError("invalid bounded minute window")
    clock = conn.execute("SELECT clock_timestamp() AS ts").fetchone()
    if not clock or not isinstance(clock.get("ts"), datetime):
        raise ValueError("missing authoritative DB clock")
    current = _utc(clock["ts"]).replace(second=0, microsecond=0)
    start = current - minutes * _MINUTE
    rows = conn.execute(
        """SELECT symbol,tf,bar_ts,version,decision,reason
           FROM strategy_signals
           WHERE symbol=%s AND tf='1'
             AND version='eth_observed_minute_shadow_v1'
             AND bar_ts >= %s AND bar_ts < %s
           ORDER BY bar_ts""", (symbol, start, current)
    ).fetchall()
    if len(rows) > minutes:
        raise ValueError("duplicate or out-of-bounds research population")
    seen = set()
    counts = dict(no_signal=0, candidate=0, unavailable=0, malformed=0)
    for row in rows:
        bar = _utc(row["bar_ts"])
        if not start <= bar < current or bar.second or bar.microsecond:
            raise ValueError("out-of-grid research observation")
        if bar in seen:
            raise ValueError("duplicate research observation")
        seen.add(bar)
        counts[_classify(row, symbol=symbol)] += 1
    return {
        "contract": "eth_research_observability_v1",
        "symbol": symbol, "tf": "1",
        "window_start_utc": start.isoformat(),
        "window_end_utc": current.isoformat(),
        "expected_closed_minutes": minutes,
        "recorded_minutes": len(seen),
        "missing_minutes": minutes - len(seen),
        **counts,
        "source": "E_strategy_signals_diagnostic_only",
        "observations_are_original_exchange_receipts": False,
        "future_outcomes_verified": False,
        "economic_cohort_verified": False,
        "source_coverage_complete": False,
        "selection_bias_excluded": False,
        "oos_eligible": False,
        "ml_eligible": False,
        "model_promotion_eligible": False,
        "paper_only": True,
        "status": "ML_NO_GO_UNCERTIFIED_PROSPECTIVE_DIAGNOSTICS",
    }
