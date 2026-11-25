"""Fail-closed real-time paper window, never a causal ML certificate."""
from datetime import datetime, timedelta, timezone

LIVE_PAPER_TF = "1"
LIVE_DEADLINE_SECONDS = 90


def bounded_live_window(bar_start, sampled_at, *, deadline_seconds=90):
    """Use the real observed UTC clock, not a retrospectively chosen trade time."""
    if any(not isinstance(x, datetime) or x.tzinfo is None
           for x in (bar_start, sampled_at)):
        raise ValueError("timezone_aware_instants_required")
    if deadline_seconds < 1 or deadline_seconds > LIVE_DEADLINE_SECONDS:
        raise ValueError("unsupported_live_paper_deadline")
    start = bar_start.astimezone(timezone.utc)
    now = sampled_at.astimezone(timezone.utc)
    if start.second or start.microsecond:
        raise ValueError("unaligned_one_minute_bar")
    end = start + timedelta(minutes=1)
    if now < end:
        return {"status": "pending", "reason": "candle_not_closed",
                "bar_start": start, "bar_end": end}
    if now > end + timedelta(seconds=deadline_seconds):
        return {"status": "expired", "reason": "decision_window_elapsed",
                "bar_start": start, "bar_end": end}
    return {"status": "eligible_window", "reason": "independent_proof_required",
            "bar_start": start, "bar_end": end}


def live_source_readiness(*, window, source_witness, sampled_at):
    """A witness is only a candidate until B/D reverify original DB evidence.

    No caller flag upgrades a source to complete trade-ID coverage or ML.
    """
    if window["status"] != "eligible_window":
        return {"status": window["status"], "reason": window["reason"]}
    if not isinstance(source_witness, dict):
        return {"status": "pending", "reason": "b_authoritative_source_witness_missing"}
    if (source_witness.get("contract") != "eth_paper_live_evidence_v1" or
            source_witness.get("quality") != "live_observed_bounded" or
            source_witness.get("paper_only") is not True or
            source_witness.get("ml_eligible") is not False or
            source_witness.get("complete_trade_ids_proven") is not False):
        return {"status": "pending", "reason": "invalid_live_paper_evidence_contract"}
    now = sampled_at.astimezone(timezone.utc)
    for field in ("source_received_at", "source_checked_at", "computed_at"):
        moment = source_witness.get(field)
        if (not isinstance(moment, datetime) or moment.tzinfo is None or
                moment.astimezone(timezone.utc) > now):
            return {"status": "pending", "reason": "source_clock_missing_or_future"}
    if not (window["bar_end"] <=
            source_witness["source_received_at"] <=
            source_witness["source_checked_at"] <=
            source_witness["computed_at"] <= now):
        return {"status": "pending", "reason": "source_clock_inconsistent"}
    return {"status": "pending", "reason": "db_reverification_required",
            "paper_only": True, "ml_eligible": False}


def inspect_latest_live_bar(settings):
    """Read only: never create a false-positive independent source witness."""
    from .database import connection
    with connection(settings) as conn:
        now = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        latest = conn.execute("""SELECT ts FROM candles
            WHERE symbol=%s AND kind='trade' AND tf='1' AND closed
              AND ts<=date_trunc('minute',%s)
            ORDER BY ts DESC LIMIT 1""", (settings.symbol, now)).fetchone()
    if latest is None:
        return {"status": "pending", "reason": "closed_1m_candle_unavailable"}
    state = bounded_live_window(latest["ts"], now)
    if state["status"] != "eligible_window":
        return {"status": state["status"], "reason": state["reason"],
                "tf": LIVE_PAPER_TF}
    return {"status": "pending",
            "reason": "b_authoritative_source_witness_missing",
            "tf": LIVE_PAPER_TF, "paper_only": True, "ml_eligible": False}
