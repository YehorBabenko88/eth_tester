"""Strict paper-only binding for B's immutable bounded LIVE observation.

This contract deliberately excludes feature_snapshots and F training. It never
certifies a complete exchange tape or backdates the decision/quote clock.
"""
from datetime import datetime, timedelta
import re

from .execution import utc
from .identity import require_new_strategy_identity

_SHA = re.compile(r"[0-9a-f]{64}\Z")
_CONTRACT = "eth_paper_live_evidence_v1"
_QUALITY = "live_observed_bounded"


def _timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Missing ISO live evidence clock")
    try:
        return utc(datetime.fromisoformat(value))
    except (ValueError, TypeError) as exc:
        raise ValueError("Invalid live observation timestamp") from exc


def _ms(value):
    return int(utc(value).timestamp() * 1000)


def bind_live_paper_reference(reason, *, bar_ts, decision_at):
    """Pin an independently B-issued original evidence row at real decision time."""
    context = require_new_strategy_identity(reason)
    decision, bar = utc(decision_at), utc(bar_ts)
    flow = context.get("orderflow")
    if not isinstance(flow, dict) or flow.get("admissible") is not True:
        raise ValueError("D live signal must be admitted before E binding")
    proof = flow.get("immutable_live_evidence")
    if not isinstance(proof, dict):
        raise ValueError("Missing D-authenticated bounded live evidence")
    key, checksum = proof.get("snapshot_key"), proof.get("source_checksum")
    if (not isinstance(key, str) or _SHA.fullmatch(key) is None
            or not isinstance(checksum, str) or _SHA.fullmatch(checksum) is None
            or proof.get("contract") != _CONTRACT
            or proof.get("quality") != _QUALITY
            or proof.get("paper_only") is not True
            or proof.get("ml_eligible") is not False):
        raise ValueError("Unverified bounded paper-only provenance")
    recorded_bar = _timestamp(proof.get("bar_ts"))
    observed = _timestamp(proof.get("observed_at"))
    computed = _timestamp(proof.get("computed_at"))
    expires = _timestamp(proof.get("expires_at"))
    end = bar + timedelta(minutes=1)
    if (recorded_bar != bar or not end <= observed <= computed <= decision
            or decision > end + timedelta(seconds=90) or decision > expires):
        raise ValueError("Late, future or mismatched bounded-live evidence")
    if any(k.startswith("ml_") for k in context):
        raise ValueError("Bounded-live paper evidence cannot carry ML binding or labels")
    original = {
        "paper_evidence_tier": _QUALITY,
        "paper_live_snapshot_key": key,
        "paper_live_source_checksum": checksum,
        "paper_live_decision_ms": _ms(decision),
        "paper_live_bar_end_ms": _ms(end),
        "paper_only": True,
        "ml_eligible": False,
    }
    for k, v in original.items():
        if k in context and context[k] != v:
            raise ValueError("Conflicting paper-only immutable admission reason")
    context.update(original)
    return context


def verify_live_paper_row(conn, *, symbol, reason, decision_at):
    """Recheck the exact immutable B row under E's settlement transaction.

    B alone issues a row after checking real WS continuity, official CLOSED
    candle base+quote parity, L2 snapshot lineage and observed OI receipt.
    A caller's JSON boolean is never used instead of B's stored row.
    """
    decision = utc(decision_at)
    key, checksum = reason.get("paper_live_snapshot_key"), reason.get("paper_live_source_checksum")
    if (not isinstance(key, str) or _SHA.fullmatch(key) is None
            or not isinstance(checksum, str) or _SHA.fullmatch(checksum) is None
            or reason.get("paper_evidence_tier") != _QUALITY
            or reason.get("paper_only") is not True
            or reason.get("ml_eligible") is not False
            or reason.get("paper_live_decision_ms") != _ms(decision)
            or any(k.startswith("ml_") for k in reason)):
        raise ValueError("Unverified original live paper decision")
    # Shared prospective-source lock; A's disconnect/reconnect writer must
    # acquire the SAME transaction lock before publishing feed/gap changes.
    # Recheck all dynamic sources while this paper entry transaction holds it.
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                 (f"eth_paper_admission_source:{symbol}",))
    row = conn.execute("""SELECT snapshot_key,symbol,tf,bar_ts,source_checksum,
          observed_at,computed_at,expires_at,quality,paper_only,ml_eligible,evidence
          FROM paper_live_evidence WHERE snapshot_key=%s FOR SHARE""",
          (key,)).fetchone()
    if row is None:
        raise ValueError("Immutable B live paper evidence missing")
    expected = reason.get("orderflow", {}).get("immutable_live_evidence")
    if not isinstance(expected, dict):
        raise ValueError("Original D live confirmation missing")
    if (row["snapshot_key"] != key or row["source_checksum"] != checksum
            or row["symbol"] != symbol or row["tf"] != "1"
            or row["quality"] != _QUALITY or row["paper_only"] is not True
            or row["ml_eligible"] is not False):
        raise ValueError("Bounded live provenance/quality mismatch")
    bar, observed, computed, expires = (utc(row[x]) for x in
                                       ("bar_ts", "observed_at", "computed_at", "expires_at"))
    end = bar + timedelta(minutes=1)
    if (not end <= observed <= computed <= decision <= expires
            or decision > end + timedelta(seconds=90)
            or _ms(end) != reason.get("paper_live_bar_end_ms")
            or expected.get("snapshot_key") != key
            or expected.get("source_checksum") != checksum
            or expected.get("contract") != _CONTRACT
            or expected.get("quality") != _QUALITY
            or expected.get("paper_only") is not True
            or expected.get("ml_eligible") is not False
            or any(_timestamp(expected.get(name)) != value for name, value in
                   (("bar_ts",bar), ("observed_at",observed),
                    ("computed_at",computed), ("expires_at",expires)))):
        raise ValueError("Live evidence does not predate original paper decision")
    if (not isinstance(row["evidence"], dict)
            or row["evidence"].get("contract") != _CONTRACT):
        raise ValueError("Missing immutable B-issued source evidence envelope")
    # The original proof was genuine when published, but source continuity
    # can be lost AFTER publication. Recheck using a NEW server timestamp,
    # not the earlier D decision time or the caller's JSON receipt clock.
    current = utc(conn.execute(
        "SELECT clock_timestamp() AS ts").fetchone()["ts"])
    if not timedelta(0) <= current-decision <= timedelta(seconds=5):
        raise ValueError("Stale or backdated live admission clock")
    from ..storage.live_evidence import (
        require_bounded_paper_evidence, _no_overlap,
    )
    checked=require_bounded_paper_evidence(
        conn, symbol=symbol, bar_ts=bar,
        evidence_key=key, decision_at=current)
    if (not isinstance(checked,dict)
            or checked.get("snapshot_key") != key
            or checked.get("source_checksum") != checksum
            or checked.get("contract") != _CONTRACT
            or checked.get("quality") != _QUALITY
            or checked.get("paper_only") is not True
            or checked.get("ml_eligible") is not False
            or checked.get("exchange_complete") is not False
            or any(utc(checked.get(name)) != stamp for name,stamp in
                   (("bar_ts",bar),("observed_at",observed),
                    ("computed_at",computed),("expires_at",expires)))):
        raise ValueError("B prospective proof recheck changed identity or flags")
    # Defense in depth until all B/A operators share a reconnect/admission
    # transaction-level lock. This rejects a disconnect/gap that occurred
    # after publication, and never turns an old gap into a fake complete tape.
    _no_overlap(conn,symbol,bar,current)
    book=conn.execute("""SELECT valid,updated_at FROM market_book_state
        WHERE symbol=%s FOR SHARE""",(symbol,)).fetchone()
    if (book is None or book["valid"] is not True
            or book["updated_at"] is None
            or not timedelta(0) <= current-utc(book["updated_at"])
                                       <= timedelta(seconds=10)):
        raise ValueError("L2 book became invalid after source publication")
    return row
