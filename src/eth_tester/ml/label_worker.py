"""Bounded, no-network, historic-certified post-horizon labels for shadow ML.

A `live_observed_bounded` paper trade is NEVER ML eligible. We do not create,
change, or reclassify `ingestion_coverage`; only an independently existing
historic closed-candle receipt may authorize a new B-owned append-only label.
"""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from hashlib import sha256
import json
import re

from .contract import milliseconds, validate_snapshot
from .labels import full_future_grid, label_level_outcome
from ..database import connection
from ..storage.labels import record_paper_ml_label

_INTERVAL_MS = 60000
_HEX = re.compile(r"[0-9a-f]{64}\Z")
_PROCESSOR = "ml_post_horizon_labels"


def _stamp(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def _reason_policy(reason):
    """Require an immutable label policy *precommitted at signal time*."""
    if not isinstance(reason, dict) or reason.get("ml_label") is not None:
        raise ValueError("entry reason cannot include a future outcome")
    flow = reason.get("orderflow") or {}
    if (reason.get("ml_eligible") is False
            or reason.get("paper_evidence_quality") in (
            "live_observed_bounded", "bounded_live_observed")
            or not isinstance(flow, dict)
            or "immutable_live_evidence" in flow
            or flow.get("evidence_quality") in (
                "live_observed_bounded", "bounded_live_observed")):
        raise ValueError("bounded-live paper evidence is ML ineligible")
    policy = reason.get("ml_label_policy")
    expected = {"version": "utc_full_bars_v1",
                "method": "fixed_horizon_barrier_v1",
                "bar_interval_ms": _INTERVAL_MS,
                "horizon_bars": 3,
                "barrier_bps": 20,
                "anchor": "first_full_bar_strictly_after_decision"}
    if not isinstance(policy, dict) or policy != expected:
        raise ValueError("no precommitted immutable label policy")
    if reason.get("side") not in ("support", "resistance"):
        raise ValueError("no verified structural side")
    try:
        if isinstance(reason["level"], bool):
            raise ValueError("invalid structural level")
        level = Decimal(str(reason["level"]))
    except (TypeError, ValueError, KeyError, InvalidOperation) as exc:
        raise ValueError("no precommitted structural level") from exc
    if not level.is_finite() or level <= 0:
        raise ValueError("invalid structural level")
    return policy, level


def _source_certificate(conn, *, symbol, instrument, decision_ms,
                        horizon_ms, as_of_ms):
    """One actual verified Bybit 1m source window spanning the entire horizon.

    Multi-window composite certificates are NOT fabricated here. A/B must
    independently attest and persist such a witness before it can be used.
    """
    rows = conn.execute(
        """SELECT source,interval,instrument,start_ts,end_ts,checksum,quality,
                  evidence,checked_at,status,observed_rows,expected_rows
           FROM ingestion_coverage WHERE source='kline:trade'
             AND symbol=%s AND instrument=%s AND interval='1'
             AND status='verified' AND quality='exchange_closed_grid_checked'
             AND start_ts<=%s AND end_ts>=%s
             AND checked_at>= %s AND checked_at<=%s
           ORDER BY checked_at DESC LIMIT 2""",
        (symbol, instrument, _stamp(decision_ms), _stamp(horizon_ms),
         _stamp(horizon_ms), _stamp(as_of_ms)),
    ).fetchall()
    if len(rows) != 1:
        raise ValueError("missing_or_ambiguous_historical_candle_witness")
    proof = rows[0]
    evidence = proof["evidence"]
    if isinstance(evidence, str):
        evidence = json.loads(evidence)
    if (not isinstance(evidence, dict)
            or evidence.get("paper_only") is True
            or evidence.get("completeness_scope") in (
                "observed_bounded", "bounded_local_observations_only")
            or evidence.get("kind") != "trade"
            or evidence.get("tf") != "1"
            or evidence.get("receipt_is_independently_recorded") is not True
            or evidence.get("closed_grid_complete") is not True
            or evidence.get("fixture") is not None):
        raise ValueError("historical_receipt_not_independently_witnessed")
    digest = proof["checksum"]
    # A's real REST reader commits a receipt-bound page digest, not simply
    # caller-supplied quality flags. Recheck the exact window/row count and
    # request/receipt hash from the independently sampled DB source clock.
    first_ms, end_ms = milliseconds(proof["start_ts"]), milliseconds(proof["end_ts"])
    count = (end_ms - first_ms) // _INTERVAL_MS
    ref = sha256(json.dumps(
        [symbol, instrument, "trade", "1", first_ms, end_ms - _INTERVAL_MS,
         digest, evidence.get("source_receipt_ms")],
        separators=(",", ":")).encode()).hexdigest()
    if (end_ms <= first_ms or (end_ms-first_ms) % _INTERVAL_MS != 0
            or count < 3 or evidence.get("source_receipt_kind") !=
               "postgres_clock_timestamp_after_http"
            or evidence.get("source_verified_as_of_receipt") is not True
            or evidence.get("source_event_start_ms") != first_ms
            or evidence.get("source_event_end_exclusive_ms") != end_ms
            or evidence.get("requested_start_ms") != first_ms
            or evidence.get("requested_end_ms") != end_ms-_INTERVAL_MS
            or evidence.get("row_count") != count
            or proof.get("observed_rows") != count
            or proof.get("expected_rows") != count
            or evidence.get("rest_payload_sha256") != digest
            or evidence.get("evidence_ref") != ref
            or evidence.get("proves_all_trade_ids") is not False):
        raise ValueError("historical_rest_receipt_envelope_invalid")
    receipt = evidence.get("source_receipt_ms")
    checked = milliseconds(proof["checked_at"])
    if (not isinstance(digest, str) or not _HEX.fullmatch(digest)
            or type(receipt) is not int
            or not horizon_ms <= receipt <= checked <= as_of_ms):
        raise ValueError("late_or_backdated_horizon_source")
    gap = conn.execute(
        """SELECT 1 FROM data_gaps WHERE symbol=%s
             AND source IN ('kline:trade:1','kline:trade')
             AND resolved_at IS NULL AND start_ts<%s
             AND (end_ts IS NULL OR end_ts>%s) LIMIT 1""",
        (symbol, _stamp(horizon_ms), _stamp(decision_ms)),
    ).fetchone()
    if gap:
        raise ValueError("unresolved_candle_gap")
    return proof, evidence, receipt, checked


def _label_one(conn, row, *, now_ms):
    reason = row["reason"]
    if isinstance(reason, str):
        reason = json.loads(reason)
    policy, level = _reason_policy(reason)
    if row.get("snapshot_source") != "analytics_canonical_nine_sources":
        raise ValueError("not_independently_certified_historical_snapshot")
    _, _, decision = validate_snapshot(row, reason)
    closed = milliseconds(row["closed_at"])
    start, _, horizon = full_future_grid(
        decision, bar_interval_ms=_INTERVAL_MS,
        horizon_bars=policy["horizon_bars"])
    if closed > horizon:
        raise ValueError("paper_trade_closed_after_label_horizon")
    if now_ms < horizon:
        raise ValueError("full_future_candle_horizon_not_elapsed")
    symbol = row["symbol"]
    coverage, _, receipt, _checked = _source_certificate(
        conn, symbol=symbol, instrument="linear", decision_ms=decision,
        horizon_ms=horizon, as_of_ms=now_ms)
    bar_rows = conn.execute(
        """SELECT ts,open,high,low,close,closed,source FROM candles
           WHERE symbol=%s AND kind='trade' AND tf='1' AND closed=true
             AND ts>=%s AND ts<%s ORDER BY ts LIMIT %s""",
        (symbol, _stamp(start), _stamp(horizon), policy["horizon_bars"] + 1),
    ).fetchall()
    if len(bar_rows) != policy["horizon_bars"]:
        raise ValueError("incomplete_historically_verified_closed_bar_grid")
    bars, canonical = [], []
    for i, bar in enumerate(bar_rows):
        bar_start = start + i * _INTERVAL_MS
        if (milliseconds(bar["ts"]) != bar_start
                or bar["closed"] is not True
                or bar["source"] != "bybit_v5"):
            raise ValueError("future_bar_gap_or_wrong_source")
        item = {"start_ms": bar_start, "closed_ms": bar_start+_INTERVAL_MS,
                "high": str(bar["high"]), "low": str(bar["low"])}
        bars.append(item)
        canonical.append({**item, "open": str(bar["open"]),
                          "close": str(bar["close"])})
    outcome = label_level_outcome(
        reason["side"], level, bars, decision_ms=decision,
        horizon_ms=horizon, as_of_ms=now_ms,
        bar_interval_ms=_INTERVAL_MS, barrier_bps=policy["barrier_bps"])
    trace = {"method": policy["method"], "policy": policy,
             "paper_trade_id": row["paper_trade_id"],
             "snapshot_key": row["snapshot_key"],
             "source_checksum": row["source_checksum"],
             "decision_ms": decision, "first_full_bar_start_ms": start,
             "horizon_end_ms": horizon,
             "barrier_side": reason["side"], "barrier_level": str(reason["level"]),
             "bars": canonical}
    barrier_digest = sha256(json.dumps(
        trace, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    evidence = {"barrier_evidence_sha256": barrier_digest,
                "complete_closed_bar_grid": True,
                "expected_bars": len(bars), "verified_bars": len(bars),
                "first_full_bar_start_ms": start,
                "decision_ms": decision, "covered_from_decision": True,
                "historical_source": coverage["source"],
                "historical_source_quality": coverage["quality"],
                "historical_source_checksum": coverage["checksum"],
                "receipt_is_independently_recorded": True}
    # The witness spans original decision -> horizon, but *classification*
    # exclusively uses the full future candle grid (start > decision).
    record_paper_ml_label(
        conn, paper_trade_id=row["paper_trade_id"],
        snapshot_key=row["snapshot_key"],
        source_checksum=row["source_checksum"], outcome=outcome,
        horizon_end_at=_stamp(horizon), label_source=coverage["source"],
        label_interval="1", label_instrument="linear",
        covered_from_at=row["decision_at"],
        coverage_sha256=coverage["checksum"],
        source_receipt_at=_stamp(receipt),
        coverage_checked_at=coverage["checked_at"],
        source_evidence=evidence)
    return outcome


# Only known B label-ledger assertion exceptions are safe to isolate per row.
# Unknown SQL exceptions (connection loss, missing tables, privilege errors)
# must propagate, roll back the whole cycle and trigger G's bounded backoff.
_SQL_LABEL_REJECTIONS = {
    "label must bind a closed paper entry and its original snapshot": "invalid_entry_binding",
    "post-horizon label cannot bind an unverified or future snapshot": "uncertified_entry_snapshot",
    "post-horizon label evidence is not yet available": "horizon_not_mature",
    "label coverage has no pinned independent receipt": "missing_horizon_receipt",
    "missing structural barrier evidence digest": "missing_barrier_digest",
    "Observed-only live paper entry cannot supply historical ML label": "bounded_live_not_ml_eligible",
    "Missing precommitted historical UTC future-bar label policy": "missing_precommitted_policy",
    "Incomplete independently checked closed future-bar grid": "incomplete_grid_evidence",
    "Missing complete first-future-bar evidence": "missing_grid_evidence",
    "Label uses predecision, shifted, missing or late bar horizon": "invalid_future_grid",
    "Label has no original structural level/side": "invalid_original_level",
    "Unverified, shifted or corrupt full future OHLC bar": "invalid_future_ohlc",
    "Missing verified full future candle grid": "incomplete_future_grid",
    "Structural label outcome disagrees with stored future OHLC": "barrier_outcome_mismatch",
}


def _sql_label_rejection(exc):
    if getattr(exc, "sqlstate", None) != "P0001":
        return None
    message = getattr(getattr(exc, "diag", None), "message_primary", None)
    return _SQL_LABEL_REJECTIONS.get(message)


def _rejection_category(exc):
    if isinstance(exc, KeyError):
        return "invalid_record"
    message = str(exc)
    categories = (
        ("historical_candle_witness", "missing_or_ambiguous_historical_candle_witness"),
        ("historical_receipt", "historical_receipt_not_independently_witnessed"),
        ("horizon_source", "late_or_backdated_horizon_source"),
        ("unresolved_candle_gap", "unresolved_candle_gap"),
        ("precommitted", "no precommitted immutable label policy"),
        ("bounded-live", "bounded-live paper evidence is ML ineligible"),
        ("full_future", "full_future_candle_horizon_not_elapsed"),
        ("horizon_not_elapsed", "full_future_candle_horizon_not_elapsed"),
        ("future_bar", "future_bar_gap_or_wrong_source"),
        ("independently_certified", "not_independently_certified_historical_snapshot"),
    )
    for marker, category in categories:
        if marker in message:
            return category
    return "other_invalid_record"


def run_label_cycle(settings, *, limit=100):
    """One bounded, restart-idempotent cycle; never contacts the exchange.

    Generic analytics_cursor is advanced transactionally. At end of one pass
    it wraps to zero, so proof-pending candidates are retried on future calls.
    B SQL immutable ledger deduplicates independently of this scan cursor.
    """
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("invalid label cycle bound")
    with connection(settings) as conn:
        acquired = conn.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS ok",
            ("ml_labels:"+settings.symbol,)).fetchone()["ok"]
        if not acquired:
            return {"status": "already_running", "created": 0, "scanned": 0}
        now = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        now_ms = milliseconds(now)
        cursor = conn.execute(
            """SELECT details FROM analytics_cursor WHERE processor=%s
               AND symbol=%s AND tf='1' FOR UPDATE""",
            (_PROCESSOR,settings.symbol)).fetchone()
        details = (cursor or {}).get("details") or {}
        if isinstance(details,str):
            details=json.loads(details)
        last = details.get("last_id",0)
        if type(last) is not int or last<0:
            last=0
        rows = conn.execute(
            """SELECT p.id AS paper_trade_id,p.symbol,p.signal_ts AS decision_at,
                 p.reason,p.opened_at,p.closed_at,p.notional,
                 f.snapshot_key,f.source_checksum,f.source AS snapshot_source,
                 f.bar_ts,f.available_at,f.observed_at,f.computed_at,
                 f.quality AS snapshot_quality,f.is_causal,f.values_json,f.provenance
               FROM paper_trades p
               LEFT JOIN paper_ml_labels l ON l.paper_trade_id=p.id
               LEFT JOIN feature_snapshots f
                 ON f.snapshot_key=(p.reason->>'ml_snapshot_key')
               WHERE p.symbol=%s AND p.state='closed'
                 AND l.paper_trade_id IS NULL AND p.id>%s
               ORDER BY p.id LIMIT %s""",
            (settings.symbol,last,limit)).fetchall()
        wrapped = not rows or len(rows)<limit
        scanned=0
        created=0
        rejected={}
        for row in rows:
            scanned+=1
            try:
                # Already-in-progress outer transaction -> psycopg SAVEPOINT.
                # Any trigger rejection rolls back *only this entry*, not the
                # preceding validated labels or durable scan cursor.
                with conn.transaction():
                    _label_one(conn,row,now_ms=now_ms)
                created+=1
            except (ValueError, TypeError, KeyError, OverflowError, InvalidOperation) as exc:
                category=_rejection_category(exc)
                rejected[category]=rejected.get(category,0)+1
            except Exception as exc:
                # No broad SQL swallow: accept *only* known B trigger
                # SQLSTATE=P0001 messages. Connectivity/serialization/DDL
                # failures abort the cycle so G can retry safely.
                category=_sql_label_rejection(exc)
                if category is None:
                    raise
                rejected[category]=rejected.get(category,0)+1
        next_id = 0 if wrapped else rows[-1]["paper_trade_id"]
        conn.execute(
            """INSERT INTO analytics_cursor(
                 processor,symbol,tf,cursor_ts,quality,state,details)
               VALUES(%s,%s,'1',%s,%s,%s,%s::jsonb)
               ON CONFLICT(processor,symbol,tf) DO UPDATE SET
                 cursor_ts=EXCLUDED.cursor_ts,quality=EXCLUDED.quality,
                 state=EXCLUDED.state,details=EXCLUDED.details,updated_at=now()""",
            (_PROCESSOR,settings.symbol,now,
             "verified_only" if created else "no_eligible_verified_labels",
             "processed" if created else "pending",
             json.dumps({"last_id":next_id,"scanned":scanned,
                         "created":created,"rejections":rejected,
                         "bounded_live_ml_eligible":False},sort_keys=True)))
        return {"status":"labels_written" if created else "insufficient_verified_horizon",
                "created":created,"scanned":scanned,"rejections":rejected,
                "rejection_counts":rejected,
                "next_after_id":next_id,"approved":False}


def run_label_horizons(settings, *, limit=100):
    """Compatibility alias for G scheduler; same bounded single-pass contract."""
    return run_label_cycle(settings,limit=limit)
