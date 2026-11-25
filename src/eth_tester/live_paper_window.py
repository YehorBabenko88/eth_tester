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


def _verified_row_as_of(conn, *, symbol, start, now):
    """Recheck any already issued immutable B record at real decision time."""
    from .storage.live_evidence import (
        require_bounded_paper_evidence, _no_overlap,
    )
    row = conn.execute("""SELECT snapshot_key FROM paper_live_evidence
        WHERE symbol=%s AND tf='1' AND bar_ts=%s
          AND computed_at<=%s AND expires_at>=%s
        ORDER BY computed_at DESC,snapshot_key DESC LIMIT 1""",
        (symbol, start, now, now)).fetchone()
    if row is None:
        return None
    proof = require_bounded_paper_evidence(
        conn, symbol=symbol, bar_ts=start,
        evidence_key=row["snapshot_key"], decision_at=now)
    # A later feed interruption may invalidate *current* paper admission even
    # if an older B record was immutable and valid when first published.
    _no_overlap(conn, symbol, start, now)
    return proof


def publish_latest_bounded_live(settings, *, replay_batch=1500,
                                max_replay_batches=4):
    """Run bounded L2 replay, select REAL persisted receipts, invoke B publisher.

    The only published row comes from Storage B and remains PAPER-ONLY.
    No caller-supplied coverage flags, no ML-v2 feature_snapshots insert.
    Failure to find even one upstream source is an explicit pending status;
    the real DB clock and B's 90-second window prevent hindsight admission.
    """
    from .book_analysis import replay_orderbook
    from .database import connection
    from .storage.live_evidence import (
        publish_bounded_paper_evidence, require_bounded_paper_evidence,
    )

    if getattr(settings, "category", None) != "linear":
        raise ValueError("bounded_paper_requires_linear_instrument")
    # Unlike the historical 13-frame job, L2 replay must precede every fast
    # closed-minute attempt; a batch limit prevents a perpetual replay loop.
    replayed = replay_orderbook(
        settings, batch=replay_batch, max_batches=max_replay_batches)
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                     (f"bounded_paper:{settings.symbol}:1",))
        now = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        # Replay may hit its per-pass budget under heavy L2 traffic. Never
        # publish from an apparently fresh book with unprocessed raw deltas.
        state = conn.execute("""SELECT last_event_id,valid FROM market_book_state
            WHERE symbol=%s""", (settings.symbol,)).fetchone()
        newest = conn.execute("""SELECT id FROM book_events WHERE symbol=%s
            ORDER BY id DESC LIMIT 1""", (settings.symbol,)).fetchone()
        if (not state or state["valid"] is not True or not newest
                or state["last_event_id"] < newest["id"]):
            return {"status": "pending",
                    "reason": "l2_replay_backlog_or_invalid",
                    "l2_replayed": replayed,
                    "paper_only": True, "ml_eligible": False}
        bar = conn.execute("""SELECT ts FROM candles
            WHERE symbol=%s AND kind='trade' AND tf='1'
              AND source='bybit_v5' AND closed=true
              AND ts<=date_trunc('minute',%s) - interval '1 minute'
            ORDER BY ts DESC LIMIT 1""",
            (settings.symbol, now)).fetchone()
        if bar is None:
            return {"status": "pending", "reason": "official_closed_1m_missing",
                    "tf": "1", "l2_replayed": replayed,
                    "paper_only": True, "ml_eligible": False}
        start = bar["ts"]
        window = bounded_live_window(start, now)
        if window["status"] != "eligible_window":
            return {"status": window["status"], "reason": window["reason"],
                    "tf": "1", "l2_replayed": replayed,
                    "paper_only": True, "ml_eligible": False}
        # A real closed REST kline has its own DB receipt. Its quality is
        # *closed candle only* and must never assert complete public trades.
        rest = conn.execute("""SELECT checked_at FROM ingestion_coverage
            WHERE source='kline:trade' AND symbol=%s
              AND instrument='linear' AND interval='1'
              AND status='verified' AND quality='exchange_closed_grid_checked'
              AND start_ts<=%s AND end_ts>=%s AND checked_at<=%s
              AND evidence->>'closed_bar_only'='true'
            ORDER BY checked_at DESC LIMIT 1""",
            (settings.symbol, start, window["bar_end"], now)).fetchone()
        if rest is None or rest["checked_at"] < window["bar_end"]:
            return {"status": "pending",
                    "reason": "independent_closed_1m_rest_receipt_missing",
                    "tf": "1", "l2_replayed": replayed,
                    "paper_only": True, "ml_eligible": False}
        rest_latency_ms = int(
            (rest["checked_at"]-window["bar_end"]).total_seconds()*1000)
        # Reuse the same immutable B key on repeated five-second cycles.
        existing = _verified_row_as_of(
            conn, symbol=settings.symbol, start=start, now=now)
        if existing is not None:
            return {"status": "published", "reason": "b_witness_already_issued",
                    "snapshot_key": existing["snapshot_key"],
                    "source_checksum": existing["source_checksum"],
                    "computed_at": existing["computed_at"].isoformat(),
                    "bar_close_at": window["bar_end"].isoformat(),
                    "rest_close_receipt_latency_ms": rest_latency_ms,
                    "publication_latency_ms": int(
                        (existing["computed_at"]-window["bar_end"])
                        .total_seconds()*1000),
                    "tf": "1", "l2_replayed": replayed,
                    "paper_only": True, "ml_eligible": False}

        # Select a *single* real WS session in this complete event-minute.
        # Multiple sessions cannot be silently merged across reconnect gaps.
        sessions = conn.execute("""SELECT session_id,count(*) AS prints,
              max(received_at) AS last_receipt
            FROM live_trade_receipts
            WHERE symbol=%s AND source_event_at >= %s
              AND source_event_at < %s AND received_at <= %s
            GROUP BY session_id ORDER BY prints DESC,session_id LIMIT 2""",
            (settings.symbol, start, window["bar_end"], now)).fetchall()
        if len(sessions) != 1:
            return {"status": "pending", "reason":
                    "missing_or_mixed_ws_trade_receipts", "tf": "1",
                    "l2_replayed": replayed,
                    "paper_only": True, "ml_eligible": False}
        session_id = sessions[0]["session_id"]
        # Join actual replay state to avoid selecting a B book receipt whose
        # raw event has not yet been applied to market_book_state.
        book = conn.execute("""SELECT b.id,b.received_at,
                   state.last_event_id-b.event_id AS replay_ahead
            FROM live_book_receipts b
            JOIN market_book_state state ON state.symbol=b.symbol
            WHERE b.symbol=%s AND b.session_id=%s
              AND b.received_at <= %s
              AND b.received_at >= %s - interval '10 seconds'
              AND state.valid=true AND state.last_event_id >= b.event_id
            ORDER BY b.received_at DESC,b.id DESC LIMIT 1""",
            (settings.symbol, session_id, now, now)).fetchone()
        if book is None:
            return {"status": "pending",
                    "reason": "fresh_replayed_l2_receipt_missing", "tf": "1",
                    "l2_replayed": replayed,
                    "paper_only": True, "ml_eligible": False}
        oi = conn.execute("""SELECT id,source_event_at,received_at
            FROM live_oi_receipts
            WHERE symbol=%s AND instrument='linear' AND interval='5min'
              AND source_event_at <= %s AND received_at <= %s
              AND source_event_at >= %s - interval '10 minutes'
              AND received_at >= %s - interval '10 minutes'
            ORDER BY source_event_at DESC,received_at DESC,id DESC LIMIT 1""",
            (settings.symbol, now, now, now, now)).fetchone()
        if oi is None:
            return {"status": "pending",
                    "reason": "independent_recent_oi_receipt_missing", "tf": "1",
                    "l2_replayed": replayed,
                    "paper_only": True, "ml_eligible": False}
        # B checks original WS trade identities, the official base AND quote,
        # unresolved gaps, session match, 10-second replayed L2, REST OI
        # receipt, exact DB publication time and the immutable SQL contract.
        try:
            # A B rejection at either stage MUST roll back a tentative insert.
            with conn.transaction():
                key = publish_bounded_paper_evidence(
                    conn, settings.symbol, start, session_id,
                    book["id"], oi["id"])
                checked = conn.execute(
                    "SELECT clock_timestamp() AS ts").fetchone()["ts"]
                verified = require_bounded_paper_evidence(
                    conn, symbol=settings.symbol, bar_ts=start,
                    evidence_key=key, decision_at=checked)
        except ValueError as exc:
            # A ValueError is a genuine B proof rejection, not source success.
            return {"status": "pending", "reason":
                    "b_source_validation_rejected",
                    "detail": str(exc)[:180], "tf": "1",
                    "l2_replayed": replayed,
                    "paper_only": True, "ml_eligible": False}
        return {"status": "published", "reason": "b_immutable_paper_only",
                "snapshot_key": verified["snapshot_key"],
                "source_checksum": verified["source_checksum"],
                "computed_at": verified["computed_at"].isoformat(),
                "bar_close_at": window["bar_end"].isoformat(),
                "rest_close_receipt_latency_ms": rest_latency_ms,
                "publication_latency_ms": int(
                    (verified["computed_at"]-window["bar_end"])
                    .total_seconds()*1000),
                "trade_last_receipt_age_ms": int(
                    (now-sessions[0]["last_receipt"]).total_seconds()*1000)
                    if sessions[0].get("last_receipt") else None,
                "book_receipt_age_ms": int(
                    (now-book["received_at"]).total_seconds()*1000)
                    if book.get("received_at") else None,
                "l2_replay_ahead_events": book.get("replay_ahead"),
                "oi_event_age_ms": int(
                    (now-oi["source_event_at"]).total_seconds()*1000)
                    if oi.get("source_event_at") else None,
                "oi_receipt_age_ms": int(
                    (now-oi["received_at"]).total_seconds()*1000)
                    if oi.get("received_at") else None,
                "tf": "1", "l2_replayed": replayed,
                "paper_only": True, "ml_eligible": False}


def read_live_paper_status(settings):
    """Read actual C fast-lane publication state, not a synthetic flag."""
    from .database import connection
    with connection(settings) as conn:
        row = conn.execute("""SELECT status,detail,updated_at,
                 clock_timestamp() AS checked_at FROM bot_heartbeat
            WHERE component='live_paper_window'""").fetchone()
    if row is None or not isinstance(row.get("detail"), dict):
        return {"status": "pending", "reason": "fast_lane_not_started",
                "paper_only": True, "ml_eligible": False}
    if (row["checked_at"]-row["updated_at"] > timedelta(seconds=25)):
        return {"status": "pending", "reason": "fast_lane_heartbeat_stale",
                "paper_only": True, "ml_eligible": False}
    detail = row["detail"]
    # A process heartbeat is not permission to issue a D/E entry.
    return {"status": detail.get("status", "pending"),
            "reason": detail.get("reason", "fast_lane_no_result"),
            "paper_only": True, "ml_eligible": False}
