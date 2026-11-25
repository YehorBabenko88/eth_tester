"""Prospective source revalidation for bounded, paper-only admission.

The immutable evidence is a genuine *past* observation, not permission to
enter after a WebSocket disconnect or an L2 invalidation. E calls this through
B.require_bounded_paper_evidence inside the paper transaction. The DB owns the
clock; no caller can rewrite old receipts or backdate a trade.
"""
from datetime import timedelta
from hashlib import sha256
import json

_BOOK_AGE = timedelta(seconds=10)
_OI_AGE = timedelta(minutes=10)
_TRADE_AGE = timedelta(seconds=20)
_DECISION_AGE = timedelta(seconds=5)


def _digest(values):
    return sha256(json.dumps(values, sort_keys=True, default=str,
                             separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _db_now(conn):
    return conn.execute("SELECT clock_timestamp() AS observed_at").fetchone()["observed_at"]


def recheck_admission_sources(conn, *, row, symbol, bar_start, decision,
                              at_admission=False):
    """Check *current* same-session provenance before returning an eligible row.

    at_admission adds strict DB decision-clock freshness and must be set by E
    under its original entry transaction, after its paper-account/position
    locks. The non-admission D call also enforces current source validity.
    """
    # Serialize database-visible source disconnect/gap writes against paper
    # admissions. A and E must acquire THIS EXACT lock in their transactions.
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                 ("eth_paper_admission_source:" + symbol,))
    stamp = _db_now(conn)
    if (stamp < row["computed_at"] or stamp > row["expires_at"] or
            stamp < row["bar_close_at"] or
            decision > stamp + timedelta(seconds=1) or
            stamp - decision > _DECISION_AGE):
        raise ValueError("Outdated or future prospective paper decision clock")
    if at_admission and decision > stamp:
        raise ValueError("Original admission decision is in the DB future")

    session = row["trade_session"]
    if (not isinstance(session, str) or not session or
            row["computed_at"] != row["observed_at"] or
            row["trade_last_receipt_at"] > row["computed_at"]):
        raise ValueError("Original WS session or observed clock is inconsistent")
    current = conn.execute(
        """SELECT status,ts,detail FROM feed_health
           WHERE symbol=%s AND topic='connection' AND ts<=%s
           ORDER BY ts DESC LIMIT 1""",
        (symbol, stamp),
    ).fetchone()
    if (current is None or current["status"] != "connected" or
            not isinstance(current["detail"], dict) or
            current["detail"].get("session_id") != session):
        raise ValueError("WebSocket disconnected or changed original session")
    if conn.execute(
        """SELECT 1 FROM feed_health WHERE symbol=%s
           AND topic='connection' AND status='disconnected'
           AND ts>%s AND ts<=%s LIMIT 1""",
        (symbol, row["computed_at"], stamp),
    ).fetchone() is not None:
        raise ValueError("WebSocket disconnected since original publication")
    if conn.execute(
        """SELECT 1 FROM data_gaps WHERE symbol=%s
           AND source IN ('publicTrade','orderbook')
           AND start_ts<=%s AND (end_ts IS NULL OR end_ts >= %s)
           LIMIT 1""",
        (symbol, stamp, bar_start),
    ).fetchone() is not None:
        raise ValueError("Current paper window overlaps an observed source gap")
    if conn.execute(
        """SELECT 1 FROM data_gaps WHERE symbol=%s
           AND source IN ('publicTrade','orderbook')
           AND detected_at>%s AND detected_at<=%s LIMIT 1""",
        (symbol, row["computed_at"], stamp),
    ).fetchone() is not None:
        raise ValueError("New trade/book interruption after publication")

    candle = conn.execute(
        """SELECT open,high,low,close,volume,turnover,source
           FROM candles WHERE symbol=%s AND kind='trade' AND tf='1'
             AND ts=%s AND closed=true""",
        (symbol, bar_start),
    ).fetchone()
    if (candle is None or candle["source"] != "bybit_v5" or
            _digest([str(candle[k]) for k in
                     ("open","high","low","close","volume","turnover","source")])
            != row["evidence"].get("closed_candle_sha256") or
            candle["volume"] != row["base_volume"] or
            candle["turnover"] != row["quote_turnover"]):
        raise ValueError("Original official closed candle changed or vanished")

    tape = conn.execute(
        """SELECT count(*) AS n,coalesce(sum(qty),0) AS base,
                  coalesce(sum(notional),0) AS quote,
                  count(DISTINCT session_id) AS sessions,
                  min(session_id) AS session
           FROM live_trade_receipts
           WHERE symbol=%s AND source_event_at >= %s
             AND source_event_at < %s""",
        (symbol, bar_start, row["bar_close_at"]),
    ).fetchone()
    raw = conn.execute(
        """SELECT count(*) AS n,coalesce(sum(qty),0) AS base,
                  coalesce(sum(notional),0) AS quote
           FROM trades WHERE symbol=%s AND source='websocket'
             AND ts >= %s AND ts < %s""",
        (symbol, bar_start, row["bar_close_at"]),
    ).fetchone()
    if (tape is None or raw is None or tape["n"] != row["trade_count"]
            or tape["sessions"] != 1 or tape["session"] != session
            or tape["n"] != raw["n"] or tape["base"] != raw["base"]
            or tape["quote"] != raw["quote"]
            or tape["base"] != row["base_volume"]
            or tape["quote"] != row["quote_turnover"]):
        raise ValueError("New or missing WS trade invalidated original minute")
    latest_print = conn.execute(
        """SELECT max(source_event_at) AS source_at,
                  max(received_at) AS received_at
           FROM live_trade_receipts
           WHERE symbol=%s AND session_id=%s""",
        (symbol, session),
    ).fetchone()
    if (latest_print is None or latest_print["source_at"] is None
            or latest_print["received_at"] is None or
            not stamp-_TRADE_AGE <= latest_print["source_at"] <= stamp
            or latest_print["received_at"] > stamp):
        raise ValueError("Recent WS trade is absent or retrospectively received")

    pinned = conn.execute(
        """SELECT symbol,session_id,event_id,received_at
           FROM live_book_receipts WHERE id=%s""",
        (row["book_receipt_id"],),
    ).fetchone()
    newest = conn.execute(
        """SELECT symbol,session_id,event_id,received_at
           FROM live_book_receipts WHERE symbol=%s
           ORDER BY received_at DESC,id DESC LIMIT 1""",
        (symbol,),
    ).fetchone()
    state = conn.execute(
        """SELECT last_event_id,valid,updated_at
           FROM market_book_state WHERE symbol=%s""",
        (symbol,),
    ).fetchone()
    if (pinned is None or pinned["symbol"] != symbol
            or pinned["session_id"] != session
            or pinned["received_at"] > row["computed_at"]
            or newest is None or newest["session_id"] != session
            or newest["received_at"] > stamp
            or newest["received_at"] < stamp-_BOOK_AGE
            or newest["event_id"] < pinned["event_id"]
            or state is None or state["valid"] is not True
            or state["last_event_id"] < newest["event_id"]
            or state["updated_at"] is None
            or not stamp-_BOOK_AGE <= state["updated_at"] <= stamp):
        raise ValueError("Latest L2 receipt or replay state is stale/invalid")

    oi = conn.execute(
        """SELECT symbol,instrument,interval,source_event_at,received_at,
                  oi_value,response_sha256
           FROM live_oi_receipts WHERE id=%s""",
        (row["oi_receipt_id"],),
    ).fetchone()
    if (oi is None or oi["symbol"] != symbol or oi["instrument"] != "linear"
            or oi["interval"] != "5min"
            or not stamp-_OI_AGE <= oi["source_event_at"] <= stamp
            or not stamp-_OI_AGE <= oi["received_at"] <= stamp
            or oi["response_sha256"] != row["evidence"].get("oi_source_checksum")):
        raise ValueError("Original OI source observation is stale or mismatched")
    actual_oi = conn.execute(
        """SELECT value FROM open_interest
           WHERE symbol=%s AND interval='5min' AND ts=%s""",
        (symbol, oi["source_event_at"]),
    ).fetchone()
    if actual_oi is None or actual_oi["value"] != oi["oi_value"]:
        raise ValueError("Canonical REST OI sample no longer matches original")

    after = _db_now(conn)
    if (after < stamp or after > row["expires_at"] or
            after-decision > _DECISION_AGE or
            (at_admission and decision > after)):
        raise ValueError("Paper decision expired during admission verification")
    return after
