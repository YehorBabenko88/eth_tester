"""Immutable observed-bounded, PAPER-ONLY source receipts and 1m evidence.

This module cannot certify all exchange trades, past archives or ML feature
snapshots. Source observation is recorded using PostgreSQL's actual clock.
Only a live, independently observed closed minute may be published, strictly
within the future paper decision window; late data never rewrites a decision.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
import re

_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_UTC = timezone.utc
_ONE_MINUTE = timedelta(minutes=1)
_LATENCY = timedelta(seconds=90)
_BOOK_AGE = timedelta(seconds=10)
_OI_AGE = timedelta(minutes=10)
_TRADE_AGE = timedelta(seconds=20)
_BASE_EPS = Decimal("0.00000001")
_QUOTE_EPS = Decimal("0.000001")


def _sha(value):
    return sha256(json.dumps(value, sort_keys=True, default=str,
                             separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Timezone-aware actual source timestamp required")
    return value.astimezone(_UTC)


def _key(value, label):
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValueError("Invalid " + label)
    return value


def _hash(value):
    if not isinstance(value, str) or not _HEX64.fullmatch(value):
        raise ValueError("Lowercase SHA256 required")
    return value


def _source_clock(conn):
    row = conn.execute("SELECT clock_timestamp() AS observed_at").fetchone()
    return _utc(row["observed_at"])


def record_trade_receipts(conn, symbol, session_id, trade_ids):
    """Capture first witnessed WS receipt of already validated trade rows.

    Caller invokes this in the SAME transaction as A's publicTrade ingestion.
    Existing global (symbol, trade_id) is checked; no incoming caller price,
    receipt timestamp or synthetic completeness claim can be submitted.
    Repeated WS prints retain the earliest witnessed session/clock.
    """
    _key(symbol, "symbol")
    _key(session_id, "WebSocket session")
    if (not isinstance(trade_ids, (list, tuple)) or not trade_ids
            or len(trade_ids) > 2048 or
            any(not isinstance(x, str) or not x for x in trade_ids)
            or len(set(trade_ids)) != len(trade_ids)):
        raise ValueError("Bounded unique trade IDs required")
    rows = conn.execute(
        """SELECT symbol,trade_id,ts,price,qty,notional,side,source
           FROM trades WHERE symbol=%s AND trade_id=ANY(%s)""",
        (symbol, list(trade_ids)),
    ).fetchall()
    by_id = {row["trade_id"]: row for row in rows}
    if len(by_id) != len(trade_ids):
        raise ValueError("Missing persisted WS trade payload")
    now = _source_clock(conn)
    candidates = []
    for identifier in trade_ids:
        row = by_id[identifier]
        event = _utc(row["ts"])
        if (row["source"] != "websocket" or event > now
                or row["price"] <= 0 or row["qty"] <= 0
                or row["notional"] != row["price"] * row["qty"]
                or row["side"] not in ("Buy", "Sell")):
            raise ValueError("WS event not independently stored as canonical trade")
        payload_hash = _sha([
            symbol, identifier, event.isoformat(),
            str(row["price"]), str(row["qty"]),
            str(row["notional"]), row["side"],
        ])
        candidates.append((symbol, identifier, session_id, event, row["price"],
                           row["qty"], row["notional"], row["side"], payload_hash))
    with conn.cursor() as cursor:
        cursor.executemany(
            """INSERT INTO live_trade_receipts(
                 symbol,trade_id,session_id,source_event_at,
                 price,qty,notional,side,payload_sha256)
               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(symbol,trade_id) DO NOTHING""",
            candidates,
        )
    stored = conn.execute(
        """SELECT trade_id,source_event_at,price,qty,notional,side,
                  payload_sha256,received_at
           FROM live_trade_receipts
           WHERE symbol=%s AND trade_id=ANY(%s)""",
        (symbol, list(trade_ids)),
    ).fetchall()
    if len(stored) != len(trade_ids):
        raise ValueError("Missing immutable trade receipt")
    for receipt in stored:
        row = by_id[receipt["trade_id"]]
        check = _sha([
            symbol, receipt["trade_id"], _utc(row["ts"]).isoformat(),
            str(row["price"]), str(row["qty"]),
            str(row["notional"]), row["side"],
        ])
        if (receipt["payload_sha256"] != check or
                receipt["source_event_at"] != row["ts"] or
                receipt["price"] != row["price"] or
                receipt["qty"] != row["qty"] or
                receipt["side"] != row["side"] or
                receipt["notional"] != row["notional"] or
                receipt["received_at"] > _source_clock(conn)):
            raise ValueError("Conflicting original immutable WS receipt")
    return {"recorded_ids": len(stored), "quality": "observed_ws_trade",
            "exchange_complete": False}


def record_book_lineage(conn, symbol, session_id, snapshot_event_id, event_id):
    """Pin an actual replayed L2 snapshot plus monotone DB event chain."""
    _key(symbol, "symbol")
    _key(session_id, "WebSocket session")
    if (type(snapshot_event_id) is not int or type(event_id) is not int
            or snapshot_event_id <= 0 or event_id < snapshot_event_id):
        raise ValueError("Invalid L2 replay event lineage")
    start = conn.execute(
        """SELECT symbol,event_type,valid,recv_ts
           FROM book_events WHERE id=%s""",
        (snapshot_event_id,),
    ).fetchone()
    end = conn.execute(
        """SELECT symbol,event_type,valid,recv_ts,update_id
           FROM book_events WHERE id=%s""",
        (event_id,),
    ).fetchone()
    now = _source_clock(conn)
    if (start is None or end is None
            or start["symbol"] != symbol or end["symbol"] != symbol
            or start["event_type"] != "snapshot" or not start["valid"]
            or not end["valid"] or end["recv_ts"] < start["recv_ts"]
            or end["recv_ts"] > now
            or end["recv_ts"] < now-_BOOK_AGE):
        raise ValueError("L2 snapshot/update not actually observed")
    # A calls during the original WS transaction, before the separate C
    # market_book_state replay worker. Record immutable raw lineage NOW;
    # require actual *replayed* L2 state only when issuing paper evidence.
    # Bybit update ID may skip values: require strictly monotone updates,
    # never fabricate a gapless +1 counter.
    rows = conn.execute(
        """SELECT id,event_type,update_id,valid
           FROM book_events WHERE symbol=%s AND id >= %s AND id <= %s
           ORDER BY id""",
        (symbol, snapshot_event_id, event_id),
    ).fetchall()
    previous = None
    for item in rows:
        if not item["valid"] or (item["event_type"] == "snapshot"
                                 and item["id"] != snapshot_event_id):
            raise ValueError("Invalid L2 event within replay lineage")
        if (item["event_type"] == "delta"
                and (previous is None or item["update_id"] is None
                     or item["update_id"] <= previous)):
            raise ValueError("Discontinuous L2 delta lineage")
        if item["update_id"] is not None:
            previous = item["update_id"]
    if not rows or rows[0]["id"] != snapshot_event_id or rows[-1]["id"] != event_id:
        raise ValueError("Missing L2 replay event")
    inserted = conn.execute(
        """INSERT INTO live_book_receipts(
             symbol,session_id,snapshot_event_id,event_id,latest_update_id)
           VALUES(%s,%s,%s,%s,%s)
           ON CONFLICT(symbol,session_id,snapshot_event_id,event_id)
           DO NOTHING RETURNING id""",
        (symbol, session_id, snapshot_event_id, event_id, end["update_id"]),
    ).fetchone()
    if inserted:
        return inserted["id"]
    stored = conn.execute(
        """SELECT id,latest_update_id FROM live_book_receipts
           WHERE symbol=%s AND session_id=%s AND snapshot_event_id=%s
             AND event_id=%s""",
        (symbol, session_id, snapshot_event_id, event_id),
    ).fetchone()
    if stored is None or stored["latest_update_id"] != end["update_id"]:
        raise ValueError("Conflicting L2 observation replay")
    return stored["id"]


def record_oi_observation(conn, symbol, interval, event_ts, source_checksum,
                          *, instrument="linear"):
    """Witness actual REST OI data already inserted by A, without backdating."""
    _key(symbol, "symbol")
    _key(interval, "OI interval")
    _hash(source_checksum)
    if instrument not in ("linear", "inverse", "spot"):
        raise ValueError("Unknown OI instrument")
    event = _utc(event_ts)
    now = _source_clock(conn)
    if not event <= now <= event + _OI_AGE:
        raise ValueError("Stale or future REST OI event")
    row = conn.execute(
        """SELECT value FROM open_interest
           WHERE symbol=%s AND interval=%s AND ts=%s""",
        (symbol, interval, event),
    ).fetchone()
    if row is None or row["value"] < 0:
        raise ValueError("No independently persisted REST OI value")
    inserted = conn.execute(
        """INSERT INTO live_oi_receipts(
             symbol,instrument,interval,source_event_at,
             oi_value,response_sha256)
           VALUES(%s,%s,%s,%s,%s,%s)
           ON CONFLICT(symbol,interval,source_event_at,response_sha256)
           DO NOTHING RETURNING id""",
        (symbol, instrument, interval, event, row["value"], source_checksum),
    ).fetchone()
    if inserted:
        return inserted["id"]
    old = conn.execute(
        """SELECT id,instrument,oi_value FROM live_oi_receipts
           WHERE symbol=%s AND interval=%s AND source_event_at=%s
             AND response_sha256=%s""",
        (symbol, interval, event, source_checksum),
    ).fetchone()
    if old is None or old["instrument"] != instrument or old["oi_value"] != row["value"]:
        raise ValueError("Conflicting immutable REST OI observation")
    return old["id"]


def _no_overlap(conn, symbol, start, end):
    """Never treat reconnect or gap resolution as all-print recovery."""
    gap = conn.execute(
        """SELECT 1 FROM data_gaps
           WHERE symbol=%s AND source IN ('publicTrade','orderbook')
             AND start_ts < %s
             AND coalesce(end_ts,resolved_at,'infinity'::timestamptz) >= %s
           LIMIT 1""",
        (symbol, end, start),
    ).fetchone()
    if gap is not None:
        raise ValueError("Public stream interruption overlaps evidence window")
    status = conn.execute(
        """SELECT status FROM feed_health
           WHERE symbol=%s AND topic='connection'
           ORDER BY ts DESC LIMIT 1""",
        (symbol,),
    ).fetchone()
    if status is None or status["status"] != "connected":
        raise ValueError("No independently recorded connected public WS session")


def publish_bounded_paper_evidence(
    conn, symbol, bar_ts, trade_session, book_receipt_id, oi_receipt_id,
):
    """Publish *paper-only* observed 1m evidence, never an ML-v2 certificate.

    All input source receipts must already exist and precede this DB
    publication. No caller can supply a verified flag or decision timestamp.
    An admission AFTER 90 seconds is impossible; E must use its actual
    independent decision clock and not a historical bar close.
    """
    _key(symbol, "symbol")
    _key(trade_session, "WebSocket session")
    if (type(book_receipt_id) is not int or book_receipt_id <= 0
            or type(oi_receipt_id) is not int or oi_receipt_id <= 0):
        raise ValueError("Pinned book and OI observation references required")
    start = _utc(bar_ts)
    if (start.second != 0 or start.microsecond != 0
            or start.minute < 0):
        raise ValueError("Natural UTC closed 1-minute bar required")
    end = start + _ONE_MINUTE
    now = _source_clock(conn)
    if now < end or now > end + _LATENCY:
        raise ValueError("Live paper witness outside prospective 90s admission")
    _no_overlap(conn, symbol, start, now)
    bar = conn.execute(
        """SELECT volume,turnover,open,high,low,close,source
           FROM candles
           WHERE symbol=%s AND kind='trade' AND tf='1'
             AND ts=%s AND closed=true""",
        (symbol, start),
    ).fetchone()
    if (bar is None or bar["volume"] is None or bar["turnover"] is None or
            bar["volume"] <= 0 or bar["turnover"] <= 0 or
            bar["source"] != "bybit_v5"):
        raise ValueError("Official 1m candle not independently closed")
    observed = conn.execute(
        """SELECT count(*) AS n,coalesce(sum(qty),0) AS base,
                  coalesce(sum(notional),0) AS quote,
                  min(received_at) AS first_receipt,
                  max(received_at) AS last_receipt,
                  count(DISTINCT session_id) AS sessions
           FROM live_trade_receipts
           WHERE symbol=%s AND source_event_at >= %s
             AND source_event_at < %s""",
        (symbol, start, end),
    ).fetchone()
    if (observed["n"] <= 0 or observed["sessions"] != 1
            or observed["first_receipt"] is None or
            observed["last_receipt"] is None or
            observed["first_receipt"] > now or
            observed["last_receipt"] > now or
            observed["last_receipt"] < now - _TRADE_AGE):
        raise ValueError("Trade stream is incomplete, mixed-session or stale")
    session = conn.execute(
        """SELECT 1 FROM live_trade_receipts
           WHERE symbol=%s AND source_event_at >= %s AND source_event_at < %s
             AND session_id=%s LIMIT 1""",
        (symbol, start, end, trade_session),
    ).fetchone()
    if session is None:
        raise ValueError("Trade source session identity mismatch")
    # Source receipts must account for EVERY persisted WS trade in this
    # interval, not only the caller's chosen subset of print IDs.
    actual = conn.execute(
        """SELECT count(*) AS n,coalesce(sum(qty),0) AS base,
                  coalesce(sum(notional),0) AS quote
           FROM trades
           WHERE symbol=%s AND source='websocket'
             AND ts >= %s AND ts < %s""",
        (symbol, start, end),
    ).fetchone()
    if (actual["n"] != observed["n"] or
            actual["base"] != observed["base"] or
            actual["quote"] != observed["quote"]):
        raise ValueError("WS receipt coverage omits stored publicTrade IDs")
    expected_base = Decimal(bar["volume"])
    expected_quote = Decimal(bar["turnover"])
    if (abs(observed["base"] - expected_base) >
            max(_BASE_EPS, abs(expected_base)*Decimal("1e-10")) or
            abs(observed["quote"] - expected_quote) >
            max(_QUOTE_EPS, abs(expected_quote)*Decimal("1e-10"))):
        raise ValueError("Observed trades disagree with closed Bybit base/quote")
    book = conn.execute(
        """SELECT symbol,session_id,snapshot_event_id,event_id,
                  latest_update_id,received_at
           FROM live_book_receipts WHERE id=%s""",
        (book_receipt_id,),
    ).fetchone()
    state = conn.execute(
        """SELECT valid,last_event_id,updated_at
           FROM market_book_state WHERE symbol=%s""",
        (symbol,),
    ).fetchone()
    if (book is None or book["symbol"] != symbol
            or book["session_id"] != trade_session
            or book["received_at"] > now
            or book["received_at"] < now-_BOOK_AGE
            or state is None or not state["valid"]
            or state["last_event_id"] < book["event_id"]
            or state["updated_at"] > now):
        raise ValueError("Missing current independently replayed L2 lineage")
    oi = conn.execute(
        """SELECT symbol,instrument,interval,source_event_at,received_at,
                  oi_value,response_sha256
           FROM live_oi_receipts WHERE id=%s""",
        (oi_receipt_id,),
    ).fetchone()
    if (oi is None or oi["symbol"] != symbol or oi["instrument"] != "linear"
            or oi["interval"] != "5min"
            or not oi["source_event_at"] <= oi["received_at"] <= now
            or oi["received_at"] < now-_OI_AGE
            or oi["source_event_at"] < now-_OI_AGE):
        raise ValueError("Missing fresh DB-observed REST open interest")
    recorded_oi = conn.execute(
        """SELECT value FROM open_interest
           WHERE symbol=%s AND interval='5min' AND ts=%s""",
        (symbol, oi["source_event_at"]),
    ).fetchone()
    if recorded_oi is None or recorded_oi["value"] != oi["oi_value"]:
        raise ValueError("REST OI receipt no longer matches canonical OI")
    provenance = {
        "contract": "eth_paper_live_evidence_v1",
        "data_scope": "bounded_local_observations_only",
        "quality": "live_observed_bounded",
        "exchange_complete": False, "is_ml_certified": False,
        "complete_trade_ids_proven": False, "ml_eligible": False,
        "is_historical_certified": False,
        "paper_only": True,
        "bar_ts": start.isoformat(), "bar_close_at": end.isoformat(),
        "trade_session": trade_session,
        "trade_count": observed["n"],
        "trade_first_receipt_at": observed["first_receipt"].isoformat(),
        "trade_last_receipt_at": observed["last_receipt"].isoformat(),
        "base_qty": str(observed["base"]),
        "quote_notional": str(observed["quote"]),
        "official_base": str(expected_base),
        "official_quote": str(expected_quote),
        "closed_candle_sha256": _sha([str(bar[k]) for k in
            ("open", "high", "low", "close", "volume", "turnover", "source")]),
        "book_receipt_id": book_receipt_id,
        "book_snapshot_event_id": book["snapshot_event_id"],
        "book_event_id": book["event_id"],
        "book_received_at": book["received_at"].isoformat(),
        "oi_receipt_id": oi_receipt_id,
        "oi_event_at": oi["source_event_at"].isoformat(),
        "oi_received_at": oi["received_at"].isoformat(),
        "oi_source_checksum": oi["response_sha256"],
        "gap_checked_at": now.isoformat(),
        "no_exchange_wide_trade_completeness_claim": True,
    }
    source_checksum = _sha({k: v for k, v in provenance.items()
                            if k != "gap_checked_at"})
    key = _sha(["eth_paper_live_evidence_v1", symbol,
                start.isoformat(), source_checksum])
    result = conn.execute(
        """WITH publication AS MATERIALIZED (
             SELECT clock_timestamp() AS stamp)
           INSERT INTO paper_live_evidence(
             snapshot_key,source_checksum,symbol,bar_ts,bar_close_at,
             trade_session,trade_count,trade_first_receipt_at,
             trade_last_receipt_at,book_receipt_id,oi_receipt_id,
             base_volume,quote_turnover,evidence,
             observed_at,computed_at,expires_at)
           SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,
                  publication.stamp,publication.stamp,%s
           FROM publication
           WHERE publication.stamp >= %s
             AND publication.stamp <= %s
           ON CONFLICT(snapshot_key) DO NOTHING RETURNING snapshot_key""",
        (key, source_checksum, symbol, start, end, trade_session, observed["n"],
         observed["first_receipt"], observed["last_receipt"],
         book_receipt_id, oi_receipt_id, observed["base"], observed["quote"],
         json.dumps(provenance, sort_keys=True), end+_LATENCY,
         end, end+_LATENCY),
    ).fetchone()
    if result is None:
        prior = conn.execute(
            """SELECT snapshot_key,source_checksum FROM paper_live_evidence
               WHERE snapshot_key=%s""", (key,),
        ).fetchone()
        if prior is None:
            raise ValueError("Evidence became late during DB publication")
        if prior["source_checksum"] != source_checksum:
            raise ValueError("Conflicting immutable bounded-live checksum")
    return key

def require_bounded_paper_evidence(conn, *, symbol, bar_ts, evidence_key,
                                   decision_at):
    """Read-only prospective check for D/E; never returns ML eligibility."""
    _hash(evidence_key)
    start, decision = _utc(bar_ts), _utc(decision_at)
    row = conn.execute(
        """SELECT snapshot_key,source_checksum,contract,symbol,tf,bar_ts,
                  bar_close_at,observed_at,computed_at,expires_at,
                  quality,paper_only,ml_eligible,evidence,
                  is_ml_certified,is_historical_certified,exchange_complete
           FROM paper_live_evidence WHERE snapshot_key=%s""",
        (evidence_key,),
    ).fetchone()
    if (row is None or row["symbol"] != symbol or row["tf"] != "1"
            or row["bar_ts"] != start
            or row["contract"] != "eth_paper_live_evidence_v1"
            or row["quality"] != "live_observed_bounded"
            or row["paper_only"] is not True
            or row["ml_eligible"] is not False
            or row["is_ml_certified"] or row["is_historical_certified"]
            or row["exchange_complete"] or
            not row["bar_close_at"] <= row["observed_at"] <=
            row["computed_at"] <= decision <= row["expires_at"]):
        raise ValueError("No current prospective bounded PAPER-only evidence")
    evidence = row["evidence"]
    if not isinstance(evidence, dict):
        raise ValueError("Missing canonical immutable live source provenance")
    expected_source_checksum = _sha({
        k: v for k, v in evidence.items() if k != "gap_checked_at"
    })
    expected_key = _sha(["eth_paper_live_evidence_v1", symbol,
                         start.isoformat(), expected_source_checksum])
    if (row["source_checksum"] != expected_source_checksum or
            row["snapshot_key"] != expected_key or
            evidence.get("paper_only") is not True or
            evidence.get("ml_eligible") is not False or
            evidence.get("exchange_complete") is not False):
        raise ValueError("Tampered immutable live observation identity")
    return {"snapshot_key": row["snapshot_key"],
            "source_checksum": row["source_checksum"],
            "quality": row["quality"], "contract": row["contract"],
            "bar_ts": row["bar_ts"],
            "observed_at": row["observed_at"],
            "computed_at": row["computed_at"],
            "expires_at": row["expires_at"], "paper_only": True,
            "ml_eligible": False, "exchange_complete": False}
