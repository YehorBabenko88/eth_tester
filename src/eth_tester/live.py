import asyncio
from datetime import datetime,timezone
from decimal import Decimal
import json
import random
import time
from hashlib import sha256
from uuid import uuid4
import websockets
from .bybit import MarketClient
from .data_quality import DataIntegrityError
from .database import connection
from .history_integrity import record_gap, collect_latest_closed_trade_minute
from .history_series import collect_recent_oi_receipt
from .history import (
    KIND_ENDPOINTS, INTERVAL_MS, OI_INTERVALS, collect_klines,
    collect_funding, collect_oi, collect_account_ratios,
)

def validate_book_sequence(previous_seq, previous_update, payload):
    """Reject replayed/out-of-order L2 deltas; snapshot resets continuity."""
    row = payload.get("data")
    if not isinstance(row, dict):
        raise DataIntegrityError("Malformed Bybit orderbook payload")
    sequence = row.get("seq")
    update_id = row.get("u")
    if sequence is None or update_id is None:
        raise DataIntegrityError("Missing Bybit orderbook sequence/update ID")
    sequence, update_id = int(sequence), int(update_id)
    if payload.get("type") != "snapshot":
        if previous_seq is None or previous_update is None:
            raise DataIntegrityError("Orderbook delta without exchange snapshot")
        if sequence <= previous_seq or update_id <= previous_update:
            raise DataIntegrityError("Out-of-order/replayed Bybit orderbook delta")
    return sequence, update_id


def stamp_ms(value):
    return datetime.fromtimestamp(int(value)/1000,timezone.utc)


_SOURCE_LOCK_PREFIX = "eth_paper_admission_source:"


def lock_admission_source(conn, symbol):
    """Serialize DB-visible WS outages with B/E same-transaction paper gate.

    Acquire FIRST in a source status/gap transaction. Never hold across network,
    file IO or asyncio sleep. This does not prove an OS-level disconnect was
    recorded before its real time; only committed DB transitions are fenced.
    """
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                 (_SOURCE_LOCK_PREFIX + symbol,))


def reconcile_unclean_ws_start(conn, symbol, last_trade=None):
    """Recover a crash while the last DB connection still said 'connected'.

    A healthy restart cannot silently reuse a previous session as current
    market connectivity. Persist an unresolved tick gap and a bounded L2 gap
    before any new 'connected' row can become visible to paper admission.
    Returns the current open publicTrade gap ID, if any.
    """
    lock_admission_source(conn, symbol)
    previous = conn.execute(
        """SELECT ts,status FROM feed_health WHERE symbol=%s
           AND topic='connection' ORDER BY ts DESC LIMIT 1""",
        (symbol,)).fetchone()
    pending = conn.execute(
        """SELECT id FROM data_gaps WHERE symbol=%s
           AND source='publicTrade' AND end_ts IS NULL
           AND resolved_at IS NULL ORDER BY id DESC LIMIT 1""",
        (symbol,)).fetchone()
    pending_id = pending["id"] if pending else None
    if not previous or previous["status"] != "connected":
        return pending_id
    now = conn.execute(
        "SELECT clock_timestamp() AS ts").fetchone()["ts"]
    detail = {
        "reason": "unclean_process_restart_or_missing_disconnect_receipt",
        "source_quality": "gap", "ml_eligible": False,
    }
    conn.execute(
        """INSERT INTO feed_health(symbol,topic,ts,status,detail)
           VALUES(%s,'connection',clock_timestamp(),
                  'disconnected',%s::jsonb)""",
        (symbol, json.dumps(detail)))
    if pending_id is None:
        stamp = last_trade or previous["ts"]
        gap = conn.execute(
            """INSERT INTO data_gaps(source,symbol,start_ts,explanation)
               VALUES('publicTrade',%s,%s,%s) RETURNING id""",
            (symbol, stamp, "unclean websocket exit; trade IDs unknown")
        ).fetchone()
        pending_id = gap["id"]
    last_book = conn.execute(
        """SELECT max(recv_ts) AS ts FROM book_events WHERE symbol=%s""",
        (symbol,)).fetchone()
    if last_book and last_book["ts"] and last_book["ts"] < now:
        record_gap(
            conn, symbol, "orderbook",
            int(last_book["ts"].timestamp()*1000),
            int(now.timestamp()*1000),
            "Unclean WS exit; lost L2 deltas cannot be reconstructed")
    return pending_id


async def await_ws_subscription(ws, topics, *, timeout_seconds=10):
    """A TCP socket is not a verified Bybit market subscription."""
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout_seconds)
    except asyncio.TimeoutError as exc:
        raise TimeoutError("Bybit subscription ACK timed out") from exc
    try:
        acknowledgment = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise DataIntegrityError("Malformed Bybit subscription ACK") from exc
    if (not isinstance(acknowledgment, dict)
            or acknowledgment.get("op") != "subscribe"
            or acknowledgment.get("success") is not True):
        raise DataIntegrityError("Bybit subscription not acknowledged")
    returned = acknowledgment.get("args")
    if returned is not None and (
            not isinstance(returned, list)
            or set(returned) != set(topics)):
        raise DataIntegrityError("Bybit subscription topics mismatch")
    return acknowledgment


async def next_ws_payload(ws, *, idle_seconds=30):
    """Force reconnect on a half-open socket with no market event activity."""
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=idle_seconds)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(
            f"WebSocket half-open or idle: {idle_seconds}s without market frames"
        ) from exc
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise DataIntegrityError("Malformed WebSocket JSON") from exc
    if not isinstance(payload, dict):
        raise DataIntegrityError("Non-object Bybit websocket message")
    return payload


def _record_ws_trade_receipts(conn, symbol, session_id, trade_ids):
    """B's transactional source ledger; never infer completeness from an ACK."""
    from .storage.live_evidence import record_trade_receipts
    return record_trade_receipts(conn, symbol, session_id, trade_ids)


def _record_ws_book_lineage(conn, symbol, session_id, snapshot_event_id,
                            event_id):
    """B independently validates actual source event sequence and DB receipt."""
    from .storage.live_evidence import record_book_lineage
    return record_book_lineage(
        conn, symbol, session_id, snapshot_event_id, event_id)


def canonical_ws_trade_rows(data, symbol):
    """Deduplicate only truly identical normalized prints inside ONE WS frame.

    The first raw event remains the source of record. A conflicting reuse of a
    Bybit trade ID fails the whole frame before ANY database insert. Duplicate
    raw rows never create extra observed identity or inflate minute volume.
    """
    if not isinstance(data, list):
        raise DataIntegrityError("Bybit publicTrade data must be a list")
    original = {}
    unique = []
    for row in data:
        if not isinstance(row, dict):
            raise DataIntegrityError("Malformed Bybit publicTrade record")
        identifier = row["i"]
        if not isinstance(identifier, str) or not identifier.strip():
            raise DataIntegrityError("Empty or non-string Bybit trade ID")
        side = row["S"]
        if side not in ("Buy", "Sell"):
            raise DataIntegrityError("Unknown trade side")
        if row.get("s", symbol) != symbol:
            raise DataIntegrityError("WebSocket trade row has wrong symbol")
        event = stamp_ms(row["T"])
        price, qty = Decimal(str(row["p"])), Decimal(str(row["v"]))
        if (not price.is_finite() or not qty.is_finite()
                or price <= 0 or qty <= 0):
            raise DataIntegrityError("Invalid WebSocket trade price/quantity")
        fields = (event, price, qty, side)
        if identifier in original:
            if original[identifier] != fields:
                raise DataIntegrityError(
                    f"Conflicting duplicate WS trade ID: {identifier}")
            continue
        original[identifier] = fields
        unique.append((identifier, row, event, price, qty, side))
    return unique


def ingest_ws(conn,symbol,payload,*,session_id=None):
    """Persist actual WS event payloads, optionally attaching a live session receipt.

    Health receipts are observations; they do not prove all exchange prints
    or imply permission to certify historical ML features.
    """
    topic=payload.get("topic","")
    data=payload.get("data")
    if not topic or data is None:
        return
    if not topic.endswith("." + symbol):
        raise DataIntegrityError(f"WebSocket topic symbol mismatch: {topic}")
    if topic.startswith("publicTrade."):
        unique = canonical_ws_trade_rows(data, symbol)
        # Fail malformed whole frames before any write, then fence all WS writes.
        lock_admission_source(conn, symbol)
        for identifier, row, event, price, qty, side in unique:
            saved = conn.execute("""INSERT INTO trades(symbol,ts,trade_id,price,qty,side,notional,source,raw)
             VALUES(%s,%s,%s,%s,%s,%s,%s,'websocket',%s::jsonb)
             ON CONFLICT(symbol,trade_id) DO UPDATE
             SET price=EXCLUDED.price, source='websocket'
             WHERE trades.ts=EXCLUDED.ts AND trades.price=EXCLUDED.price
             AND trades.qty=EXCLUDED.qty AND trades.side=EXCLUDED.side
             AND trades.notional=EXCLUDED.notional
             RETURNING trade_id""",
             (symbol,event,identifier,price,qty,side,price*qty,json.dumps(row))).fetchone()
            if saved is None:
                raise DataIntegrityError(f"Conflicting WS trade ID: {identifier}")
        if session_id is not None and unique:
            trade_ids = [item[0] for item in unique]
            # B accepts at most 2048 distinct IDs per call. A single WS frame
            # may have more; keep all bounded chunks in the SAME transaction.
            # Failure of any chunk rolls back the entire ingest/receipt batch.
            for offset in range(0, len(trade_ids), 2048):
                _record_ws_trade_receipts(
                    conn, symbol, session_id, trade_ids[offset:offset+2048])
            stamps = [int(row["T"]) for row in data]
            raw_sha = sha256(json.dumps(
                payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            ids_sha = sha256(json.dumps(
                sorted({str(row["i"]) for row in data}),
                separators=(",", ":")).encode()).hexdigest()
            detail = {
                "contract": "eth_ws_observation_v1",
                "session_id": session_id, "payload_sha256": raw_sha,
                "trade_ids_sha256": ids_sha,
                "trade_ids": trade_ids,
                "source_event_start_ms": min(stamps),
                "source_event_end_ms": max(stamps),
                "observed_rows": len(data), "distinct_trade_ids":
                    len(unique), "identical_replayed_ids": len(data)-len(unique),
                "proves_all_trade_ids": False,
                "source_quality": "observed_partial",
                "ml_eligible": False,
            }
            conn.execute("""INSERT INTO feed_health(
                symbol,topic,ts,status,detail)
                VALUES(%s,'publicTrade',clock_timestamp(),
                       'observed_partial',%s::jsonb)""",
                (symbol, json.dumps(detail, sort_keys=True)))
    elif topic.startswith("orderbook."):
        row=data if isinstance(data,dict) else {}
        lock_admission_source(conn, symbol)
        book_result = conn.execute("""INSERT INTO book_events(
          symbol,exchange_ts,recv_ts,update_id,sequence_no,
          event_type,bids,asks)
          VALUES(%s,%s,clock_timestamp(),%s,%s,%s,%s::jsonb,%s::jsonb)
          RETURNING id,recv_ts""",
          (symbol,stamp_ms(payload["ts"]) if payload.get("ts") else None,
           row.get("u"),row.get("seq"),payload.get("type","unknown"),
           json.dumps(row.get("b",[])),json.dumps(row.get("a",[]))))
        if session_id is not None:
            saved_book = book_result.fetchone()
            detail = {
                "contract": "eth_ws_observation_v1",
                "session_id": session_id,
                "book_event_id": saved_book["id"],
                "source_event_ms": int(payload["ts"]) if payload.get("ts") else None,
                "source_receipt_ms": int(saved_book["recv_ts"].timestamp()*1000),
                "snapshot": payload.get("type") == "snapshot",
                "update_id": row.get("u"), "sequence_no": row.get("seq"),
                "payload_sha256": sha256(json.dumps(
                    payload, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(), "source_quality": "observed_partial",
                "orderbook_replay_complete": False, "ml_eligible": False,
            }
            conn.execute("""INSERT INTO feed_health(
                symbol,topic,ts,status,detail)
                VALUES(%s,'orderbook',clock_timestamp(),
                       'observed_partial',%s::jsonb)""",
                (symbol, json.dumps(detail, sort_keys=True)))
            return saved_book["id"]

    elif topic.startswith('tickers.'):
        row=data if isinstance(data,dict) else {}
        def numeric(name):
            value=row.get(name)
            return Decimal(str(value)) if value not in (None,'') else None
        lock_admission_source(conn, symbol)
        conn.execute('''INSERT INTO market_ticker_events(symbol,exchange_ts,event_type,
          mark_price,index_price,last_price,open_interest,funding_rate,bid_price,ask_price,raw)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)''',
          (symbol,stamp_ms(payload['ts']) if payload.get('ts') else None,
           payload.get('type','delta'),numeric('markPrice'),numeric('indexPrice'),
           numeric('lastPrice'),numeric('openInterest'),numeric('fundingRate'),
           numeric('bid1Price'),numeric('ask1Price'),json.dumps(row)))
    elif topic.startswith('allLiquidation.'):
        rows=data if isinstance(data,list) else [data]
        lock_admission_source(conn, symbol)
        for row in rows:
            conn.execute('''INSERT INTO liquidation_events(symbol,ts,side,price,qty,raw)
              VALUES(%s,%s,%s,%s,%s,%s::jsonb)''',
              (symbol,stamp_ms(row['T']),row['S'],Decimal(row['p']),
               Decimal(row['v']),json.dumps(row)))


def persist_ws_disconnect(conn, symbol, session_id, reason, *,
                          pending_id=None, last_trade=None, connected_at=None,
                          last_book_ts=None):
    """One atomic disconnect status + publicTrade/L2 gap under E/B source fence."""
    lock_admission_source(conn, symbol)
    conn.execute(
        """INSERT INTO feed_health(symbol,topic,ts,status,detail)
           VALUES(%s,'connection',clock_timestamp(),'disconnected',%s::jsonb)""",
        (symbol, json.dumps({
            "reason": str(reason)[:400], "session_id": session_id,
            "source_quality": "gap", "ml_eligible": False})))
    if pending_id is None:
        existing=conn.execute(
            """SELECT id FROM data_gaps WHERE symbol=%s
               AND source='publicTrade' AND end_ts IS NULL
               AND resolved_at IS NULL ORDER BY id DESC LIMIT 1""",
            (symbol,)).fetchone()
        if existing:
            pending_id=existing["id"]
        else:
            opened_at = last_trade or connected_at
            if opened_at is None:
                opened_at = conn.execute(
                    "SELECT clock_timestamp() AS ts").fetchone()["ts"]
            recorded = conn.execute(
                """INSERT INTO data_gaps(source,symbol,start_ts,explanation)
                   VALUES('publicTrade',%s,%s,'websocket interrupted')
                   RETURNING id""",
                (symbol, opened_at)).fetchone()
            pending_id = recorded["id"]
    if last_book_ts is not None:
        ended = conn.execute(
            "SELECT clock_timestamp() AS ts").fetchone()["ts"]
        if ended > last_book_ts:
            record_gap(
                conn, symbol, "orderbook",
                int(last_book_ts.timestamp()*1000),
                int(ended.timestamp()*1000),
                "WebSocket L2 delta loss; snapshot does not restore history")
    return pending_id


async def live_feed(settings):
    """Observe actual public market data forever, fail closed across outages.

    Source connection/gap mutations take the B/E paper-admission DB fence.
    Network operations, websocket ACK and sleep NEVER hold database locks.
    """
    if settings.book_depth not in (1,50,200,1000):
        raise ValueError("Unsupported orderbook depth")
    topics=[f"publicTrade.{settings.symbol}",
            f"orderbook.{settings.book_depth}.{settings.symbol}",
            f"tickers.{settings.symbol}",f"allLiquidation.{settings.symbol}"]
    attempt=0
    with connection(settings) as conn:
        last=conn.execute(
            """SELECT ts FROM trades WHERE symbol=%s AND source='websocket'
               ORDER BY ts DESC LIMIT 1""",
            (settings.symbol,)).fetchone()
        last_trade=last["ts"] if last else None
        pending_id=reconcile_unclean_ws_start(
            conn, settings.symbol, last_trade=last_trade)
    last_book_ts=None
    while True:
        connected_at=datetime.now(timezone.utc)
        attempt_started=time.monotonic()
        session_id=uuid4().hex
        try:
            async with websockets.connect(
                    settings.bybit_ws, ping_interval=20, ping_timeout=15,
                    open_timeout=10, close_timeout=5, max_size=8*1024*1024,
                    max_queue=32) as ws:
                await ws.send(json.dumps({"op":"subscribe","args":topics}))
                await await_ws_subscription(ws, topics)
                # Only an actual acknowledged subscription is 'connected'.
                with connection(settings) as conn:
                    pending_id=reconcile_unclean_ws_start(
                        conn, settings.symbol, last_trade=last_trade)
                    conn.execute("""INSERT INTO feed_health(
                      symbol,topic,ts,status,detail)
                      VALUES(%s,'connection',clock_timestamp(),
                             'connected',%s::jsonb)""",
                      (settings.symbol,json.dumps({
                          "session_id":session_id,
                          "orderbook_depth":settings.book_depth,
                          "source_quality":"observed_partial",
                          "ml_eligible":False})))
                book_ready=False
                book_seq=None
                book_update=None
                book_snapshot_id=None
                while True:
                    payload=await next_ws_payload(ws)
                    if payload.get("op")=="subscribe" and not payload.get("success"):
                        raise DataIntegrityError("WebSocket subscription revoked")
                    topic=payload.get("topic","")
                    if topic.startswith("orderbook."):
                        if payload.get("type")=="snapshot":
                            book_ready=True
                        elif not book_ready:
                            raise DataIntegrityError("Orderbook delta before snapshot")
                        book_seq,book_update=validate_book_sequence(
                            book_seq, book_update, payload)
                        new_book_ts=(stamp_ms(payload["ts"])
                                     if payload.get("ts")
                                     else datetime.now(timezone.utc))
                    else:
                        new_book_ts=None
                    if topic.startswith(("publicTrade.","orderbook.",
                                         "tickers.","allLiquidation.")):
                        new_pending_id=pending_id
                        new_last_trade=last_trade
                        with connection(settings) as conn:
                            # A source event can close an earlier *open-ended*
                            # gap only as a bounded interval; it never sets
                            # resolved_at or declares lost exchange IDs found.
                            if topic.startswith("publicTrade.") and payload.get("data"):
                                stamps=[stamp_ms(x["T"]) for x in payload["data"]]
                                latest=max(stamps)
                                unseen=[stamp for stamp in stamps
                                        if last_trade is None or stamp > last_trade]
                                # ingest_ws takes the same B/E lock for EVERY WS frame.
                                event_id=ingest_ws(
                                    conn,settings.symbol,payload,session_id=session_id)
                                if pending_id and unseen:
                                    conn.execute(
                                        """UPDATE data_gaps SET end_ts=%s
                                           WHERE id=%s AND end_ts IS NULL
                                             AND resolved_at IS NULL""",
                                        (min(unseen),pending_id))
                                    new_pending_id=None
                                new_last_trade=(max(last_trade,latest)
                                                if last_trade else latest)
                            else:
                                event_id=ingest_ws(
                                    conn,settings.symbol,payload,session_id=session_id)
                            if topic.startswith("orderbook."):
                                if payload.get("type")=="snapshot":
                                    book_snapshot_id=event_id
                                if event_id is None or book_snapshot_id is None:
                                    raise DataIntegrityError("L2 source event ID missing")
                                _record_ws_book_lineage(
                                    conn,settings.symbol,session_id,
                                    book_snapshot_id,event_id)
                        # Update in-memory checkpoint ONLY after SQL COMMIT.
                        pending_id=new_pending_id
                        last_trade=new_last_trade
                        if new_book_ts is not None:
                            last_book_ts=new_book_ts
        except asyncio.CancelledError:
            # Stop/shutdown is not an exchange data-completeness certificate.
            # Persist the interrupted source before leaving whenever the DB
            # is reachable; startup repair handles a power loss during this.
            try:
                with connection(settings) as conn:
                    persist_ws_disconnect(
                        conn, settings.symbol, session_id, "worker_cancelled",
                        pending_id=pending_id, last_trade=last_trade,
                        connected_at=connected_at,
                        last_book_ts=last_book_ts)
            except Exception as log_error:
                print(f"Failed to persist shutdown gap: {log_error}",flush=True)
            raise
        except Exception as exc:
            try:
                with connection(settings) as conn:
                    next_pending=persist_ws_disconnect(
                        conn, settings.symbol, session_id, exc,
                        pending_id=pending_id, last_trade=last_trade,
                        connected_at=connected_at, last_book_ts=last_book_ts)
                # Advance in-memory state ONLY after the disconnect transaction
                # has successfully COMMITTED.
                pending_id=next_pending
                last_book_ts=None
            except Exception as log_error:
                print(f"Failed to persist outage: {log_error}",flush=True)
                # A failed COMMIT must not fabricate an open gap ID or a
                # connected receipt. Next successful connection rechecks the
                # previous persisted state before marking its new session.
                try:
                    with connection(settings) as conn:
                        lock_admission_source(conn,settings.symbol)
                        pending=conn.execute(
                            """SELECT id FROM data_gaps WHERE symbol=%s
                               AND source='publicTrade' AND end_ts IS NULL
                               AND resolved_at IS NULL
                               ORDER BY id DESC LIMIT 1""",
                            (settings.symbol,)).fetchone()
                        pending_id=pending["id"] if pending else None
                except Exception:
                    pass
            if time.monotonic()-attempt_started >= 60:
                attempt=0
            attempt+=1
            await asyncio.sleep(min(120,2**min(7,attempt))+
                                random.uniform(0,2))


async def reconcile_rest_cycle(settings, client):
    """Try EVERY independent Bybit series even when one endpoint fails.

    Each failure is observable, never promoted to a successful source receipt.
    This slow backfill lane does not block the independent fast closed-minute lane.
    """
    failures = []
    jobs = [(f"kline:{kind}:{tf}", collect_klines, (settings, client, kind, tf))
            for kind in KIND_ENDPOINTS for tf in INTERVAL_MS]
    jobs.append(("funding", collect_funding, (settings, client)))
    for interval in OI_INTERVALS:
        jobs.append((f"oi:{interval}", collect_oi, (settings, client, interval)))
        jobs.append((f"account_ratio:{interval}", collect_account_ratios,
                     (settings, client, interval)))
    for name, function, args in jobs:
        try:
            await function(*args)
        except Exception as exc:
            failures.append({"source": name, "error": str(exc)[:400]})
            try:
                with connection(settings) as conn:
                    conn.execute("""INSERT INTO feed_health(
                        symbol,topic,ts,status,detail)
                        VALUES(%s,'rest_reconcile',clock_timestamp(),
                               'error',%s::jsonb)""",
                        (settings.symbol,json.dumps(
                            failures[-1],sort_keys=True)))
            except Exception as telemetry_error:
                print(f"REST source {name} failed, telemetry unavailable: "
                      f"{telemetry_error}", flush=True)
    return {"status": "partial" if failures else "completed",
            "errors": failures, "attempted": len(jobs)}


async def live_rest_reconcile(settings):
    async with MarketClient(settings.bybit_rest,settings.rest_rps) as client:
        while True:
            await reconcile_rest_cycle(settings, client)
            await asyncio.sleep(120)


async def live_fast_reconcile(settings, *, interval_seconds=10):
    """Capture closed official 1m candles and observed 5m OI promptly.

    This is a paper-only *source observation* lane. It cannot assert all trade
    identity, L2 continuity or certified ML provenance. C/B must validate the
    joint DB witness BEFORE a paper-only decision; E never backdates.
    """
    cycle = 0
    async with MarketClient(settings.bybit_rest,settings.rest_rps) as client:
        while True:
            jobs = [("closed_1m", collect_latest_closed_trade_minute)]
            if cycle % 3 == 0:
                jobs.append(("observed_5m_oi", collect_recent_oi_receipt))
            for name, function in jobs:
                try:
                    # Do not let one slow OI/REST call consume the next
                    # closed-minute paper admission window. Timeout leaves no
                    # positive receipt and the next cycle can retry.
                    receipt = await asyncio.wait_for(
                        function(settings, client), timeout=8)
                    with connection(settings) as conn:
                        conn.execute("""INSERT INTO feed_health(
                            symbol,topic,ts,status,detail)
                            VALUES(%s,%s,clock_timestamp(),
                                   'observed_partial',%s::jsonb)""",
                            (settings.symbol, name, json.dumps(
                                receipt, sort_keys=True)))
                except Exception as exc:
                    try:
                        with connection(settings) as conn:
                            conn.execute("""INSERT INTO feed_health(
                                symbol,topic,ts,status,detail)
                                VALUES(%s,%s,clock_timestamp(),
                                       'error',%s::jsonb)""",
                                (settings.symbol,name,json.dumps({
                                    "error":str(exc)[:400],
                                    "source_quality":"unavailable",
                                    "ml_eligible":False})))
                    except Exception as telemetry_error:
                        print(f"Fast source {name} failed, telemetry unavailable: "
                              f"{telemetry_error}",flush=True)
            cycle += 1
            await asyncio.sleep(interval_seconds)

async def run_live(settings):
    # Keep WS ingestion independent from slow historical REST backfills.
    from .recent_recovery import run_recent_recovery
    await asyncio.gather(
        live_feed(settings), live_rest_reconcile(settings),
        live_fast_reconcile(settings), run_recent_recovery(settings))
