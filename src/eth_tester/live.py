import asyncio
from datetime import datetime,timezone
from decimal import Decimal
import json
import random
import websockets
from .bybit import MarketClient
from .data_quality import DataIntegrityError
from .database import connection
from .history_integrity import record_gap
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

def ingest_ws(conn,symbol,payload):
    topic=payload.get("topic","")
    data=payload.get("data")
    if not topic or data is None:
        return
    if not topic.endswith("." + symbol):
        raise DataIntegrityError(f"WebSocket topic symbol mismatch: {topic}")
    if topic.startswith("publicTrade."):
        for row in data:
            side=row["S"]
            if side not in ("Buy","Sell"):
                raise ValueError("Unknown trade side")
            if row.get("s", symbol) != symbol:
                raise DataIntegrityError("WebSocket trade row has wrong symbol")
            price,qty=Decimal(row["p"]),Decimal(row["v"])
            if price <= 0 or qty <= 0:
                raise DataIntegrityError("Invalid WebSocket trade price/quantity")
            saved = conn.execute("""INSERT INTO trades(symbol,ts,trade_id,price,qty,side,notional,source,raw)
             VALUES(%s,%s,%s,%s,%s,%s,%s,'websocket',%s::jsonb)
             ON CONFLICT(symbol,trade_id) DO UPDATE
             SET price=EXCLUDED.price, source='websocket'
             WHERE trades.ts=EXCLUDED.ts AND trades.price=EXCLUDED.price
             AND trades.qty=EXCLUDED.qty AND trades.side=EXCLUDED.side
             AND trades.notional=EXCLUDED.notional
             RETURNING trade_id""",
             (symbol,stamp_ms(row["T"]),row["i"],price,qty,side,price*qty,json.dumps(row))).fetchone()
            if saved is None:
                raise DataIntegrityError(f"Conflicting WS trade ID: {row['i']}")
    elif topic.startswith("orderbook."):
        row=data if isinstance(data,dict) else {}
        conn.execute("""INSERT INTO book_events(symbol,exchange_ts,update_id,sequence_no,
          event_type,bids,asks) VALUES(%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)""",
          (symbol,stamp_ms(payload["ts"]) if payload.get("ts") else None,
           row.get("u"),row.get("seq"),payload.get("type","unknown"),
           json.dumps(row.get("b",[])),json.dumps(row.get("a",[]))))

    elif topic.startswith('tickers.'):
        row=data if isinstance(data,dict) else {}
        def numeric(name):
            value=row.get(name)
            return Decimal(str(value)) if value not in (None,'') else None
        conn.execute('''INSERT INTO market_ticker_events(symbol,exchange_ts,event_type,
          mark_price,index_price,last_price,open_interest,funding_rate,bid_price,ask_price,raw)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)''',
          (symbol,stamp_ms(payload['ts']) if payload.get('ts') else None,
           payload.get('type','delta'),numeric('markPrice'),numeric('indexPrice'),
           numeric('lastPrice'),numeric('openInterest'),numeric('fundingRate'),
           numeric('bid1Price'),numeric('ask1Price'),json.dumps(row)))
    elif topic.startswith('allLiquidation.'):
        rows=data if isinstance(data,list) else [data]
        for row in rows:
            conn.execute('''INSERT INTO liquidation_events(symbol,ts,side,price,qty,raw)
              VALUES(%s,%s,%s,%s,%s,%s::jsonb)''',
              (symbol,stamp_ms(row['T']),row['S'],Decimal(row['p']),
               Decimal(row['v']),json.dumps(row)))


async def live_feed(settings):
    if settings.book_depth not in (1,50,200,1000):
        raise ValueError("Unsupported orderbook depth")
    topics=[f"publicTrade.{settings.symbol}",
            f"orderbook.{settings.book_depth}.{settings.symbol}",
            f"tickers.{settings.symbol}",f"allLiquidation.{settings.symbol}"]
    attempt=0
    with connection(settings) as conn:
        last=conn.execute("SELECT ts FROM trades WHERE symbol=%s AND source='websocket' ORDER BY ts DESC LIMIT 1",(settings.symbol,)).fetchone()
        pending=conn.execute("SELECT id FROM data_gaps WHERE symbol=%s AND source='publicTrade' AND end_ts IS NULL ORDER BY id DESC LIMIT 1",(settings.symbol,)).fetchone()
    last_trade=last['ts'] if last else None
    pending_id=pending['id'] if pending else None
    last_book_ts=None
    while True:
        connected_at=datetime.now(timezone.utc)
        try:
            async with websockets.connect(settings.bybit_ws,ping_interval=20,ping_timeout=15,
                                          max_size=8*1024*1024) as ws:
                await ws.send(json.dumps({"op":"subscribe","args":topics}))
                attempt=0
                book_ready=False
                book_seq=None
                book_update=None
                with connection(settings) as conn:
                    conn.execute("""INSERT INTO feed_health(symbol,topic,status)
                      VALUES(%s,'connection','connected')""",(settings.symbol,))
                async for raw in ws:
                    payload=json.loads(raw)
                    if payload.get("op")=="subscribe" and not payload.get("success"):
                        raise RuntimeError("WS subscription rejected")
                    topic=payload.get("topic","")
                    if topic.startswith("orderbook."):
                        if payload.get("type")=="snapshot":
                            book_ready=True
                        elif not book_ready:
                            raise DataIntegrityError("Orderbook delta before snapshot")
                        book_seq, book_update = validate_book_sequence(
                            book_seq, book_update, payload)
                        last_book_ts = (stamp_ms(payload["ts"])
                                        if payload.get("ts")
                                        else datetime.now(timezone.utc))
                    if topic.startswith(("publicTrade.","orderbook.","tickers.","allLiquidation.")):
                        with connection(settings) as conn:
                            ingest_ws(conn,settings.symbol,payload)
                            if topic.startswith('publicTrade.') and payload.get('data'):
                                stamps=[stamp_ms(x['T']) for x in payload['data']]
                                first=min(stamps)
                                latest=max(stamps)
                                unseen = [stamp for stamp in stamps
                                          if last_trade is None or stamp > last_trade]
                                if pending_id and unseen:
                                    conn.execute(
                                        'UPDATE data_gaps SET end_ts=%s WHERE id=%s AND end_ts IS NULL',
                                        (min(unseen),pending_id))
                                    pending_id=None
                                last_trade=max(last_trade,latest) if last_trade else latest
                raise ConnectionError("WebSocket stream closed without an exception")
        except Exception as exc:
            try:
                with connection(settings) as conn:
                    conn.execute("INSERT INTO feed_health(symbol,topic,status,detail) VALUES(%s,'connection','disconnected',%s::jsonb)",
                                 (settings.symbol,json.dumps({'reason':str(exc)[:400]})))
                    if pending_id is None:
                        start_gap=last_trade or connected_at
                        item=conn.execute("INSERT INTO data_gaps(source,symbol,start_ts,explanation) VALUES('publicTrade',%s,%s,'websocket interrupted') RETURNING id",
                                          (settings.symbol,start_gap)).fetchone()
                        pending_id=item['id']
                    if last_book_ts is not None:
                        outage_end=datetime.now(timezone.utc)
                        if outage_end > last_book_ts:
                            record_gap(
                                conn, settings.symbol, "orderbook",
                                int(last_book_ts.timestamp()*1000),
                                int(outage_end.timestamp()*1000),
                                "WebSocket L2 deltas lost; snapshot cannot restore "
                                "intermediate add/cancel events")
                        last_book_ts=None
            except Exception as log_error:
                print(f'Failed to persist outage: {log_error}',flush=True)
            attempt+=1
            await asyncio.sleep(min(120,2**min(7,attempt))+random.uniform(0,2))

async def live_rest_reconcile(settings):
    async with MarketClient(settings.bybit_rest,settings.rest_rps) as client:
        while True:
            try:
                for kind in KIND_ENDPOINTS:
                    for tf in INTERVAL_MS:
                        await collect_klines(settings,client,kind,tf)
                await collect_funding(settings,client)
                for interval in OI_INTERVALS:
                    await collect_oi(settings,client,interval)
                    await collect_account_ratios(settings,client,interval)
            except Exception as exc:
                with connection(settings) as conn:
                    conn.execute("""INSERT INTO feed_health(symbol,topic,status,detail)
                     VALUES(%s,'rest_reconcile','error',%s::jsonb)""",
                     (settings.symbol,json.dumps({"error":str(exc)[:400]})))
            await asyncio.sleep(120)

async def run_live(settings):
    # Keep WS ingestion independent from slow historical REST backfills.
    from .recent_recovery import run_recent_recovery
    await asyncio.gather(
        live_feed(settings), live_rest_reconcile(settings),
        run_recent_recovery(settings))
