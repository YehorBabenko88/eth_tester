import asyncio
from datetime import datetime,timezone
from decimal import Decimal
import json
import random
import websockets
from .bybit import MarketClient
from .database import connection
from .history import collect_klines,collect_funding,collect_oi,collect_account_ratios

def stamp_ms(value):
    return datetime.fromtimestamp(int(value)/1000,timezone.utc)

def ingest_ws(conn,symbol,payload):
    topic=payload.get("topic","")
    data=payload.get("data")
    if not topic or data is None:
        return
    if topic.startswith("publicTrade."):
        for row in data:
            side=row["S"]
            if side not in ("Buy","Sell"):
                raise ValueError("Unknown trade side")
            price,qty=Decimal(row["p"]),Decimal(row["v"])
            conn.execute("""INSERT INTO trades(symbol,ts,trade_id,price,qty,side,notional,source,raw)
             VALUES(%s,%s,%s,%s,%s,%s,%s,'websocket',%s::jsonb)
             ON CONFLICT(symbol,trade_id) DO NOTHING""",
             (symbol,stamp_ms(row["T"]),row["i"],price,qty,side,price*qty,json.dumps(row)))
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
    while True:
        try:
            async with websockets.connect(settings.bybit_ws,ping_interval=20,ping_timeout=15,
                                          max_size=8*1024*1024) as ws:
                await ws.send(json.dumps({"op":"subscribe","args":topics}))
                attempt=0
                book_ready=False
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
                            continue
                    if topic.startswith(("publicTrade.","orderbook.","tickers.","allLiquidation.")):
                        with connection(settings) as conn:
                            ingest_ws(conn,settings.symbol,payload)
                            if topic.startswith('publicTrade.') and payload.get('data'):
                                stamp=max(stamp_ms(x['T']) for x in payload['data'])
                                if pending_id:
                                    conn.execute('UPDATE data_gaps SET end_ts=%s WHERE id=%s AND end_ts IS NULL',(stamp,pending_id))
                                    pending_id=None
                                last_trade=stamp
        except Exception as exc:
            try:
                with connection(settings) as conn:
                    conn.execute("INSERT INTO feed_health(symbol,topic,status,detail) VALUES(%s,'connection','disconnected',%s::jsonb)",
                                 (settings.symbol,json.dumps({'reason':str(exc)[:400]})))
                    if last_trade and pending_id is None:
                        item=conn.execute("INSERT INTO data_gaps(source,symbol,start_ts,explanation) VALUES('publicTrade',%s,%s,'websocket interrupted') RETURNING id",
                                          (settings.symbol,last_trade)).fetchone()
                        pending_id=item['id']
            except Exception as log_error:
                print(f'Failed to persist outage: {log_error}',flush=True)
            attempt+=1
            await asyncio.sleep(min(120,2**min(7,attempt))+random.uniform(0,2))

async def live_rest_reconcile(settings):
    async with MarketClient(settings.bybit_rest,settings.rest_rps) as client:
        while True:
            try:
                for tf in ("1","5","60"):
                    await collect_klines(settings,client,"trade",tf)
                await collect_funding(settings,client)
                await collect_oi(settings,client,"5min")
                await collect_account_ratios(settings,client,"5min")
            except Exception as exc:
                with connection(settings) as conn:
                    conn.execute("""INSERT INTO feed_health(symbol,topic,status,detail)
                     VALUES(%s,'rest_reconcile','error',%s::jsonb)""",
                     (settings.symbol,json.dumps({"error":str(exc)[:400]})))
            await asyncio.sleep(120)

async def run_live(settings):
    await asyncio.gather(live_feed(settings),live_rest_reconcile(settings))
