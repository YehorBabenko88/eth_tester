from datetime import datetime,timezone
from decimal import Decimal
import json
from .bybit import MarketClient
from .database import connection,cursor_ts,save_cursor
from .data_quality import FIXED_MS
from .history_integrity import collect_klines

UTC=timezone.utc
INTERVAL_MS = {**FIXED_MS, "M": None}  # calendar months are not fixed durations
KIND_ENDPOINTS={"trade":"kline","mark":"mark-price-kline",
 "index":"index-price-kline","premium":"premium-index-price-kline"}
OI_INTERVALS=("5min","15min","30min","1h","4h","1d")

def dtms(ms):
    return datetime.fromtimestamp(int(ms)/1000,UTC)
def toms(dt):
    return int(dt.timestamp()*1000)

async def collect_funding(settings,client):
    start=datetime.combine(settings.archive_start,datetime.min.time(),UTC)
    with connection(settings) as conn:
        previous=cursor_ts(conn,"funding",settings.symbol,"")
    pos=toms(previous or start)
    now=toms(datetime.now(UTC))
    while pos<now:
        end=min(now-1,pos+30*86400000-1)
        while True:
            data=await client.get("funding/history",category=settings.category,symbol=settings.symbol,
                                   startTime=pos,endTime=end,limit=200)
            items=data.get("list",[])
            if len(items)<200:
                break
            if end-pos<60000:
                raise RuntimeError("Funding page still saturated at 1-minute window")
            end=pos+max(1,(end-pos)//2)
        with connection(settings) as conn:
            for row in items:
                ts=int(row["fundingRateTimestamp"])
                if pos<=ts<=end:
                    conn.execute("INSERT INTO funding(symbol,ts,rate) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
                                 (settings.symbol,dtms(ts),Decimal(row["fundingRate"])))
            save_cursor(conn,"funding",settings.symbol,"",dtms(end+1))
        pos=end+1

async def collect_oi(settings,client,interval):
    start=datetime.combine(settings.archive_start,datetime.min.time(),UTC)
    with connection(settings) as conn:
        previous=cursor_ts(conn,"oi",settings.symbol,interval)
    pos=toms(previous or start)
    now=toms(datetime.now(UTC))
    while pos<now:
        end=min(now-1,pos+7*86400000-1)
        cursor=None
        seen=set()
        with connection(settings) as conn:
            while True:
                data=await client.get("open-interest",category=settings.category,symbol=settings.symbol,
                    intervalTime=interval,startTime=pos,endTime=end,limit=200,cursor=cursor)
                for item in data.get("list",[]):
                    stamp=int(item["timestamp"])
                    if pos<=stamp<=end:
                        conn.execute("""INSERT INTO open_interest(symbol,interval,ts,value)
                           VALUES(%s,%s,%s,%s) ON CONFLICT(symbol,interval,ts)
                           DO UPDATE SET value=EXCLUDED.value""",
                           (settings.symbol,interval,dtms(stamp),Decimal(item["openInterest"])))
                cursor=data.get("nextPageCursor")
                if not cursor:
                    break
                if cursor in seen:
                    raise RuntimeError("Repeated OI page cursor")
                seen.add(cursor)
            save_cursor(conn,"oi",settings.symbol,interval,dtms(end+1))
        pos=end+1

async def collect_account_ratios(settings,client,period):
    start=datetime.combine(settings.archive_start,datetime.min.time(),UTC)
    with connection(settings) as conn:
        previous=cursor_ts(conn,'account_ratio',settings.symbol,period)
    pos=toms(previous or start)
    now=toms(datetime.now(UTC))
    while pos<now:
        end=min(now-1,pos+7*86400000-1)
        cursor=None
        seen=set()
        with connection(settings) as conn:
            while True:
                data=await client.get('account-ratio',category=settings.category,symbol=settings.symbol,
                                      period=period,startTime=pos,endTime=end,limit=500,cursor=cursor)
                for item in data.get('list',[]):
                    stamp=int(item['timestamp'])
                    if pos<=stamp<=end:
                        conn.execute('''INSERT INTO ratios(symbol,period,ts,buy_ratio,sell_ratio,raw)
                        VALUES(%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT(symbol,period,ts)
                        DO UPDATE SET buy_ratio=EXCLUDED.buy_ratio,sell_ratio=EXCLUDED.sell_ratio''',
                        (settings.symbol,period,dtms(stamp),Decimal(item['buyRatio']),
                         Decimal(item['sellRatio']),json.dumps(item)))
                next_cursor=data.get('nextPageCursor')
                if not next_cursor:
                    if len(data.get('list',[]))>=500:
                        raise RuntimeError('Account-ratio page at limit without cursor')
                    break
                if next_cursor in seen:
                    raise RuntimeError('Repeated account-ratio cursor')
                seen.add(next_cursor)
                cursor=next_cursor
            save_cursor(conn,'account_ratio',settings.symbol,period,dtms(end+1))
        pos=end+1

async def backfill_rest(settings):
    async with MarketClient(settings.bybit_rest,settings.rest_rps) as client:
        instrument=await client.instrument(settings.symbol,settings.category)
        with connection(settings) as conn:
            conn.execute("""INSERT INTO instrument_metadata(symbol,category,payload)
             VALUES(%s,%s,%s::jsonb) ON CONFLICT(symbol)
             DO UPDATE SET payload=EXCLUDED.payload,updated_at=now()""",
             (settings.symbol,settings.category,json.dumps(instrument)))
        errors=[]
        for kind in KIND_ENDPOINTS:
            for tf in INTERVAL_MS:
                try:
                    await collect_klines(settings,client,kind,tf)
                    print(f"completed {kind} {tf}",flush=True)
                except Exception as exc:
                    errors.append((kind,tf,str(exc)))
                    print(f"failed {kind} {tf}: {exc}",flush=True)
        try:
            await collect_funding(settings,client)
        except Exception as exc:
            errors.append(("funding","",str(exc)))
        for interval in OI_INTERVALS:
            try:
                await collect_oi(settings,client,interval)
            except Exception as exc:
                errors.append(("oi",interval,str(exc)))
        for period in OI_INTERVALS:
            try:
                await collect_account_ratios(settings,client,period)
            except Exception as exc:
                errors.append(('account_ratio',period,str(exc)))
        if errors:
            raise RuntimeError(f"Incomplete backfill: {errors}")

