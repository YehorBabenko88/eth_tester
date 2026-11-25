from datetime import datetime,timedelta,timezone
from decimal import Decimal
from .database import connection

TF_SECONDS={"1":60,"3":180,"5":300,"15":900,"30":1800,
 "60":3600,"120":7200,"240":14400,"360":21600,"720":43200,"D":86400}

def floor_time(ts,seconds):
    return datetime.fromtimestamp(int(ts.timestamp())//seconds*seconds,timezone.utc)

def poc(price_volume):
    if not price_volume:
        return None
    return min(price_volume,key=lambda price:(-price_volume[price],price))

def cluster(trades):
    bins={}
    buys=sells=Decimal(0)
    count=0
    for row in trades:
        price,qty,side=Decimal(row["price"]),Decimal(row["qty"]),row["side"]
        bins.setdefault(price,{"buy":Decimal(0),"sell":Decimal(0),"n":0})[side.lower()]+=qty
        bins[price]["n"]+=1
        count+=1
        if side=="Buy":
            buys+=qty
        else:
            sells+=qty
    return {"poc":poc({p:v["buy"]+v["sell"] for p,v in bins.items()}),
      "delta":buys-sells,"volume":buys+sells,"tps":count,"levels":bins}

def confirmed_pivots(candles,flank=2):
    levels=[]
    for i in range(flank,len(candles)-flank):
        bar=candles[i]
        others=candles[i-flank:i]+candles[i+1:i+flank+1]
        if all(bar["high"]>x["high"] for x in others):
            levels.append(("resistance",bar["ts"],bar["high"],candles[i+flank]["ts"]))
        if all(bar["low"]<x["low"] for x in others):
            levels.append(("support",bar["ts"],bar["low"],candles[i+flank]["ts"]))
    return levels

def classify_levels(candles,flank=2):
    result=[]
    for side,origin,price,confirmed in confirmed_pivots(candles,flank):
        broken=next((row["ts"] for row in candles if row["ts"]>confirmed and
         (row["close"]>price if side=="resistance" else row["close"]<price)),None)
        result.append((side,origin,price,confirmed,broken))
    return result

def build_levels(settings,tf="60",flank=2):
    with connection(settings) as conn:
        candles=conn.execute("""SELECT ts,high,low,close FROM candles WHERE
          symbol=%s AND kind='trade' AND tf=%s AND closed ORDER BY ts""",
          (settings.symbol,tf)).fetchall()
        levels=classify_levels(candles,flank)
        for side,origin,price,_,broken in levels:
            conn.execute("""INSERT INTO levels(symbol,tf,side,origin_ts,level_price,status,broken_ts,source)
             VALUES(%s,%s,%s,%s,%s,%s,%s,'confirmed_pivot')
             ON CONFLICT(symbol,tf,side,origin_ts,level_price)
             DO UPDATE SET status=EXCLUDED.status,broken_ts=EXCLUDED.broken_ts""",
             (settings.symbol,tf,side,origin,price,"broken" if broken else "unbroken",broken))
    return len(levels)

def build_footprint(settings,tf="1",hours=24):
    if tf not in TF_SECONDS:
        raise ValueError("Only fixed-size periods supported")
    span=TF_SECONDS[tf]
    now=datetime.now(timezone.utc)
    with connection(settings) as conn:
        rows=conn.execute("""SELECT ts,price,qty,side FROM trades WHERE
         symbol=%s AND ts >= %s AND ts < %s ORDER BY ts,trade_id""",
         (settings.symbol,now-timedelta(hours=hours),now)).fetchall()
        buckets={}
        for r in rows:
            bucket=floor_time(r["ts"],span)
            values=buckets.setdefault(bucket,{}).setdefault(r["price"],
                {"buy":Decimal(0),"sell":Decimal(0),"trades":0})
            values[r["side"].lower()]+=r["qty"]
            values["trades"]+=1
        for bucket,bins in buckets.items():
            for price,values in bins.items():
                conn.execute("""INSERT INTO footprint(symbol,tf,bucket,price,buy_qty,sell_qty,
                 buy_notional,sell_notional,trades) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                 ON CONFLICT(symbol,tf,bucket,price) DO UPDATE SET
                 buy_qty=EXCLUDED.buy_qty,sell_qty=EXCLUDED.sell_qty,
                 buy_notional=EXCLUDED.buy_notional,sell_notional=EXCLUDED.sell_notional,
                 trades=EXCLUDED.trades""",
                 (settings.symbol,tf,bucket,price,values["buy"],values["sell"],
                  values["buy"]*price,values["sell"]*price,values["trades"]))
            buy=sum((v["buy"] for v in bins.values()),Decimal(0))
            sell=sum((v["sell"] for v in bins.values()),Decimal(0))
            peak=poc({p:v["buy"]+v["sell"] for p,v in bins.items()})
            conn.execute("""INSERT INTO features(symbol,tf,ts,poc,delta,volume,tape_tps,quality)
              VALUES(%s,%s,%s,%s,%s,%s,%s,'partial_unverified')
              ON CONFLICT(symbol,tf,ts) DO UPDATE SET poc=EXCLUDED.poc,
              delta=EXCLUDED.delta,volume=EXCLUDED.volume,
              tape_tps=EXCLUDED.tape_tps,quality=EXCLUDED.quality""",
              (settings.symbol,tf,bucket,peak,buy-sell,buy+sell,
               sum(v["trades"] for v in bins.values())/span))
    return len(buckets)


def enrich_features(settings,tf="1",hours=24):
    """Windowed CVD and quality-flagged OI, funding, book snapshot and tick volume."""
    if tf not in TF_SECONDS:
        raise ValueError("Unsupported interval")
    with connection(settings) as conn:
        rows=conn.execute("""SELECT ts,delta,volume FROM features WHERE symbol=%s AND
          tf=%s AND ts>=now()-(%s || ' hours')::interval ORDER BY ts""",
          (settings.symbol,tf,hours)).fetchall()
        cvd=Decimal(0)
        for item in rows:
            ts=item["ts"]
            cvd+=item["delta"] or Decimal(0)
            candle=conn.execute("""SELECT volume FROM candles WHERE symbol=%s
              AND kind='trade' AND tf=%s AND ts=%s AND closed""",
              (settings.symbol,tf,ts)).fetchone()
            oi=conn.execute("""SELECT value FROM open_interest WHERE symbol=%s
              AND interval='5min' AND ts<=%s ORDER BY ts DESC LIMIT 1""",
              (settings.symbol,ts)).fetchone()
            funding=conn.execute("""SELECT rate FROM funding WHERE symbol=%s
              AND ts<=%s ORDER BY ts DESC LIMIT 1""",
              (settings.symbol,ts)).fetchone()
            book=conn.execute("""SELECT bids,asks FROM book_events WHERE symbol=%s
              AND event_type='snapshot' AND recv_ts<=%s ORDER BY recv_ts DESC LIMIT 1""",
              (settings.symbol,ts)).fetchone()
            imbalance=None
            if book:
                b=sum((Decimal(str(x[1])) for x in book["bids"]),Decimal(0))
                a=sum((Decimal(str(x[1])) for x in book["asks"]),Decimal(0))
                if b+a:
                    imbalance=(b-a)/(b+a)
            archive=conn.execute("""SELECT state FROM ingestion_job WHERE
              source='trades_archive' AND symbol=%s AND day=%s""",
              (settings.symbol,ts.date())).fetchone()
            quality="partial_unverified"
            if archive and archive["state"]=="complete" and candle and candle["volume"] is not None:
                err=abs(Decimal(item["volume"])-Decimal(candle["volume"]))
                tolerance=max(Decimal("0.001"),Decimal(candle["volume"])*Decimal("0.0005"))
                quality="verified_volume" if err<=tolerance else "volume_mismatch"
            conn.execute("""UPDATE features SET cvd=%s,oi=%s,funding_rate=%s,imbalance=%s,
              quality=%s,metadata=%s::jsonb WHERE symbol=%s AND tf=%s AND ts=%s""",
              (cvd,oi["value"] if oi else None,funding["rate"] if funding else None,
               imbalance,quality,'{"cvd_baseline":"window_start","orderbook":"last_snapshot"}',
               settings.symbol,tf,ts))
    return len(rows)
