"""Live-only confirmation; missing evidence never becomes positive evidence."""
from datetime import datetime,timedelta,timezone
from decimal import Decimal as D
from .strategies import Parameters
from .strategy import economics

def confirm_rejection(conn,settings,signal,bar):
    start=bar["ts"]
    end=start+timedelta(minutes=1)
    now=datetime.now(timezone.utc)
    symbol=settings.symbol
    def reject(reason):
        return False,{"reason":reason,"score":0}
    if not timedelta(0)<=now-end<=timedelta(seconds=90):
        return reject("stale_or_open_bar")
    rows=conn.execute("""SELECT price, sum(qty) FILTER(WHERE side='Buy') AS buys,
      sum(qty) FILTER(WHERE side='Sell') AS sells,count(*) AS n
      FROM trades WHERE symbol=%s AND ts>=%s AND ts<%s GROUP BY price""",
      (symbol,start,end)).fetchall()
    buys=sum((D(r["buys"] or 0) for r in rows),D(0))
    sells=sum((D(r["sells"] or 0) for r in rows),D(0))
    n=sum(r["n"] for r in rows)
    volume=buys+sells
    if n<20 or not volume or bar.get("volume") is None or abs(volume-D(bar["volume"]))>max(D("0.001"),D(bar["volume"])*D("0.0005")):
        return reject("tick_volume_unverified")
    earlier=conn.execute("""SELECT count(*) AS n,coalesce(sum(qty) FILTER(WHERE side='Buy'),0)-
      coalesce(sum(qty) FILTER(WHERE side='Sell'),0) AS delta FROM trades
      WHERE symbol=%s AND ts>=%s AND ts<%s""",(symbol,start-timedelta(minutes=5),start)).fetchone()
    if not earlier["n"]:
        return reject("missing_tape_baseline")
    oi=conn.execute("""SELECT ts,value FROM open_interest WHERE symbol=%s AND interval='5min'
      AND ts<=%s ORDER BY ts DESC LIMIT 2""",(symbol,end)).fetchall()
    if len(oi)!=2 or oi[0]["ts"]-oi[1]["ts"]!=timedelta(minutes=5) or end-oi[0]["ts"]>timedelta(minutes=10) or min(r["value"] for r in oi)<=0:
        return reject("stale_or_gapped_oi")
    book=conn.execute("SELECT bids,asks,updated_at,valid FROM market_book_state WHERE symbol=%s",(symbol,)).fetchone()
    if not book or not book["valid"] or not timedelta(0)<=now-book["updated_at"]<=timedelta(seconds=10):
        return reject("stale_book")
    bid=sum((D(x) for x in book["bids"].values()),D(0))
    ask=sum((D(x) for x in book["asks"].values()),D(0))
    if min(bid,ask)<=0:
        return reject("empty_book")
    sign=1 if signal.direction=="long" else -1
    delta=(buys-sells)/volume
    speed=D(n*5)/D(earlier["n"])
    imbalance=(bid-ask)/(bid+ask)
    change=D(oi[0]["value"])/D(oi[1]["value"])-1
    trend=[]
    for tf,minutes in (("5",5),("15",15),("60",60)):
        span=timedelta(minutes=minutes)
        bars=conn.execute("""SELECT ts,close FROM candles WHERE symbol=%s AND kind='trade'
          AND tf=%s AND closed AND ts<=%s ORDER BY ts DESC LIMIT 3""",(symbol,tf,end-span)).fetchall()
        if len(bars)!=3 or end-bars[0]["ts"]>2*span or any(a["ts"]-b["ts"]!=span for a,b in zip(bars,bars[1:])):
            return reject("missing_multitimeframe_"+tf)
        trend.append(sign*(D(bars[0]["close"])-D(bars[-1]["close"]))>0)
    poc=max(rows,key=lambda r:(D(r["buys"] or 0)+D(r["sells"] or 0),-D(r["price"])))["price"]
    near=[r for r in rows if abs(D(r["price"])-signal.level)<=D(bar["close"])*D("0.001")]
    local=sum((D(r["buys"] or 0)-D(r["sells"] or 0) for r in near),D(0))
    reversal=signal.reason.get("strategy") in ("rejection","false_breakout")
    absorption=reversal and sign*delta<D("-0.15") and sign*local<0
    cvd=D(earlier["delta"])+buys-sells
    liq=conn.execute("""SELECT side,coalesce(sum(qty),0) AS qty FROM liquidation_events
      WHERE symbol=%s AND ts>=%s AND ts<%s GROUP BY side""",(symbol,start,end)).fetchall()
    liquidation={r["side"]:str(r["qty"]) for r in liq}
    checks={"directional_flow_or_absorption":(absorption if reversal else sign*delta>D("0.1")),
            "tape_acceleration":speed>=D("1.15"),"oi_building":change>D("0.001"),
            "book_agrees":sign*imbalance>D("0.1"),"multitimeframe":sum(trend)>=2,
            "cvd_agrees":sign*cvd>0,"footprint_near_level":bool(near)}
    weights=(25,10,10,15,20,10,10)
    score=sum(w for w,v in zip(weights,checks.values()) if v)
    recent=conn.execute("""SELECT price,ts FROM trades WHERE symbol=%s AND source='websocket'
      AND ts<=%s ORDER BY ts DESC,trade_id DESC LIMIT 1""",(symbol,now)).fetchone()
    if not recent or now-recent["ts"]>timedelta(seconds=10):
        return reject("stale_entry")
    costs=economics(signal.direction,D(recent["price"]),signal.stop,signal.take,
                    settings.taker_fee_rate,settings.slippage_bps)
    accepted=score>=Parameters().min_score and checks["directional_flow_or_absorption"] and checks["multitimeframe"] and costs is not None
    return accepted,{"score":score,"minimum":Parameters().min_score,"checks":checks,
      "delta_fraction":str(delta),"cvd_6m":str(cvd),"tape_speed_ratio":str(speed),
      "oi_change":str(change),"book_imbalance":str(imbalance),"poc":str(poc),
      "local_delta":str(local),"absorption_proxy":absorption,"liquidations_observed":liquidation,
      "economics_at_observed_entry":costs,"reason":"accepted" if accepted else "confirmation_or_cost_gate",
      "warning":"Absorption is a proxy; liquidation coverage is not proven; score is not a probability."}
