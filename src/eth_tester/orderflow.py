"""Causal confirmation of closed-candle pivot signals using recorded market flow."""
from datetime import datetime,timedelta,timezone
from decimal import Decimal

def confirm_rejection(conn,settings,signal,bar):
    start=bar["ts"]
    end=start+timedelta(minutes=1)
    sample=conn.execute("""SELECT count(*) AS n,
      coalesce(sum(qty) FILTER(WHERE side='Buy'),0) AS buys,
      coalesce(sum(qty) FILTER(WHERE side='Sell'),0) AS sells
      FROM trades WHERE symbol=%s AND ts>=%s AND ts<%s""",
      (settings.symbol,start,end)).fetchone()
    earlier=conn.execute("""SELECT count(*) AS n FROM trades WHERE symbol=%s
      AND ts>=%s AND ts<%s""",
      (settings.symbol,start-timedelta(minutes=5),start)).fetchone()
    if sample["n"]<20 or not earlier["n"]:
        return False,{"reason":"insufficient_verified_tick_window","count":sample["n"]}
    buys,sells=Decimal(sample["buys"]),Decimal(sample["sells"])
    absorbed=(sells>buys if signal.direction=="long" else buys>sells)
    speed=Decimal(sample["n"]*5)/Decimal(earlier["n"])
    fast=speed>=Decimal("1.15")
    oi=conn.execute("""SELECT ts,value FROM open_interest WHERE symbol=%s
      AND interval='5min' AND ts<=%s AND ts>=%s ORDER BY ts DESC LIMIT 2""",
      (settings.symbol,end,start-timedelta(minutes=30))).fetchall()
    oi_change=(Decimal(oi[0]["value"])/Decimal(oi[1]["value"])-1
               if len(oi)==2 and oi[1]["value"] else None)
    oi_build=oi_change is not None and oi_change>Decimal("0.001")
    book=conn.execute("""SELECT bids,asks,updated_at,valid FROM market_book_state
      WHERE symbol=%s""",(settings.symbol,)).fetchone()
    imbalance=None
    if book and book["valid"] and timedelta(0)<=datetime.now(timezone.utc)-book["updated_at"]<timedelta(minutes=4):
        bid=sum((Decimal(x) for x in book["bids"].values()),Decimal(0))
        ask=sum((Decimal(x) for x in book["asks"].values()),Decimal(0))
        if bid+ask:
            imbalance=(bid-ask)/(bid+ask)
    book_agrees=(imbalance is not None and
      (imbalance>Decimal("0.1") if signal.direction=="long" else imbalance<Decimal("-0.1")))
    score=sum((absorbed,fast,oi_build,book_agrees))
    evidence={"source":"public_market_data","prints":sample["n"],
      "delta_qty":str(buys-sells),"tape_speed_ratio":str(speed),
      "open_interest_change":str(oi_change) if oi_change is not None else None,
      "book_imbalance":str(imbalance) if imbalance is not None else None,
      "absorption_proxy":absorbed,"tape_acceleration":fast,
      "open_interest_building":oi_build,"orderbook_agrees":book_agrees,
      "score":score,"minimum":2,
      "warning":"Orderbook reductions are not uniquely attributable to executions"}
    return score>=2,evidence
