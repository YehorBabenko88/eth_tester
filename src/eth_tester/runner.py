"""Paper-only signal engine, never exchange order routing."""
import asyncio
from datetime import datetime,timedelta,timezone
from decimal import Decimal
import json
from .database import connection
from .paper import open_paper,close_paper
from .strategy import price_signal
from .orderflow import confirm_rejection

def estimated_funding(conn,settings,opened,observed,qty,entry):
    rows=conn.execute("""SELECT ts,rate FROM funding WHERE symbol=%s AND
      ts>%s AND ts<=%s""",(settings.symbol,opened,observed)).fetchall()
    return sum((qty*entry*Decimal(x["rate"]) for x in rows),Decimal(0))

def engine_tick(settings):
    now=datetime.now(timezone.utc)
    with connection(settings) as conn:
        status=conn.execute("""SELECT status FROM feed_health WHERE symbol=%s
          AND topic='connection' ORDER BY ts DESC LIMIT 1""",(settings.symbol,)).fetchone()
        recent=conn.execute("""SELECT ts,price FROM trades WHERE symbol=%s
          AND ts>=%s AND ts<=%s AND source='websocket'
          ORDER BY ts DESC,trade_id DESC LIMIT 1""",
          (settings.symbol,now-timedelta(seconds=15),now+timedelta(seconds=2))).fetchone()
        if not recent or not status or status["status"]!="connected":
            return "waiting_live_feed"
        trade=conn.execute("SELECT * FROM paper_trades WHERE state='open' LIMIT 1").fetchone()
        if trade:
            price=Decimal(recent["price"])
            stop,take=Decimal(trade["stop"]),Decimal(trade["take"])
            if trade["direction"]=="long":
                reason="stop" if price<=stop else "take" if price>=take else None
            else:
                reason="stop" if price>=stop else "take" if price<=take else None
            if not reason:
                return "monitoring_position"
            funding=estimated_funding(conn,settings,trade["opened_at"],
                                       recent["ts"],trade["qty"],trade["entry"])
        else:
            enabled=conn.execute("SELECT enabled,paused FROM bot_control WHERE singleton=true").fetchone()
            if not enabled or not enabled["enabled"] or enabled["paused"]:
                return "paused"
            missing=conn.execute("SELECT 1 FROM data_gaps WHERE symbol=%s AND source='publicTrade' AND resolved_at IS NULL LIMIT 1",(settings.symbol,)).fetchone()
            if missing:
                return "blocked_unresolved_tick_gap"
            bars=conn.execute("""SELECT ts,open,high,low,close,volume FROM candles
              WHERE symbol=%s AND kind='trade' AND tf='1' AND closed
              AND ts<=%s ORDER BY ts DESC LIMIT 180""",
              (settings.symbol,now-timedelta(minutes=1))).fetchall()
            bars.reverse()
            if len(bars)<30 or now-bars[-1]["ts"]>timedelta(minutes=5):
                return "stale_or_insufficient_candles"
            last=bars[-1]["ts"]
            exists=conn.execute("""SELECT 1 FROM strategy_signals WHERE
              symbol=%s AND tf='1' AND bar_ts=%s AND version='pivot_rejection_v1'""",
              (settings.symbol,last)).fetchone()
            if exists:
                return "already_evaluated"
            signal=price_signal(bars,settings.taker_fee_rate,settings.slippage_bps)
            flow_ok,flow=(confirm_rejection(conn,settings,signal,bars[-1])
                          if signal else (False,{}))
            if signal:
                signal.reason['orderflow']=flow
            conn.execute("""INSERT INTO strategy_signals(symbol,tf,bar_ts,version,decision,reason)
              VALUES(%s,'1',%s,'pivot_rejection_v1',%s,%s::jsonb) ON CONFLICT DO NOTHING""",
              (settings.symbol,last,("candidate" if flow_ok else "rejected_orderflow") if signal else "no_signal",
               json.dumps(signal.reason if signal else {})))
            if not signal:
                return "no_signal"
            if not flow_ok:
                return "rejected_orderflow"
            if abs(Decimal(recent["price"])/Decimal(bars[-1]["close"])-1)>Decimal("0.005"):
                return "signal_expired_price_gap"
    if trade:
        return "closed_"+str(close_paper(settings,recent["price"],reason,funding))
    try:
        return "opened_"+str(open_paper(settings,signal.direction,recent["price"],
                                        signal.stop,signal.take,signal.reason))
    except (RuntimeError,ValueError) as exc:
        return "rejected_"+str(exc)

async def run_engine(settings):
    while True:
        try:
            state=await asyncio.to_thread(engine_tick,settings)
            with connection(settings) as conn:
                conn.execute("""INSERT INTO bot_heartbeat(component,status)
                VALUES('paper_engine',%s) ON CONFLICT(component) DO UPDATE SET
                status=EXCLUDED.status,updated_at=now()""",(state[:250],))
        except Exception as exc:
            with connection(settings) as conn:
                conn.execute("""INSERT INTO bot_events(severity,event,data)
                  VALUES('error','engine_failure',%s::jsonb)""",
                  (json.dumps({"error":str(exc)[:500]}),))
        await asyncio.sleep(15)
