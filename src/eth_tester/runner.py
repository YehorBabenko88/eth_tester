"""Paper-only signal engine, never exchange order routing."""
import asyncio
from datetime import datetime,timedelta,timezone
from decimal import Decimal
import json
from .database import connection
from .paper import open_paper,close_paper
from .strategy import price_signal
from .orderflow import confirm_rejection
from .simulation.crossing import resolve_first_crossing
from .simulation.identity import (bind_snapshot_reference, require_new_strategy_identity,
                                  rule_identity)

def estimated_funding(conn,settings,opened,observed,qty,entry,direction,*,with_events=False):
    """Never synthesize funding from entry price: require event-time mark evidence.

    Historical funding chronology may still be incomplete: a returned 'marked'
    result is not proof all funding events were supplied.
    """
    rows=conn.execute("""SELECT f.ts,f.rate,
      (SELECT m.mark_price FROM market_ticker_events m
         WHERE m.symbol=f.symbol AND m.exchange_ts<=f.ts
         AND m.exchange_ts>=f.ts-interval '1 second'
         AND m.mark_price>0 ORDER BY m.exchange_ts DESC LIMIT 1) AS mark_price
      FROM funding f WHERE f.symbol=%s AND f.ts>%s AND f.ts<=%s
      ORDER BY f.ts""",(settings.symbol,opened,observed)).fetchall()
    if not rows or any(row["mark_price"] is None for row in rows):
        return (Decimal(0),"unverified",[]) if with_events else (Decimal(0),"unverified")
    sign=Decimal(1) if direction=="long" else Decimal(-1)
    amount=sign*sum((Decimal(qty)*Decimal(row["mark_price"])*Decimal(row["rate"])
                     for row in rows),Decimal(0))
    return (amount,"marked_not_completeness_proven",rows) if with_events else (
        amount,"marked_not_completeness_proven")

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
            stop,take=Decimal(trade["stop"]),Decimal(trade["take"])
            gap=conn.execute("""SELECT 1 FROM data_gaps WHERE symbol=%s
              AND source='publicTrade' AND resolved_at IS NULL
              AND start_ts<=%s AND (end_ts IS NULL OR end_ts>=%s) LIMIT 1""",
              (settings.symbol,recent["ts"],trade["opened_at"])).fetchone()
            if gap:
                return "blocked_unresolved_tick_gap"
            lower,upper=min(stop,take),max(stop,take)
            hit=conn.execute("""SELECT ts,
              min(price) FILTER (WHERE price<=%s) AS low_cross,
              max(price) FILTER (WHERE price>=%s) AS high_cross
              FROM trades WHERE symbol=%s AND source='websocket'
              AND ts>%s AND ts<=%s AND (price<=%s OR price>=%s)
              GROUP BY ts ORDER BY ts LIMIT 1""",
              (lower,upper,settings.symbol,trade["opened_at"],recent["ts"],
               lower,upper)).fetchone()
            crossing=resolve_first_crossing(hit,trade["direction"],stop,take)
            if crossing is None:
                return "monitoring_position"
            recent={"ts":crossing["ts"],"price":crossing["price"]}
            reason=crossing["reason"]
            if crossing["ambiguous_same_timestamp"]:
                reason+="_same_timestamp_ambiguous_stop_first"
            funding,funding_quality,funding_evidence=estimated_funding(
                conn,settings,trade["opened_at"],recent["ts"],
                trade["qty"],trade["entry"],trade["direction"],with_events=True)
            if funding_quality!="marked_not_completeness_proven":
                reason+="_funding_unverified"
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
            if any(b["ts"]-a["ts"]!=timedelta(minutes=1) for a,b in zip(bars,bars[1:])):
                return "blocked_candle_gap"
            last=bars[-1]["ts"]
            engine_version,engine_metadata=rule_identity()
            exists=conn.execute("""SELECT 1 FROM strategy_signals WHERE
              symbol=%s AND tf='1' AND bar_ts=%s AND version=%s""",
              (settings.symbol,last,engine_version)).fetchone()
            if exists:
                return "already_evaluated"
            signal=price_signal(bars,settings.taker_fee_rate,settings.slippage_bps)
            invalid_identity=False
            invalid_snapshot=False
            decision_at=None
            if signal:
                try:
                    require_new_strategy_identity(signal.reason)
                except ValueError:
                    invalid_identity=True
                    flow_ok,flow=False,{"reason":"invalid_strategy_identity","admissible":False}
                else:
                    flow_ok,flow=confirm_rejection(conn,settings,signal,bars[-1],
                                                   observed_at=now)
                signal.reason['orderflow']=flow
                if flow_ok:
                    # Actual DB decision clock, never backdate to a public trade.
                    decision_at=conn.execute(
                        "SELECT clock_timestamp() AS ts").fetchone()["ts"]
                    try:
                        bound=bind_snapshot_reference(
                            signal.reason,decision_at=decision_at,bar_ts=last)
                        # Market print timestamp is price evidence, NOT decision time.
                        bound['market_trade_ts']=recent['ts'].isoformat()
                        if recent['ts']>decision_at:
                            raise ValueError('future trade print')
                        signal.reason.update(bound)
                    except (ValueError,TypeError,KeyError,AttributeError):
                        invalid_snapshot=True
                        flow_ok=False
            else:
                flow_ok,flow=False,{}
            payload=dict(signal.reason) if signal else {}
            if signal is None:
                payload.update({
                    "strategy_version":engine_version,
                    "rules_version":engine_metadata["parameters"]["version"],
                    "parameters":engine_metadata["parameters"],
                    "parameter_sha256":engine_metadata["parameter_sha256"],
                })
            payload.update({
                "engine_identity":engine_version,
                "engine_parameters":engine_metadata["parameters"],
                "engine_parameter_sha256":engine_metadata["parameter_sha256"],
            })
            decision=("no_signal" if signal is None else
                      "rejected_strategy_identity" if invalid_identity else
                      "rejected_snapshot_binding" if invalid_snapshot else
                      "candidate" if flow_ok else "rejected_orderflow")
            conn.execute("""INSERT INTO strategy_signals(symbol,tf,bar_ts,version,decision,reason)
              VALUES(%s,'1',%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING""",
              (settings.symbol,last,engine_version,decision,json.dumps(payload)))
            if not signal:
                return "no_signal"
            if invalid_identity:
                return "rejected_strategy_identity"
            if invalid_snapshot:
                return "rejected_snapshot_binding"
            if not flow_ok:
                return "rejected_orderflow"
            if abs(Decimal(recent["price"])/Decimal(bars[-1]["close"])-1)>Decimal("0.005"):
                return "signal_expired_price_gap"
    if trade:
        identifier=close_paper(settings,recent["price"],reason,funding,recent["ts"],trade["id"],
                               funding_evidence=funding_evidence)
        if identifier:
            try:
                from .charts import render_trade
                render_trade(settings,identifier)
            except Exception as exc:
                with connection(settings) as conn:
                    conn.execute("""INSERT INTO bot_events(severity,event,data)
                        VALUES('warning','exit_chart_failed',%s::jsonb)""",
                        (json.dumps({"trade":identifier,"error":str(exc)[:400]}),))
        return "closed_"+str(identifier)
    try:
        identifier=open_paper(settings,signal.direction,recent["price"],
                              signal.stop,signal.take,signal.reason,observed_at=decision_at)
        try:
            from .charts import render_trade
            render_trade(settings,identifier)
        except Exception as exc:
            with connection(settings) as conn:
                conn.execute("""INSERT INTO bot_events(severity,event,data)
                    VALUES('warning','entry_chart_failed',%s::jsonb)""",
                    (json.dumps({"trade":identifier,"error":str(exc)[:400]}),))
        return "opened_"+str(identifier)
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
