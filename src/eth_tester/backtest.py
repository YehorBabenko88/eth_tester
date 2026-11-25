"""Causal OHLC backtest; enters next bar, pessimistic stop-first fills."""
from decimal import Decimal
import json
from .database import connection
from .paper import estimate_trade
from .strategy import price_signal

def simulate(candles,fees=Decimal("0.00055"),slip=Decimal("2"),
             notional=Decimal(1000),funding_events=None):
    events=funding_events or []
    deals=[]
    i=20
    while i<len(candles)-1:
        signal=price_signal(candles[:i],fees,slip)
        if signal is None:
            i+=1
            continue
        start=candles[i]
        entry=Decimal(start["open"])
        if entry<=0 or (entry<=signal.stop or entry>=signal.take if signal.direction=="long"
                        else entry>=signal.stop or entry<=signal.take):
            i+=1
            continue
        j=i
        while j<len(candles):
            bar=candles[j]
            low,high=Decimal(bar["low"]),Decimal(bar["high"])
            stop_hit=low<=signal.stop if signal.direction=="long" else high>=signal.stop
            take_hit=high>=signal.take if signal.direction=="long" else low<=signal.take
            if stop_hit or take_hit:
                exit_price=signal.stop if stop_hit else signal.take
                opening=Decimal(bar["open"])
                if stop_hit:
                    exit_price=(min(exit_price,opening) if signal.direction=="long"
                                else max(exit_price,opening))
                outcome="stop" if stop_hit else "take"
                break
            j+=1
        else:
            break
        paid=sum((notional*Decimal(str(event["rate"]))*
                  (Decimal(1) if signal.direction=="long" else Decimal(-1))
                  for event in events if start["ts"]<event["ts"]<=bar["ts"]),Decimal(0))
        result=estimate_trade(signal.direction,entry,exit_price,notional,fees,slip,paid)
        deals.append({"opened_at":start["ts"],"closed_at":bar["ts"],
           "direction":signal.direction,"entry":result["fill_entry"],"exit":result["fill_exit"],
           "stop":signal.stop,"take":signal.take,"qty":result["quantity"],
           "gross_pnl":result["gross"],"fees":result["fees"],
           "funding":result["funding"],"net_pnl":result["net"],"outcome":outcome,
           "reason":signal.reason})
        i=j+1
    return deals

def run_backtest(settings,tf="5"):
    with connection(settings) as conn:
        bars=conn.execute("""SELECT ts,open,high,low,close,volume FROM candles
          WHERE symbol=%s AND kind='trade' AND tf=%s AND closed ORDER BY ts""",
          (settings.symbol,tf)).fetchall()
        funding=conn.execute("SELECT ts,rate FROM funding WHERE symbol=%s ORDER BY ts",
          (settings.symbol,)).fetchall()
    if len(bars)<50:
        raise RuntimeError("At least 50 closed bars required")
    deals=simulate(bars,settings.taker_fee_rate,settings.slippage_bps,
                   settings.initial_notional_usd,funding)
    total=sum((x["net_pnl"] for x in deals),Decimal(0))
    wins=sum(x["net_pnl"]>0 for x in deals)
    coverage=("funding_coverage_bounds_only" if funding and
       funding[0]["ts"]<=bars[0]["ts"] and funding[-1]["ts"]>=bars[-1]["ts"]
       else "funding_unverified")
    with connection(settings) as conn:
        run=conn.execute("""INSERT INTO backtest_runs(symbol,strategy,tf,config,data_start,
          data_end,completeness,trades,pnl,stats)
          VALUES(%s,'pivot_rejection_v1',%s,%s::jsonb,%s,%s,%s,%s,%s,%s::jsonb)
          RETURNING id""",
          (settings.symbol,tf,json.dumps({"notional":str(settings.initial_notional_usd),
           "taker_fee":str(settings.taker_fee_rate),"slippage":str(settings.slippage_bps),
           "intrabar":"stop_first"}),bars[0]["ts"],bars[-1]["ts"],coverage,len(deals),total,
           json.dumps({"wins":wins,"win_rate":wins/len(deals) if deals else None}))).fetchone()
        for i,x in enumerate(deals,1):
            conn.execute("""INSERT INTO backtest_trades(run_id,seq,opened_at,closed_at,
              direction,entry,exit,stop,take,qty,gross_pnl,fees,funding,net_pnl,outcome,reason)
              VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
              (run["id"],i,x["opened_at"],x["closed_at"],x["direction"],x["entry"],
              x["exit"],x["stop"],x["take"],x["qty"],x["gross_pnl"],x["fees"],
              x["funding"],x["net_pnl"],x["outcome"],json.dumps(x["reason"])))
    return (run["id"],len(deals),total,coverage)
