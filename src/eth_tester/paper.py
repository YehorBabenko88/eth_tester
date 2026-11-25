from decimal import Decimal
import json
from .database import connection

def estimate_trade(side,entry,exit,notional,taker_rate,slippage_bps,funding=Decimal(0)):
    if side not in ("long","short") or min(entry,exit,notional)<=0:
        raise ValueError("Invalid trade")
    sign=Decimal(1) if side=="long" else Decimal(-1)
    slip=slippage_bps/Decimal(10000)
    fill_entry=entry*(1+sign*slip)
    fill_exit=exit*(1-sign*slip)
    qty=notional/fill_entry
    gross=sign*qty*(fill_exit-fill_entry)
    fees=(qty*fill_entry+qty*fill_exit)*taker_rate
    return {"gross":gross,"fees":fees,"funding":funding,"net":gross-fees-funding,
            "quantity":qty,"fill_entry":fill_entry,"fill_exit":fill_exit}

def min_viable_target(notional,taker_rate,slippage_bps,safety_factor=Decimal("2")):
    return (taker_rate*2+slippage_bps/Decimal(10000)*2)*safety_factor

def open_paper(settings,direction,observed_price,stop,take,reason):
    if direction not in ("long","short"):
        raise ValueError("Invalid direction")
    price,stop,take=map(lambda x:Decimal(str(x)),(observed_price,stop,take))
    if price<=0 or not (stop<price<take if direction=="long" else take<price<stop):
        raise ValueError("Bad stop/take")
    if abs(take/price-1)<min_viable_target(settings.initial_notional_usd,
                                           settings.taker_fee_rate,settings.slippage_bps):
        raise ValueError("Expected take does not cover costs")
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20991991)")
        state=conn.execute("SELECT enabled,paused FROM bot_control WHERE singleton=true").fetchone()
        if not state or not state["enabled"] or state["paused"]:
            raise RuntimeError("Paused")
        if conn.execute("SELECT 1 FROM paper_trades WHERE state='open'").fetchone():
            raise RuntimeError("One trade only")
        account=conn.execute("SELECT equity FROM paper_account WHERE id=1 FOR UPDATE").fetchone()
        sign=Decimal(1) if direction=="long" else Decimal(-1)
        fill=price*(1+sign*settings.slippage_bps/Decimal(10000))
        qty=settings.initial_notional_usd/fill
        if not account or abs(qty*(fill-stop))>account["equity"]*settings.risk_fraction:
            raise RuntimeError("Risk cap exceeded")
        entry_fee=settings.initial_notional_usd*settings.taker_fee_rate
        row=conn.execute("""INSERT INTO paper_trades(symbol,state,direction,signal_ts,
          opened_at,entry,qty,notional,stop,take,reason,fee_paid,slippage_paid,strategy_version)
          VALUES(%s,'open',%s,now(),now(),%s,%s,%s,%s,%s,%s::jsonb,%s,%s,'pivot_v1')
          RETURNING id""",
          (settings.symbol,direction,fill,qty,settings.initial_notional_usd,stop,take,
           json.dumps(reason),entry_fee,abs(fill-price)*qty)).fetchone()
        return row["id"]

def close_paper(settings,observed_price,reason="manual",funding_paid=Decimal(0)):
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20991991)")
        trade=conn.execute("SELECT * FROM paper_trades WHERE state='open' FOR UPDATE").fetchone()
        if not trade:
            return None
        sign=Decimal(1) if trade["direction"]=="long" else Decimal(-1)
        price=Decimal(str(observed_price))
        fill=price*(1-sign*settings.slippage_bps/Decimal(10000))
        gross=sign*trade["qty"]*(fill-trade["entry"])
        exit_fee=trade["qty"]*fill*settings.taker_fee_rate
        funding=Decimal(str(funding_paid))
        net=gross-trade["fee_paid"]-exit_fee-funding
        conn.execute("""UPDATE paper_trades SET state='closed',closed_at=now(),exit=%s,
          fee_paid=fee_paid+%s,slippage_paid=slippage_paid+%s,funding_paid=%s,
          gross_pnl=%s,net_pnl=%s,exit_reason=%s WHERE id=%s""",
          (fill,exit_fee,abs(fill-price)*trade["qty"],funding,gross,net,reason,trade["id"]))
        conn.execute("""UPDATE paper_account SET equity=equity+%s,
          realized_pnl=realized_pnl+%s WHERE id=1""",(net,net))
        return trade["id"]
