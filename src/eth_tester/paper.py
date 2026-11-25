from datetime import datetime, timezone
from decimal import Decimal
from .simulation.execution import number, utc
import json
from .database import connection

def estimate_trade(side,entry,exit,notional,taker_rate,slippage_bps,funding=Decimal(0)):
    entry,exit,notional,taker_rate,slippage_bps,funding = (number(v) for v in (entry,exit,notional,taker_rate,slippage_bps,funding))
    if side not in ("long","short") or min(entry,exit,notional)<=0 or min(taker_rate,slippage_bps)<0 or slippage_bps>=10000:
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
    notional,taker_rate,slippage_bps,safety_factor = (number(v) for v in (notional,taker_rate,slippage_bps,safety_factor))
    if notional<=0 or min(taker_rate,slippage_bps,safety_factor)<0:
        raise ValueError('Invalid costs')
    return (taker_rate*2+slippage_bps/Decimal(10000)*2)*safety_factor

def open_paper(settings,direction,observed_price,stop,take,reason,observed_at=None):
    if direction not in ("long","short"):
        raise ValueError("Invalid direction")
    if not isinstance(reason,dict):
        raise ValueError('Reason must be a dict')
    price,stop,take=map(number,(observed_price,stop,take))
    event_at=utc(observed_at) if observed_at is not None else datetime.now(timezone.utc)
    if number(settings.initial_notional_usd)!=Decimal('1000'):
        raise ValueError('Paper nominal must be 1000 USD')
    if price<=0 or not (stop<price<take if direction=="long" else take<price<stop):
        raise ValueError("Bad stop/take")
    if abs(take/price-1)<min_viable_target(settings.initial_notional_usd,
                                           settings.taker_fee_rate,settings.slippage_bps):
        raise ValueError("Expected take does not cover costs")
    context=dict(reason)
    context['execution_quality']='trade_print_proxy_no_executable_book_proof'
    strategy_version=str(context.get('strategy_version') or 'unversioned')[:100]
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20991991)")
        state=conn.execute("SELECT enabled,paused FROM bot_control WHERE singleton=true").fetchone()
        if not state or not state["enabled"] or state["paused"]:
            raise RuntimeError("Paused")
        if conn.execute("SELECT 1 FROM paper_trades WHERE state='open'").fetchone():
            raise RuntimeError("One trade only")
        account=conn.execute("SELECT equity FROM paper_account WHERE id=1 FOR UPDATE").fetchone()
        daily=conn.execute("""SELECT coalesce(sum(net_pnl),0) AS pnl FROM paper_trades
          WHERE state='closed' AND closed_at>=%s""",(event_at.replace(hour=0,minute=0,second=0,microsecond=0),)).fetchone()["pnl"]
        if account and daily<0 and -daily>=account["equity"]*settings.max_daily_drawdown:
            raise RuntimeError("Daily drawdown limit reached")
        sign=Decimal(1) if direction=="long" else Decimal(-1)
        fill=price*(1+sign*settings.slippage_bps/Decimal(10000))
        qty=settings.initial_notional_usd/fill
        entry_fee=settings.initial_notional_usd*settings.taker_fee_rate
        exposure=abs(qty*(fill-stop))+entry_fee+qty*stop*settings.taker_fee_rate
        if not account or exposure>account["equity"]*settings.risk_fraction:
            raise RuntimeError("Risk cap exceeded")
        row=conn.execute("""INSERT INTO paper_trades(symbol,state,direction,signal_ts,
          opened_at,entry,qty,notional,stop,take,reason,fee_paid,slippage_paid,strategy_version)
          VALUES(%s,'open',%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)
          RETURNING id""",
          (settings.symbol,direction,event_at,event_at,fill,qty,settings.initial_notional_usd,stop,take,
           json.dumps(context,default=str),entry_fee,abs(fill-price)*qty,strategy_version)).fetchone()
        return row["id"]

def close_paper(settings,observed_price,reason="manual",funding_paid=Decimal(0),observed_at=None,expected_id=None):
    price=number(observed_price)
    funding=number(funding_paid)
    if price<=0:
        raise ValueError('Invalid exit price')
    event_at=utc(observed_at) if observed_at is not None else datetime.now(timezone.utc)
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20991991)")
        trade=conn.execute("SELECT * FROM paper_trades WHERE state='open' FOR UPDATE").fetchone()
        if not trade or (expected_id is not None and trade["id"]!=expected_id):
            return None
        if event_at<utc(trade['opened_at']):
            raise ValueError('Exit precedes entry')
        sign=Decimal(1) if trade["direction"]=="long" else Decimal(-1)
        fill=price*(1-sign*settings.slippage_bps/Decimal(10000))
        if fill<=0:
            raise ValueError('Nonpositive exit fill')
        gross=sign*trade["qty"]*(fill-trade["entry"])
        exit_fee=trade["qty"]*fill*settings.taker_fee_rate
        net=gross-trade["fee_paid"]-exit_fee-funding
        conn.execute("""UPDATE paper_trades SET state='closed',closed_at=%s,exit=%s,
          fee_paid=fee_paid+%s,slippage_paid=slippage_paid+%s,funding_paid=%s,
          gross_pnl=%s,net_pnl=%s,exit_reason=%s WHERE id=%s AND state='open'""",
          (event_at,fill,exit_fee,abs(fill-price)*trade["qty"],funding,gross,net,reason,trade["id"]))
        conn.execute("""UPDATE paper_account SET equity=equity+%s,
          realized_pnl=realized_pnl+%s WHERE id=1""",(net,net))
        return trade["id"]
