from datetime import datetime, timezone
from decimal import Decimal
from .simulation.execution import number, utc
from .simulation.identity import require_new_strategy_identity
from .simulation.persistence import (advance_state, lock_state, mark_funding_event,
                                     observation_time, record_fill, validate_transition)
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
    context=require_new_strategy_identity(reason)
    context['execution_quality']='trade_print_proxy_no_executable_book_proof'
    strategy_version=context['strategy_version']
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20991991)")
        position_state=lock_state(conn)
        validate_transition(position_state,trade_id=None,at=event_at,action='open')
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
        trade_id=row["id"]
        record_fill(conn,trade_id=trade_id,symbol=settings.symbol,
                    event_id=f"paper:{trade_id}:entry:{event_at.isoformat()}",
                    at=event_at,observed_at=observation_time(event_at),
                    side='Buy' if direction=='long' else 'Sell',action='entry',
                    qty=qty,price=fill,fee=entry_fee,slippage_cost=abs(fill-price)*qty,
                    quality='trade_print_proxy_no_liquidity',
                    provenance={'strategy':strategy_version,'reason':context})
        conn.execute("""UPDATE paper_trades SET execution_quality=%s WHERE id=%s""",
                     ('trade_print_proxy_no_liquidity',trade_id))
        advance_state(conn,position_state,trade_id=trade_id,at=event_at,action='open')
        return trade_id

def close_paper(settings,observed_price,reason="manual",funding_paid=Decimal(0),observed_at=None,expected_id=None,*,funding_evidence=None):
    price=number(observed_price)
    funding=number(funding_paid)
    if price<=0:
        raise ValueError('Invalid exit price')
    event_at=utc(observed_at) if observed_at is not None else datetime.now(timezone.utc)
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(20991991)")
        position_state=lock_state(conn)
        trade=conn.execute("SELECT * FROM paper_trades WHERE state='open' FOR UPDATE").fetchone()
        if not trade or (expected_id is not None and trade["id"]!=expected_id):
            return None
        if event_at<utc(trade['opened_at']):
            raise ValueError('Exit precedes entry')
        legacy_exit=False
        if position_state['paper_trade_id'] is None:
            # Old open positions predate migration 0005. Never fabricate entry.
            entry_event=conn.execute("""SELECT 1 FROM paper_order_events
              WHERE paper_trade_id=%s AND action IN ('entry','partial_entry')
              LIMIT 1""",(trade['id'],)).fetchone()
            if entry_event:
                raise RuntimeError('Paper ledger position state mismatch')
            validate_transition(position_state,trade_id=trade['id'],
                                at=event_at,action='close_legacy')
            legacy_exit=True
        else:
            validate_transition(position_state,trade_id=trade['id'],
                                at=event_at,action='close')
        sign=Decimal(1) if trade["direction"]=="long" else Decimal(-1)
        fill=price*(1-sign*settings.slippage_bps/Decimal(10000))
        if fill<=0:
            raise ValueError('Nonpositive exit fill')
        gross=sign*trade["qty"]*(fill-trade["entry"])
        exit_fee=trade["qty"]*fill*settings.taker_fee_rate
        verified_sum=Decimal(0)
        evidence=list(funding_evidence or ())
        for event in evidence:
            if not utc(trade['opened_at']) < utc(event['ts']) <= event_at:
                raise ValueError('Funding event outside position lifetime')
            verified_sum+=sign*number(trade['qty'])*number(event['mark_price'])*number(event['rate'])
        if evidence and verified_sum!=funding:
            raise ValueError('Funding event breakdown does not match settlement')
        if not evidence and funding!=0:
            raise ValueError('Nonzero funding without event evidence')
        net=gross-trade["fee_paid"]-exit_fee-funding
        conn.execute("""UPDATE paper_trades SET state='closed',closed_at=%s,exit=%s,
          fee_paid=fee_paid+%s,slippage_paid=slippage_paid+%s,funding_paid=%s,
          gross_pnl=%s,net_pnl=%s,exit_reason=%s WHERE id=%s AND state='open'""",
          (event_at,fill,exit_fee,abs(fill-price)*trade["qty"],funding,gross,net,reason,trade["id"]))
        record_fill(conn,trade_id=trade['id'],symbol=settings.symbol,
                    event_id=f"paper:{trade['id']}:exit:{event_at.isoformat()}",
                    at=event_at,observed_at=observation_time(event_at),
                    side='Sell' if trade['direction']=='long' else 'Buy',
                    action='exit',qty=trade['qty'],price=fill,fee=exit_fee,
                    slippage_cost=abs(fill-price)*trade['qty'],
                    quality='trade_print_proxy_no_liquidity',
                    provenance={'exit_reason':reason,'funding_quality':'unverified'})
        for event in evidence:
            mark_funding_event(conn,trade_id=trade['id'],funding_at=event['ts'],
                               observed_at=observation_time(event['ts']),
                               rate=event['rate'],mark_price=event['mark_price'],
                               position_qty=trade['qty'],direction=trade['direction'],
                               quality='mark_within_1s_completeness_unverified',
                               provenance={'source':'market_ticker_events'})
        conn.execute("""UPDATE paper_account SET equity=equity+%s,
          realized_pnl=realized_pnl+%s WHERE id=1""",(net,net))
        advance_state(conn,position_state,trade_id=trade['id'],at=event_at,
                      action='close_legacy' if legacy_exit else 'close')
        return trade["id"]
