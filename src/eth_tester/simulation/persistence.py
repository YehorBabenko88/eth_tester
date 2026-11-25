"""B-owned migration 0005 paper settlement contract, no schema ownership here.

All functions act on an existing psycopg transaction with dict_row cursor.
Do not call commit(): caller's transaction must encompass trade/account writes,
event rows and the monotonic state cursor. Lock state before other paper writes.
"""
from datetime import datetime, timezone
from hashlib import sha256
import json

from .execution import number, utc


def lock_state(conn):
    state = conn.execute("""SELECT paper_trade_id,version,pending_exit,processed_cursor
                            FROM paper_position_state WHERE singleton=true
                            FOR UPDATE""").fetchone()
    if state is None:
        raise RuntimeError('paper_position_state migration 0005 not initialized')
    return state


def validate_transition(state, *, trade_id, at, action):
    ts = utc(at)
    previous = state['processed_cursor']
    if previous is not None and ts < utc(previous):
        raise RuntimeError('replayed or out-of-order paper cursor')
    active = state['paper_trade_id']
    if action == 'open':
        if active is not None:
            raise RuntimeError('existing durable paper position')
    elif action == 'close':
        if active is None or active != trade_id:
            raise RuntimeError('paper position identity/cursor mismatch')
    else:
        raise ValueError('unknown paper state transition')
    return ts


def record_fill(conn, *, trade_id, symbol, event_id, at, observed_at,
                side, action, qty, price, fee, slippage_cost,
                source='paper_proxy', maker=False, spread_cost=0, quality='unverified',
                provenance=None):
    if not event_id or action not in ('entry','exit','partial_entry','partial_exit'):
        raise ValueError('invalid fill event identity')
    if side not in ('Buy','Sell') or not symbol:
        raise ValueError('invalid fill side/symbol')
    ts, observed = utc(at), utc(observed_at)
    if observed < ts:
        raise ValueError('observation before exchange timestamp')
    values = [number(x) for x in (qty,price,fee,slippage_cost,spread_cost)]
    if min(values[:2]) <= 0 or min(values[2:]) < 0:
        raise ValueError('invalid paper fill amount')
    # No random key. Same source/symbol/event ID always creates same key.
    raw = json.dumps([source,symbol,str(event_id)], separators=(',',':'))
    key = sha256(raw.encode()).hexdigest()
    conn.execute("""INSERT INTO paper_order_events(
      event_key,source,symbol,event_id,paper_trade_id,order_id,action,side,
      qty,price,maker,fee,spread_cost,slippage_cost,event_ts,observed_ts,quality,provenance)
      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
      (key,source,symbol,str(event_id),trade_id,str(trade_id),action,side,
       values[0],values[1],maker,values[2],values[4],values[3],ts,observed,
       quality,json.dumps(provenance or {},default=str)))
    return key


def advance_state(conn, state, *, trade_id, at, action):
    ts = validate_transition(state, trade_id=trade_id, at=at, action=action)
    new_id = trade_id if action == 'open' else None
    conn.execute("""UPDATE paper_position_state SET paper_trade_id=%s,
      version=version+1,pending_exit=false,processed_cursor=%s,updated_at=now()
      WHERE singleton=true AND version=%s""",(new_id,ts,state['version']))
    return int(state['version'])+1


def mark_funding_event(conn, *, trade_id, funding_at, observed_at,
                       rate, mark_price, position_qty, direction,
                       quality='verified', provenance=None):
    if direction not in ('long','short'):
        raise ValueError('invalid funding side')
    ts, observed = utc(funding_at), utc(observed_at)
    if observed < ts:
        raise ValueError('funding observation precedes event')
    rate, price, qty = [number(v) for v in (rate,mark_price,position_qty)]
    if price <= 0 or qty <= 0:
        raise ValueError('funding mark/qty must be positive')
    sign = 1 if direction == 'long' else -1
    amount = sign * qty * price * rate
    conn.execute("""INSERT INTO paper_funding_events(
      paper_trade_id,funding_ts,rate,mark_price,position_qty,amount,
      observed_ts,quality,provenance)
      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
      (trade_id,ts,rate,price,qty,amount,observed,quality,
       json.dumps(provenance or {},default=str)))
    return amount


def observation_time(at):
    """For replayed past events, observed_at is NOW, never time-travel fabricated."""
    return max(utc(at),datetime.now(timezone.utc))
