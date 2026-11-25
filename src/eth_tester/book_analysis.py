"""Replay raw order book deltas; decreases cannot be attributed to fills vs cancellations."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from .database import connection

def parse_levels(rows):
    return {str(p):str(q) for p,q in rows if Decimal(str(q))>0}

def apply_updates(book,rows):
    changes=[]
    for price,quantity in rows:
        price=str(price)
        old=Decimal(book.get(price,"0"))
        new=Decimal(str(quantity))
        if new<0:
            raise ValueError("Negative L2 quantity")
        if new==0:
            book.pop(price,None)
        else:
            book[price]=str(new)
        changes.append((price,old,new))
    return changes

def significant(changes,side,threshold):
    events=[]
    for price,old,new in changes:
        action=None
        if old<threshold<=new:
            action="appeared"
        elif old>=threshold>new:
            action="vanished_unknown"
        elif old>=threshold and new>=threshold and new>old*Decimal("1.5"):
            action="increased"
        elif old>=threshold and new>=threshold and new<old*Decimal("0.5"):
            action="decreased_unknown"
        if action:
            events.append((side,price,old,new,action))
    return events

def snapshot_metrics(bids, asks, *, observed_at, as_of, max_age=timedelta(seconds=10)):
    """Valid-at-time L2 imbalance; never interpret removals as executions."""
    if observed_at is None or as_of is None or observed_at.tzinfo is None or as_of.tzinfo is None:
        return {"valid": False, "reason": "missing_utc_timestamps", "imbalance": None}
    age = as_of - observed_at
    if not timedelta(0) <= age <= max_age:
        return {"valid": False, "reason": "stale_or_future_book", "imbalance": None}
    bid = {Decimal(str(p)): Decimal(str(q)) for p, q in bids.items()}
    ask = {Decimal(str(p)): Decimal(str(q)) for p, q in asks.items()}
    if not bid or not ask or any(q <= 0 for q in (*bid.values(), *ask.values())):
        return {"valid": False, "reason": "empty_or_invalid_book", "imbalance": None}
    if max(bid) >= min(ask):
        return {"valid": False, "reason": "crossed_book", "imbalance": None}
    b, a = sum(bid.values(), Decimal(0)), sum(ask.values(), Decimal(0))
    return {"valid": True, "reason": "valid_depth_snapshot",
            "imbalance": (b - a) / (b + a), "best_bid": max(bid),
            "best_ask": min(ask), "spread": min(ask) - max(bid),
            "bid_qty": b, "ask_qty": a, "age_seconds": Decimal(str(age.total_seconds()))}


def replay_orderbook(settings,batch=2000):
    total=0
    while True:
        with connection(settings) as conn:
            row=conn.execute("""SELECT last_event_id,bids,asks,last_update_id,valid
             FROM market_book_state WHERE symbol=%s FOR UPDATE""",
             (settings.symbol,)).fetchone()
            last=row["last_event_id"] if row else 0
            bids=dict(row["bids"]) if row else {}
            asks=dict(row["asks"]) if row else {}
            valid=row["valid"] if row else False
            last_u=row["last_update_id"] if row else None
            latest_received=None
            events=conn.execute("""SELECT id,recv_ts,event_type,bids,asks,update_id
             FROM book_events WHERE symbol=%s AND id>%s ORDER BY id LIMIT %s""",
             (settings.symbol,last,batch)).fetchall()
            if not events:
                return total
            for item in events:
                latest_received=item["recv_ts"]
                typ=item["event_type"]
                u=item["update_id"]
                if typ=="snapshot":
                    bids=parse_levels(item["bids"])
                    asks=parse_levels(item["asks"])
                    valid=bool(bids and asks and max(map(Decimal,bids)) < min(map(Decimal,asks)))
                elif typ=="delta":
                    if not valid:
                        last=item["id"]
                        continue
                    if last_u is not None and u is not None and u>last_u+1:
                        valid=False
                        conn.execute("""INSERT INTO data_gaps(source,symbol,start_ts,end_ts,
                         explanation) VALUES('orderbook',%s,%s,%s,'L2 update_id gap')""",
                         (settings.symbol,item["recv_ts"],item["recv_ts"]))
                        last=item["id"]
                        last_u=u
                        continue
                    if last_u is not None and u is not None and u<last_u:
                        valid=False
                        last=item["id"]
                        last_u=u
                        continue
                    b=apply_updates(bids,item["bids"])
                    a=apply_updates(asks,item["asks"])
                    if bids and asks and max(map(Decimal,bids)) >= min(map(Decimal,asks)):
                        valid=False
                        conn.execute("""INSERT INTO data_gaps(source,symbol,start_ts,end_ts,
                          explanation) VALUES('orderbook',%s,%s,%s,'crossed L2 book')""",
                          (settings.symbol,item["recv_ts"],item["recv_ts"]))
                    for side,price,old,new,action in (
                        significant(b,"bid",settings.min_wall_qty)+
                        significant(a,"ask",settings.min_wall_qty)):
                        conn.execute("""INSERT INTO orderbook_wall_events(symbol,ts,side,price,
                         prior_qty,new_qty,action,source_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                         ON CONFLICT(symbol,source_id,side,price) DO NOTHING""",
                         (settings.symbol,item["recv_ts"],side,Decimal(price),old,new,
                          action,item["id"]))
                last=item["id"]
                last_u=u
            conn.execute("""INSERT INTO market_book_state(symbol,last_event_id,bids,asks,
               last_update_id,valid,updated_at) VALUES(%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s)
               ON CONFLICT(symbol) DO UPDATE SET last_event_id=EXCLUDED.last_event_id,
               bids=EXCLUDED.bids,asks=EXCLUDED.asks,last_update_id=EXCLUDED.last_update_id,
               valid=EXCLUDED.valid,updated_at=EXCLUDED.updated_at""",
               (settings.symbol,last,json.dumps(bids),json.dumps(asks),last_u,valid,
                latest_received))
            total+=len(events)
        if len(events)<batch:
            return total
