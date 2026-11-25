"""Replay raw order book deltas; decreases cannot be attributed to fills vs cancellations."""
from datetime import timedelta
from decimal import Decimal, InvalidOperation
import json
from .database import connection

def parse_levels(rows):
    """Snapshot is authoritative; corrupt/negative levels must fail closed."""
    book = {}
    for raw_price, raw_qty in rows:
        price, qty = Decimal(str(raw_price)), Decimal(str(raw_qty))
        if (not price.is_finite() or not qty.is_finite()
                or price <= 0 or qty < 0):
            raise ValueError("Invalid L2 snapshot price or quantity")
        if qty > 0:
            book[str(price)] = str(qty)
    return book

def apply_updates(book,rows):
    changes=[]
    for price,quantity in rows:
        price=str(price)
        old=Decimal(book.get(price,"0"))
        new=Decimal(str(quantity))
        if (not Decimal(price).is_finite() or Decimal(price) <= 0
                or not new.is_finite() or new < 0):
            raise ValueError("Invalid L2 update price or quantity")
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


def replay_orderbook(settings, batch=2000, *, max_batches=None):
    """Atomic, single-writer, bounded L2 state replay with poison-event recovery.

    Source events and gaps are append-only; a malformed delta invalidates the
    book and advances the raw cursor, but only a later genuine full snapshot
    can make the book valid again. An unresolved source gap is never silently
    turned into proof of continuity for B/D/E paper admission.
    """
    if (type(batch) is not int or batch < 1 or batch > 5000
            or (max_batches is not None and
                (type(max_batches) is not int or not 1 <= max_batches <= 32))):
        raise ValueError("bounded positive L2 batch/max_batches required")
    total, batches = 0, 0
    while True:
        with connection(settings) as conn:
            # E/B admission and A WS writes share this exact source fence.
            # Take source BEFORE replay to avoid source<->L2 lock inversion.
            # A contention timeout rolls back the entire batch; no skipped
            # cursor, forged source continuity or gap/state partial commit.
            conn.execute("SET LOCAL lock_timeout = '2000ms'")
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                         (f"eth_paper_admission_source:{settings.symbol}",))
            # Must precede row FOR UPDATE: first boot has NO row to lock.
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                         (f"eth_l2_replay:{settings.symbol}",))
            row = conn.execute("""SELECT last_event_id,bids,asks,last_update_id,valid
                FROM market_book_state WHERE symbol=%s FOR UPDATE""",
                (settings.symbol,)).fetchone()
            last = row["last_event_id"] if row else 0
            bids = dict(row["bids"]) if row else {}
            asks = dict(row["asks"]) if row else {}
            valid = row["valid"] if row else False
            last_u = row["last_update_id"] if row else None
            latest_received = None
            events = conn.execute("""SELECT id,recv_ts,event_type,bids,asks,
                      update_id,valid FROM book_events
                WHERE symbol=%s AND id>%s ORDER BY id LIMIT %s""",
                (settings.symbol,last,batch)).fetchall()
            if not events:
                return total
            for item in events:
                latest_received = item["recv_ts"]
                kind = item["event_type"]
                update = item["update_id"]
                try:
                    if item.get("valid", True) is False:
                        raise ValueError("raw_l2_invalid_flag")
                    if kind == "snapshot":
                        bids = parse_levels(item["bids"])
                        asks = parse_levels(item["asks"])
                        if not bids or not asks or (
                                max(map(Decimal,bids)) >= min(map(Decimal,asks))):
                            raise ValueError("empty_or_crossed_snapshot")
                        valid = True
                        last_u = update
                    elif kind == "delta":
                        if not valid:
                            # Do not restore an invalid book using deltas.
                            last = item["id"]
                            continue
                        if (last_u is not None and update is not None
                                and update < last_u):
                            raise ValueError("regressing_update_id")
                        bid_updates = apply_updates(bids,item["bids"])
                        ask_updates = apply_updates(asks,item["asks"])
                        if not bids or not asks or (
                                max(map(Decimal,bids)) >= min(map(Decimal,asks))):
                            raise ValueError("empty_or_crossed_delta")
                        for side,price,old,new,action in (
                                significant(bid_updates,"bid",settings.min_wall_qty)
                                + significant(ask_updates,"ask",
                                              settings.min_wall_qty)):
                            conn.execute("""INSERT INTO orderbook_wall_events(
                                symbol,ts,side,price,prior_qty,new_qty,action,
                                source_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT(symbol,source_id,side,price)
                                DO NOTHING""",
                                (settings.symbol,item["recv_ts"],side,
                                 Decimal(price),old,new,action,item["id"]))
                        last_u = update
                    else:
                        raise ValueError("unknown_orderbook_event_type")
                except (ValueError,TypeError,InvalidOperation) as exc:
                    # A poison event is consumed exactly once; durable gap
                    # records prevent false paper evidence or history rewrites.
                    valid = False
                    bids, asks, last_u = {}, {}, None
                    conn.execute("""INSERT INTO data_gaps(
                        source,symbol,start_ts,end_ts,explanation)
                        VALUES('orderbook',%s,%s,%s,%s)""",
                        (settings.symbol,item["recv_ts"],item["recv_ts"],
                         "invalid L2 source event: "+str(exc)[:90]))
                last = item["id"]
            conn.execute("""INSERT INTO market_book_state(
                symbol,last_event_id,bids,asks,last_update_id,valid,updated_at)
                VALUES(%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s)
                ON CONFLICT(symbol) DO UPDATE SET
                  last_event_id=EXCLUDED.last_event_id,
                  bids=EXCLUDED.bids,asks=EXCLUDED.asks,
                  last_update_id=EXCLUDED.last_update_id,
                  valid=EXCLUDED.valid,updated_at=EXCLUDED.updated_at
                WHERE market_book_state.last_event_id IS NULL
                   OR EXCLUDED.last_event_id >= market_book_state.last_event_id""",
                (settings.symbol,last,json.dumps(bids),json.dumps(asks),
                 last_u,valid,latest_received))
            total += len(events)
            batches += 1
        if len(events) < batch or (max_batches is not None
                                  and batches >= max_batches):
            return total
