"""Checkpointed causal level lifecycle for all 13 timeframes.

Requires Storage migration 0002 (levels timestamps, level_events, analytics_cursor).
The cursor and events are committed in the SAME transaction as the level rows.
A closed candle confirms a pivot only after the right flank has closed.
"""
from decimal import Decimal
import json

from .analytics_core import LevelEngine, TrackedLevel, close_time
from .database import connection


EVENT_NAMES = {
    "confirmed": "confirmed",
    "touch": "touched",
    "break": "broken",
    "retest": "retested",
    "invalidation": "invalidated",
}


def _restore(conn, settings, tf, flank, cursor, tolerance):
    engine = LevelEngine(tf, flank=flank, tolerance=tolerance)
    if cursor is None:
        return engine
    engine.last_closed = close_time(cursor, tf)
    # Only a bounded two-flank tail is needed for future pivot detection.
    tail = conn.execute("""SELECT ts,high,low,close
        FROM candles WHERE symbol=%s AND kind='trade' AND tf=%s
          AND closed AND ts<=%s
        ORDER BY ts DESC LIMIT %s""",
        (settings.symbol, tf, cursor, 2 * flank)).fetchall()
    for bar in reversed(tail):
        engine.history.append(bar)
    active = conn.execute("""SELECT side,origin_ts,confirmed_at,level_price,status,
            broken_at,retested_at,invalidated_at
        FROM levels
        WHERE symbol=%s AND tf=%s AND confirmed_at IS NOT NULL
          AND status IN ('unbroken','broken','retested')""",
        (settings.symbol, tf)).fetchall()
    for row in active:
        level = TrackedLevel(
            side=row["side"], origin_ts=row["origin_ts"],
            confirmation_ts=row["confirmed_at"],
            price=Decimal(row["level_price"]), tf=tf,
            status=row["status"], broken_at=row["broken_at"],
            retested_at=row["retested_at"],
            invalidated_at=row["invalidated_at"])
        engine.levels[(level.side, level.origin_ts, level.price)] = level
    return engine


def _store_event(conn, symbol, tf, level, event, provenance):
    evidence = {"bar_close": level.events[-1][1].isoformat(),
                "tf": tf, "source": "closed_trade_candle",
                "confirmation_ts": level.confirmation_ts.isoformat()}
    record = conn.execute("""INSERT INTO levels(
        symbol,tf,side,origin_ts,level_price,status,broken_ts,source,
        confirmed_at,touched_at,broken_at,retested_at,invalidated_at,
        last_evidence)
      VALUES(%s,%s,%s,%s,%s,%s,%s,'causal_closed_pivot',
             %s,%s,%s,%s,%s,%s::jsonb)
      ON CONFLICT(symbol,tf,side,origin_ts,level_price)
      DO UPDATE SET status=EXCLUDED.status,broken_ts=EXCLUDED.broken_ts,
        confirmed_at=EXCLUDED.confirmed_at,
        touched_at=coalesce(EXCLUDED.touched_at,levels.touched_at),
        broken_at=coalesce(EXCLUDED.broken_at,levels.broken_at),
        retested_at=coalesce(EXCLUDED.retested_at,levels.retested_at),
        invalidated_at=coalesce(EXCLUDED.invalidated_at,levels.invalidated_at),
        last_evidence=EXCLUDED.last_evidence
      RETURNING id""",
      (symbol, tf, level.side, level.origin_ts, level.price,
       level.status, level.broken_at, level.confirmation_ts,
       level.events[-1][1] if event == "touch" else None,
       level.broken_at, level.retested_at, level.invalidated_at,
       json.dumps(evidence))).fetchone()
    conn.execute("""INSERT INTO level_events(
      symbol,tf,level_id,event_type,event_ts,provenance,evidence)
      VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb)
      ON CONFLICT DO NOTHING""",
      (symbol, tf, record["id"], EVENT_NAMES[event], level.events[-1][1],
       provenance, json.dumps(evidence)))


def process_levels(settings, tf="60", flank=2, *, batch=2000,
                   tolerance=Decimal("0"), max_batches=1):
    """Incrementally process closed bars without rescanning old history.

    The cursor records candle OPEN timestamps; every emitted event records the
    source candle's CLOSE time. A missing candle fails closed, no cursor advance.
    Concurrent workers on the same symbol/frame are serialized.
    """
    if batch < 1 or max_batches < 1:
        raise ValueError("batch and max_batches must be positive")
    processed = 0
    batches = 0
    while True:
        with connection(settings) as conn:
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                         (f"levels:{settings.symbol}:{tf}",))
            row = conn.execute("""SELECT cursor_ts FROM analytics_cursor
                WHERE processor='levels' AND symbol=%s AND tf=%s FOR UPDATE""",
                (settings.symbol, tf)).fetchone()
            cursor = row["cursor_ts"] if row else None
            engine = _restore(conn, settings, tf, flank, cursor, tolerance)
            rows = conn.execute("""SELECT ts,high,low,close,closed
                FROM candles WHERE symbol=%s AND kind='trade' AND tf=%s
                  AND closed AND (%s::timestamptz IS NULL OR ts>%s)
                ORDER BY ts LIMIT %s""",
                (settings.symbol, tf, cursor, cursor, batch)).fetchall()
            previous = cursor
            for bar in rows:
                if previous is not None and bar["ts"] != close_time(previous, tf):
                    raise RuntimeError(
                        f"Closed-candle gap {settings.symbol}/{tf}: "
                        f"expected {close_time(previous, tf)}, got {bar['ts']}")
                for level, event in engine.update(bar):
                    _store_event(conn, settings.symbol, tf, level, event,
                                 "causal_closed_candle")
                previous = bar["ts"]
            if rows:
                conn.execute("""INSERT INTO analytics_cursor(
                    processor,symbol,tf,cursor_ts,quality,state,details)
                    VALUES('levels',%s,%s,%s,'closed_candle','complete',
                           %s::jsonb)
                    ON CONFLICT(processor,symbol,tf)
                    DO UPDATE SET cursor_ts=EXCLUDED.cursor_ts,
                                  quality=EXCLUDED.quality,state=EXCLUDED.state,
                                  details=EXCLUDED.details,updated_at=now()""",
                    (settings.symbol, tf, previous,
                     json.dumps({"flank": flank, "tolerance": str(tolerance)})))
            processed += len(rows)
        batches += 1
        if len(rows) < batch or batches >= max_batches:
            return processed
