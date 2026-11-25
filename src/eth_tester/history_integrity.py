"""Strict historic Bybit candle ingestion; no claim of completeness for absent windows."""
from datetime import datetime, timezone

from .data_quality import (
    DataIntegrityError, FIXED_MS, next_open, open_time, validate_candle_page,
)
from .database import connection, cursor_ts, save_cursor

UTC = timezone.utc
KINDS = {"trade": "kline", "mark": "mark-price-kline",
         "index": "index-price-kline", "premium": "premium-index-price-kline"}
TIMEFRAMES = (*FIXED_MS, "M")


def ms(value):
    return int(value.timestamp() * 1000)


def as_utc(value):
    return datetime.fromtimestamp(value / 1000, UTC)


def page_end(tf, start, latest):
    if tf == "M":
        end = start
        for _ in range(999):
            end = next_open(tf, end)
        return min(end, latest)
    return min(start + 999 * FIXED_MS[tf], latest)


def latest_closed(tf, now):
    bucket = open_time(tf, now)
    return open_time(tf, bucket - 1) if next_open(tf, bucket) > now else bucket


def record_gap(conn, symbol, source, start, end, reason):
    exists = conn.execute(
        """SELECT id FROM data_gaps WHERE source=%s AND symbol=%s
           AND start_ts=%s AND end_ts=%s AND resolved_at IS NULL LIMIT 1""",
        (source, symbol, as_utc(start), as_utc(end)),
    ).fetchone()
    if not exists:
        conn.execute(
            """INSERT INTO data_gaps(source,symbol,start_ts,end_ts,explanation)
               VALUES(%s,%s,%s,%s,%s)""",
            (source, symbol, as_utc(start), as_utc(end), reason))


def store_candle(conn, symbol, kind, tf, stamp, numbers):
    first = numbers[:4]
    volume = numbers[4] if len(numbers) > 4 else None
    turnover = numbers[5] if len(numbers) > 5 else None
    inserted = conn.execute(
        """INSERT INTO candles(symbol,kind,tf,ts,open,high,low,close,
               volume,turnover,source)
           VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'bybit_v5')
           ON CONFLICT(symbol,kind,tf,ts) DO UPDATE SET open=EXCLUDED.open
           WHERE candles.open=EXCLUDED.open AND candles.high=EXCLUDED.high
             AND candles.low=EXCLUDED.low AND candles.close=EXCLUDED.close
             AND candles.volume IS NOT DISTINCT FROM EXCLUDED.volume
             AND candles.turnover IS NOT DISTINCT FROM EXCLUDED.turnover
           RETURNING ts""",
        (symbol, kind, tf, as_utc(stamp), *first, volume, turnover),
    ).fetchone()
    if not inserted:
        raise DataIntegrityError(f"Stored candle conflicts: {symbol} {kind} {tf} {stamp}")


async def collect_klines(settings, client, kind, tf):
    if kind not in KINDS or tf not in TIMEFRAMES:
        raise ValueError("Unknown candle kind/time frame")
    first = ms(datetime.combine(settings.archive_start, datetime.min.time(), UTC))
    start = open_time(tf, first)
    if start < first:
        start = next_open(tf, start)
    source = f"kline:{kind}"
    gap_source = f"{source}:{tf}"
    with connection(settings) as conn:
        previous = cursor_ts(conn, source, settings.symbol, tf)
    if previous:
        lookback = ms(previous)
        for _ in range(2):
            lookback = open_time(tf, lookback - 1)
        pos = max(start, lookback)
    else:
        pos = start
    now = ms(datetime.now(UTC))
    end_history = latest_closed(tf, now)
    while pos <= end_history:
        end = page_end(tf, pos, end_history)
        result = await client.get(
            KINDS[kind], category=settings.category, symbol=settings.symbol,
            interval=tf, start=pos, end=end, limit=1000)
        values, status = validate_candle_page(
            result.get("list", []), tf, pos, end, now)
        with connection(settings) as conn:
            if status == "verified":
                for stamp, row in values.items():
                    store_candle(conn, settings.symbol, kind, tf, stamp, row)
                conn.execute(
                    """UPDATE data_gaps SET resolved_at=now()
                       WHERE source=%s AND symbol=%s AND start_ts=%s AND end_ts=%s
                       AND resolved_at IS NULL""",
                    (gap_source, settings.symbol, as_utc(pos), as_utc(end)))
            else:
                record_gap(conn, settings.symbol, gap_source, pos, end,
                           f"Historical candle request returned {status}")
            missing = conn.execute(
                """SELECT count(*) AS total FROM data_gaps
                   WHERE source=%s AND symbol=%s AND resolved_at IS NULL""",
                (gap_source, settings.symbol)).fetchone()["total"]
            quality = "observed_partial" if missing else "verified"
            save_cursor(conn, source, settings.symbol, tf,
                        as_utc(next_open(tf, end)), status=quality)
            conn.execute(
                """UPDATE ingestion_cursor SET meta=jsonb_build_object(
                  'window_start',%s::text,'window_end',%s::text,
                  'window_status',%s::text,'unresolved_windows',%s::bigint)
                   WHERE source=%s AND symbol=%s AND instrument='linear'
                     AND interval=%s""",
                (as_utc(pos).isoformat(), as_utc(end).isoformat(),
                 status, missing, source, settings.symbol, tf))
        pos = next_open(tf, end)
