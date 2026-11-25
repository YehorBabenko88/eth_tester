"""Strict historic Bybit candle ingestion; no claim of completeness for absent windows."""
from datetime import datetime, timezone
from hashlib import sha256
import json

from .data_quality import (
    DataIntegrityError, FIXED_MS, next_open, open_time, validate_candle_page,
    window_grid,
)
from .database import connection, cursor_ts, save_cursor
from .data_coverage import digest_rows, write_coverage

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
        # F's three FULL FUTURE UTC 1m candles require a SINGLE
        # independently re-fetched historical page beginning before the
        # original signal, not three separately inferred candle rows.
        # Eight extra closed bars also tolerate bounded collector lag.
        for _ in range(8 if kind == "trade" and tf == "1" else 2):
            lookback = open_time(tf, lookback - 1)
        pos = max(start, lookback)
    else:
        # Import lazily: depth probes depend on this module's time-grid helpers.
        from .data_depth import discover_candle_depth

        observed_start = await discover_candle_depth(settings, client, kind, tf)
        if observed_start is None:
            return  # unavailable is logged, never marked as downloaded
        pos = max(start, observed_start)
    now = ms(datetime.now(UTC))
    end_history = latest_closed(tf, now)
    while pos <= end_history:
        end = page_end(tf, pos, end_history)
        result = await client.get(
            KINDS[kind], category=settings.category, symbol=settings.symbol,
            interval=tf, start=pos, end=end, limit=1000)
        raw = result.get("list", [])
        expected = len(list(window_grid(tf, pos, end, now)))
        checksum = digest_rows(raw)
        try:
            values, status = validate_candle_page(raw, tf, pos, end, now)
        except DataIntegrityError as exc:
            with connection(settings) as conn:
                record_gap(conn, settings.symbol, gap_source, pos, end,
                           f"Integrity failure: {exc}")
                write_coverage(
                    conn, source=source, symbol=settings.symbol, interval=tf,
                    start_ms=pos, end_exclusive_ms=next_open(tf, end),
                    status="gap", observed_rows=len(raw), expected_rows=expected,
                    checksum=checksum, quality="rejected",
                    evidence={"error": str(exc), "kind": kind, "tf": tf},
                    instrument=settings.category)
            raise
        try:
            with connection(settings) as conn:
                source_received_at = conn.execute(
                    "SELECT clock_timestamp() AS ts").fetchone()["ts"]
                if source_received_at.tzinfo is None:
                    raise DataIntegrityError(
                        "Independent REST receipt requires database clock")
                receipt_ms = ms(source_received_at)
                source_end_ms = next_open(tf, end)
                closed_grid_from_http = (
                    kind == "trade" and tf == "1" and
                    status == "verified" and
                    len(values) == expected and
                    expected > 0 and
                    source_end_ms <= receipt_ms and
                    len(raw) >= expected)
                # The reference binds EXACT returned REST payload SHA,
                # requested UTC window and *actual post-HTTP DB receipt*.
                # Never infer source proof from existing stored candles.
                source_reference = sha256(json.dumps(
                    [settings.symbol, settings.category, kind, tf,
                     pos, end, checksum, receipt_ms],
                    separators=(",", ":")).encode()).hexdigest()
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
                write_coverage(
                    conn, source=source, symbol=settings.symbol, interval=tf,
                    start_ms=pos, end_exclusive_ms=next_open(tf, end),
                    status="verified" if status == "verified" else "unavailable",
                    observed_rows=len(values), expected_rows=expected,
                    checksum=checksum if values else None,
                    quality="exchange_closed_grid_checked" if values else "not_observed",
                    evidence={"kind": kind, "tf": tf, "endpoint": KINDS[kind],
                              "requested_start_ms": pos, "requested_end_ms": end,
                              "row_count": len(values),
                              "source_event_start_ms": pos,
                              "source_event_end_exclusive_ms": source_end_ms,
                              "source_receipt_ms": receipt_ms,
                              "source_receipt_kind": "postgres_clock_timestamp_after_http",
                              "evidence_ref": source_reference,
                              "rest_payload_sha256": checksum,
                              # These source-witness flags refer ONLY to a
                              # genuinely fetched, fully closed REST grid.
                              # No archive trade-ID or nine-feature ML claim.
                              "closed_grid_complete": closed_grid_from_http,
                              "receipt_is_independently_recorded":
                                  closed_grid_from_http,
                              "source_verified_as_of_receipt": closed_grid_from_http,
                              "proves_all_trade_ids": False,
                              "exchange_trades_complete": False},
                    instrument=settings.category)
                missing = conn.execute(
                    """SELECT count(*) AS total FROM data_gaps
                       WHERE source=%s AND symbol=%s AND resolved_at IS NULL""",
                    (gap_source, settings.symbol)).fetchone()["total"]
                quality = "observed_partial" if missing else "verified"
                save_cursor(conn, source, settings.symbol, tf,
                            as_utc(next_open(tf, end)), status=quality)
                conn.execute(
                    """UPDATE ingestion_cursor SET meta=COALESCE(meta,'{}'::jsonb) || jsonb_build_object(
                      'window_start',%s::text,'window_end',%s::text,
                      'window_status',%s::text,'unresolved_windows',%s::bigint)
                       WHERE source=%s AND symbol=%s AND instrument='linear'
                         AND interval=%s""",
                    (as_utc(pos).isoformat(), as_utc(end).isoformat(),
                     status, missing, source, settings.symbol, tf))
        except DataIntegrityError as exc:
            # The failed candle transaction rolls back. Preserve independent
            # conflict evidence in a new transaction before surfacing failure.
            with connection(settings) as conn:
                record_gap(conn, settings.symbol, gap_source, pos, end,
                           f"Stored market-data conflict: {exc}")
                write_coverage(
                    conn, source=source, symbol=settings.symbol, interval=tf,
                    start_ms=pos, end_exclusive_ms=next_open(tf, end),
                    status="gap", observed_rows=len(values), expected_rows=expected,
                    checksum=checksum, quality="conflict_rejected",
                    evidence={"error": str(exc), "kind": kind, "tf": tf,
                              "reason": "stored_immutable_candle_conflict"},
                    instrument=settings.category)
            raise
        pos = next_open(tf, end)

async def collect_latest_closed_trade_minute(settings, client):
    """Fast, bounded real Bybit V5 1m REST receipt, independent of backfill.

    This witnesses the exchange *closed candle grid only*, NEVER all trade IDs.
    The source event time is bar close; actual receipt/check are PostgreSQL time.
    """
    with connection(settings) as conn:
        now = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
    if now.tzinfo is None:
        raise DataIntegrityError("Database clock must be timezone aware")
    now_ms = ms(now)
    start = open_time("1", now_ms) - FIXED_MS["1"]
    end = next_open("1", start)
    result = await client.get(
        "kline", category=settings.category, symbol=settings.symbol,
        interval="1", start=start, end=start, limit=1)
    raw = result.get("list", [])
    if not isinstance(raw, list):
        raise DataIntegrityError("Invalid live closed-minute response")
    with connection(settings) as conn:
        received = conn.execute(
            "SELECT clock_timestamp() AS ts").fetchone()["ts"]
        if received.tzinfo is None:
            raise DataIntegrityError("Database receipt clock missing UTC offset")
        receipt_ms = ms(received)
        values, quality = validate_candle_page(raw, "1", start, start, receipt_ms)
        if quality != "verified" or set(values) != {start}:
            raise DataIntegrityError("Latest requested 1m exchange candle not closed")
        numbers = values[start]
        if len(numbers) < 6 or any(v is None for v in numbers[:6]):
            raise DataIntegrityError("Missing official base volume or quote turnover")
        volume, turnover = numbers[4], numbers[5]
        if (not volume.is_finite() or not turnover.is_finite()
                or volume < 0 or turnover < 0):
            raise DataIntegrityError("Invalid 1m base/quote volume")
        store_candle(conn, settings.symbol, "trade", "1", start, numbers)
        checksum = digest_rows(raw)
        write_coverage(
            conn, source="kline:trade", symbol=settings.symbol, interval="1",
            start_ms=start, end_exclusive_ms=end,
            status="verified", observed_rows=1, expected_rows=1,
            checksum=checksum, quality="exchange_closed_grid_checked",
            evidence={"endpoint": "kline", "tf": "1", "closed_bar_only": True,
                      "source_event_start_ms": start,
                      "source_event_end_exclusive_ms": end,
                      "source_receipt_ms": receipt_ms,
                      "source_receipt_kind": "postgres_clock_timestamp",
                      "proves_all_trade_ids": False,
                      "ml_snapshot_eligible": False},
            instrument=settings.category)
    return {"status": "closed_grid_observed", "source": "kline:trade",
            "bar_start_ms": start, "bar_end_ms": end,
            "source_receipt_ms": receipt_ms, "checksum": checksum,
            "proves_all_trade_ids": False}
