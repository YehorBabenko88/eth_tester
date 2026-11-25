"""Gap-aware Funding, Open Interest and account ratio collectors.

Historical source retention is exchange-defined. An API window with no records
is recorded as unavailable, never as complete; finite coverage is observed_partial.
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json

from .data_quality import DataIntegrityError, PERIOD_MS, validate_period_page
from .database import connection, cursor_ts, save_cursor
from .history_integrity import as_utc, ms, record_gap

UTC = timezone.utc


def _begin(settings, source, interval):
    start = datetime.combine(settings.archive_start, datetime.min.time(), UTC)
    with connection(settings) as conn:
        previous = cursor_ts(conn, source, settings.symbol, interval)
    if previous:
        # Revalidate recent boundary samples; old data remain immutable.
        start = max(start, previous - timedelta(days=1))
    return ms(start)


def _status(conn, source, symbol, interval, start, end, status, earliest=None):
    save_cursor(conn, source, symbol, interval, as_utc(end + 1), status=status)
    conn.execute(
        """UPDATE ingestion_cursor SET meta=COALESCE(meta,'{}'::jsonb) ||
           jsonb_build_object(
             'last_window_start',%s::text,'last_window_end',%s::text,
             'last_window_status',%s::text,
             'last_observed_min',%s::text)
           WHERE source=%s AND symbol=%s AND instrument='linear' AND interval=%s""",
        (as_utc(start).isoformat(), as_utc(end).isoformat(), status,
         as_utc(earliest).isoformat() if earliest is not None else None,
         source, symbol, interval),
    )


async def _pages(client, endpoint, params, start, end, limit, min_step):
    """Read every page; bisect saturated cursorless ranges rather than truncating."""
    while True:
        cursor = None
        visited = set()
        rows = []
        saturated = False
        while True:
            result = await client.get(endpoint, **{
                **params, "startTime": start, "endTime": end,
                "limit": limit, "cursor": cursor})
            page = result.get("list", [])
            if not isinstance(page, list):
                raise DataIntegrityError("Bybit page list is not an array")
            rows.extend(page)
            next_cursor = result.get("nextPageCursor")
            if next_cursor:
                if next_cursor in visited:
                    raise DataIntegrityError(f"Repeated cursor from {endpoint}")
                visited.add(next_cursor)
                cursor = next_cursor
                continue
            saturated = len(page) >= limit
            break
        if not saturated:
            return end, rows
        # A full final page without a next cursor may be truncated.
        new_end = start + max(min_step, (end - start) // 2)
        if new_end >= end:
            raise DataIntegrityError(f"{endpoint} saturated at minimum range")
        end = new_end


def _funding_rows(rows, start, end):
    result = {}
    for row in rows:
        ts = int(row["fundingRateTimestamp"])
        if not start <= ts <= end:
            raise DataIntegrityError("Funding timestamp outside request")
        rate = Decimal(str(row["fundingRate"]))
        if not rate.is_finite():
            raise DataIntegrityError("Non-finite funding rate")
        if ts in result and result[ts] != rate:
            raise DataIntegrityError(f"Conflicting Funding duplicate at {ts}")
        result[ts] = rate
    return result


def _save_funding(conn, symbol, ts, rate):
    saved = conn.execute(
        """INSERT INTO funding(symbol,ts,rate) VALUES(%s,%s,%s)
           ON CONFLICT(symbol,ts) DO UPDATE SET rate=EXCLUDED.rate
           WHERE funding.rate=EXCLUDED.rate RETURNING ts""",
        (symbol, as_utc(ts), rate)).fetchone()
    if saved is None:
        raise DataIntegrityError(f"Funding conflicts with database at {ts}")


async def collect_funding(settings, client):
    source = "funding"
    pos = _begin(settings, source, "")
    now = ms(datetime.now(UTC))
    while pos < now:
        end = min(now - 1, pos + 30 * 86400000 - 1)
        try:
            final_end, rows = await _pages(
                client, "funding/history",
                {"category": settings.category, "symbol": settings.symbol},
                pos, end, 200, 60000)
            values = _funding_rows(rows, pos, final_end)
            with connection(settings) as conn:
                for ts, rate in values.items():
                    _save_funding(conn, settings.symbol, ts, rate)
                if not values:
                    record_gap(conn, settings.symbol, "funding:history",
                               pos, final_end, "Funding API returned no history")
                _status(conn, source, settings.symbol, "", pos, final_end,
                        "observed_partial" if values else "unavailable",
                        min(values) if values else None)
        except DataIntegrityError as exc:
            with connection(settings) as conn:
                record_gap(conn, settings.symbol, "funding:history",
                           pos, end, f"Integrity failure: {exc}")
            raise
        pos = final_end + 1


async def _collect_period(settings, client, endpoint, source, interval,
                          value_names, db_table):
    if interval not in PERIOD_MS:
        raise ValueError(f"Unsupported history period {interval}")
    step = PERIOD_MS[interval]
    pos = _begin(settings, source, interval)
    now = ms(datetime.now(UTC))
    while pos < now:
        end = min(now - 1, pos + 7 * 86400000 - 1)
        try:
            params = {"category": settings.category, "symbol": settings.symbol}
            if source == "oi":
                params["intervalTime"] = interval
                limit = 200
            else:
                params["period"] = interval
                limit = 500
            final_end, rows = await _pages(
                client, endpoint, params, pos, end, limit, step)
            values, quality = validate_period_page(
                rows, interval, pos, final_end, values=value_names)
            with connection(settings) as conn:
                for stamp, record in values.items():
                    if db_table == "open_interest":
                        saved = conn.execute(
                            """INSERT INTO open_interest(symbol,interval,ts,value)
                               VALUES(%s,%s,%s,%s)
                               ON CONFLICT(symbol,interval,ts)
                               DO UPDATE SET value=EXCLUDED.value
                               WHERE open_interest.value=EXCLUDED.value
                               RETURNING ts""",
                            (settings.symbol, interval, as_utc(stamp), record[0]))
                    else:
                        raw = next(row for row in rows
                                   if int(row["timestamp"]) == stamp)
                        saved = conn.execute(
                            """INSERT INTO ratios(symbol,period,ts,buy_ratio,sell_ratio,raw)
                               VALUES(%s,%s,%s,%s,%s,%s::jsonb)
                               ON CONFLICT(symbol,period,ts) DO UPDATE SET
                               buy_ratio=EXCLUDED.buy_ratio
                               WHERE ratios.buy_ratio IS NOT DISTINCT FROM EXCLUDED.buy_ratio
                                 AND ratios.sell_ratio IS NOT DISTINCT FROM EXCLUDED.sell_ratio
                               RETURNING ts""",
                            (settings.symbol, interval, as_utc(stamp),
                             record[0], record[1], json.dumps(raw)))
                    if saved.fetchone() is None:
                        raise DataIntegrityError(
                            f"Stored {source} {interval} conflicts at {stamp}")
                if not values:
                    record_gap(conn, settings.symbol, f"{source}:{interval}",
                               pos, final_end, "No samples returned by Bybit")
                _status(conn, source, settings.symbol, interval,
                        pos, final_end, quality,
                        min(values) if values else None)
        except DataIntegrityError as exc:
            with connection(settings) as conn:
                record_gap(conn, settings.symbol, f"{source}:{interval}",
                           pos, end, f"Integrity failure: {exc}")
            raise
        pos = final_end + 1


async def collect_oi(settings, client, interval):
    await _collect_period(settings, client, "open-interest", "oi", interval,
                          ("openInterest",), "open_interest")


async def collect_account_ratios(settings, client, period):
    await _collect_period(settings, client, "account-ratio", "account_ratio",
                          period, ("buyRatio", "sellRatio"), "ratios")
