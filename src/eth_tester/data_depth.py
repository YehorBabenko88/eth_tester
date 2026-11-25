"""Discover earliest *observable* Bybit closed candle within a configured horizon.

A bounded prefix search is monotonic: once ANY valid candle exists in
[configured_start, end], later end boundaries must still expose one. Every
sample validates response bounds. Exchange retention outside configured
horizon is NOT asserted.
"""
from datetime import datetime, timezone

from .data_quality import DataIntegrityError, open_time, next_open
from .history_integrity import as_utc, latest_closed, ms, KINDS
from .data_coverage import write_coverage
from .database import connection

UTC = timezone.utc


async def first_observed_candle(client, symbol, category, kind, tf, start_ms, now_ms):
    """Return earliest observed closed candle timestamp or None.

    Strictly verifies that response timestamps lie within the exact requested
    range. Does not silently reinterpret endpoint-ignored start/end filters.
    """
    if kind not in KINDS:
        raise ValueError(f"Unknown candle kind {kind}")
    start = open_time(tf, start_ms)
    if start < start_ms:
        start = next_open(tf, start)
    last = latest_closed(tf, now_ms)
    if start > last:
        return None

    async def contains_any(cutoff):
        data = await client.get(
            KINDS[kind], category=category, symbol=symbol, interval=tf,
            start=start, end=cutoff, limit=1)
        rows = data.get("list", [])
        if len(rows) > 1:
            raise DataIntegrityError("Availability probe exceeded limit=1")
        if not rows:
            return False
        stamp = int(rows[0][0])
        if stamp < start or stamp > cutoff:
            raise DataIntegrityError(
                f"History depth response outside requested UTC range: {stamp}")
        if open_time(tf, stamp) != stamp or next_open(tf, stamp) > now_ms:
            raise DataIntegrityError("History depth returned off-grid/open candle")
        return True

    if not await contains_any(last):
        return None
    lo, hi = start, last
    while lo < hi:
        midpoint = open_time(tf, (lo + hi) // 2)
        if midpoint < lo:
            midpoint = lo
        if await contains_any(midpoint):
            hi = midpoint
        else:
            lo = next_open(tf, midpoint)
    # Distinguish backend boundary behavior from actual earliest observation.
    if not await contains_any(lo):
        raise DataIntegrityError("History depth search was not monotonic")
    return lo


async def discover_candle_depth(settings, client, kind, tf, now=None):
    """Record configured-horizon prefix as unavailable, without inventing bars."""
    now_ms = ms(now or datetime.now(UTC))
    start_ms = ms(datetime.combine(settings.archive_start, datetime.min.time(), UTC))
    first = await first_observed_candle(
        client, settings.symbol, settings.category, kind, tf, start_ms, now_ms)
    aligned = open_time(tf, start_ms)
    if aligned < start_ms:
        aligned = next_open(tf, aligned)
    prefix_end = first if first is not None else next_open(tf, latest_closed(tf, now_ms))
    if aligned < prefix_end:
        with connection(settings) as conn:
            write_coverage(
                conn, source=f"kline:{kind}", symbol=settings.symbol, interval=tf,
                start_ms=aligned, end_exclusive_ms=prefix_end,
                status="unavailable", observed_rows=0, expected_rows=None,
                quality="observed_empty_prefix_not_loaded",
                evidence={
                    "method": "bounded_prefix_binary_search",
                    "scope_start_utc": as_utc(aligned).isoformat(),
                    "scope_end_utc": as_utc(prefix_end).isoformat(),
                    "earliest_observed_utc": (
                        as_utc(first).isoformat() if first is not None else None),
                    "no_assertion_before_configured_start": True,
                    "no_assertion_about_future_availability": True,
                },
                instrument=settings.category)
    return first
