"""Exchange timestamp-grid and immutable duplicate validators (no database schema dependency)."""
from calendar import monthrange
from datetime import datetime, timedelta, timezone
from decimal import Decimal

UTC = timezone.utc
FIXED_MS = {"1": 60000, "3": 180000, "5": 300000, "15": 900000,
            "30": 1800000, "60": 3600000, "120": 7200000,
            "240": 14400000, "360": 21600000, "720": 43200000,
            "D": 86400000, "W": 604800000}
PERIOD_MS = {"5min": 300000, "15min": 900000, "30min": 1800000,
             "1h": 3600000, "4h": 14400000, "1d": 86400000}


class DataIntegrityError(ValueError):
    """Conflicting, missing or off-grid exchange data."""


def open_time(tf: str, stamp: int) -> int:
    """UTC start of the true Bybit candle, including ISO-week and calendar month."""
    dt = datetime.fromtimestamp(stamp / 1000, UTC)
    if tf == "M":
        return int(datetime(dt.year, dt.month, 1, tzinfo=UTC).timestamp() * 1000)
    if tf == "W":
        dt = (dt - timedelta(days=dt.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0)
        return int(dt.timestamp() * 1000)
    return stamp // FIXED_MS[tf] * FIXED_MS[tf]


def next_open(tf: str, stamp: int) -> int:
    stamp = open_time(tf, stamp)
    if tf == "M":
        dt = datetime.fromtimestamp(stamp / 1000, UTC)
        year, month = dt.year + (dt.month == 12), dt.month % 12 + 1
        return int(datetime(year, month, 1, tzinfo=UTC).timestamp() * 1000)
    return stamp + FIXED_MS[tf]


def window_grid(tf: str, start: int, end: int, now: int):
    """Expected *closed* bar starts within inclusive request bounds."""
    current = open_time(tf, start)
    if current < start:
        current = next_open(tf, current)
    while current <= end:
        next_stamp = next_open(tf, current)
        if next_stamp > now:
            break
        yield current
        current = next_stamp


def validate_candle_page(rows, tf: str, start: int, end: int, now: int):
    """Verify page, detect duplicates, and reject interior/edge gaps when bounded."""
    by_ts = {}
    for row in rows:
        stamp = int(row[0])
        if stamp < start or stamp > end:
            raise DataIntegrityError("Candle timestamp outside requested window")
        if open_time(tf, stamp) != stamp:
            raise DataIntegrityError(f"Off-grid {tf} candle: {stamp}")
        if next_open(tf, stamp) > now:
            continue  # current unfinished candle is not historical evidence
        if len(row) < 5:
            raise DataIntegrityError("Truncated OHLC")
        op, high, low, close = (Decimal(str(x)) for x in row[1:5])
        if min(op, high, low, close) <= 0 or high < max(op, close) or low > min(op, close):
            raise DataIntegrityError(f"Invalid OHLC at {stamp}")
        normalized = tuple(Decimal(str(x)) if x not in ("", None) else None for x in row[1:])
        if stamp in by_ts and by_ts[stamp] != normalized:
            raise DataIntegrityError(f"Conflicting candle duplicate at {stamp}")
        by_ts[stamp] = normalized
    expected = list(window_grid(tf, start, end, now))
    if not by_ts:
        return {}, "unavailable" if expected else "no_closed_bars"
    found = set(by_ts)
    missing = set(expected) - found
    if missing:
        first = min(missing)
        raise DataIntegrityError(f"Missing candle at {first} ({len(missing)} missing)")
    return by_ts, "verified"


def validate_period_page(rows, period: str, start: int, end: int, *,
                         ts_key="timestamp", values=(), now=None):
    """Validate fixed UTC OI/ratio grids and immutable normalized duplicates."""
    step = PERIOD_MS[period]
    found = {}
    for row in rows:
        ts = int(row[ts_key])
        if not start <= ts <= end:
            raise DataIntegrityError("Timestamp outside period window")
        if ts % step:
            raise DataIntegrityError(f"Off-grid {period}: {ts}")
        value = tuple(Decimal(str(row[name])) for name in values)
        if ts in found and found[ts] != value:
            raise DataIntegrityError(f"Conflicting {period} duplicate at {ts}")
        found[ts] = value
    if not found:
        return {}, "unavailable"
    first, last = min(found), max(found)
    for ts in range(first, last + 1, step):
        if ts not in found:
            raise DataIntegrityError(f"Missing {period} sample at {ts}")
    return found, "observed_partial"  # endpoints/history depth not yet independently proven


def reconcile_minute_volume(trades, candles, *, tolerance=Decimal("0.00000001")):
    """Compare full-day BASE-quantity trade tape to 1440 official linear 1m candles.

    This checks volume equality, not completeness of every individual trade ID.
    """
    minute = {}
    for stamp, qty in trades:
        t = int(stamp.timestamp() * 1000)
        k = t // 60000 * 60000
        minute[k] = minute.get(k, Decimal(0)) + Decimal(str(qty))
    official = {}
    for stamp, volume in candles:
        t = int(stamp.timestamp() * 1000)
        k = t // 60000 * 60000
        v = Decimal(str(volume))
        if k in official and official[k] != v:
            raise DataIntegrityError(f"Conflicting official minute volume {k}")
        official[k] = v
    if len(official) != 1440:
        return {"status": "incomplete_candles", "minutes": len(official)}
    if min(official) // 86400000 != max(official) // 86400000:
        raise DataIntegrityError("Candles cross UTC day")
    if not trades:
        return {"status": "incomplete_trades", "minutes": 0}
    mismatches = [k for k, v in official.items()
                  if abs(v - minute.get(k, Decimal(0))) > tolerance]
    return {"status": "verified" if not mismatches else "volume_mismatch",
            "minutes": 1440, "mismatches": len(mismatches),
            "exchange_base_qty": str(sum(official.values())),
            "trade_base_qty": str(sum(minute.values()))}
