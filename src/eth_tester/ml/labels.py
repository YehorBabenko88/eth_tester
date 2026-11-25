"""Post-decision structural labels on exact UTC closed-candle grid.

Never alter the original decision timestamp, use the overlapping entry candle,
interpret same-bar first-touch order, or cast exchange NUMERIC prices to float.
"""
from decimal import Decimal, InvalidOperation, localcontext


def _price(value, name):
    if isinstance(value, bool):
        raise ValueError("invalid " + name)
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("invalid " + name) from exc
    if not number.is_finite() or number <= 0:
        raise ValueError("invalid " + name)
    return number


def full_future_grid(decision_ms, *, bar_interval_ms, horizon_bars):
    """Exclude even an exactly-on-boundary decision minute from label bars."""
    if (type(decision_ms) is not int or decision_ms <= 0
            or type(bar_interval_ms) is not int or bar_interval_ms <= 0
            or type(horizon_bars) is not int or not 1 <= horizon_bars <= 10000):
        raise ValueError("invalid post-decision grid policy")
    start = (decision_ms // bar_interval_ms + 1) * bar_interval_ms
    return start, start + bar_interval_ms, start + horizon_bars * bar_interval_ms


def label_level_outcome(side, level, closed_bars, *, decision_ms, horizon_ms,
                        as_of_ms, bar_interval_ms, barrier_bps=20):
    """Classify strictly future full-bar outcomes with SQL NUMERIC-equivalent math."""
    if side not in ("support", "resistance"):
        raise ValueError("invalid reference level")
    price = _price(level, "reference level")
    bps = _price(barrier_bps, "barrier bps")
    if bps > 1000 or type(horizon_ms) is not int or type(as_of_ms) is not int:
        raise ValueError("invalid or unresolved horizon")
    bars = list(closed_bars)
    first_start, _, expected_horizon = full_future_grid(
        decision_ms, bar_interval_ms=bar_interval_ms, horizon_bars=len(bars))
    if (not bars or horizon_ms != expected_horizon or
            not decision_ms < first_start or as_of_ms < horizon_ms):
        raise ValueError("unresolved or invalid horizon")
    # PostgreSQL NUMERIC computes level*(1 +/- bps/10000), not float
    # level + level*float(bps)/10000. Keep all numeric digits from the DB.
    with localcontext() as ctx:
        ctx.prec = max(80, len(price.as_tuple().digits) + len(bps.as_tuple().digits) + 32)
        upper = price * (Decimal(1) + bps / Decimal(10000))
        lower = price * (Decimal(1) - bps / Decimal(10000))
    outcome = None
    for index, bar in enumerate(bars):
        expected_start = first_start + index * bar_interval_ms
        expected_close = expected_start + bar_interval_ms
        if (type(bar.get("closed_ms")) is not int
                or bar["closed_ms"] != expected_close
                or ("start_ms" in bar and bar["start_ms"] != expected_start)):
            raise ValueError("unordered or out-of-horizon bar")
        low, high = _price(bar["low"], "bar low"), _price(bar["high"], "bar high")
        if low > high:
            raise ValueError("invalid bar")
        if outcome is None:
            up, down = high >= upper, low <= lower
            if up and down:
                outcome = "uncertain"
            elif up:
                outcome = "breakout" if side == "resistance" else "bounce"
            elif down:
                outcome = "bounce" if side == "resistance" else "breakout"
    return outcome or "uncertain"
