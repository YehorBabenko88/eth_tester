"""Post-decision structural labels on the full UTC closed-candle grid.

The decision timestamp is NEVER rounded or backdated. The bar overlapping
that decision is excluded. This function never trusts PnL as a class label.
"""
from math import isfinite


def full_future_grid(decision_ms, *, bar_interval_ms, horizon_bars):
    """Return first full bar start/end and horizon (UTC epoch milliseconds)."""
    if (type(decision_ms) is not int or decision_ms <= 0
            or type(bar_interval_ms) is not int or bar_interval_ms <= 0
            or type(horizon_bars) is not int or not 1 <= horizon_bars <= 10000):
        raise ValueError("invalid post-decision grid policy")
    start = (decision_ms // bar_interval_ms + 1) * bar_interval_ms
    return start, start + bar_interval_ms, start + horizon_bars * bar_interval_ms


def label_level_outcome(side, level, closed_bars, *, decision_ms, horizon_ms,
                        as_of_ms, bar_interval_ms, barrier_bps=20):
    """Classify breakout, bounce or uncertain from strictly future full bars.

    Never include OHLC from a candle that was already in progress at entry.
    Concurrent upper+lower touch in the same bar is ambiguous and stays uncertain.
    A gap, late close, synthetic shifted grid or unknown high/low is rejected.
    """
    if (side not in ("support", "resistance") or isinstance(level, bool)
            or not isfinite(float(level)) or float(level) <= 0):
        raise ValueError("invalid reference level")
    if (type(horizon_ms) is not int or type(as_of_ms) is not int
            or type(barrier_bps) not in (int, float) or isinstance(barrier_bps, bool)
            or not isfinite(float(barrier_bps))
            or not 0 < barrier_bps <= 1000):
        raise ValueError("invalid or unresolved horizon")
    bars = list(closed_bars)
    first_start, _, expected_horizon = full_future_grid(
        decision_ms, bar_interval_ms=bar_interval_ms, horizon_bars=len(bars))
    if (not bars or horizon_ms != expected_horizon or
            not decision_ms < first_start or as_of_ms < horizon_ms):
        raise ValueError("unresolved or invalid horizon")
    width = float(level) * barrier_bps / 10000
    upper, lower = float(level) + width, float(level) - width
    outcome = None
    for index, bar in enumerate(bars):
        expected_start = first_start + index * bar_interval_ms
        expected_close = expected_start + bar_interval_ms
        if (type(bar.get("closed_ms")) is not int
                or bar["closed_ms"] != expected_close
                or ("start_ms" in bar and bar["start_ms"] != expected_start)):
            raise ValueError("unordered or out-of-horizon bar")
        low, high = float(bar["low"]), float(bar["high"])
        if not (isfinite(low) and isfinite(high) and 0 < low <= high):
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
