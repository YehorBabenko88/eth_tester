"""Deterministic barrier labels, resolved only after the precommitted horizon.

Future bars are used ONLY to create historical labels, never input features.
"""
from math import isfinite


def label_level_outcome(side, level, closed_bars, *, decision_ms, horizon_ms,
                        as_of_ms, bar_interval_ms, barrier_bps=20):
    """Classify confirmed level test: breakout, bounce or uncertain.

    Resistance: high crossing upper barrier -> breakout, low crossing lower
    barrier -> bounce. Support: the directions are reversed. If both barriers
    hit in one closed bar, outcome is uncertain (no intrabar path assumption).
    Event is not eligible until horizon has elapsed in observed time.
    """
    if side not in ("support", "resistance") or not isfinite(level) or level <= 0:
        raise ValueError("invalid reference level")
    if (not isinstance(decision_ms, int) or not isinstance(horizon_ms, int)
            or not isinstance(as_of_ms, int) or horizon_ms <= decision_ms
            or as_of_ms < horizon_ms or barrier_bps <= 0 or barrier_bps > 1000
            or not isinstance(bar_interval_ms, int) or bar_interval_ms <= 0
            or (horizon_ms-decision_ms) % bar_interval_ms != 0):
        raise ValueError("unresolved or invalid horizon")
    step = level * barrier_bps / 10000
    upper, lower = level + step, level - step
    bars = list(closed_bars)
    if len(bars) != (horizon_ms-decision_ms)//bar_interval_ms:
        raise ValueError("incomplete label horizon")
    outcome = None
    for index, bar in enumerate(bars, 1):
        stamp = bar["closed_ms"]
        if not isinstance(stamp, int) or stamp != decision_ms + index*bar_interval_ms:
            raise ValueError("unordered or out-of-horizon bar")
        low, high = float(bar["low"]), float(bar["high"])
        if not all(isfinite(v) for v in (low, high)) or low > high or low <= 0:
            raise ValueError("invalid bar")
        crossed_up, crossed_down = high >= upper, low <= lower
        if outcome is None:
            if crossed_up and crossed_down:
                outcome = "uncertain"
            elif crossed_up:
                outcome = "breakout" if side == "resistance" else "bounce"
            elif crossed_down:
                outcome = "bounce" if side == "resistance" else "breakout"
    return outcome or "uncertain"
