"""Resolve first-timestamp trade-print SL/TP ambiguity pessimistically."""
from .execution import number, utc


def resolve_first_crossing(row, direction, stop, take):
    """Return stop priority when both thresholds crossed at same timestamp.

    The exchange's event order within one millisecond is not proven by trade_id
    sorting. Take price is capped at the intended target, and stop price uses
    the most adverse observed print in that timestamp bucket.
    """
    if row is None:
        return None
    if direction not in ('long', 'short'):
        raise ValueError('invalid position side')
    stop, take = number(stop), number(take)
    if not (stop < take if direction == 'long' else take < stop):
        raise ValueError('invalid protection levels')
    timestamp = utc(row['ts'])
    low = number(row['low_cross']) if row.get('low_cross') is not None else None
    high = number(row['high_cross']) if row.get('high_cross') is not None else None
    stop_hit = (low is not None and low <= stop) if direction == 'long' else (
        high is not None and high >= stop)
    take_hit = (high is not None and high >= take) if direction == 'long' else (
        low is not None and low <= take)
    if stop_hit:
        observed = low if direction == 'long' else high
        return {'ts': timestamp, 'price': observed, 'reason': 'stop',
                'ambiguous_same_timestamp': take_hit}
    if take_hit:
        return {'ts': timestamp, 'price': take, 'reason': 'take',
                'ambiguous_same_timestamp': False}
    return None
