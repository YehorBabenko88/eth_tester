"""Causal replay and leakage-resistant chronological evaluation utilities.

Tick replay accepts executable Quote objects only, not trade prints pretending
there was bid/ask liquidity. Funding is mandatory for complete cost coverage.
"""
from dataclasses import dataclass, field
from datetime import datetime

from .execution import PaperEngine, Quote, utc
from .metrics import equity_metrics


@dataclass(frozen=True)
class SignalEvent:
    ts: datetime
    side: str
    stop: object
    take: object
    order_type: str = 'market'
    limit: object = None
    reason: dict = field(default_factory=dict)

    def __post_init__(self):
        utc(self.ts)


@dataclass(frozen=True)
class FundingEvent:
    ts: datetime
    rate: object
    mark_price: object
    event_id: str

    def __post_init__(self):
        utc(self.ts)
        if not self.event_id:
            raise ValueError('funding event_id required')


def _ordered(events):
    previous = None
    for event in events:
        if not hasattr(event, "ts"):
            raise TypeError("event must provide timestamp and executable quote data")
        current = utc(event.ts)
        if previous is not None and current < previous:
            raise ValueError('events must be chronologically ordered')
        previous = current
        yield event


def replay_ticks(quotes, signals=(), *, funding_events=None, engine=None):
    """Replay strictly causal signals at the first quote AFTER signal timestamp.

    No auto-close at EOF. A quote gap freezes execution until the caller has
    verified and explicitly reset the engine. Funding None means NOT VERIFIED.
    """
    engine = engine or PaperEngine()
    signals = iter(_ordered(signals))
    funding = iter(_ordered(funding_events or ()))
    next_signal = next(signals, None)
    next_funding = next(funding, None)
    rejected = []
    n_quotes = 0
    previous_ts = None
    for quote in _ordered(quotes):
        if not isinstance(quote, Quote):
            raise TypeError('tick replay requires verified Quote with bid/ask/depth')
        ts = utc(quote.ts)
        if previous_ts is not None and ts == previous_ts and quote.event_id in engine.seen:
            continue
        previous_ts = ts
        n_quotes += 1
        # Funding event at quote timestamp is settled before the quote can open.
        while next_funding is not None and utc(next_funding.ts) <= ts:
            if next_funding.mark_price is None:
                raise ValueError('missing historical funding mark')
            if engine.last_ts is not None and utc(next_funding.ts) >= engine.last_ts:
                engine.funding(event_id='fund:' + next_funding.event_id,
                               ts=next_funding.ts, rate=next_funding.rate,
                               mark_price=next_funding.mark_price)
            next_funding = next(funding, None)
        while next_signal is not None and utc(next_signal.ts) < ts:
            if engine.position is not None or engine.order is not None:
                rejected.append((next_signal.ts, 'position_or_order_active'))
            else:
                try:
                    engine.submit(next_signal.side, next_signal.stop, next_signal.take,
                                  order_type=next_signal.order_type, limit=next_signal.limit,
                                  reason=next_signal.reason)
                except (RuntimeError, ValueError) as exc:
                    rejected.append((next_signal.ts, str(exc)))
            next_signal = next(signals, None)
        engine.quote(quote)
    while next_signal is not None:
        rejected.append((next_signal.ts, 'no_later_quote'))
        next_signal = next(signals, None)
    return {
        'engine': engine,
        'metrics': equity_metrics(engine.history),
        'quotes_seen': n_quotes,
        'signals_rejected': rejected,
        'open_at_end': engine.position is not None,
        'outstanding_order': engine.order is not None,
        'gap_blocked': engine.blocked,
        'funding_coverage': 'provided_not_completeness_proven' if funding_events is not None
                            else 'not_provided',
        'execution_provenance': 'supplied_executable_quotes_not_exchange_confirmed',
    }


def walk_forward_splits(records, train_size, test_size, *, purge=0, embargo=0, step=None):
    """Return causal rolling train/test index ranges, half-open [start,end).

    Purge removes adjacent train observations; embargo leaves a gap after each
    test window before the following training cutoff. No random shuffle.
    """
    if min(train_size, test_size) < 1 or min(purge, embargo) < 0:
        raise ValueError('invalid window configuration')
    stride = step if step is not None else test_size + embargo
    if stride < 1:
        raise ValueError('step must be positive')
    count = len(records)
    for i in range(1, count):
        if utc(records[i]['ts']) <= utc(records[i - 1]['ts']):
            raise ValueError('timestamps must increase strictly')
    end_train = train_size
    windows = []
    while end_train + purge + test_size <= count:
        start_train = end_train - train_size
        start_test = end_train + purge
        end_test = start_test + test_size
        windows.append({'train': (start_train, end_train), 'test': (start_test, end_test),
                        'train_end': records[end_train - 1]['ts'],
                        'test_start': records[start_test]['ts']})
        end_train += stride
    return windows


def walk_forward_evaluate(records, *, train_size, test_size, fit, evaluate,
                          purge=0, embargo=0, step=None):
    """Fit on train only, evaluate never-seen OOS observations.

    Callbacks receive detached slices, not the full dataset. Evaluation should
    be causal and must not train on the test slice.
    """
    folds = []
    for window in walk_forward_splits(records, train_size, test_size,
                                      purge=purge, embargo=embargo, step=step):
        lo, hi = window['train']
        tl, th = window['test']
        trained = fit(records[lo:hi])
        score = evaluate(records[tl:th], trained)
        folds.append({**window, 'parameters': trained, 'oos': score})
    return folds


def holdout_split(records, *, train_fraction=0.7, purge=0):
    """One final untouched chronological OOS split for model selection."""
    if not 0 < train_fraction < 1 or purge < 0:
        raise ValueError('invalid holdout specification')
    cut = int(len(records) * train_fraction)
    if not 0 < cut < len(records) - purge:
        raise ValueError('insufficient OOS observations')
    walk_forward_splits(records, cut, len(records) - cut - purge, purge=purge)
    return records[:cut], records[cut + purge:]
