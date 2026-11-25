"""Explicitly hypothetical OHLC fallback, never a proof of exchange fills."""
from datetime import timedelta
from decimal import Decimal as D

from .execution import Costs, Fill, Position, number, utc
from .metrics import equity_metrics


def _bar(row):
    for key in ('ts', 'open', 'high', 'low', 'close'):
        if key not in row:
            raise ValueError('incomplete OHLC bar')
    timestamp = utc(row['ts'])
    opening, high, low, close = (number(row[key]) for key in ('open', 'high', 'low', 'close'))
    if row.get('closed') is False or not 0 < low <= min(opening, close) <= max(opening, close) <= high:
        raise ValueError('invalid or unclosed OHLC bar')
    return {'ts': timestamp, 'open': opening, 'high': high, 'low': low, 'close': close}


def simulate_ohlc_fallback(bars, signals, *, timeframe_seconds, costs=None,
                           spread_bps=D('2'), notional=D('1000'),
                           initial_equity=D('10000'), risk_fraction=D('0.01'),
                           daily_loss_fraction=D('0.03'), funding_events=None):
    """Causal next-bar entry and stop-first pessimistic intrabar scenario.

    Missing candles reject the entire range rather than imply continuous fills.
    Funding is not invented: absent events => result carries funding_unverified.
    OHLC cannot establish order queue, liquidity, or partial fills.
    """
    candles = [_bar(row) for row in bars]
    if timeframe_seconds <= 0 or len(candles) < 2:
        raise ValueError('at least two bars and valid timeframe required')
    for left, right in zip(candles, candles[1:]):
        if right['ts'] - left['ts'] != timedelta(seconds=timeframe_seconds):
            raise ValueError('OHLC gap or non-monotonic timestamp')
    fee_model = costs or Costs()
    spread = number(spread_bps)
    amount = number(notional)
    equity = number(initial_equity)
    risk = number(risk_fraction)
    loss_limit = number(daily_loss_fraction)
    if spread < 0 or amount <= 0 or equity <= 0 or not 0 < risk <= 1 or not 0 < loss_limit <= 1:
        raise ValueError('invalid OHLC assumptions')
    fees = fee_model.taker
    slip = fee_model.slippage_bps / D(10000)
    half_spread = spread / D(20000)
    indexed = {row['ts']: i for i, row in enumerate(candles)}
    ordered_signals = sorted(signals, key=lambda s: utc(s.ts))
    outcomes = []
    positions = []
    rejected = []
    next_available = 1
    day = None
    day_start_equity = equity
    daily_pnl = D(0)
    funded = sorted(funding_events or (), key=lambda x: utc(x.ts))
    for signal in ordered_signals:
        signal_ts = utc(signal.ts)
        if signal_ts not in indexed:
            rejected.append((signal_ts, 'signal_bar_missing'))
            continue
        start = indexed[signal_ts] + 1
        if start >= len(candles):
            rejected.append((signal_ts, 'no_next_bar'))
            continue
        if start < next_available:
            rejected.append((signal_ts, 'one_position_only'))
            continue
        row = candles[start]
        if row['ts'].date() != day:
            day = row['ts'].date()
            day_start_equity = equity
            daily_pnl = D(0)
        if daily_pnl <= -day_start_equity * loss_limit:
            rejected.append((signal_ts, 'daily_loss_lockout'))
            continue
        if signal.side not in ('long', 'short'):
            raise ValueError('invalid signal direction')
        sign = D(1) if signal.side == 'long' else D(-1)
        if getattr(signal, 'order_type', 'market') != 'market':
            rejected.append((signal_ts, 'OHLC_limit_queue_unverifiable'))
            continue
        entry = row['open'] * (1 + sign * (half_spread + slip))
        stop, take = number(signal.stop), number(signal.take)
        if not (stop < entry < take if sign == 1 else take < entry < stop):
            rejected.append((signal_ts, 'invalid_entry_or_protection'))
            continue
        quantity = amount / entry
        initial_fee = quantity * entry * fees
        if quantity * abs(entry - stop) + initial_fee > equity * risk:
            rejected.append((signal_ts, 'risk_cap'))
            continue
        position = Position(signal.side, stop, take, dict(signal.reason), opened_at=row['ts'])
        position.entry_qty = position.qty = quantity
        position.cost_basis = quantity * entry
        position.entry_fees = position.fees = initial_fee
        position.fills.append(Fill('ohlc-entry-' + str(start), row['ts'], 'scenario_market',
                                   'entry', quantity, entry, initial_fee,
                                   quantity * row['open'] * slip, 'signal'))
        exit_index = None
        for index in range(start, len(candles)):
            bar = candles[index]
            low, high, opening = bar['low'], bar['high'], bar['open']
            move_low = sign * (low - entry) * quantity
            move_high = sign * (high - entry) * quantity
            position.mae = min(position.mae, move_low, move_high)
            position.mfe = max(position.mfe, move_low, move_high)
            stop_hit = low <= stop if sign == 1 else high >= stop
            target_hit = high >= take if sign == 1 else low <= take
            if not stop_hit and not target_hit:
                continue
            reason = 'stop' if stop_hit else 'take'
            observed = (min(stop, opening) if sign == 1 else max(stop, opening)) if stop_hit else take
            exit_fill = observed * (1 - sign * (half_spread + slip))
            exit_fee = quantity * exit_fill * fees
            funding = D(0)
            if funding_events is not None:
                for event in funded:
                    if row['ts'] < utc(event.ts) <= bar['ts']:
                        if event.mark_price is None:
                            raise ValueError('missing funding mark')
                        funding += sign * quantity * number(event.mark_price) * number(event.rate)
            gross = sign * quantity * (exit_fill - entry)
            net = gross - initial_fee - exit_fee - funding
            position.realized = gross
            position.fees += exit_fee
            position.funding = funding
            position.qty = D(0)
            position.close_reason = reason
            position.fills.append(Fill('ohlc-exit-' + str(index), bar['ts'], 'scenario_market',
                                       'exit', quantity, exit_fill, exit_fee,
                                       quantity * observed * slip, reason))
            equity += net
            daily_pnl += net
            if bar['ts'].date() != day:
                day = bar['ts'].date()
                day_start_equity = equity - net
                daily_pnl = net
            outcome = {'opened_at': row['ts'], 'closed_at': bar['ts'], 'direction': signal.side,
                       'entry': entry, 'exit': exit_fill, 'qty': quantity, 'stop': stop,
                       'take': take, 'gross_pnl': gross, 'fees': position.fees,
                       'funding': funding, 'net_pnl': net, 'outcome': reason,
                       'reason': dict(signal.reason), 'mae': position.mae, 'mfe': position.mfe,
                       'quality': 'OHLC_CONSERVATIVE_SCENARIO_NOT_EXECUTION_PROOF'}
            outcomes.append(outcome)
            positions.append(position)
            exit_index = index
            break
        if exit_index is None:
            rejected.append((signal_ts, 'position_unresolved_at_end'))
            next_available = len(candles)
        else:
            next_available = exit_index + 1
    return {
        'deals': outcomes, 'metrics': equity_metrics(positions, initial_equity=initial_equity),
        'rejected': rejected, 'assumption': 'pessimistic_stop_first_next_bar_taker',
        'funding_coverage': 'provided_not_completeness_proven' if funding_events is not None
                            else 'unverified_no_funding_assumed',
        'execution_provenance': 'hypothetical_OHLC_no_confirmed_liquidity',
    }
