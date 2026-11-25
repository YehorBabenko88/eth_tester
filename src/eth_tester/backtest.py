"""Causal OHLC scenario backtest; real tick replay is in simulation.replay.

OHLC fills are hypothetical, not exchange executions. Funding without a verified
mark is reported unverified and excluded, never silently invented.
"""
from decimal import Decimal
import json

from .database import connection
from .simulation.execution import Costs
from .simulation.ohlc import simulate_ohlc_fallback
from .simulation.replay import FundingEvent, SignalEvent
from .strategy import price_signal


D = Decimal


def _step(tf):
    if str(tf).isdigit() and int(tf) > 0:
        return int(tf) * 60
    return {'D': 86400, 'W': 604800}.get(str(tf))


def simulate_report(candles, fees=D('0.00055'), slip=D('2'),
                    notional=D('1000'), funding_events=None, *,
                    spread_bps=D('2'), initial_equity=D('10000'), tf_seconds=None):
    """Return full scenario provenance, metrics, rejected signals and deals."""
    if len(candles) < 21:
        raise ValueError('at least 21 causally ordered OHLC candles required')
    if tf_seconds is None:
        tf_seconds = (candles[1]['ts'] - candles[0]['ts']).total_seconds()
    if tf_seconds <= 0:
        raise ValueError('invalid timeframe')
    signals = []
    for i in range(20, len(candles)):
        signal = price_signal(candles[:i], fees, slip)
        if signal is not None:
            signals.append(SignalEvent(candles[i - 1]['ts'], signal.direction,
                                       signal.stop, signal.take, reason=signal.reason))
    # Funding table historically has only {ts,rate}; a settlement mark cannot
    # be derived from entry price without changing the accounting claim.
    event_list = None
    funding_quality = 'unverified_missing_funding_history'
    if funding_events is not None:
        if all(e.get('mark_price') is not None for e in funding_events):
            event_list = [FundingEvent(e['ts'], e['rate'], e['mark_price'],
                                       str(e.get('event_id', e['ts']))) for e in funding_events]
            funding_quality = 'events_with_mark_supplied_not_completeness_proven'
        else:
            funding_quality = 'unverified_funding_mark_unavailable'
    report = simulate_ohlc_fallback(
        candles, signals, timeframe_seconds=tf_seconds,
        costs=Costs(taker=fees, slippage_bps=slip), spread_bps=spread_bps,
        notional=notional, initial_equity=initial_equity, funding_events=event_list,
    )
    report['funding_coverage'] = funding_quality
    report['price_source'] = 'closed_OHLC_scenario_only'
    return report


def simulate(candles, fees=D('0.00055'), slip=D('2'),
             notional=D('1000'), funding_events=None):
    """Compatibility API: return deals with explicit scenario-quality metadata."""
    return simulate_report(candles, fees, slip, notional, funding_events)['deals']


def run_backtest(settings, tf='5'):
    seconds = _step(tf)
    if seconds is None:
        raise ValueError('unsupported OHLC timeframe: supply fixed-minute, D or W')
    with connection(settings) as conn:
        bars = conn.execute("""SELECT ts,open,high,low,close,volume,closed,quality,source
          FROM candles WHERE symbol=%s AND kind='trade' AND tf=%s AND closed ORDER BY ts""",
          (settings.symbol, tf)).fetchall()
        funding = conn.execute("SELECT ts,rate FROM funding WHERE symbol=%s ORDER BY ts",
                               (settings.symbol,)).fetchall()
    if len(bars) < 50:
        raise RuntimeError('At least 50 closed bars required')
    if any(bar['quality'] not in ('exchange', 'complete', 'verified') for bar in bars):
        raise RuntimeError('OHLC source quality not verified')
    report = simulate_report(bars, settings.taker_fee_rate, settings.slippage_bps,
                             settings.initial_notional_usd, funding, tf_seconds=seconds,
                             initial_equity=settings.initial_equity_usd)
    deals = report['deals']
    summary = report['metrics']
    total = summary['net_pnl']
    coverage = 'ohlc_scenario_' + report['funding_coverage']
    config = {'notional': str(settings.initial_notional_usd),
              'taker_fee': str(settings.taker_fee_rate),
              'slippage_bps': str(settings.slippage_bps),
              'spread_bps': '2', 'intrabar': 'stop_first',
              'quality': 'OHLC_CONSERVATIVE_SCENARIO_NOT_EXECUTION_PROOF'}
    stats = {key: value for key, value in summary.items() if key != 'equity_curve'}
    stats['equity_curve'] = summary['equity_curve']
    stats['rejected_signals'] = len(report['rejected'])
    with connection(settings) as conn:
        run = conn.execute("""INSERT INTO backtest_runs(symbol,strategy,tf,config,data_start,
          data_end,completeness,trades,pnl,stats)
          VALUES(%s,'pivot_rejection_v1',%s,%s::jsonb,%s,%s,%s,%s,%s,%s::jsonb)
          RETURNING id""",
          (settings.symbol, tf, json.dumps(config), bars[0]['ts'], bars[-1]['ts'],
           coverage, len(deals), total, json.dumps(stats, default=str))).fetchone()
        for i, result in enumerate(deals, 1):
            conn.execute("""INSERT INTO backtest_trades(run_id,seq,opened_at,closed_at,
              direction,entry,exit,stop,take,qty,gross_pnl,fees,funding,net_pnl,outcome,reason)
              VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
              (run['id'], i, result['opened_at'], result['closed_at'],
               result['direction'], result['entry'], result['exit'], result['stop'],
               result['take'], result['qty'], result['gross_pnl'], result['fees'],
               result['funding'], result['net_pnl'], result['outcome'],
               json.dumps({'signal': result['reason'], 'quality': result['quality'],
                           'mae': str(result['mae']), 'mfe': str(result['mfe'])}, default=str)))
    return run['id'], len(deals), total, coverage
