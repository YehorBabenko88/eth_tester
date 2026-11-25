from datetime import timedelta
from decimal import Decimal as D

import pytest

from eth_tester.simulation.ohlc import simulate_ohlc_fallback
from eth_tester.simulation.replay import SignalEvent
from test_simulation_execution import BASE


def bars():
    return [{'ts': BASE + timedelta(minutes=i), 'open': D(100), 'high': D(101),
             'low': D(99), 'close': D(100), 'closed': True} for i in range(8)]


def test_ohlc_next_bar_stop_first_costs_and_funding_status():
    c = bars()
    c[3]['low'], c[3]['high'] = D(95), D(108)
    result = simulate_ohlc_fallback(c, [SignalEvent(c[2]['ts'], 'long', 98, 104)],
                                    timeframe_seconds=60)
    assert len(result['deals']) == 1
    trade = result['deals'][0]
    assert trade['opened_at'] == c[3]['ts']
    assert trade['outcome'] == 'stop'
    assert trade['net_pnl'] < 0 and trade['fees'] > 0
    assert trade['quality'].startswith('OHLC_CONSERVATIVE')
    assert result['funding_coverage'].startswith('unverified')


def test_ohlc_short_gap_stop_is_adverse_open_not_stop():
    c = bars()
    c[4].update(open=D(110), high=D(112), low=D(109), close=D(110))
    result = simulate_ohlc_fallback(c, [SignalEvent(c[2]['ts'], 'short', 103, 96)],
                                    timeframe_seconds=60)
    trade = result['deals'][0]
    assert trade['outcome'] == 'stop'
    assert trade['exit'] >= D('110')


def test_ohlc_gap_or_unclosed_rejects():
    c = bars()
    c[3]['ts'] += timedelta(minutes=2)
    with pytest.raises(ValueError):
        simulate_ohlc_fallback(c, [], timeframe_seconds=60)
    c = bars()
    c[1]['closed'] = False
    with pytest.raises(ValueError):
        simulate_ohlc_fallback(c, [], timeframe_seconds=60)


def test_ohlc_limit_without_queue_proof_never_filled():
    c = bars()
    outcome = simulate_ohlc_fallback(c, [SignalEvent(c[2]['ts'], 'long', 98, 110,
                                                    order_type='limit', limit=99)],
                                     timeframe_seconds=60)
    assert not outcome['deals']
    assert outcome['rejected'][0][1] == 'OHLC_limit_queue_unverifiable'
