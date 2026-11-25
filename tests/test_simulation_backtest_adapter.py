"""Backtest adapter checks do not require an actual PostgreSQL installation."""
import importlib
import sys
import types
from datetime import timedelta
from decimal import Decimal as D

import pytest

from test_simulation_execution import BASE


def backtest_with_stub(monkeypatch):
    db = types.ModuleType('eth_tester.database')
    db.connection = lambda *a, **kw: None
    strategy = types.ModuleType('eth_tester.strategy')
    strategy.price_signal = lambda *a, **kw: None
    monkeypatch.setitem(sys.modules, 'eth_tester.database', db)
    monkeypatch.setitem(sys.modules, 'eth_tester.strategy', strategy)
    sys.modules.pop('eth_tester.backtest', None)
    return importlib.import_module('eth_tester.backtest')


def test_backtest_adapter_uses_next_bar_and_quality(monkeypatch):
    bt = backtest_with_stub(monkeypatch)
    candles = [{'ts': BASE + timedelta(minutes=i), 'open': D(100), 'high': D(101),
                'low': D(99), 'close': D(100)} for i in range(32)]
    candles[20]['low'], candles[20]['high'] = D(95), D(105)
    bt.price_signal = lambda c, *a: types.SimpleNamespace(direction='long', stop=D(98),
                                      take=D(104), reason={'source': 'unit'}) if len(c) == 20 else None
    report = bt.simulate_report(candles)
    assert len(report['deals']) == 1
    assert report['deals'][0]['opened_at'] == candles[20]['ts']
    assert report['deals'][0]['outcome'] == 'stop'
    assert report['deals'][0]['quality'].startswith('OHLC_')
    assert 'unverified' in report['funding_coverage']


def test_backtest_adapter_fails_closed_on_gap(monkeypatch):
    bt = backtest_with_stub(monkeypatch)
    candles = [{'ts': BASE + timedelta(minutes=i), 'open': D(100), 'high': D(101),
                'low': D(99), 'close': D(100)} for i in range(30)]
    candles[10]['ts'] += timedelta(minutes=1)
    with pytest.raises(ValueError):
        bt.simulate_report(candles)
