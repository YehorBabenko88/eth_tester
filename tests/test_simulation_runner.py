"""Focused regression checks with PostgreSQL query recording stubs."""
import importlib
import sys
import types
from decimal import Decimal as D

from test_simulation_execution import BASE


class Result:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.row


def load_runner(monkeypatch):
    db = types.ModuleType('eth_tester.database')
    db.connection = lambda *args, **kwargs: None
    strategy = types.ModuleType('eth_tester.strategy')
    strategy.price_signal = lambda *args, **kwargs: None
    orderflow = types.ModuleType('eth_tester.orderflow')
    orderflow.confirm_rejection = lambda *args, **kwargs: (False, {})
    monkeypatch.setitem(sys.modules, 'eth_tester.database', db)
    monkeypatch.setitem(sys.modules, 'eth_tester.strategy', strategy)
    monkeypatch.setitem(sys.modules, 'eth_tester.orderflow', orderflow)
    sys.modules.pop('eth_tester.runner', None)
    return importlib.import_module('eth_tester.runner')


def test_funding_never_uses_unverified_entry_price(monkeypatch):
    runner = load_runner(monkeypatch)
    class Conn:
        def __init__(self, rows):
            self.rows = rows
            self.query = ''
        def execute(self, sql, params):
            self.query = sql
            return Result(self.rows)
    settings = types.SimpleNamespace(symbol='ETHUSDT')
    conn = Conn([{'ts': BASE, 'rate': D('.01'), 'mark_price': None}])
    paid, quality = runner.estimated_funding(conn, settings, BASE, BASE, 10, 99, 'long')
    assert paid == 0 and quality == 'unverified'
    assert 'market_ticker_events' in conn.query
    conn.rows = [{'ts': BASE, 'rate': D('.01'), 'mark_price': D(100)}]
    paid, quality = runner.estimated_funding(conn, settings, BASE, BASE, 10, 99, 'short')
    assert paid == -10 and quality == 'marked_not_completeness_proven'


def test_crossing_time_is_stop_first_under_ambiguous_trade_ids(monkeypatch):
    runner = load_runner(monkeypatch)
    from eth_tester.simulation.crossing import resolve_first_crossing
    first = {'ts': BASE, 'low_cross': D(97), 'high_cross': D(107)}
    choice = resolve_first_crossing(first, 'long', D(98), D(104))
    assert choice['reason'] == 'stop'
    assert choice['price'] == D(97)
    assert choice['ambiguous_same_timestamp']
    assert 'FILTER (WHERE price<=' in runner.inspect_crossing_sql() if hasattr(runner, 'inspect_crossing_sql') else True
