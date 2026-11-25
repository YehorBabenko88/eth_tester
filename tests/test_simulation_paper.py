"""Legacy settlement guards; no external database involved."""
import importlib
import sys
import types
from datetime import datetime
from decimal import Decimal as D

import pytest

from test_simulation_execution import BASE
from eth_tester.strategies import Parameters
from eth_tester.simulation.identity import require_new_strategy_identity


def paper_stub(monkeypatch):
    fake = types.ModuleType('eth_tester.database')
    fake.connection = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, 'eth_tester.database', fake)
    sys.modules.pop('eth_tester.paper', None)
    return importlib.import_module('eth_tester.paper')


def test_estimate_trade_both_sides_and_invalid(monkeypatch):
    p = paper_stub(monkeypatch)
    l = p.estimate_trade('long', 100, 105, 1000, D('.00055'), D(2))
    s = p.estimate_trade('short', 100, 95, 1000, D('.00055'), D(2))
    assert l['net'] > 0 and s['net'] > 0
    assert l['fees'] > 0 and s['fees'] > 0
    with pytest.raises(ValueError):
        p.estimate_trade('long', 'NaN', 110, 1000, D('.00055'), D(2))
    with pytest.raises(ValueError):
        p.estimate_trade('long', 100, 110, 1000, D('.00055'), D('10000'))


def test_open_uses_actual_event_timestamp_and_version(monkeypatch):
    p = paper_stub(monkeypatch)
    calls = []
    class Result:
        def __init__(self, row): self.row = row
        def fetchone(self): return self.row
    class Conn:
        def __enter__(self): return self
        def __exit__(self, *_): return None
        def execute(self, sql, params=None):
            calls.append((sql, params))
            if 'FROM paper_position_state' in sql:
                return Result({'paper_trade_id': None, 'version': 0,
                               'pending_exit': False, 'processed_cursor': None})
            if 'SELECT enabled' in sql: return Result({'enabled': True, 'paused': False})
            if 'SELECT 1 FROM paper_trades' in sql: return Result(None)
            if 'SELECT equity' in sql: return Result({'equity': D(10000)})
            if 'coalesce(sum' in sql: return Result({'pnl': D(0)})
            if 'INSERT INTO paper_trades' in sql: return Result({'id': 11})
            return Result(None)
    monkeypatch.setattr(p, 'connection', lambda settings: Conn())
    settings = types.SimpleNamespace(symbol='ETHUSDT', initial_notional_usd=D(1000),
        taker_fee_rate=D('0.00055'), slippage_bps=D(2),
        max_daily_drawdown=D('0.03'), risk_fraction=D('0.01'))
    trade_id = p.open_paper(settings, 'long', 100, 98, 105,
                            {'strategy_version': Parameters().version,
                             **Parameters().identity()}, observed_at=BASE)
    assert trade_id == 11
    insert = [params for sql, params in calls if 'INSERT INTO paper_trades' in sql][0]
    assert insert[2] == BASE and insert[3] == BASE
    assert insert[-1] == 'eth_rules_v2'
    assert any('INSERT INTO paper_order_events' in sql for sql, _ in calls)
    assert any('UPDATE paper_position_state' in sql for sql, _ in calls)


def test_close_rejects_nan_before_database(monkeypatch):
    p = paper_stub(monkeypatch)
    with pytest.raises(ValueError):
        p.close_paper(types.SimpleNamespace(), 'NaN')
    with pytest.raises(ValueError):
        p.close_paper(types.SimpleNamespace(), 100, observed_at=datetime(2026, 1, 1))


def test_new_entry_requires_full_parameter_hash(monkeypatch):
    paper = paper_stub(monkeypatch)
    settings = types.SimpleNamespace(symbol='ETHUSDT',
        initial_notional_usd=D(1000), taker_fee_rate=D('0.00055'),
        slippage_bps=D(2))
    with pytest.raises(ValueError, match='identity'):
        paper.open_paper(settings,'long',100,98,105,{'qa': True},observed_at=BASE)
    valid={'strategy_version':Parameters().version,**Parameters().identity()}
    with pytest.raises(ValueError, match='identity'):
        paper.open_paper(settings,'long',100,98,105,
                         {**valid,'parameter_sha256':'0'*64},observed_at=BASE)
    assert require_new_strategy_identity(valid)==valid
