"""Focused regression checks with PostgreSQL query recording stubs."""
import importlib
import sys
import types
from decimal import Decimal as D

from test_simulation_execution import BASE
from eth_tester.simulation.identity import rule_identity, require_new_strategy_identity
from eth_tester.strategies import Parameters, strategy_identity


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
    load_runner(monkeypatch)
    from eth_tester.simulation.crossing import resolve_first_crossing
    first = {'ts': BASE, 'low_cross': D(97), 'high_cross': D(107)}
    choice = resolve_first_crossing(first, 'long', D(98), D(104))
    assert choice['reason'] == 'stop'
    assert choice['price'] == D(97)
    assert choice['ambiguous_same_timestamp']
    assert choice['ts'] == BASE


def test_engine_rule_identity_hash_changes_with_parameters():
    version,meta=rule_identity()
    assert version==Parameters().version+':'+meta['parameter_sha256'][:12]
    reason={'strategy_version':strategy_identity(),
            'rules_version':Parameters().version,**meta}
    assert require_new_strategy_identity(reason)==reason
    changed=Parameters(version='eth_rules_v4')
    assert changed.engine_identity()!=version
    assert strategy_identity(changed)==changed.engine_identity()


def test_no_signal_records_full_rules_identity(monkeypatch):
    runner=load_runner(monkeypatch)
    from datetime import datetime, timedelta, timezone
    from eth_tester.strategies import Parameters
    saved=[]
    now=datetime.now(timezone.utc)
    last=now.replace(second=0,microsecond=0)-timedelta(minutes=1)
    bars=[{'ts':last-timedelta(minutes=34-i),'open':D(100),'high':D(101),
           'low':D(99),'close':D(100),'volume':D(1)} for i in range(35)]
    class Conn:
        def __enter__(self): return self
        def __exit__(self,*a): return None
        def execute(self,sql,params=None):
            if 'INSERT INTO strategy_signals' in sql:
                saved.append(params)
                import json
                return Result({'decision':params[3],'reason':json.loads(params[4])})
            if 'feed_health' in sql: return Result({'status':'connected'})
            if 'ORDER BY ts DESC,trade_id DESC LIMIT 1' in sql:
                return Result({'ts':now-timedelta(seconds=1),'price':D(100)})
            if "paper_trades WHERE state='open'" in sql: return Result(None)
            if 'FROM bot_control' in sql: return Result({'enabled':True,'paused':False})
            if 'FROM data_gaps' in sql: return Result(None)
            if 'FROM candles' in sql: return Result(list(reversed(bars)))
            if 'FROM strategy_signals' in sql: return Result(None)
            if "pg_try_advisory_xact_lock(20991991)" in sql:
                return Result({"acquired": True})
            return Result(None)
    monkeypatch.setattr(runner,'connection',lambda *a:Conn())
    assert runner.engine_tick(types.SimpleNamespace(symbol='ETHUSDT',
        taker_fee_rate=D('.00055'),slippage_bps=D(2)))=='no_signal'
    assert len(saved)==1
    import json
    payload=json.loads(saved[0][-1])
    params=Parameters()
    assert saved[0][2]==strategy_identity()
    assert payload['strategy_version']==strategy_identity()
    assert payload['rules_version']==params.version
    assert payload['parameter_sha256']==params.identity()['parameter_sha256']
    assert payload['parameters']==params.identity()['parameters']
