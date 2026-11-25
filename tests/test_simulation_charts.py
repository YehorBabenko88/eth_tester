import importlib
import sys
import types
from datetime import timedelta
from decimal import Decimal as D

from test_simulation_execution import BASE


def test_png_entry_exit_evidence(tmp_path, monkeypatch):
    stub = types.ModuleType('eth_tester.database')
    stub.connection = lambda *a, **kw: None
    monkeypatch.setitem(sys.modules, 'eth_tester.database', stub)
    sys.modules.pop('eth_tester.charts', None)
    chart = importlib.import_module('eth_tester.charts')
    bars = [{'ts': BASE + timedelta(minutes=i), 'open': D(100),
             'high': D(102), 'low': D(98), 'close': D(100)} for i in range(6)]
    deals = [{'opened_at': bars[2]['ts'], 'closed_at': bars[3]['ts'],
              'entry': D(100), 'exit': D(98), 'direction': 'long',
              'stop': D(98), 'take': D(105), 'outcome': 'stop'}]
    output = chart.render_backtest_evidence(bars, deals, tmp_path, run_id='../bad')
    from pathlib import Path
    first, second = (Path(output[0][k]) for k in ('entry_chart', 'exit_chart'))
    assert first.is_file() and second.is_file()
    assert first.read_bytes()[:8] == b'\x89PNG\r\n\x1a\n'
    assert second.read_bytes()[:8] == b'\x89PNG\r\n\x1a\n'
    assert first.read_bytes() != second.read_bytes()
    assert first.parent == second.parent == tmp_path
