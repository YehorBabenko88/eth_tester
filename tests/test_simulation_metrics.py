from decimal import Decimal as D

import pytest

from eth_tester.simulation import PaperEngine
from eth_tester.simulation.metrics import daily_sharpe, equity_metrics
from test_simulation_execution import q


def positions_two_results():
    e = PaperEngine()
    e.submit('long', 98, 104)
    e.quote(q(0))
    e.quote(q(1, bid='105', ask='105.1', last='105'))
    e.submit('short', 110, 96)
    e.quote(q(2, bid='100', ask='100.1', last='100'))
    e.quote(q(3, bid='111', ask='111.1', last='111'))
    return e.history


def test_metrics_net_cost_and_drawdown():
    positions = positions_two_results()
    assert len(positions) == 2
    m = equity_metrics(positions)
    assert m['trades'] == 2
    assert m['net_pnl'] == sum((p.realized - p.fees - p.funding for p in positions), D(0))
    assert m['profit_factor'] is not None
    assert m['win_rate'] == D('0.5')
    assert m['max_drawdown'] > 0
    assert m['sharpe_annualized'] is None
    assert m['equity_curve'][-1]['equity'] == m['ending_equity']
    assert m['mae_mean'] <= 0 <= m['mfe_mean']


def test_open_positions_rejected_from_metrics():
    e = PaperEngine()
    e.submit('long', 98, 104)
    e.quote(q(0))
    with pytest.raises(ValueError):
        equity_metrics([e.position])


def test_empty_metrics_not_inf():
    m = equity_metrics([])
    assert m['profit_factor'] is None and m['win_rate'] is None
    assert m['net_pnl'] == 0
    assert daily_sharpe([]) is None
