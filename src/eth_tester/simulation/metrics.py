"""Closed-trade performance: never annualize a trade Sharpe without a time basis."""
from decimal import Decimal
from math import sqrt
from statistics import mean, stdev

from .execution import number


D = Decimal


def equity_metrics(positions, *, initial_equity=D('10000')):
    """Calculate net metrics on closed positions; excludes unrealized PnL."""
    starting = number(initial_equity)
    if starting <= 0:
        raise ValueError('initial equity must be positive')
    curve = [{'at': None, 'equity': starting, 'drawdown': D(0)}]
    gains = losses = D(0)
    peak = equity = starting
    total_net = D(0)
    max_dd = D(0)
    nets = []
    excursions_adverse = []
    excursions_favorable = []
    for p in positions:
        if p.qty != 0 or not p.fills or p.fills[-1].action != 'exit':
            raise ValueError('only fully closed positions can be measured')
        net = number(p.realized) - number(p.fees) - number(p.funding)
        total_net += net
        equity = starting + total_net
        peak = max(peak, equity)
        drawdown = (peak - equity) / peak if peak > 0 else D(1)
        max_dd = max(max_dd, drawdown)
        curve.append({'at': p.fills[-1].ts, 'equity': equity, 'drawdown': drawdown})
        gains += max(net, D(0))
        losses += max(-net, D(0))
        nets.append(net)
        excursions_adverse.append(number(p.mae))
        excursions_favorable.append(number(p.mfe))
    count = len(nets)
    returns = [float(v / starting) for v in nets]
    trade_sharpe = (mean(returns) / stdev(returns) if len(returns) > 1
                    and stdev(returns) > 0 else None)
    return {
        'trades': count,
        'net_pnl': total_net,
        'ending_equity': equity,
        'equity_curve': curve,
        'profit_factor': gains / losses if losses else None,
        'win_rate': D(sum(n > 0 for n in nets)) / count if count else None,
        'expectancy': sum(nets, D(0)) / count if count else None,
        'sharpe_per_trade': trade_sharpe,
        'sharpe_annualized': None,
        'max_drawdown': max_dd,
        'mae_mean': sum(excursions_adverse, D(0)) / count if count else None,
        'mfe_mean': sum(excursions_favorable, D(0)) / count if count else None,
        'wins': sum(n > 0 for n in nets),
        'losses': sum(n < 0 for n in nets),
        'breakeven': sum(n == 0 for n in nets),
        'sample_standard_deviation': stdev(returns) if len(returns) > 1 else None,
        'confidence': 'closed_trades_only',
    }


def daily_sharpe(equity_by_day, *, days_per_year=365):
    """Annualized Sharpe only for consecutive, equally spaced daily equity points."""
    if days_per_year <= 0 or len(equity_by_day) < 3:
        return None
    returns = []
    for (d0, v0), (d1, v1) in zip(equity_by_day, equity_by_day[1:]):
        if (d1 - d0).days != 1 or number(v0) <= 0:
            return None
        returns.append(float(number(v1) / number(v0) - 1))
    std = stdev(returns)
    return mean(returns) / std * sqrt(days_per_year) if std > 0 else None
