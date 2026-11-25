from datetime import timedelta
from decimal import Decimal as D

import pytest

from eth_tester.simulation.replay import (
    FundingEvent, SignalEvent, holdout_split, replay_ticks,
    walk_forward_evaluate, walk_forward_splits,
)
from test_simulation_execution import BASE, q


def test_causal_replay_and_position_pnl():
    signals = [SignalEvent(BASE, 'long', 98, 104, reason={'features': {'poc': 100}})]
    result = replay_ticks([q(0), q(1), q(2, bid='105', ask='105.1', last='105')], signals)
    assert result['metrics']['trades'] == 1
    assert result['engine'].history[0].opened_at == BASE + timedelta(seconds=1)
    assert result['engine'].history[0].reason['features']['poc'] == 100
    assert result['funding_coverage'] == 'not_provided'
    assert not result['gap_blocked']


def test_tick_replay_rejects_trade_only_inputs():
    with pytest.raises(TypeError):
        replay_ticks([{'ts': BASE, 'price': D(100), 'qty': D(2)}])


def test_replay_gap_and_open_position_are_flagged():
    signals = [SignalEvent(BASE, 'long', 98, 104)]
    outcome = replay_ticks([q(0), q(1), q(50, bid='105', ask='105.1', last='105')], signals)
    assert outcome['gap_blocked'] and outcome['open_at_end']
    assert outcome['metrics']['trades'] == 0


def test_funding_only_with_true_mark():
    signals = [SignalEvent(BASE, 'long', 98, 104)]
    f = FundingEvent(BASE + timedelta(seconds=2), D('0.01'), D(100), 'event-1')
    r = replay_ticks([q(0), q(1), q(3, bid='105', ask='105.1', last='105')],
                     signals, funding_events=[f])
    assert r['engine'].history[0].funding > 0
    with pytest.raises(ValueError):
        replay_ticks([q(0), q(1), q(3)], signals,
                     funding_events=[FundingEvent(f.ts, f.rate, None, f.event_id)])


def test_wfo_and_holdout_have_no_time_overlap():
    rows = [{'ts': BASE + timedelta(minutes=i), 'value': i} for i in range(30)]
    folds = walk_forward_splits(rows, 10, 5, purge=2, embargo=1)
    assert folds[0]['train'] == (0, 10)
    assert folds[0]['test'] == (12, 17)
    assert folds[1]['train'] == (6, 16)
    trained = []
    def fit(train):
        trained.append(max(r['value'] for r in train))
        return trained[-1]
    reports = walk_forward_evaluate(rows, train_size=10, test_size=5, purge=2,
                                    fit=fit, evaluate=lambda test, p: (test[0]['value'], p))
    assert all(result['oos'][0] > result['oos'][1] for result in reports)
    train, test = holdout_split(rows, train_fraction=.7, purge=2)
    assert train[-1]['ts'] < test[0]['ts']


def test_wfo_rejects_unsorted():
    with pytest.raises(ValueError):
        walk_forward_splits([{'ts': BASE}, {'ts': BASE}], 1, 1)


def test_overlapping_out_of_sample_windows_rejected():
    rows = [{'ts': BASE + timedelta(minutes=i), 'value': i} for i in range(40)]
    with pytest.raises(ValueError, match='overlapping OOS'):
        walk_forward_splits(rows, train_size=12, test_size=6, step=3)


def test_fit_cannot_mutate_future_original_records():
    records = [{'ts': BASE + timedelta(minutes=i), 'value': i} for i in range(35)]
    def fit(rows):
        rows[0]['value'] = -999
        return {'threshold': rows[-1]['value']}
    def evaluate(rows, model):
        rows[0]['value'] = -888
        model['threshold'] = -777
        return True
    reports = walk_forward_evaluate(records, train_size=10, test_size=5,
                                    fit=fit, evaluate=evaluate)
    assert reports and [row['value'] for row in records] == list(range(35))


def test_prequote_funding_is_explicitly_rejected_not_fabricated():
    event = FundingEvent(BASE, D('.001'), D(100), 'early')
    report = replay_ticks([q(0), q(1)], funding_events=[event])
    assert report['funding_rejected'] == [(BASE, 'no_prior_market_quote')]
    assert report['engine'].equity == D(10000)
