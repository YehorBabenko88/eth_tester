from datetime import timedelta
from decimal import Decimal as D
import pytest

from eth_tester.simulation.persistence import (
    advance_state, lock_state, mark_funding_event, observation_time,
    record_fill, validate_transition,
)
from test_simulation_execution import BASE


class FakeDB:
    def __init__(self, row):
        self.row = row
        self.queries = []
    def execute(self, sql, params=None):
        self.queries.append((sql, params))
        return self
    def fetchone(self):
        return self.row


def test_lock_identity_version_cursor_and_atomic_order():
    db = FakeDB({'paper_trade_id': None, 'version': 4,
                 'pending_exit': False, 'processed_cursor': BASE})
    state = lock_state(db)
    assert 'FOR UPDATE' in db.queries[-1][0]
    key = record_fill(db, trade_id=7, symbol='ETHUSDT', event_id='q:1', at=BASE,
                      observed_at=BASE, side='Buy', action='entry',
                      qty=D('2'), price=D('100.1'), fee=D('.1'),
                      slippage_cost=D('.05'))
    version = advance_state(db, state, trade_id=7, at=BASE, action='open')
    assert version == 5
    assert 'INSERT INTO paper_order_events' in db.queries[1][0]
    assert 'UPDATE paper_position_state' in db.queries[2][0]
    assert db.queries[-1][1][-1] == 4
    other = FakeDB(None)
    assert record_fill(other, trade_id=7, symbol='ETHUSDT', event_id='q:1',
                       at=BASE, observed_at=BASE, side='Buy', action='entry',
                       qty=2, price=100.1, fee=.1, slippage_cost=.05) == key


def test_transition_rejects_wrong_active_trade_and_regressing_time():
    state = {'paper_trade_id': 11, 'version': 8,
             'processed_cursor': BASE + timedelta(seconds=10)}
    with pytest.raises(RuntimeError):
        validate_transition(state, trade_id=12,
                            at=BASE + timedelta(seconds=11), action='close')
    with pytest.raises(RuntimeError):
        validate_transition(state, trade_id=11, at=BASE, action='close')
    with pytest.raises(RuntimeError):
        validate_transition(state, trade_id=11,
                            at=BASE + timedelta(seconds=11), action='open')


def test_funding_is_mark_based_and_provenance_persisted():
    db = FakeDB(None)
    amount = mark_funding_event(db, trade_id=5, funding_at=BASE,
                                observed_at=BASE, rate=D('.001'),
                                mark_price=D(100), position_qty=D(2),
                                direction='short', provenance={'origin': 'ticker'})
    assert amount == D('-.2')
    query, values = db.queries[-1]
    assert 'paper_funding_events' in query
    assert values[5] == amount and 'ticker' in values[-1]
    with pytest.raises(ValueError):
        mark_funding_event(db, trade_id=5, funding_at=BASE,
                           observed_at=BASE, rate=D('.001'), mark_price=0,
                           position_qty=2, direction='long')


def test_funding_and_fill_future_observation_rejected():
    db = FakeDB(None)
    with pytest.raises(ValueError):
        record_fill(db, trade_id=1, symbol='ETHUSDT', event_id='x', at=BASE,
                    observed_at=BASE - timedelta(seconds=1),
                    side='Buy', action='entry', qty=1, price=100,
                    fee=0, slippage_cost=0)
    assert observation_time(BASE) >= BASE


def test_legacy_close_transition_never_adopts_an_active_entry():
    state = {'paper_trade_id': None, 'version': 7, 'processed_cursor': BASE}
    later = BASE + timedelta(seconds=1)
    assert validate_transition(state, trade_id=91, at=later,
                               action='close_legacy') == later
    db = FakeDB(None)
    assert advance_state(db, state, trade_id=91, at=later,
                         action='close_legacy') == 8
    assert db.queries[-1][1] == (None, later, 7)
    state['paper_trade_id'] = 91
    with pytest.raises(RuntimeError, match='legacy'):
        validate_transition(state, trade_id=91, at=later,
                            action='close_legacy')
