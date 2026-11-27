"""D R40: original complete runner function with isolated fake transaction.
These tests DO NOT run PostgreSQL, real paper ledger or an exchange.
"""
import asyncio
import copy
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from eth_tester import runner


class StopHarness(Exception):
    pass


class Result:
    def __init__(self, value):
        self.value = value

    def fetchone(self):
        return self.value


class SimulatedTransactionalStore:
    """Pure mock. Database writes are staged until a context manager commits."""
    def __init__(self, *, existing_status='monitoring_position', exposed=False,
                 error_on_conflict_warning=False):
        self.status = existing_status
        self.exposed = exposed
        self.events = []
        self.snapshots = []
        self.unexpected_sql = []
        self.fail_warning = error_on_conflict_warning
        self.tx = None

    @contextmanager
    def connection(self, _settings):
        assert self.tx is None
        self.tx = {'status': self.status, 'events': copy.deepcopy(self.events)}
        try:
            yield self
        except Exception:
            self.tx = None  # fake rollback
            raise
        else:
            self.status = self.tx['status']
            self.events = self.tx['events']
            self.snapshots.append((self.status, tuple(copy.deepcopy(self.events))))
            self.tx = None

    def execute(self, sql, args=()):
        if "FROM paper_trades" in sql and "state='open'" in sql:
            return Result({'exists': True} if self.exposed else None)
        if "SELECT status FROM bot_heartbeat" in sql:
            return Result({'status': self.tx['status']} if self.tx['status'] is not None else None)
        if 'INSERT INTO bot_heartbeat' in sql:
            status, detail = args
            parsed = json.loads(detail)
            assert parsed['open_position_risk'] is self.exposed
            assert parsed['automatic_fake_exit'] is False
            self.tx['status'] = status
            return Result(None)
        if 'INSERT INTO bot_events' in sql:
            import re
            match = re.search(r"VALUES\('(?:warning|error)','([^']+)'", sql)
            assert match, sql
            name = match.group(1)
            if name == 'paper_close_conflict' and self.fail_warning:
                raise RuntimeError('SIMULATED_DB_EVENT_FAILURE')
            self.tx['events'].append((name,json.loads(args[0])))
            return Result(None)
        self.unexpected_sql.append(sql)
        raise AssertionError('Unexpected or mutating financial SQL: '+sql[:140])


async def run_states(monkeypatch, store, states):
    sequence = iter(states)

    def tick(_settings):
        return next(sequence)

    monkeypatch.setattr(runner, 'connection', store.connection)
    monkeypatch.setattr(runner, 'engine_tick', tick)
    monkeypatch.setattr(runner, 'record_closed_bar_research', lambda settings: None)
    monkeypatch.setattr(runner, 'review_one_closed_trade', lambda settings: None)
    count = 0

    async def stop_on_last_cycle(_seconds):
        nonlocal count
        count += 1
        if count >= len(states):
            raise StopHarness

    monkeypatch.setattr(runner.asyncio, 'sleep', stop_on_last_cycle)
    with pytest.raises(StopHarness):
        await runner.run_engine(SimpleNamespace(symbol='ETHUSDT'))


def test_transition_from_monitoring_to_conflict_emits_once(monkeypatch):
    s=SimulatedTransactionalStore()
    asyncio.run(run_states(monkeypatch,s,['paper_close_conflict','paper_close_conflict']))
    assert s.status=='paper_close_conflict'
    assert [e[0] for e in s.events]==['paper_close_conflict']
    assert s.unexpected_sql==[]


def test_repeated_conflict_after_distinct_status_is_new_event(monkeypatch):
    s=SimulatedTransactionalStore()
    asyncio.run(run_states(monkeypatch,s,['paper_close_conflict','monitoring_position','paper_close_conflict']))
    assert [e[0] for e in s.events]==['paper_close_conflict','paper_close_conflict']
    assert len(s.snapshots)==3


def test_healthy_close_never_emits_conflict(monkeypatch):
    s=SimulatedTransactionalStore()
    asyncio.run(run_states(monkeypatch,s,['closed_41','monitoring_position']))
    assert s.events==[]
    assert s.status=='monitoring_position'


@pytest.mark.parametrize('exposed',[False,True])
def test_transition_payload_truthfully_reflects_DB_exposure(monkeypatch,exposed):
    s=SimulatedTransactionalStore(exposed=exposed)
    asyncio.run(run_states(monkeypatch,s,['paper_close_conflict']))
    k,payload=s.events[0]
    assert k=='paper_close_conflict'
    assert payload=={'status':'paper_close_conflict','symbol':'ETHUSDT',
        'paper_only':True,'open_position_risk':exposed,
        'close_committed_by_this_tick':False,
        'financial_results_certified':False}
    assert s.unexpected_sql==[]


def test_event_failure_rolls_back_heartbeat_without_ledger_write(monkeypatch):
    s=SimulatedTransactionalStore(error_on_conflict_warning=True)
    asyncio.run(run_states(monkeypatch,s,['paper_close_conflict']))
    assert s.status=='monitoring_position'
    assert s.events==[]
    assert s.snapshots==[]
    assert s.unexpected_sql==[]


def test_failed_warning_can_be_retried_after_recovery(monkeypatch):
    s=SimulatedTransactionalStore(error_on_conflict_warning=True)
    asyncio.run(run_states(monkeypatch,s,['paper_close_conflict']))
    assert s.events==[]
    s.fail_warning=False
    asyncio.run(run_states(monkeypatch,s,['paper_close_conflict']))
    assert [e[0] for e in s.events]==['paper_close_conflict']


def test_existing_source_failure_emits_only_original_warning(monkeypatch):
    s=SimulatedTransactionalStore(exposed=True)
    asyncio.run(run_states(monkeypatch,s,['engine_error']))
    assert [e[0] for e in s.events]==['paper_source_risk']
    assert s.events[0][1]['open_position_risk'] is True


def test_no_event_for_previously_conflicted_status(monkeypatch):
    s=SimulatedTransactionalStore(existing_status='paper_close_conflict')
    asyncio.run(run_states(monkeypatch,s,['paper_close_conflict']))
    assert s.events==[]
