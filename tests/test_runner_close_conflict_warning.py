"""ETH TESTER D R42: native-repo pytest for conflict warning semantics.

Imports eth_tester.runner and exercises the original run_engine coroutine.
Heartbeat/event storage is replaced with a fake DB adapter; no PostgreSQL,
Telegram push, or financial settlement is performed or certified.
"""
import asyncio
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from eth_tester import runner


class _StopAfterOneCycle(Exception):
    pass


class _Result:
    def __init__(self, row):
        self.row = row
    def fetchone(self):
        return self.row


class _FakeHeartbeatDB:
    def __init__(self, *, exposed, previous=None):
        self.exposed = exposed
        self.status = previous
        self.events = []
        self.heartbeats = []
        self.all_sql = []

    def execute(self, sql, args=()):
        self.all_sql.append((sql, args))
        if "FROM paper_trades" in sql and "state='open'" in sql:
            return _Result({"exists": 1} if self.exposed else None)
        if "SELECT status FROM bot_heartbeat" in sql:
            return _Result({"status": self.status} if self.status is not None else None)
        if "INSERT INTO bot_heartbeat" in sql:
            status, detail = args
            self.status = status
            self.heartbeats.append((status, json.loads(detail)))
            return _Result(None)
        if "INSERT INTO bot_events" in sql:
            import re
            name = re.search(r"VALUES\('(?:warning|error)','([^']+)'", sql)
            assert name, sql
            self.events.append((name.group(1), json.loads(args[0])))
            return _Result(None)
        raise AssertionError("Unexpected fake DB query: " + sql)


def _run_one_cycle(monkeypatch, state, *, exposed=False, previous=None):
    fake = _FakeHeartbeatDB(exposed=exposed, previous=previous)
    @contextmanager
    def connection(_settings):
        yield fake
    monkeypatch.setattr(runner, "connection", connection)
    monkeypatch.setattr(runner, "record_closed_bar_research", lambda s: None, raising=False)
    monkeypatch.setattr(runner, "engine_tick", lambda s: state)
    monkeypatch.setattr(runner, "review_one_closed_trade", lambda s: None, raising=False)
    async def exit_after_cycle(_seconds):
        raise _StopAfterOneCycle()
    monkeypatch.setattr(runner.asyncio, "sleep", exit_after_cycle)
    with pytest.raises(_StopAfterOneCycle):
        asyncio.run(runner.run_engine(SimpleNamespace(symbol="ETHUSDT")))
    return fake


@pytest.mark.parametrize("exposed", [False, True])
def test_new_conflict_event_distinct_and_heartbeat_risk(monkeypatch, exposed):
    db = _run_one_cycle(monkeypatch, "paper_close_conflict", exposed=exposed)
    assert db.status == "paper_close_conflict"
    assert len(db.heartbeats) == 1
    assert db.heartbeats[0][1] == {
        "paper_only": True, "open_position_risk": exposed,
        "automatic_fake_exit": False,
    }
    assert db.events == [(
        "paper_close_conflict",
        {"status": "paper_close_conflict", "symbol": "ETHUSDT",
         "paper_only": True, "open_position_risk": exposed,
         "close_committed_by_this_tick": False,
         "financial_results_certified": False},
    )]
    assert not any(e[0] == "paper_source_risk" for e in db.events)


def test_no_duplicate_alert_for_unchanged_conflict_status(monkeypatch):
    db = _run_one_cycle(monkeypatch, "paper_close_conflict", previous="paper_close_conflict")
    assert db.status == "paper_close_conflict"
    assert db.events == []


def test_normal_close_does_not_alert(monkeypatch):
    db = _run_one_cycle(monkeypatch, "closed_41")
    assert db.status == "closed_41" and db.events == []


def test_existing_source_warning_is_unchanged(monkeypatch):
    db = _run_one_cycle(monkeypatch, "engine_error", exposed=True)
    assert len(db.events) == 1
    assert db.events[0][0] == "paper_source_risk"
    assert db.events[0][1]["open_position_risk"] is True
