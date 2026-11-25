from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from eth_tester.book_analysis import (
    apply_updates, parse_levels, significant, snapshot_metrics,
)


NOW = datetime(2026, 11, 25, 17, tzinfo=timezone.utc)


def test_orderbook_snapshot_fresh_and_uncrossed():
    metrics = snapshot_metrics(
        {"100": "5", "99": "2"}, {"101": "3", "102": "1"},
        observed_at=NOW - timedelta(seconds=3), as_of=NOW)
    assert metrics["valid"]
    assert metrics["spread"] == Decimal(1)
    assert metrics["imbalance"] == Decimal(3) / Decimal(11)
    assert metrics["age_seconds"] == 3


@pytest.mark.parametrize("age,reason", [
    (timedelta(seconds=11), "stale_or_future_book"),
    (timedelta(seconds=-1), "stale_or_future_book"),
])
def test_orderbook_freshness_rejects_stale_and_future(age, reason):
    assert snapshot_metrics(
        {"100": "5"}, {"101": "3"},
        observed_at=NOW - age, as_of=NOW)["reason"] == reason


def test_orderbook_crossed_book_fails_closed():
    assert snapshot_metrics(
        {"101": "5"}, {"100": "3"},
        observed_at=NOW, as_of=NOW)["valid"] is False


def test_decreased_order_quantity_is_not_proven_execution():
    book = parse_levels([["100", "60"]])
    changes = apply_updates(book, [["100", "0"]])
    assert book == {}
    assert significant(changes, "bid", Decimal(50)) == [
        ("bid", "100", Decimal(60), Decimal(0), "vanished_unknown")]
    with pytest.raises(ValueError):
        apply_updates({}, [["100", "-1"]])



class _ReplayCursor:
    def __init__(self, row=None, rows=None):
        self.row, self.rows = row, rows
    def fetchone(self):
        return self.row
    def fetchall(self):
        return self.rows or []


def _fake_replay_engine(monkeypatch, events):
    """Deterministic bounded DB model; real race acceptance is separate PG test."""
    from types import SimpleNamespace
    from eth_tester import book_analysis
    state = {"row": None, "events": events, "gaps": [], "calls": [], "leases": []}
    class Conn:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def execute(self, query, params=None):
            state["calls"].append(query)
            if "SET LOCAL lock_timeout" in query:
                assert "2000ms" in query
                return _ReplayCursor()
            if "pg_advisory_xact_lock" in query:
                assert params in (
                    ("eth_paper_admission_source:ETHUSDT",),
                    ("eth_l2_replay:ETHUSDT",))
                state["leases"].append(params[0])
                return _ReplayCursor()
            if "FROM market_book_state" in query:
                return _ReplayCursor(row=state["row"])
            if "FROM book_events" in query:
                last, limit = params[1], params[2]
                return _ReplayCursor(rows=[
                    x for x in events if x["id"] > last][:limit])
            if "INSERT INTO data_gaps" in query:
                state["gaps"].append(params)
                return _ReplayCursor()
            if "INSERT INTO orderbook_wall_events" in query:
                return _ReplayCursor()
            if "INSERT INTO market_book_state" in query:
                symbol, last, bids, asks, last_u, valid, received = params
                import json
                if state["row"] is None or last >= state["row"]["last_event_id"]:
                    state["row"] = {
                        "last_event_id": last,
                        "bids": json.loads(bids), "asks": json.loads(asks),
                        "last_update_id": last_u, "valid": valid,
                        "updated_at": received}
                return _ReplayCursor()
            raise AssertionError("Unexpected replay SQL: "+query)
    monkeypatch.setattr(book_analysis, "connection", lambda _: Conn())
    settings = SimpleNamespace(symbol="ETHUSDT", min_wall_qty=Decimal("10000"))
    return state, settings


def test_round9_l2_snapshot_rejects_negative_nan_and_invalid_price():
    for bad in (["100", "-1"], ["100", "NaN"], ["-1", "1"]):
        with pytest.raises(ValueError):
            parse_levels([bad])
    for bad in (["100", "-1"], ["100", "Infinity"], ["NaN", "2"]):
        with pytest.raises(ValueError):
            apply_updates({}, [bad])


def test_round9_replay_is_bounded_restart_idempotent_and_serialized(monkeypatch):
    from eth_tester.book_analysis import replay_orderbook
    events = [
        {"id": 1, "recv_ts": NOW, "event_type": "snapshot",
         "bids": [["100", "5"]], "asks": [["101", "3"]],
         "update_id": 10, "valid": True},
        {"id": 2, "recv_ts": NOW+timedelta(seconds=1),
         "event_type": "delta", "bids": [["100", "7"]],
         "asks": [], "update_id": 11, "valid": True},
    ]
    state, settings = _fake_replay_engine(monkeypatch, events)
    assert replay_orderbook(settings, batch=1, max_batches=1) == 1
    assert state["row"]["last_event_id"] == 1
    assert replay_orderbook(settings, batch=1, max_batches=1) == 1
    assert state["row"]["last_event_id"] == 2
    assert state["row"]["bids"]["100"] == "7"
    assert replay_orderbook(settings, batch=1, max_batches=1) == 0
    assert sum("pg_advisory_xact_lock" in q for q in state["calls"]) == 6
    # Before EVERY state read, the source fence precedes the L2 lease.
    for i, query in enumerate(state["calls"]):
        if "SET LOCAL lock_timeout" in query:
            assert "pg_advisory_xact_lock" in state["calls"][i+1]
            assert "pg_advisory_xact_lock" in state["calls"][i+2]
            assert "FROM market_book_state" in state["calls"][i+3]
    assert sum("SET LOCAL lock_timeout" in q for q in state["calls"]) == 3
    assert state["leases"] == [
        lease for _ in range(3) for lease in (
            "eth_paper_admission_source:ETHUSDT",
            "eth_l2_replay:ETHUSDT")]
    assert any("EXCLUDED.last_event_id >= market_book_state.last_event_id"
               in q for q in state["calls"])
    assert state["gaps"] == []


def test_round9_l2_poison_event_invalidates_until_new_real_snapshot(monkeypatch):
    from eth_tester.book_analysis import replay_orderbook
    events = [
        {"id": 1, "recv_ts": NOW, "event_type": "snapshot",
         "bids": [["100", "-2"]], "asks": [["101", "1"]],
         "update_id": 1, "valid": True},
        {"id": 2, "recv_ts": NOW+timedelta(seconds=1),
         "event_type": "delta", "bids": [["100", "10"]],
         "asks": [], "update_id": 2, "valid": True},
        {"id": 3, "recv_ts": NOW+timedelta(seconds=2),
         "event_type": "snapshot", "bids": [["100", "10"]],
         "asks": [["101", "10"]], "update_id": 20, "valid": True},
    ]
    state, settings = _fake_replay_engine(monkeypatch, events)
    assert replay_orderbook(settings,batch=1,max_batches=1) == 1
    assert state["row"]["valid"] is False
    assert state["row"]["bids"] == {}
    assert len(state["gaps"]) == 1
    assert replay_orderbook(settings,batch=1,max_batches=1) == 1
    assert state["row"]["valid"] is False
    assert replay_orderbook(settings,batch=1,max_batches=1) == 1
    assert state["row"]["valid"] is True
    assert state["row"]["last_event_id"] == 3
    assert len(state["gaps"]) == 1  # Do NOT fabricate repaired source coverage.
