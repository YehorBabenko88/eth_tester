"""Agent A R9 24/7 fault-injection contracts. No network, keys or real orders.

Only the focused fake-ledger and OS-local rate-budget tests execute without PG.
PostgreSQL serialization is covered by the separate disposable integration suite.
"""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from eth_tester import bybit, live
from eth_tester.data_quality import DataIntegrityError

UTC = timezone.utc


class One:
    def __init__(self, row=None):
        self.row = row

    def fetchone(self):
        return self.row


class SourceDB:
    def __init__(self, *, status="connected", open_gap=False, book_ts=None):
        self.status = status
        self.open_gap = open_gap
        self.book_ts = book_ts
        self.clock = datetime(2026, 11, 25, 20, 3, tzinfo=UTC)
        self.calls = []

    def execute(self, sql, params=None):
        query = str(sql)
        self.calls.append((query, params))
        if "SELECT ts,status FROM feed_health" in query:
            return One({"ts": self.clock-timedelta(minutes=1),
                        "status": self.status} if self.status else None)
        if "SELECT id FROM data_gaps" in query:
            return One({"id": 12} if self.open_gap else None)
        if "SELECT clock_timestamp()" in query:
            return One({"ts": self.clock})
        if "max(recv_ts)" in query:
            return One({"ts": self.book_ts})
        if "INSERT INTO data_gaps" in query:
            return One({"id": 44})
        return One()


def _writes(db):
    return [i for i, (q, _) in enumerate(db.calls)
            if "INSERT INTO feed_health" in q
            or "INSERT INTO data_gaps" in q or "UPDATE data_gaps" in q]


def test_paper_source_fence_exact_b_e_hash_contract():
    db = SourceDB()
    live.lock_admission_source(db, "ETHUSDT")
    query, args = db.calls[0]
    assert "pg_advisory_xact_lock(hashtext(%s))" in query
    assert args == ("eth_paper_admission_source:ETHUSDT",)
    assert all("paper_trades" not in q for q, _ in db.calls)


def test_unclean_restart_creates_disconnected_and_unresolved_gap_under_lock():
    db = SourceDB(book_ts=datetime(2026, 11, 25, 19, 59, tzinfo=UTC))
    seen_books = []
    original = live.record_gap
    live.record_gap = lambda *args: seen_books.append(args)
    try:
        gap = live.reconcile_unclean_ws_start(
            db, "ETHUSDT", last_trade=db.clock-timedelta(seconds=40))
    finally:
        live.record_gap = original
    assert gap == 44
    assert _writes(db)
    assert all(i > 0 for i in _writes(db))
    assert db.calls[0][1] == ("eth_paper_admission_source:ETHUSDT",)
    statuses = [params for q, params in db.calls if "INSERT INTO feed_health" in q]
    assert len(statuses) == 1
    assert "unclean_process_restart" in statuses[0][1]
    gaps = [params for q, params in db.calls if "INSERT INTO data_gaps" in q]
    assert len(gaps) == 1
    assert "trade IDs unknown" in gaps[0][-1]
    assert seen_books and seen_books[0][2] == "orderbook"
    assert not any("resolved_at=now()" in q for q, _ in db.calls)


def test_unclean_restart_does_not_duplicate_preexisting_open_gap():
    db = SourceDB(open_gap=True)
    assert live.reconcile_unclean_ws_start(db, "ETHUSDT") == 12
    assert not any("INSERT INTO data_gaps" in q for q, _ in db.calls)
    assert any("INSERT INTO feed_health" in q for q, _ in db.calls)


def test_previous_disconnected_status_never_falsely_reopened():
    db = SourceDB(status="disconnected")
    assert live.reconcile_unclean_ws_start(db, "ETHUSDT") is None
    assert len(db.calls) == 3
    assert not _writes(db)


def test_atomic_disconnect_receipt_records_gap_before_transaction_commit():
    db = SourceDB()
    gap = live.persist_ws_disconnect(
        db, "ETHUSDT", "s1", "connection dropped",
        last_trade=db.clock-timedelta(seconds=4),
        connected_at=db.clock-timedelta(minutes=2))
    assert gap == 44
    assert db.calls[0][1] == ("eth_paper_admission_source:ETHUSDT",)
    assert any("INSERT INTO feed_health" in q for q, _ in db.calls)
    assert any("INSERT INTO data_gaps" in q for q, _ in db.calls)
    assert all(i > 0 for i in _writes(db))
    assert not any("resolved_at=now()" in q for q, _ in db.calls)


def test_disconnect_does_not_duplicate_existing_unknown_gap():
    db = SourceDB()
    assert live.persist_ws_disconnect(
        db, "ETHUSDT", "s2", "offline", pending_id=81) == 81
    assert not any("INSERT INTO data_gaps" in q for q, _ in db.calls)
    assert len([q for q, _ in db.calls if "INSERT INTO feed_health" in q]) == 1


class WS:
    def __init__(self, raw):
        self.raw = raw

    async def recv(self):
        return self.raw


def test_actual_subscribe_ack_required_before_connected_status():
    topics = ["publicTrade.ETHUSDT", "orderbook.50.ETHUSDT"]
    result = asyncio.run(live.await_ws_subscription(
        WS(json.dumps({"op": "subscribe", "success": True, "args": topics})),
        topics))
    assert result["success"] is True


@pytest.mark.parametrize("value", [
    "not json", "[]", '{"op":"subscribe","success":false}',
    '{"op":"subscribe","success":true,"args":["other"]}',
    '{"op":"pong","success":true}', '{"success":true}',
])
def test_missing_bad_or_wrong_subscription_ack_fails_closed(value):
    with pytest.raises(DataIntegrityError):
        asyncio.run(live.await_ws_subscription(
            WS(value), ["publicTrade.ETHUSDT"]))


def test_half_open_socket_with_no_market_frames_times_out():
    class Stalled:
        async def recv(self):
            await asyncio.Event().wait()
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(live.next_ws_payload(Stalled(), idle_seconds=0.005))


def test_invalid_websocket_json_and_non_object_rejected():
    for invalid in ("{", "null", "[]", '"not-object"'):
        with pytest.raises(DataIntegrityError):
            asyncio.run(live.next_ws_payload(WS(invalid), idle_seconds=0.1))


def test_shared_budget_corruption_conservatively_delays_not_crashes(tmp_path, monkeypatch):
    path = tmp_path / "budget.json"
    path.write_text("{interrupted write", encoding="utf-8")
    monkeypatch.setattr(bybit.time, "time", lambda: 1700000000.0)
    budget = bybit.SharedRateBudget("https://api.bybit.com", 2.0, path=path)
    delay = budget._update()
    assert delay >= 60
    persisted = json.loads(path.read_text())
    assert persisted["next"] >= 1700000060
    assert budget._update() > 0


def test_shared_budget_rejects_nan_and_infinite_rps():
    for rate in (float("nan"), float("inf"), 0, -1):
        with pytest.raises(ValueError):
            bybit.SharedRateBudget("https://api.bybit.com", rate)


def test_preexisting_empty_budget_after_power_loss_fails_closed(tmp_path, monkeypatch):
    path = tmp_path / "budget.json"
    path.touch()  # Crash between O_EXCL CREATE and first atomic rewrite.
    monkeypatch.setattr(bybit.time, "time", lambda: 1700000000.0)
    budget = bybit.SharedRateBudget("https://api.bybit.com", 2.0, path=path)
    assert budget._update() >= 60
    assert json.loads(path.read_text())["next"] >= 1700000060


def test_fresh_shared_budget_first_slot_is_not_spuriously_delayed(tmp_path, monkeypatch):
    monkeypatch.setattr(bybit.time, "time", lambda: 1700000000.0)
    budget = bybit.SharedRateBudget(
        "https://api.bybit.com", 2.0, path=tmp_path/"new.json")
    assert budget._update() == pytest.approx(0)
    assert budget._update() == pytest.approx(0.5)




@pytest.mark.skipif(os.name == "nt", reason="OS symlink protection tested on POSIX")
def test_shared_budget_refuses_symlink_write(tmp_path):
    owned = tmp_path / "do_not_truncate.txt"
    owned.write_text("user owns this")
    pointer = tmp_path / "budget.json"
    pointer.symlink_to(owned)
    budget = bybit.SharedRateBudget("https://api.bybit.com", 1.0, path=pointer)
    with pytest.raises(bybit.BybitError, match="symlink"):
        budget._update()
    assert owned.read_text() == "user owns this"


def test_shared_rate_budget_serializes_parallel_process_like_slots(tmp_path, monkeypatch):
    monkeypatch.setattr(bybit.time, "time", lambda: 1700000000.0)
    budget = bybit.SharedRateBudget(
        "https://api.bybit.com", 2.0, path=tmp_path/"budget.json")
    with ThreadPoolExecutor(max_workers=8) as pool:
        delays = sorted(pool.map(lambda _: budget._update(), range(8)))
    assert len(delays) == 8
    assert delays[0] == pytest.approx(0)
    assert delays[-1] == pytest.approx(3.5, abs=0.0001)
    assert all(earlier < later for earlier, later in zip(delays, delays[1:]))


def test_http_429_retry_reset_header_capped_and_rest_only(monkeypatch):
    request = httpx.Request("GET", "https://api.bybit.com/v5/market/kline")
    pending = [
        httpx.Response(429, headers={"Retry-After": "999999999"},
                       request=request),
        httpx.Response(200, json={"retCode": 0, "result": {"list": []}},
                       request=request),
    ]
    class HTTP:
        async def get(self, url, params=None):
            assert url == "https://api.bybit.com/v5/market/kline"
            return pending.pop(0)
    class Budget:
        def __init__(self):
            self.delays = []
        async def reserve(self):
            return 0
        async def defer(self, wait):
            self.delays.append(wait)

    # Supply the test budget before the call without actual network.
    original_factory = bybit.SharedRateBudget
    budget = Budget()
    monkeypatch.setattr(bybit, "SharedRateBudget", lambda *_: budget)
    async def check_only():
        client = bybit.MarketClient("https://api.bybit.com", 3)
        original_http = client.client
        client.client = HTTP()
        original_sleep = bybit.asyncio.sleep
        async def no_wait(seconds):
            assert seconds <= 300
            return None
        monkeypatch.setattr(bybit.asyncio, "sleep", no_wait)
        monkeypatch.setattr(bybit.random, "uniform", lambda *_: 0)
        try:
            assert await client.get("kline", category="linear") == {"list": []}
            assert budget.delays == [300]
        finally:
            monkeypatch.setattr(bybit.asyncio, "sleep", original_sleep)
            await original_http.aclose()
    asyncio.run(check_only())
    monkeypatch.setattr(bybit, "SharedRateBudget", original_factory)
