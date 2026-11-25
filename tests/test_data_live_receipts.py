"""Agent A round-6 source receipts: isolated, honest producer contract tests.

Fake clients/DBs are CONTRACT TESTS, not exchange coverage or PG acceptance.
Real disposable-PostgreSQL and live-source tests are kept separate.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
import json

import pytest

from eth_tester.data_quality import DataIntegrityError
from eth_tester import history_integrity, history_series, live

UTC = timezone.utc


class Rows:
    def __init__(self, row=None):
        self.row = row

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, instant):
        self.instant = instant
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, statement, params=None):
        self.calls.append((str(statement), params))
        if "SELECT clock_timestamp()" in str(statement):
            return Rows({"ts": self.instant})
        if "INSERT INTO candles" in str(statement):
            return Rows({"ts": self.instant})
        if "INSERT INTO open_interest" in str(statement):
            return Rows({"ts": self.instant})
        if "INSERT INTO trades" in str(statement):
            return Rows({"trade_id": params[2]})
        if "INSERT INTO book_events" in str(statement):
            return Rows({"id": 31, "recv_ts": self.instant})
        return Rows()

    def inserted(self, table):
        return [(sql, args) for sql, args in self.calls
                if "INSERT INTO " + table in sql]


class FakeClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def get(self, path, **params):
        self.calls.append((path, params))
        return {"list": self.rows}


SETTINGS = SimpleNamespace(symbol="ETHUSDT", category="linear")


def test_latest_closed_minute_uses_real_receipt_and_exact_rest_window(monkeypatch):
    instant = datetime(2026, 11, 25, 19, 3, 15, tzinfo=UTC)
    bar = datetime(2026, 11, 25, 19, 2, tzinfo=UTC)
    conn = FakeConnection(instant)
    monkeypatch.setattr(history_integrity, "connection", lambda settings: conn)
    client = FakeClient([[str(int(bar.timestamp()*1000)),
                          "100", "101", "99", "100", "1.5", "150"]])
    result = asyncio.run(
        history_integrity.collect_latest_closed_trade_minute(SETTINGS, client))
    assert result["status"] == "closed_grid_observed"
    assert result["source_receipt_ms"] == int(instant.timestamp()*1000)
    assert result["bar_end_ms"] <= result["source_receipt_ms"]
    assert result["proves_all_trade_ids"] is False
    assert client.calls[0][1]["start"] == int(bar.timestamp()*1000)
    assert client.calls[0][1]["end"] == int(bar.timestamp()*1000)
    receipts = conn.inserted("ingestion_coverage")
    assert len(receipts) == 1
    args = receipts[0][1]
    assert args[6] == "verified"
    assert args[10] == "exchange_closed_grid_checked"
    evidence = json.loads(args[11])
    assert evidence["closed_bar_only"] is True
    assert evidence["ml_snapshot_eligible"] is False
    assert evidence["source_receipt_ms"] == result["source_receipt_ms"]


@pytest.mark.parametrize("volume,turnover", [
    ("NaN", "100"), ("-1", "100"), ("1", "Infinity"),
    ("", "100"),
])
def test_closed_minute_refuses_bad_base_quote(monkeypatch, volume, turnover):
    instant = datetime(2026, 11, 25, 19, 3, 15, tzinfo=UTC)
    bar = datetime(2026, 11, 25, 19, 2, tzinfo=UTC)
    conn = FakeConnection(instant)
    monkeypatch.setattr(history_integrity, "connection", lambda settings: conn)
    client = FakeClient([[str(int(bar.timestamp()*1000)),
                          "100", "101", "99", "100", volume, turnover]])
    with pytest.raises((DataIntegrityError, ValueError)):
        asyncio.run(
            history_integrity.collect_latest_closed_trade_minute(SETTINGS, client))
    assert not conn.inserted("ingestion_coverage")
    assert not conn.inserted("candles")


def test_live_oi_receipt_is_partial_not_historical_verified(monkeypatch):
    seen = []
    monkeypatch.setattr(
        history_series, "_record_bounded_oi_observation",
        lambda *args: seen.append(args))
    instant = datetime(2026, 11, 25, 19, 6, 18, tzinfo=UTC)
    conn = FakeConnection(instant)
    monkeypatch.setattr(history_series, "connection", lambda settings: conn)
    last = int(datetime(2026, 11, 25, 19, 0, tzinfo=UTC).timestamp()*1000)
    client = FakeClient([{"timestamp": str(last), "openInterest": "123.25"}])
    result = asyncio.run(history_series.collect_recent_oi_receipt(SETTINGS, client))
    assert result["status"] == "oi_observed_partial"
    assert result["source_event_ms"] < result["source_receipt_ms"]
    assert result["ml_snapshot_eligible"] is False
    rows = conn.inserted("ingestion_coverage")
    assert len(rows) == 1
    args = rows[0][1]
    assert args[0] == "oi"
    assert args[6] == "partial"
    assert args[10] == "bounded_oi_samples_not_complete_grid"
    evidence = json.loads(args[11])
    assert evidence["source_receipt_ms"] == result["source_receipt_ms"]
    assert evidence["oi_receipt_and_grid_verified"] is False
    assert seen and seen[0][1:3] == ("ETHUSDT", "5min")


def test_live_oi_nonfinite_never_persists(monkeypatch):
    instant = datetime(2026, 11, 25, 19, 6, 18, tzinfo=UTC)
    conn = FakeConnection(instant)
    monkeypatch.setattr(history_series, "connection", lambda settings: conn)
    last = int(datetime(2026, 11, 25, 19, 0, tzinfo=UTC).timestamp()*1000)
    client = FakeClient([{"timestamp": str(last), "openInterest": "Infinity"}])
    with pytest.raises(DataIntegrityError):
        asyncio.run(history_series.collect_recent_oi_receipt(SETTINGS, client))
    assert not conn.inserted("open_interest")
    assert not conn.inserted("ingestion_coverage")


def test_ws_trade_session_receipt_cannot_certify_all_prints(monkeypatch):
    proven = []
    monkeypatch.setattr(live, "_record_ws_trade_receipts",
                        lambda *args: proven.append(args))
    instant = datetime(2026, 11, 25, 19, 3, tzinfo=UTC)
    conn = FakeConnection(instant)
    event_ms = int(instant.timestamp()*1000)
    payload = {"topic": "publicTrade.ETHUSDT", "data": [{
        "i": "real-identity-1", "T": event_ms, "p": "100",
        "v": "0.5", "S": "Buy"}]}
    live.ingest_ws(conn, "ETHUSDT", payload, session_id="session-a")
    rows = conn.inserted("feed_health")
    assert len(rows) == 1
    assert rows[0][1][0] == "ETHUSDT"
    proof = json.loads(rows[0][1][1])
    assert proof["session_id"] == "session-a"
    assert proof["observed_rows"] == proof["distinct_trade_ids"] == 1
    assert len(proof["payload_sha256"]) == 64
    assert proof["proves_all_trade_ids"] is False
    assert proof["source_quality"] == "observed_partial"
    assert proof["ml_eligible"] is False
    assert proven and proven[0][1:] == (
        "ETHUSDT", "session-a", ["real-identity-1"])


def test_ws_book_snapshot_receipt_is_observation_not_full_l2_replay():
    instant = datetime(2026, 11, 25, 19, 3, tzinfo=UTC)
    conn = FakeConnection(instant)
    payload = {"topic": "orderbook.200.ETHUSDT", "type": "snapshot",
        "ts": int(instant.timestamp()*1000),
        "data": {"u": 22, "seq": 44, "b": [["100", "1"]],
                 "a": [["101", "2"]]}}
    live.ingest_ws(conn, "ETHUSDT", payload, session_id="session-b")
    receipt = json.loads(conn.inserted("feed_health")[0][1][1])
    assert receipt["session_id"] == "session-b"
    assert receipt["book_event_id"] == 31
    assert receipt["snapshot"] is True
    assert receipt["orderbook_replay_complete"] is False
    assert receipt["source_receipt_ms"] == int(instant.timestamp()*1000)
    assert receipt["ml_eligible"] is False


def test_rest_failure_one_series_does_not_starve_other_sources(monkeypatch):
    conn = FakeConnection(datetime(2026, 11, 25, tzinfo=UTC))
    monkeypatch.setattr(live, "connection", lambda settings: conn)
    attempted = []
    async def kline(_settings, _client, kind, tf):
        attempted.append(("kline", kind, tf))
        if kind == "trade" and tf == "1":
            raise RuntimeError("public endpoint temporarily unavailable")
    async def funding(*_):
        attempted.append(("funding",))
    async def oi(_settings, _client, interval):
        attempted.append(("oi", interval))
    async def ratio(_settings, _client, interval):
        attempted.append(("ratio", interval))
    monkeypatch.setattr(live, "collect_klines", kline)
    monkeypatch.setattr(live, "collect_funding", funding)
    monkeypatch.setattr(live, "collect_oi", oi)
    monkeypatch.setattr(live, "collect_account_ratios", ratio)
    result = asyncio.run(live.reconcile_rest_cycle(SETTINGS, object()))
    assert result["status"] == "partial"
    assert len(result["errors"]) == 1
    assert ("oi", "5min") in attempted and ("ratio", "1d") in attempted
    assert result["attempted"] == len(attempted)
    assert len(conn.inserted("feed_health")) == 1


def test_replay_out_of_order_book_sequence_fails_before_receipt():
    with pytest.raises(DataIntegrityError, match="Out-of-order"):
        live.validate_book_sequence(
            12, 20, {"type": "delta", "data": {"seq": 12, "u": 21}})

def test_fast_live_lane_observes_closed_minute_and_oi_without_day_archive(monkeypatch):
    """Always use bounded public REST; no daily gz prerequisite for a new minute."""
    calls = []
    conn = FakeConnection(datetime(2026, 11, 25, tzinfo=UTC))
    monkeypatch.setattr(live, "connection", lambda _: conn)
    async def minute(settings, client):
        calls.append("closed_1m")
        return {"status": "closed_grid_observed",
                "proves_all_trade_ids": False}
    async def oi(settings, client):
        calls.append("observed_5m_oi")
        return {"status": "oi_observed_partial", "ml_snapshot_eligible": False}
    class Client:
        def __init__(self, *args):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False
    class StopCycle(BaseException):
        pass
    async def end_first_cycle(_):
        raise StopCycle()
    monkeypatch.setattr(live, "MarketClient", Client)
    monkeypatch.setattr(live, "collect_latest_closed_trade_minute", minute)
    monkeypatch.setattr(live, "collect_recent_oi_receipt", oi)
    monkeypatch.setattr(live.asyncio, "sleep", end_first_cycle)
    with pytest.raises(StopCycle):
        asyncio.run(live.live_fast_reconcile(
            SimpleNamespace(bybit_rest="https://api.invalid", rest_rps=3.0,
                            symbol="ETHUSDT")))
    assert calls == ["closed_1m", "observed_5m_oi"]
    assert len(conn.inserted("feed_health")) == 2
