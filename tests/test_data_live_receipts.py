"""Agent A round-6 source receipts: isolated, honest producer contract tests.

Fake clients/DBs are CONTRACT TESTS, not exchange coverage or PG acceptance.
Real disposable-PostgreSQL and live-source tests are kept separate.
"""
import asyncio
from datetime import datetime, timedelta, timezone
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


def test_identical_duplicate_trade_ids_are_one_immutable_receipt(monkeypatch):
    """Duplicate wire records cannot double-count source identities."""
    seen = []
    monkeypatch.setattr(
        live, "_record_ws_trade_receipts",
        lambda *args: seen.append(args))
    at = datetime(2026, 11, 25, 19, 3, tzinfo=UTC)
    conn = FakeConnection(at)
    wire = {"topic": "publicTrade.ETHUSDT", "data": [
        {"i": "repeated", "T": int(at.timestamp()*1000),
         "p": "100.00", "v": "0.500", "S": "Buy"},
        {"i": "repeated", "T": int(at.timestamp()*1000),
         "p": "100", "v": "0.5", "S": "Buy"}]}
    live.ingest_ws(conn, "ETHUSDT", wire, session_id="true-session")
    assert len(conn.inserted("trades")) == 1
    assert len(seen) == 1
    assert seen[0][1:] == ("ETHUSDT", "true-session", ["repeated"])
    record = json.loads(conn.inserted("feed_health")[0][1][1])
    assert record["observed_rows"] == 2
    assert record["distinct_trade_ids"] == 1
    assert record["identical_replayed_ids"] == 1
    assert record["proves_all_trade_ids"] is False
    assert record["ml_eligible"] is False


@pytest.mark.parametrize("changed", [
    {"v": "0.6"}, {"p": "101"}, {"S": "Sell"},
    {"T": int(datetime(2026, 11, 25, 19, 3, 1, tzinfo=UTC).timestamp()*1000)},
])
def test_conflicting_duplicate_trade_id_rejected_before_any_sql(changed):
    at = datetime(2026, 11, 25, 19, 3, tzinfo=UTC)
    first = {"i": "collision", "T": int(at.timestamp()*1000),
             "p": "100", "v": "0.5", "S": "Buy"}
    other = dict(first, **changed)
    conn = FakeConnection(at)
    with pytest.raises(DataIntegrityError, match="Conflicting duplicate"):
        live.ingest_ws(
            conn, "ETHUSDT",
            {"topic": "publicTrade.ETHUSDT", "data": [first, other]},
            session_id="true-session")
    assert conn.calls == []


def test_large_ws_frame_receipts_chunked_without_losing_trade_ids(monkeypatch):
    at = datetime(2026, 11, 25, 19, 3, tzinfo=UTC)
    conn = FakeConnection(at)
    chunks = []
    monkeypatch.setattr(
        live, "_record_ws_trade_receipts",
        lambda _conn, _symbol, _session, ids: chunks.append(list(ids)))
    rows = [{"i": str(i), "T": int(at.timestamp()*1000),
             "p": "100", "v": "0.001", "S": "Buy"}
            for i in range(2051)]
    live.ingest_ws(conn, "ETHUSDT",
                   {"topic": "publicTrade.ETHUSDT", "data": rows},
                   session_id="live-session")
    assert len(conn.inserted("trades")) == 2051
    assert list(map(len, chunks)) == [2048, 3]
    assert len(set(chunks[0] + chunks[1])) == 2051
    assert json.loads(conn.inserted("feed_health")[0][1][1])[
        "ml_eligible"] is False


def test_real_rest_closed_grid_has_source_receipt_after_http(monkeypatch):
    """Contract-only fixed clock: original HTTP response, not stored candles."""
    from datetime import date
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 25, 0, 15, 20, tzinfo=UTC)

    clock = datetime(2026, 11, 25, 0, 16, 3, tzinfo=UTC)
    conn = FakeConnection(clock)
    original = datetime(2026, 11, 25, 0, 7, tzinfo=UTC)
    requested = [int((original + timedelta(minutes=i)).timestamp()*1000)
                 for i in range(8)]
    client = FakeClient([[str(t), "100", "101", "99", "100", "1.5", "150"]
                         for t in requested])
    monkeypatch.setattr(history_integrity, "datetime", FixedDateTime)
    monkeypatch.setattr(history_integrity, "connection", lambda _: conn)
    monkeypatch.setattr(history_integrity, "cursor_ts",
                        lambda *_: datetime(2026, 11, 25, 0, 15, tzinfo=UTC))
    monkeypatch.setattr(history_integrity, "save_cursor", lambda *a, **kw: None)

    original_execute = conn.execute
    def coverage_execute(statement, params=None):
        if "SELECT count(*) AS total FROM data_gaps" in str(statement):
            conn.calls.append((str(statement), params))
            return Rows({"total": 0})
        return original_execute(statement, params)
    conn.execute = coverage_execute
    settings = SimpleNamespace(
        symbol="ETHUSDT", category="linear", archive_start=date(2026, 11, 25))
    asyncio.run(history_integrity.collect_klines(
        settings, client, "trade", "1"))
    assert client.calls[0][1]["start"] == requested[0]
    assert client.calls[0][1]["end"] == requested[-1]
    proof = conn.inserted("ingestion_coverage")[0][1]
    witness = json.loads(proof[11])
    assert proof[6] == "verified"
    assert proof[10] == "exchange_closed_grid_checked"
    assert witness["kind"] == "trade" and witness["tf"] == "1"
    assert witness["closed_grid_complete"] is True
    assert witness["receipt_is_independently_recorded"] is True
    assert witness["source_receipt_ms"] == int(clock.timestamp()*1000)
    assert witness["source_event_end_exclusive_ms"] < witness["source_receipt_ms"]
    assert len(witness["evidence_ref"]) == 64
    assert witness["proves_all_trade_ids"] is False
    assert witness["exchange_trades_complete"] is False


def test_missing_rest_grid_cannot_issue_verified_receipt(monkeypatch):
    from datetime import date
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 25, 0, 15, 20, tzinfo=UTC)

    conn = FakeConnection(datetime(2026, 11, 25, 0, 16, 3, tzinfo=UTC))
    starts = [int(datetime(2026, 11, 25, 0, i, tzinfo=UTC).timestamp()*1000)
              for i in range(7, 15) if i != 10]
    client = FakeClient([[str(t), "100", "101", "99", "100", "1", "100"]
                         for t in starts])
    monkeypatch.setattr(history_integrity, "datetime", FixedDateTime)
    monkeypatch.setattr(history_integrity, "connection", lambda _: conn)
    monkeypatch.setattr(history_integrity, "cursor_ts",
                        lambda *_: datetime(2026, 11, 25, 0, 15, tzinfo=UTC))
    with pytest.raises(DataIntegrityError, match="Missing candle"):
        asyncio.run(history_integrity.collect_klines(
            SimpleNamespace(symbol="ETHUSDT", category="linear",
                            archive_start=date(2026, 11, 25)),
            client, "trade", "1"))
    assert not any(json.loads(args[11]).get(
        "receipt_is_independently_recorded") is True
                   for _, args in conn.inserted("ingestion_coverage"))


def test_archive_recheck_requires_original_official_candle_source(monkeypatch):
    from datetime import date
    from eth_tester import archives
    conn = FakeConnection(datetime(2026, 11, 25, tzinfo=UTC))
    executed = []
    def db(statement, params=None):
        query = str(statement)
        conn.calls.append((query, params))
        if "FROM ingestion_job WHERE" in query:
            return Rows({"state": "complete", "sha256": "a"*64})
        if "FROM archive_stage_receipts WHERE" in query:
            return Rows({"exists": 1})
        if "count(*) FILTER" in query:
            return Rows({"total": 1, "official": 0})
        return Rows()
    conn.execute = db
    monkeypatch.setattr(archives, "connection", lambda _: conn)
    from eth_tester.storage import archive_receipts
    monkeypatch.setattr(
        archive_receipts, "revalidate_archive_bins",
        lambda *args, **kw: executed.append((args, kw)))
    result = archives.revalidate_completed_archive(
        SimpleNamespace(symbol="ETHUSDT", category="linear"),
        date(2026, 11, 24))
    assert result["status"] == "non_official_1m_source"
    assert not executed
