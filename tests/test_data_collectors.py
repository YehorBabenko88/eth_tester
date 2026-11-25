import asyncio
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from eth_tester.data_quality import DataIntegrityError
from eth_tester.history_integrity import collect_klines, latest_closed, page_end, store_candle
from eth_tester.history_series import _funding_rows, _pages, _save_funding
from eth_tester.archives import parse_trade, reconcile_archive_day

UTC = timezone.utc


class Result:
    def __init__(self, value=None):
        self.value = value

    def fetchone(self):
        return self.value


class FakeConnection:
    def __init__(self):
        self.queries = []
        self.conflict = False
        self.missing = 0

    def execute(self, sql, params=None):
        self.queries.append((sql, params))
        if "SELECT count(*) AS total FROM data_gaps" in sql:
            return Result({"total": self.missing})
        if "SELECT id FROM data_gaps" in sql:
            return Result(None)
        if "INSERT INTO data_gaps" in sql:
            self.missing += 1
        if "INSERT INTO candles" in sql or "INSERT INTO funding" in sql:
            return Result(None if self.conflict else {"ts": params[3] if len(params) > 3
                                                       else params[1]})
        return Result()


def test_monthly_page_end_exact_calendar():
    start = datetime(2024, 1, 1, tzinfo=UTC)
    stamp = int(start.timestamp() * 1000)
    assert page_end("M", stamp, stamp) == stamp
    assert latest_closed("M", int(datetime(2024, 3, 20, tzinfo=UTC).timestamp()
                                  * 1000)) == int(datetime(2024, 2, 1, tzinfo=UTC)
                                                   .timestamp() * 1000)


def test_store_candle_rejects_db_conflict():
    conn = FakeConnection()
    conn.conflict = True
    with pytest.raises(DataIntegrityError, match="Stored candle"):
        store_candle(conn, "ETHUSDT", "trade", "1", 1767225600000,
                     (Decimal(1), Decimal(2), Decimal(1), Decimal(2), Decimal(1)))


def test_collect_does_not_claim_unavailable_history(monkeypatch):
    import eth_tester.history_integrity as hi
    import eth_tester.data_depth as depth

    conn = FakeConnection()
    checkpoints = []
    @contextmanager
    def connect(_settings):
        yield conn

    monkeypatch.setattr(hi, "connection", connect)
    monkeypatch.setattr(depth, "connection", connect)
    monkeypatch.setattr(hi, "cursor_ts", lambda *_: None)
    monkeypatch.setattr(hi, "save_cursor", lambda *args, **kw:
                        checkpoints.append(kw["status"]))

    class Client:
        async def get(self, *_a, **_kw):
            return {"list": []}

    settings = SimpleNamespace(
        archive_start=datetime.now(UTC).date() - timedelta(days=2),
        symbol="ETHUSDT", category="linear")
    asyncio.run(collect_klines(settings, Client(), "trade", "D"))
    assert not checkpoints
    assert any("INSERT INTO ingestion_coverage" in sql for sql, _ in conn.queries)
    assert not any("INSERT INTO candles" in sql for sql, _ in conn.queries)


def test_collect_stores_verified_closed_daily_bars(monkeypatch):
    import eth_tester.history_integrity as hi
    import eth_tester.data_depth as depth

    conn = FakeConnection()
    checkpoints = []
    @contextmanager
    def connect(_settings):
        yield conn

    monkeypatch.setattr(hi, "connection", connect)
    monkeypatch.setattr(hi, "cursor_ts", lambda *_: None)
    async def first_day(settings, client, kind, tf):
        return int(datetime.combine(settings.archive_start, datetime.min.time(), UTC)
                   .timestamp() * 1000)
    monkeypatch.setattr(depth, "discover_candle_depth", first_day)
    monkeypatch.setattr(hi, "save_cursor", lambda *args, **kw:
                        checkpoints.append(kw["status"]))

    class Client:
        async def get(self, *_a, **kw):
            start, end = kw["start"], kw["end"]
            return {"list": [[str(ts), "100", "102", "99", "101", "1", "100"]
                             for ts in range(start, end + 1, 86400000)]}

    settings = SimpleNamespace(
        archive_start=datetime.now(UTC).date() - timedelta(days=2),
        symbol="ETHUSDT", category="linear")
    asyncio.run(collect_klines(settings, Client(), "trade", "D"))
    assert checkpoints and checkpoints[-1] == "verified"
    assert any("INSERT INTO candles" in sql for sql, _ in conn.queries)


def test_pages_without_cursor_bisect_saturated_window():
    class Client:
        def __init__(self):
            self.calls = []
        async def get(self, _, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return {"list": [{}] * 200}
            return {"list": [{"timestamp": 1000}]}

    client = Client()
    end, rows = asyncio.run(_pages(client, "open-interest",
                                  {"symbol": "ETHUSDT"}, 0,
                                  600000, 200, 60000))
    assert end < 600000
    assert len(rows) == 1
    assert len(client.calls) == 2


def test_pages_reject_repeated_cursor():
    class Client:
        async def get(self, *_a, **_kw):
            return {"list": [{"timestamp": 1000}], "nextPageCursor": "A"}

    with pytest.raises(DataIntegrityError, match="Repeated cursor"):
        asyncio.run(_pages(Client(), "open-interest",
                           {"symbol": "ETHUSDT"}, 0, 600000, 200, 60000))


def test_funding_conflicting_duplicates():
    with pytest.raises(DataIntegrityError, match="Conflicting Funding"):
        _funding_rows([{"fundingRateTimestamp": "1000", "fundingRate": "0.01"},
                       {"fundingRateTimestamp": "1000", "fundingRate": "0.02"}],
                      0, 2000)


def test_funding_nonfinite_rejected():
    with pytest.raises(DataIntegrityError, match="Non-finite"):
        _funding_rows([{"fundingRateTimestamp": 1000, "fundingRate": "NaN"}],
                      0, 2000)


def test_save_funding_conflict():
    conn = FakeConnection()
    conn.conflict = True
    with pytest.raises(DataIntegrityError, match="Funding conflicts"):
        _save_funding(conn, "ETHUSDT", 1767225600000, Decimal("0.001"))


def test_trade_parser_utc_and_aggressor():
    stamp, ident, price, qty, side = parse_trade({
        "timestamp": "1767225600.125", "trdMatchID": "abc", "side": "Buy",
        "price": "100.01", "size": "1.2"})
    assert stamp.date() == date(2026, 1, 1)
    assert (ident, price, qty, side) == (
        "abc", Decimal("100.01"), Decimal("1.2"), "Buy")


def test_trade_parser_refuses_zero_price():
    with pytest.raises(ValueError, match="Nonpositive"):
        parse_trade({"timestamp": "1767225600", "id": "abc", "side": "Sell",
                     "price": "0", "qty": "1"})


def test_archive_reconcile_requires_all_1440_minutes():
    class Conn:
        def execute(self, sql, _params):
            class Rows:
                def fetchall(self):
                    return []
            return Rows()
    assert reconcile_archive_day(Conn(), "ETHUSDT", date(2026, 1, 1)
                                 )["status"] == "incomplete_candles"


def test_archive_conflict_fails_once_and_records_gap(monkeypatch):
    import eth_tester.archives as archives

    conn = FakeConnection()
    calls = []

    @contextmanager
    def connect(_settings):
        yield conn

    async def deterministic_conflict(*_args):
        calls.append("attempt")
        raise DataIntegrityError("Conflicting trade-id payload already exists")

    monkeypatch.setattr(archives, "connection", connect)
    monkeypatch.setattr(archives, "download_archive", deterministic_conflict)
    settings = SimpleNamespace(
        archive_start=datetime.now(UTC).date() - timedelta(days=1),
        symbol="ETHUSDT", category="linear", archive_downloads_per_second=1.0)
    with pytest.raises(DataIntegrityError, match="Conflicting"):
        asyncio.run(archives.backfill_archives(settings))
    assert calls == ["attempt"]
    assert any("UPDATE ingestion_job" in q for q, _ in conn.queries)
    assert any("INSERT INTO ingestion_coverage" in q for q, _ in conn.queries)


def test_archive_submillisecond_utc_identity_normalization():
    row = {"timestamp": "1767225600.123999", "trdMatchID": "exact",
           "price": "100", "size": "1", "side": "Sell"}
    stamp, ident, price, qty, side = parse_trade(row)
    assert stamp.microsecond == 123000
    assert (ident, price, qty, side) == ("exact", Decimal(100), Decimal(1), "Sell")
    row["timestamp"] = "1767225600123"
    assert parse_trade(row)[0] == stamp


def test_archive_invalid_nonfinite_rejected():
    row = {"timestamp": "1767225600.123", "trdMatchID": "exact",
           "price": "NaN", "size": "1", "side": "Sell"}
    with pytest.raises(ValueError, match="non-finite"):
        parse_trade(row)
