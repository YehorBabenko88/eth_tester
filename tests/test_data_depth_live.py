import asyncio
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal

import pytest

from eth_tester.archive_quality import build_minute_evidence, summarize_minutes
from eth_tester.data_quality import DataIntegrityError
from eth_tester.data_depth import first_observed_candle
from eth_tester.live import validate_book_sequence, ingest_ws

UTC = timezone.utc


def millis(year, month, day):
    return int(datetime(year, month, day, tzinfo=UTC).timestamp() * 1000)


class PrefixHistory:
    def __init__(self, first, tf="D"):
        self.first, self.tf = first, tf
        self.calls = 0

    async def get(self, path, **kw):
        self.calls += 1
        assert kw["limit"] == 1
        assert kw["interval"] == self.tf
        if kw["end"] < self.first:
            return {"list": []}
        from eth_tester.data_quality import open_time
        end = open_time(self.tf, kw["end"])
        return {"list": [[str(end), "100", "101", "99", "100"]]}


def test_find_earliest_available_daily_bar():
    earliest = millis(2023, 7, 1)
    cli = PrefixHistory(earliest)
    first = asyncio.run(first_observed_candle(
        cli, "ETHUSDT", "linear", "trade", "D",
        millis(2020, 1, 1), millis(2024, 1, 10)))
    assert first == earliest
    assert cli.calls < 32


def test_find_earliest_available_monthly_bar():
    earliest = millis(2024, 2, 1)
    cli = PrefixHistory(earliest, "M")
    first = asyncio.run(first_observed_candle(
        cli, "ETHUSDT", "linear", "index", "M",
        millis(2022, 1, 1), millis(2024, 6, 15)))
    assert first == earliest


def test_no_history_never_fabricated():
    class Empty:
        async def get(self, *_args, **_kw):
            return {"list": []}
    assert asyncio.run(first_observed_candle(
        Empty(), "ETHUSDT", "linear", "premium", "1",
        millis(2022, 1, 1), millis(2024, 5, 1))) is None


def test_probe_rejects_ignored_bounds():
    class Broken:
        async def get(self, *_args, **_kw):
            return {"list": [[str(millis(2025, 1, 1)), "100", "100", "100", "100"]]}
    with pytest.raises(DataIntegrityError, match="outside requested"):
        asyncio.run(first_observed_candle(
            Broken(), "ETHUSDT", "linear", "mark", "D",
            millis(2023, 1, 1), millis(2024, 1, 1)))


def test_orderbook_snapshot_and_strict_sequence():
    snap = {"type": "snapshot", "data": {"seq": 4, "u": 11}}
    assert validate_book_sequence(None, None, snap) == (4, 11)
    delta = {"type": "delta", "data": {"seq": 5, "u": 12}}
    assert validate_book_sequence(4, 11, delta) == (5, 12)
    with pytest.raises(DataIntegrityError, match="Out-of-order"):
        validate_book_sequence(5, 12, delta)
    with pytest.raises(DataIntegrityError, match="without exchange snapshot"):
        validate_book_sequence(None, None, delta)


def test_orderbook_missing_sequence_rejected():
    with pytest.raises(DataIntegrityError, match="Missing"):
        validate_book_sequence(1, 1, {"type": "delta", "data": {"seq": 2}})


def test_foreign_symbol_ws_payload_rejected():
    with pytest.raises(DataIntegrityError, match="topic symbol mismatch"):
        ingest_ws(None, "ETHUSDT", {"topic": "publicTrade.BTCUSDT", "data": []})


def test_archive_all_zero_candle_rows_do_not_prove_trades():
    day = date(2026, 1, 1)
    start = datetime.combine(day, datetime.min.time(), UTC)
    official = [
        {"ts": start + timedelta(minutes=i), "volume": Decimal("1"),
         "turnover": Decimal("100")}
        for i in range(1440)
    ]
    trades = [
        {"ts": start + timedelta(minutes=i), "trade_count": 1,
         "base_qty": Decimal("1"), "quote_qty": Decimal("100")}
        for i in range(1440)
    ]
    rows = build_minute_evidence(day, official, trades)
    assert summarize_minutes(rows)["status"] == "verified"
    trades[42]["quote_qty"] = Decimal("99")
    result = summarize_minutes(build_minute_evidence(day, official, trades))
    assert result["status"] == "volume_mismatch"
    assert result["mismatch"] == 1


def test_archive_incomplete_minute_is_not_verified():
    day = date(2026, 1, 1)
    start = datetime.combine(day, datetime.min.time(), UTC)
    official = [{"ts": start, "volume": 1, "turnover": 100}]
    report = summarize_minutes(build_minute_evidence(day, official, []))
    assert report["status"] == "incomplete_candles"
    assert report["incomplete"] == 1439
