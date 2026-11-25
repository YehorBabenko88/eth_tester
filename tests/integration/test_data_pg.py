"""Data-owned real PostgreSQL tests; marker selects them in PG17/PG18 CI matrix."""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from eth_tester.data_quality import DataIntegrityError
from eth_tester.database import connection
from eth_tester.history_integrity import collect_klines, as_utc
from eth_tester.history_series import collect_oi

pytestmark = pytest.mark.postgres
UTC = timezone.utc


class ClosedCandleClient:
    async def get(self, endpoint, **kw):
        if endpoint != "kline":
            raise AssertionError(f"Unexpected endpoint {endpoint}")
        start, end = kw["start"], kw["end"]
        return {"list": [
            [str(ts), "100", "101", "99", "100", "1", "100"]
            for ts in range(start, end + 1, 86400000)]}


class MissingCandleClient:
    async def get(self, endpoint, **kw):
        return {"list": []}


def test_data_candle_coverage_verified_and_missing(pg_settings):
    from eth_tester.config import Settings
    history_date = (datetime.now(UTC) - timedelta(days=2)).date()
    options = Settings(
        _env_file=None,
        database_url=pg_settings.database_url,
        archive_start=history_date,
        symbol="ETHUSDT", category="linear")
    asyncio.run(collect_klines(options, ClosedCandleClient(), "trade", "D"))
    with connection(options) as conn:
        rows = conn.execute("""SELECT status,expected_rows,observed_rows
            FROM ingestion_coverage WHERE source='kline:trade' AND interval='D'""").fetchall()
        assert rows
        assert all(row["status"] == "verified" and
                   row["expected_rows"] == row["observed_rows"] for row in rows)
    empty_options = Settings(
        _env_file=None, database_url=pg_settings.database_url,
        archive_start=history_date, symbol="ETHUSDT", category="linear")
    asyncio.run(collect_klines(empty_options, MissingCandleClient(), "index", "D"))
    with connection(options) as conn:
        states = conn.execute("""SELECT DISTINCT status FROM ingestion_coverage
            WHERE source='kline:index' AND interval='D'""").fetchall()
        assert [x["status"] for x in states] == ["unavailable"]
        cursor = conn.execute("""SELECT status FROM ingestion_cursor
            WHERE source='kline:index' AND symbol='ETHUSDT' AND interval='D'""").fetchone()
        assert cursor["status"] != "verified"


def test_data_oi_period_conflict_is_not_overwritten(pg_settings):
    from eth_tester.config import Settings
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    start = now - timedelta(days=2)
    settings = Settings(
        _env_file=None, database_url=pg_settings.database_url,
        archive_start=start.date(), symbol="ETHUSDT", category="linear")
    stamp = int(start.timestamp() // 300 * 300 * 1000)
    class Client:
        async def get(self, endpoint, **kw):
            if endpoint != "open-interest":
                raise AssertionError(endpoint)
            if kw["startTime"] <= stamp <= kw["endTime"]:
                return {"list": [{"timestamp": str(stamp), "openInterest": "2"}]}
            return {"list": []}
    with connection(settings) as conn:
        conn.execute("""INSERT INTO open_interest(symbol,interval,ts,value)
            VALUES('ETHUSDT','5min',%s,1)""", (as_utc(stamp),))
    with pytest.raises(DataIntegrityError, match="Stored oi"):
        asyncio.run(collect_oi(settings, Client(), "5min"))
    with connection(settings) as conn:
        assert conn.execute("""SELECT value FROM open_interest
            WHERE symbol='ETHUSDT' AND interval='5min' AND ts=%s""",
            (as_utc(stamp),)).fetchone()["value"] == 1
