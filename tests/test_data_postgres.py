"""Real PostgreSQL regression evidence for DATA #1 / QA #10.

Requires ETH_TEST_PG_DSN pointing ONLY to disposable local PostgreSQL admin.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.archive_quality import persist_minute_evidence
from eth_tester.config import Settings
from eth_tester.data_quality import DataIntegrityError
from eth_tester.database import connection, init_database
from eth_tester.history_integrity import store_candle
from eth_tester.live import ingest_ws
from eth_tester.storage.bulk import merge_staged_trades

pytestmark = pytest.mark.postgres
UTC = timezone.utc


@pytest.fixture
def data_settings():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("Set ETH_TEST_PG_DSN for isolated PostgreSQL data tests")
    dbname = "eth_tester_data_" + uuid4().hex[:12]
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    try:
        config = Settings(_env_file=None, database_url=make_conninfo(admin_dsn, dbname=dbname))
        init_database(config)
        yield config
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(dbname)))


def _utc_ms():
    return int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)


def test_live_trade_identity_conflict_rollback(data_settings):
    stamp = _utc_ms()
    def message(qty):
        return {"topic": "publicTrade.ETHUSDT", "data": [
            {"i": "same-id", "T": stamp, "p": "100", "v": str(qty), "S": "Buy"}]}
    with connection(data_settings) as conn:
        ingest_ws(conn, "ETHUSDT", message("1"))
    with pytest.raises(DataIntegrityError, match="Conflicting WS trade ID"):
        with connection(data_settings) as conn:
            ingest_ws(conn, "ETHUSDT", message("2"))
    with connection(data_settings) as conn:
        saved = conn.execute("SELECT qty FROM trades WHERE trade_id='same-id'").fetchone()
        assert saved["qty"] == Decimal("1")


def test_closed_candle_conflict_preserves_original(data_settings):
    stamp = _utc_ms()
    with connection(data_settings) as conn:
        store_candle(conn, "ETHUSDT", "trade", "1", stamp, (
            Decimal("100"), Decimal("102"), Decimal("99"), Decimal("101"),
            Decimal("3"), Decimal("300")))
        store_candle(conn, "ETHUSDT", "trade", "1", stamp, (
            Decimal("100"), Decimal("102"), Decimal("99"), Decimal("101"),
            Decimal("3"), Decimal("300")))
    with pytest.raises(DataIntegrityError, match="Stored candle"):
        with connection(data_settings) as conn:
            store_candle(conn, "ETHUSDT", "trade", "1", stamp, (
                Decimal("100"), Decimal("102"), Decimal("99"), Decimal("101"),
                Decimal("4"), Decimal("400")))
    with connection(data_settings) as conn:
        found = conn.execute(
            "SELECT volume FROM candles WHERE symbol='ETHUSDT' AND kind='trade'"
        ).fetchone()
        assert found["volume"] == Decimal("3")


def test_archive_stage_atomic_conflict_and_preservation(data_settings):
    when = datetime(2026, 1, 1, tzinfo=UTC)
    with connection(data_settings) as conn:
        conn.execute("""INSERT INTO trades
            (symbol,ts,trade_id,price,qty,side,notional,source)
            VALUES('ETHUSDT',%s,'archive-identity',100,1,'Buy',100,'websocket')""",
                     (when,))
    with pytest.raises((ValueError, psycopg.Error)):
        with connection(data_settings) as conn:
            conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
            conn.execute("""INSERT INTO eth_stage
                (symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,'archive-identity',100,2,'Buy',200,'archive')""",
                         (when,))
            merge_staged_trades(conn, "eth_stage")
    with connection(data_settings) as conn:
        assert conn.execute(
            "SELECT qty FROM trades WHERE trade_id='archive-identity'"
        ).fetchone()["qty"] == Decimal("1")


def test_archive_stage_identical_replay_idempotent(data_settings):
    when = datetime(2026, 1, 1, tzinfo=UTC)
    with connection(data_settings) as conn:
        conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
        for _ in range(2):
            conn.execute("""INSERT INTO eth_stage
                (symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,'archive-repeat',100,2,'Buy',200,'archive')""",
                         (when,))
        result = merge_staged_trades(conn, "eth_stage")
        assert result["input_rows"] == 2
    with connection(data_settings) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM trades WHERE trade_id='archive-repeat'"
        ).fetchone()["n"] == 1


def test_quality_coverage_and_1440_minute_evidence(data_settings):
    day = date(2026, 1, 1)
    base = datetime.combine(day, datetime.min.time(), UTC)
    with connection(data_settings) as conn:
        for minute in range(1440):
            ts = base + timedelta(minutes=minute)
            conn.execute("""INSERT INTO candles(
                symbol,kind,tf,ts,open,high,low,close,volume,turnover,source)
                VALUES('ETHUSDT','trade','1',%s,100,100,100,100,1,100,'qa')""",
                (ts,))
            conn.execute("""INSERT INTO trades(
                symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,%s,100,1,'Buy',100,'archive')""",
                (ts, "tick-" + str(minute)))
    with connection(data_settings) as conn:
        report = persist_minute_evidence(conn, "ETHUSDT", day, "a" * 64)
        assert report["status"] == "verified"
        assert report["match"] == 1440
    with connection(data_settings) as conn:
        rows = conn.execute("""SELECT count(*) AS n FROM trade_candle_volume_checks
            WHERE symbol='ETHUSDT' AND status='match'""").fetchone()
        assert rows["n"] == 1440


def test_quality_evidence_missing_candle_never_match(data_settings):
    day = date(2026, 1, 1)
    with connection(data_settings) as conn:
        assert persist_minute_evidence(conn, "ETHUSDT", day, "b" * 64
                                       )["status"] == "incomplete_candles"
        counts = conn.execute("""SELECT status,count(*) AS n
            FROM trade_candle_volume_checks GROUP BY status""").fetchall()
        assert len(counts) == 1
        assert counts[0]["status"] == "incomplete"
        assert counts[0]["n"] == 1440
