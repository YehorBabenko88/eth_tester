"""Real, isolated PostgreSQL 17 integration; never touches user databases."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal as D
import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.analytics import build_footprint
from eth_tester.database import connection, init_database, save_cursor, cursor_ts
from eth_tester.live import ingest_ws
from eth_tester.paper import open_paper
from eth_tester.rollups import rollup_day
from eth_tester.config import Settings


pytestmark = pytest.mark.postgres


@pytest.fixture
def pg_settings():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("Real PostgreSQL integration needs ETH_TEST_PG_DSN")
    dbname = "eth_tester_qa_" + uuid4().hex[:12]
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    try:
        settings = Settings(_env_file=None, database_url=make_conninfo(admin_dsn, dbname=dbname))
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(dbname)))


def test_schema_and_one_open_position_guard(pg_settings):
    with connection(pg_settings) as conn:
        row = conn.execute("SELECT equity FROM paper_account WHERE id=1").fetchone()
        assert row["equity"] == pg_settings.initial_equity_usd
        conn.execute("UPDATE bot_control SET enabled=true,paused=false WHERE singleton=true")
    first_id = open_paper(pg_settings, "long", D(100), D("99.5"), D(104), {"qa": True})
    assert first_id > 0
    with pytest.raises(RuntimeError, match="One trade only"):
        open_paper(pg_settings, "short", D(100), D(102), D(96), {"qa": True})
    with pytest.raises(psycopg.errors.UniqueViolation):
        with connection(pg_settings) as conn:
            conn.execute("""INSERT INTO paper_trades
                (symbol,state,direction,signal_ts,reason,strategy_version)
                VALUES('ETHUSDT','open','short',now(),'{}'::jsonb,'qa')""")


def test_cursor_repeated_save_and_transaction_rollback(pg_settings):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with connection(pg_settings) as conn:
        save_cursor(conn, "qa", "ETHUSDT", "1", now)
        save_cursor(conn, "qa", "ETHUSDT", "1", now + timedelta(minutes=1))
        assert cursor_ts(conn, "qa", "ETHUSDT", "1") == now + timedelta(minutes=1)
    with pytest.raises(RuntimeError, match="simulate power loss"):
        with connection(pg_settings) as conn:
            conn.execute("""INSERT INTO trades(symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,'aborted',100,1,'Buy',100,'qa')""", (now,))
            raise RuntimeError("simulate power loss")
    with connection(pg_settings) as conn:
        assert conn.execute("SELECT count(*) AS n FROM trades WHERE trade_id='aborted'").fetchone()["n"] == 0


def test_mock_websocket_replay_deduplicates_and_persists(pg_settings):
    stamp = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    trade = {"topic": "publicTrade.ETHUSDT", "data": [
        {"i": "mock-1", "T": stamp, "p": "100", "v": "2", "S": "Buy"}]}
    book = {"topic": "orderbook.50.ETHUSDT", "ts": stamp, "type": "snapshot",
            "data": {"u": 1, "seq": 1, "b": [["99", "5"]], "a": [["101", "6"]]}}
    with connection(pg_settings) as conn:
        ingest_ws(conn, "ETHUSDT", trade)
        ingest_ws(conn, "ETHUSDT", trade)
        ingest_ws(conn, "ETHUSDT", book)
    with connection(pg_settings) as conn:
        assert conn.execute("SELECT count(*) AS n FROM trades").fetchone()["n"] == 1
        row = conn.execute("SELECT qty,side FROM trades WHERE trade_id='mock-1'").fetchone()
        assert (row["qty"], row["side"]) == (D(2), "Buy")
        assert conn.execute("SELECT count(*) AS n FROM book_events").fetchone()["n"] == 1


def test_real_rollup_poc_delta_and_repeat_load(pg_settings):
    day = date(2026, 1, 1)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    fixtures = [("a", D(100), D(2), "Buy"), ("b", D(100), D(1), "Sell"),
                ("c", D(101), D(2), "Sell")]
    with connection(pg_settings) as conn:
        for ident, price, qty, side in fixtures:
            conn.execute("""INSERT INTO trades
                (symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,%s,%s,%s,%s,%s,'archive')""",
                (start, ident, price, qty, side, price * qty))
    assert rollup_day(pg_settings, day, "1", "sha-test-1") == 2
    assert rollup_day(pg_settings, day, "1", "sha-test-1") == 0
    with connection(pg_settings) as conn:
        rows = conn.execute("""SELECT price,buy_qty,sell_qty FROM footprint
            WHERE symbol='ETHUSDT' AND tf='1' ORDER BY price""").fetchall()
        feature = conn.execute("""SELECT poc,delta,volume FROM features
            WHERE symbol='ETHUSDT' AND tf='1'""").fetchone()
    assert [(x["price"], x["buy_qty"], x["sell_qty"]) for x in rows] == [
        (D(100), D(2), D(1)), (D(101), D(0), D(2))]
    assert (feature["poc"], feature["delta"], feature["volume"]) == (D(100), D(0), D(5))


def test_live_footprint_is_idempotent(pg_settings):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    with connection(pg_settings) as conn:
        conn.execute("""INSERT INTO trades(symbol,ts,trade_id,price,qty,side,notional,source)
            VALUES('ETHUSDT',%s,'live-1',100,1,'Buy',100,'qa')""", (now,))
    assert build_footprint(pg_settings, "1", hours=2) == 1
    assert build_footprint(pg_settings, "1", hours=2) == 1
    with connection(pg_settings) as conn:
        assert conn.execute("SELECT count(*) AS n FROM footprint WHERE tf='1'").fetchone()["n"] == 1
