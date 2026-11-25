"""Actual PostgreSQL 17/18 test for causal levels and rollup corrections."""
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal
import os
from uuid import uuid4
import pytest
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.levels_incremental import process_levels
from eth_tester.rollups import rollup_day

pytestmark = pytest.mark.postgres
UTC = timezone.utc

@pytest.fixture
def c_pg():
    admin = os.environ.get("ETH_TEST_PG_DSN")
    if not admin:
        pytest.skip("PostgreSQL integration requires ETH_TEST_PG_DSN")
    name = "eth_tester_c_" + uuid4().hex[:12]
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        settings = Settings(_env_file=None, database_url=make_conninfo(admin, dbname=name))
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))

def test_daily_levels_closed_confirmation_retest_and_resume(c_pg):
    start = datetime(2026,1,1,tzinfo=UTC)
    candles = [(10,6,8),(12,7,10),(11,8,9),(12,9,11),
               (14,11,13),(14,12,13),(13,10,11)]
    with connection(c_pg) as conn:
        for i,(high,low,close) in enumerate(candles[:6]):
            conn.execute("""INSERT INTO candles(symbol,kind,tf,ts,open,high,low,
                close,volume,source)
                VALUES('ETHUSDT','trade','D',%s,%s,%s,%s,%s,1,'qa')""",
                (start+timedelta(days=i),close,high,low,close))
    assert process_levels(c_pg,"D",flank=1)==6
    assert process_levels(c_pg,"D",flank=1)==0
    with connection(c_pg) as conn:
        events=conn.execute("""SELECT e.event_type,e.event_ts
            FROM level_events e JOIN levels l ON e.level_id=l.id
            WHERE l.symbol='ETHUSDT' AND l.tf='D' AND
                  l.side='resistance' AND l.origin_ts=%s
            ORDER BY e.event_ts""",(start+timedelta(days=1),)).fetchall()
    assert [v["event_type"] for v in events]==[
        "confirmed","touched","broken","retested"]
    assert [v["event_ts"] for v in events]==[
        start+timedelta(days=k) for k in (3,4,5,6)]
    with connection(c_pg) as conn:
        conn.execute("""INSERT INTO candles(symbol,kind,tf,ts,open,high,low,
            close,volume,source)
            VALUES('ETHUSDT','trade','D',%s,11,13,10,11,1,'qa')""",
            (start+timedelta(days=6),))
    assert process_levels(c_pg,"D",flank=1)==1
    with connection(c_pg) as conn:
        row=conn.execute("""SELECT status,invalidated_at FROM levels
            WHERE symbol='ETHUSDT' AND tf='D' AND side='resistance'
              AND origin_ts=%s""",(start+timedelta(days=1),)).fetchone()
    assert row["status"]=="invalidated"
    assert row["invalidated_at"]==start+timedelta(days=7)

def test_weekly_rollup_daily_archive_correction(c_pg):
    t=datetime(2026,1,1,tzinfo=UTC)
    with connection(c_pg) as conn:
        for n,dt,price,qty,side in [
            ("c1",t,100,2,"Buy"),("c2",t+timedelta(days=1),100,1,"Sell"),
            ("c3",t+timedelta(days=1),101,3,"Buy")]:
            conn.execute("""INSERT INTO trades(symbol,ts,trade_id,price,
                qty,side,notional,source)
                VALUES('ETHUSDT',%s,%s,%s,%s,%s,%s,'qa')""",
                (dt,n,price,qty,side,price*qty))
    assert rollup_day(c_pg,date(2026,1,1),"W","s1")==1
    assert rollup_day(c_pg,date(2026,1,2),"W","s2")==2
    with connection(c_pg) as conn:
        f=conn.execute("SELECT delta,volume,metadata FROM features WHERE tf='W'").fetchone()
    assert (f["delta"],f["volume"])==(Decimal(4),Decimal(6))
    assert f["metadata"]["val"] is not None
    with connection(c_pg) as conn:
        conn.execute("DELETE FROM trades WHERE trade_id='c3'")
    assert rollup_day(c_pg,date(2026,1,2),"W","s2-replaced")==1
    with connection(c_pg) as conn:
        bins=conn.execute("SELECT price FROM footprint WHERE tf='W' ORDER BY price").fetchall()
    assert [x["price"] for x in bins]==[Decimal(100)]


def test_indicator_asof_and_trade_vwap(c_pg):
    from eth_tester.indicator_incremental import indicator_worker_step

    start = datetime(2026, 1, 1, tzinfo=UTC)
    with connection(c_pg) as conn:
        for i, hi, lo, close in [(0, 101, 99, 100),
                                  (1, 103, 99, 102),
                                  (2, 105, 101, 104)]:
            conn.execute("""INSERT INTO candles(
                symbol,kind,tf,ts,open,high,low,close,volume,source)
                VALUES('ETHUSDT','trade','1',%s,%s,%s,%s,%s,3,'qa')""",
                (start + timedelta(minutes=i), close, hi, lo, close))
        conn.execute("""INSERT INTO footprint(
            symbol,tf,bucket,price,buy_qty,sell_qty,buy_notional,
            sell_notional,trades)
            VALUES('ETHUSDT','1',%s,100,2,1,200,100,3)""", (start,))
    assert indicator_worker_step(c_pg, "1", period=2) == 3
    assert indicator_worker_step(c_pg, "1", period=2) == 0
    with connection(c_pg) as conn:
        items = conn.execute("""SELECT ts,metadata FROM features
            WHERE symbol='ETHUSDT' AND tf='1' ORDER BY ts""").fetchall()
    assert items[0]["metadata"]["trade_vwap"] == "100"
    assert items[0]["metadata"]["trade_vwap_quality"] == "observed_volume_unverified"
    assert items[0]["metadata"]["causally_certified"] is False
    assert items[1]["metadata"]["atr"] is not None
    assert items[2]["metadata"]["realized_volatility"] is not None
