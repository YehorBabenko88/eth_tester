"""Round14 actual PostgreSQL 17/18 C safety and provenance regressions.

Only a new, uniquely named disposable database under ETH_TEST_PG_DSN is used.
No real exchange orders, live user DB, or destructive cleanup of user data.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4
import os

import pytest
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from eth_tester import analytics
from eth_tester.config import Settings
from eth_tester.database import connection, init_database

pytestmark = pytest.mark.postgres


@pytest.fixture
def c14_pg():
    admin = os.environ.get("ETH_TEST_PG_DSN")
    if not admin:
        pytest.skip("Round14 real PG needs ETH_TEST_PG_DSN and a disposable test role")
    name = "eth_tester_c14_" + uuid4().hex[:12]
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        settings = Settings(_env_file=None, database_url=make_conninfo(admin, dbname=name))
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def test_overflow_is_atomic_and_can_retry_without_truncating(c14_pg, monkeypatch):
    stamp = (datetime.now(timezone.utc).replace(second=0, microsecond=0)
             - timedelta(minutes=1))
    with connection(c14_pg) as conn:
        conn.execute("""INSERT INTO trades(
            symbol,ts,trade_id,price,qty,side,notional,source)
            SELECT 'ETHUSDT',%s,'budget-'||g::text,100+g,1,'Buy',100+g,
                   'websocket' FROM generate_series(1,3) g""", (stamp,))
    monkeypatch.setattr(analytics, "MAX_LIVE_FOOTPRINT_GROUPS", 2)
    with pytest.raises(RuntimeError, match="live_footprint_group_budget_exceeded"):
        analytics.build_footprint(c14_pg, tf="1", hours=2)
    with connection(c14_pg) as conn:
        assert conn.execute("""SELECT count(*) AS n FROM footprint
            WHERE symbol='ETHUSDT' AND tf='1' AND bucket=%s""",
            (stamp,)).fetchone()["n"] == 0
        assert conn.execute("""SELECT count(*) AS n FROM features
            WHERE symbol='ETHUSDT' AND tf='1' AND ts=%s""",
            (stamp,)).fetchone()["n"] == 0
    monkeypatch.setattr(analytics, "MAX_LIVE_FOOTPRINT_GROUPS", 4)
    assert analytics.build_footprint(c14_pg, tf="1", hours=2) == 1
    with connection(c14_pg) as conn:
        assert conn.execute("""SELECT count(*) AS n FROM footprint
            WHERE symbol='ETHUSDT' AND tf='1' AND bucket=%s""",
            (stamp,)).fetchone()["n"] == 3
        row = conn.execute("""SELECT volume,quality FROM features
            WHERE symbol='ETHUSDT' AND tf='1' AND ts=%s""", (stamp,)).fetchone()
        assert row["volume"] == Decimal("3")
        assert row["quality"] == "partial_unverified"


def test_equal_candle_volume_without_original_sha_stays_partial(c14_pg):
    """Archive job state and numeric equality must NOT invent SHA source proof."""
    now = datetime.now(timezone.utc)
    stamp = now.replace(second=0, microsecond=0) - timedelta(minutes=2)
    with connection(c14_pg) as conn:
        conn.execute("""INSERT INTO ingestion_job(source,symbol,day,state,
            row_count,sha256) VALUES('trades_archive','ETHUSDT',%s,
            'complete',2,%s)""", (stamp.date(), "a"*64))
        conn.execute("""INSERT INTO candles(
            symbol,kind,tf,ts,open,high,low,close,volume,source,closed)
            VALUES('ETHUSDT','trade','1',%s,100,101,99,100,2,'qa',true)""",
            (stamp,))
        conn.execute("""INSERT INTO features(
            symbol,tf,ts,volume,delta,quality)
            VALUES('ETHUSDT','1',%s,2,0,'archive_rollup_partial')""",
            (stamp,))
    assert analytics.enrich_features(c14_pg,tf="1",hours=2) >= 1
    with connection(c14_pg) as conn:
        row = conn.execute("""SELECT quality,metadata FROM features
            WHERE symbol='ETHUSDT' AND tf='1' AND ts=%s""",(stamp,)).fetchone()
    assert row["quality"] == "partial_unverified"
    assert row["metadata"]["volume_check"] == "archive_sha_membership_missing"
    assert row["metadata"]["volume_exchange_complete"] is False
