"""QA H real PostgreSQL recovery tests. ONLY UUID disposable DBs.

Injected rows are TEST FIXTURES, never live Bybit certification. No real
trading, no permanent cluster changes, no destructive shared DB operations.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4
import json
import os

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.book_analysis import replay_orderbook
from eth_tester.paper import open_paper
from eth_tester.retention import cleanup_diagnostics
from eth_tester.simulation.identity import rule_identity
from eth_tester.strategies import Parameters

pytestmark = pytest.mark.postgres
UTC = timezone.utc


@pytest.fixture
def h9_pg():
    admin_dsn = os.getenv("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("ETH_TEST_PG_DSN required for disposable PG17/18")
    dbname = "eth_tester_qa_h9_" + uuid4().hex[:12]
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    try:
        settings = Settings(
            _env_file=None, database_url=make_conninfo(admin_dsn, dbname=dbname)
        )
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(dbname)
                )
            )


def test_restart_preserves_operator_pause_and_zero_orders(h9_pg):
    """Reconnecting the process must never override a persisted kill switch."""
    with connection(h9_pg) as conn:
        conn.execute(
            "UPDATE bot_control SET enabled=false,paused=true WHERE singleton=true"
        )
    with connection(h9_pg) as conn:
        state = conn.execute(
            "SELECT enabled,paused FROM bot_control WHERE singleton=true"
        ).fetchone()
        assert state == {"enabled": False, "paused": True}
    version, meta = rule_identity()
    reason = {
        "strategy_version": version,
        "rules_version": Parameters().version,
        **meta, "qa_only": True,
    }
    with pytest.raises(RuntimeError, match="Paused"):
        open_paper(
            h9_pg, "long", Decimal("100"), Decimal("98"),
            Decimal("105"), reason, observed_at=datetime.now(UTC),
        )
    with connection(h9_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades"
        ).fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_order_events"
        ).fetchone()["n"] == 0


def test_bounded_diagnostic_clean_two_cycles_preserve_market_history(h9_pg):
    """Cleanup is resumable/idempotent and cannot delete immutable market tape."""
    older = datetime.now(UTC) - timedelta(days=100)
    with connection(h9_pg) as conn:
        for i in range(3):
            conn.execute(
                """INSERT INTO feed_health(symbol,topic,ts,status,detail)
                   VALUES('ETHUSDT','qa_retention',%s,'partial','{}'::jsonb)""",
                (older + timedelta(microseconds=i),),
            )
            conn.execute(
                """INSERT INTO bot_events(ts,severity,event,data)
                   VALUES(%s,'warning','qa_old_diagnostic','{}'::jsonb)""",
                (older,),
            )
        conn.execute(
            """INSERT INTO trades(
                symbol,ts,trade_id,price,qty,side,notional,source,raw)
               VALUES('ETHUSDT',%s,%s,3000,0.01,'Buy',30,
                      'archive',%s::jsonb)""",
            (older, "H9-RETAIN-" + uuid4().hex[:10], json.dumps({"qa": True})),
        )
    # B must preserve both immutable research reviews and explicitly marked
    # audit records; ordinary old diagnostics still expire in bounded batches.
    with connection(h9_pg) as conn:
        conn.execute(
            """INSERT INTO bot_events(ts,severity,event,data)
               VALUES(%s,'info','paper_trade_review_v1',
                      '{"trade_id":7,"retention":"preserve","paper_only":true}'::jsonb)""",
            (older,),
        )
        conn.execute(
            """INSERT INTO bot_events(ts,severity,event,data)
               VALUES(%s,'info','qa_other_durable',
                      '{"retention":"preserve"}'::jsonb)""",
            (older,),
        )
    first = cleanup_diagnostics(
        h9_pg, health_days=30, event_days=30, max_rows_per_table=2
    )
    assert first["feed_health_deleted"] == 2
    assert first["bot_events_deleted"] == 2
    assert first["raw_market_data_deleted"] == 0
    assert first["batch_limited"] is True
    second = cleanup_diagnostics(
        h9_pg, health_days=30, event_days=30, max_rows_per_table=2
    )
    assert second["feed_health_deleted"] == 1
    assert second["bot_events_deleted"] == 1
    third = cleanup_diagnostics(
        h9_pg, health_days=30, event_days=30, max_rows_per_table=2
    )
    assert third["bot_events_deleted"] == 0
    with connection(h9_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM trades"
        ).fetchone()["n"] == 1
        assert conn.execute(
            """SELECT count(*) AS n FROM bot_events
               WHERE event='paper_trade_review_v1'
                  OR data->>'retention'='preserve'"""
        ).fetchone()["n"] == 2
        assert conn.execute(
            "SELECT count(*) AS n FROM feed_health WHERE topic='qa_retention'"
        ).fetchone()["n"] == 0


def test_concurrent_first_boot_l2_replay_cursor_never_goes_back(h9_pg):
    """Fast+slow replay on absent state row must not publish stale checkpoint."""
    with connection(h9_pg) as conn:
        assert conn.execute(
            "SELECT 1 FROM market_book_state WHERE symbol='ETHUSDT'"
        ).fetchone() is None
        seen = []
        for i in range(6):
            row = conn.execute(
                """INSERT INTO book_events(
                    symbol,exchange_ts,recv_ts,update_id,sequence_no,
                    event_type,bids,asks,valid)
                   VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),
                          %s,%s,%s,%s::jsonb,%s::jsonb,true)
                   RETURNING id""",
                (10 + i, 10 + i, "snapshot" if i == 0 else "delta",
                 json.dumps([["2999", "1"]] if i == 0 else []),
                 json.dumps([["3001", "1"]] if i == 0 else [])),
            ).fetchone()
            seen.append(row["id"])

    def worker(_):
        return replay_orderbook(h9_pg, batch=2, max_batches=4)

    with ThreadPoolExecutor(max_workers=4) as pool:
        progressed = list(pool.map(worker, range(4)))
    with connection(h9_pg) as conn:
        state = conn.execute(
            """SELECT last_event_id,last_update_id,valid FROM market_book_state
               WHERE symbol='ETHUSDT'"""
        ).fetchone()
        assert state is not None
        assert state["last_event_id"] == max(seen), (state, progressed)
        assert state["last_update_id"] == 15
        assert state["valid"] is True
    # Idempotent resume after crash/restart must not replay old events.
    assert replay_orderbook(h9_pg, batch=2, max_batches=4) == 0
