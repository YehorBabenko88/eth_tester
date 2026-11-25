"""QA H round-eight real PostgreSQL 17/18 admission and arithmetic gates.

Each test creates its own disposable eth_tester_h8_<UUID> database. All public
market source rows are fabricated TEST DATA; these tests prove DB contracts
only, not actual Bybit completeness, profitability or native Windows GO.
No order API, network, background scheduler or user database is accessed.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json
import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.live_paper_window import publish_latest_bounded_live
from eth_tester.ml.labels import label_level_outcome, full_future_grid
from eth_tester.paper import open_paper
from eth_tester.simulation.identity import rule_identity
from eth_tester.simulation.live_evidence import (
    bind_live_paper_reference, verify_live_paper_row,
)
from eth_tester.storage.live_evidence import (
    record_trade_receipts, record_book_lineage, record_oi_observation,
)
from eth_tester.strategies import Parameters
from eth_tester.strategies.paper_live import read_live_evidence

pytestmark = pytest.mark.postgres
UTC = timezone.utc


@pytest.fixture
def qa_h8_pg():
    dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not dsn:
        pytest.skip("Disposable PostgreSQL requires ETH_TEST_PG_DSN")
    name = "eth_tester_h8_" + uuid4().hex[:12]
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    settings = Settings(_env_file=None, database_url=make_conninfo(dsn, dbname=name))
    try:
        init_database(settings)
        with connection(settings) as conn:
            conn.execute(
                "UPDATE bot_control SET enabled=true,paused=false WHERE singleton=true"
            )
        yield settings
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


def _issued_live_reason(settings):
    """Use C's actual issuer and B's DB-backed source chain, never caller flags."""
    session = "qa-h8-session-" + uuid4().hex[:9]
    ident = "H8-TRADE-" + uuid4().hex[:9]
    with connection(settings) as conn:
        now = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        bar = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        event = bar + timedelta(seconds=59)
        conn.execute(
            """INSERT INTO candles(
                symbol,kind,tf,ts,open,high,low,close,volume,turnover,
                closed,source)
               VALUES('ETHUSDT','trade','1',%s,3000,3000,3000,3000,
                      0.02,60,true,'bybit_v5')""", (bar,),
        )
        conn.execute(
            """INSERT INTO trades(
                symbol,ts,trade_id,price,qty,side,notional,source,raw)
               VALUES('ETHUSDT',%s,%s,3000,0.02,'Buy',60,
                      'websocket',%s::jsonb)""",
            (event, ident, json.dumps({"i": ident})),
        )
        record_trade_receipts(conn, "ETHUSDT", session, [ident])
        conn.execute(
            """INSERT INTO feed_health(symbol,topic,ts,status,detail)
               VALUES('ETHUSDT','connection',clock_timestamp(),
                      'connected',%s::jsonb)""",
            (json.dumps({"session_id": session}),),
        )
        snapshot = conn.execute(
            """INSERT INTO book_events(
                symbol,exchange_ts,recv_ts,update_id,sequence_no,
                event_type,bids,asks,valid)
               VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),
                      10,10,'snapshot','[["2999","1"]]'::jsonb,
                      '[["3001","1"]]'::jsonb,true) RETURNING id"""
        ).fetchone()["id"]
        record_book_lineage(conn, "ETHUSDT", session, snapshot, snapshot)
        minute = now.replace(second=0, microsecond=0)
        oi_at = minute - timedelta(minutes=minute.minute % 5)
        conn.execute(
            """INSERT INTO open_interest(symbol,interval,ts,value)
               VALUES('ETHUSDT','5min',%s,1000)""", (oi_at,),
        )
        record_oi_observation(conn, "ETHUSDT", "5min", oi_at, "a" * 64)
    issued = publish_latest_bounded_live(settings)
    assert issued["status"] == "published", issued
    assert issued["paper_only"] is True and issued["ml_eligible"] is False
    with connection(settings) as conn:
        decision = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        flow = read_live_evidence(
            conn, "ETHUSDT", bar, bar + timedelta(minutes=1), decision
        )
    assert flow is not None
    version, metadata = rule_identity()
    reason = {
        "strategy_version": version,
        "rules_version": Parameters().version,
        **metadata, "source": "closed_candles",
        "signal_bar_ts": bar.isoformat(),
        "orderflow": {"admissible": True, "immutable_live_evidence": flow},
    }
    bound = bind_live_paper_reference(
        reason, bar_ts=bar, decision_at=decision
    )
    return bar, decision, bound


@pytest.mark.parametrize("outage", ["disconnected", "orderbook_gap", "invalid_l2"])
def test_post_publication_source_loss_blocks_paper_admission(qa_h8_pg, outage):
    bar, decision, original_reason = _issued_live_reason(qa_h8_pg)
    with connection(qa_h8_pg) as conn:
        original = conn.execute(
            "SELECT count(*) AS n FROM paper_live_evidence"
        ).fetchone()["n"]
        assert original == 1
        if outage == "disconnected":
            conn.execute(
                """INSERT INTO feed_health(symbol,topic,ts,status,detail)
                   VALUES('ETHUSDT','connection',clock_timestamp(),
                          'disconnected','{}'::jsonb)"""
            )
        elif outage == "orderbook_gap":
            conn.execute(
                """INSERT INTO data_gaps(
                    source,symbol,start_ts,end_ts,explanation)
                   VALUES('orderbook','ETHUSDT',%s,clock_timestamp(),
                          'qa_disconnect_after_proof')""", (bar,),
            )
        else:
            conn.execute(
                """UPDATE market_book_state SET valid=false
                   WHERE symbol='ETHUSDT'"""
            )
        # The original immutable B witness is not deleted or rewritten.
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_live_evidence"
        ).fetchone()["n"] == 1
        # E must call B's *current* fail-closed prospective guard inside
        # the SAME transaction as paper admission, not trust an earlier row.
        with pytest.raises((ValueError, RuntimeError)):
            verify_live_paper_row(
                conn, symbol="ETHUSDT", reason=original_reason,
                decision_at=decision,
            )
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades"
        ).fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_order_events"
        ).fetchone()["n"] == 0


@pytest.mark.parametrize(
    "high,low,expected",
    [
        (Decimal("393.067314498"), Decimal("392.272749"), "breakout"),
        (Decimal("393.067314498"), Decimal("391.498183502"), "uncertain"),
    ],
)
def test_exact_f_decimal_barrier_equals_postgresql_numeric(
    qa_h8_pg, high, low, expected,
):
    level = Decimal("392.282749")
    with connection(qa_h8_pg) as conn:
        sql_limits = conn.execute(
            """SELECT %s::numeric * (1+20::numeric/10000) AS upper,
                      %s::numeric * (1-20::numeric/10000) AS lower""",
            (level, level),
        ).fetchone()
    assert sql_limits["upper"] == Decimal("393.067314498")
    assert sql_limits["lower"] == Decimal("391.498183502")
    ms = 1795610057123
    first, _, horizon = full_future_grid(
        ms, bar_interval_ms=60000, horizon_bars=3
    )
    bars = [
        {"start_ms": first + i * 60000,
         "closed_ms": first + (i + 1) * 60000,
         "high": high, "low": low}
        for i in range(3)
    ]
    assert label_level_outcome(
        "resistance", level, bars, decision_ms=ms,
        horizon_ms=horizon, as_of_ms=horizon + 1000,
        bar_interval_ms=60000, barrier_bps=20,
    ) == expected


def test_two_concurrent_paper_opens_commit_exactly_one_entry(qa_h8_pg):
    """Real PG advisory lock/state machine must serialize competing workers."""
    version, metadata = rule_identity()
    reason = {
        "strategy_version": version,
        "rules_version": Parameters().version, **metadata,
        "qa_source": "disposable_manual_paper_only",
    }
    at = datetime.now(UTC)
    def candidate(_):
        try:
            return ("opened", open_paper(
                qa_h8_pg, "long", Decimal("100"), Decimal("98"),
                Decimal("105"), dict(reason), observed_at=at,
            ))
        except (ValueError, RuntimeError, psycopg.Error) as exc:
            return ("rejected", type(exc).__name__)
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(candidate, range(2)))
    assert sorted(state for state, _ in outcomes) == ["opened", "rejected"]
    with connection(qa_h8_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades WHERE state='open'"
        ).fetchone()["n"] == 1
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_order_events WHERE action='entry'"
        ).fetchone()["n"] == 1
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_position_state WHERE paper_trade_id IS NOT NULL"
        ).fetchone()["n"] == 1
