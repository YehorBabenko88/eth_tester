"""B R22: real PG17/18 paper ledger concurrency and rollback acceptance.

Only a fresh UUID-named disposable database may be created.  All market
observations here are QA-only; no exchange, orders, certified PnL or ML labels.
This suite does not replace the independent genuine D/B runner integration.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
from threading import Barrier
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.paper import open_paper
from eth_tester.retention import cleanup_diagnostics
from eth_tester.simulation.persistence import record_fill
from eth_tester.strategies import Parameters, strategy_identity

pytestmark = pytest.mark.postgres
UTC = timezone.utc
D = Decimal


@pytest.fixture
def b22_pg(monkeypatch):
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("Explicit disposable PostgreSQL ETH_TEST_PG_DSN required")
    # A caller's optional auto-owned temp path is NEVER touched by PG QA.
    monkeypatch.delenv("ETH_TESTER_OWNED_TEMP_DIR", raising=False)
    name = "eth_tester_b22_" + uuid4().hex[:12]
    admin_url = make_conninfo(admin_dsn, dbname="postgres")
    target = make_conninfo(admin_dsn, dbname=name)
    created = False
    try:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            if admin.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s", (name,)
            ).fetchone():
                raise RuntimeError("Refusing to reuse an existing QA database")
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            created = True
        settings = Settings(_env_file=None, database_url=target)
        init_database(settings)
        with connection(settings) as conn:
            conn.execute(
                "UPDATE bot_control SET enabled=true,paused=false WHERE singleton=true"
            )
        yield settings
    finally:
        if created:
            with psycopg.connect(admin_url, autocommit=True) as admin:
                admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(name)
                ))


def _qa_reason():
    params = Parameters()
    return {"strategy_version": strategy_identity(),
            "rules_version": params.version, **params.identity(),
            "qa_only": True}


def _open(settings, at=None, *, conn=None):
    return open_paper(
        settings, "long", D("100"), D("98"), D("105"),
        _qa_reason(),
        observed_at=at or datetime.now(UTC) - timedelta(minutes=2),
        conn=conn,
    )


def _ledger(conn):
    trades = conn.execute(
        "SELECT id,state,notional FROM paper_trades ORDER BY id"
    ).fetchall()
    events = conn.execute(
        "SELECT paper_trade_id,event_key,event_id,action FROM "
        "paper_order_events ORDER BY event_ts,event_key"
    ).fetchall()
    state = conn.execute(
        "SELECT paper_trade_id,version,processed_cursor "
        "FROM paper_position_state WHERE singleton=true"
    ).fetchone()
    account = conn.execute(
        "SELECT equity,realized_pnl FROM paper_account WHERE id=1"
    ).fetchone()
    return trades, events, state, account


def test_two_real_connections_admit_exactly_one_1000_paper_position(b22_pg):
    """Two different PG connections contend through the original open_paper."""
    barrier = Barrier(2)
    at = datetime.now(UTC) - timedelta(minutes=2)

    def attempt():
        barrier.wait(timeout=10)
        try:
            return ("opened", _open(b22_pg, at))
        except (RuntimeError, psycopg.errors.LockNotAvailable,
                psycopg.errors.QueryCanceled) as exc:
            return ("rejected", type(exc).__name__)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(attempt)
        second = pool.submit(attempt)
        outcomes = (first.result(timeout=20), second.result(timeout=20))

    winners = [value for status, value in outcomes if status == "opened"]
    losers = [value for status, value in outcomes if status == "rejected"]
    assert len(winners) == len(losers) == 1, outcomes
    with connection(b22_pg) as conn:
        trades, events, state, _account = _ledger(conn)
    assert len(trades) == 1 and trades[0]["id"] == winners[0]
    assert trades[0]["state"] == "open"
    assert trades[0]["notional"] == D("1000")
    assert len(events) == 1 and events[0]["action"] == "entry"
    assert events[0]["paper_trade_id"] == winners[0]
    assert len(events[0]["event_key"]) == 64
    assert state["paper_trade_id"] == winners[0] and state["version"] == 1


def test_uncommitted_real_open_rolls_back_trade_fill_and_cursor(b22_pg):
    """Injected pre-COMMIT exception must not leave orphan paper rows."""
    with pytest.raises(RuntimeError, match="injected_before_commit"):
        with connection(b22_pg) as conn:
            ident = _open(b22_pg, conn=conn)
            trades, events, state, _ = _ledger(conn)
            assert len(trades) == len(events) == 1
            assert state["paper_trade_id"] == ident and state["version"] == 1
            raise RuntimeError("injected_before_commit")

    with connection(b22_pg) as conn:
        trades, events, state, account = _ledger(conn)
    assert trades == [] and events == []
    assert state["paper_trade_id"] is None and state["version"] == 0
    assert state["processed_cursor"] is None
    assert account["realized_pnl"] == D("0")

    # A fresh DB connection can recover without reusing an uncommitted ID.
    recovered = _open(b22_pg)
    with connection(b22_pg) as conn:
        trades, events, state, _ = _ledger(conn)
    assert len(trades) == len(events) == 1
    assert trades[0]["id"] == recovered and events[0]["paper_trade_id"] == recovered
    assert state["paper_trade_id"] == recovered and state["version"] == 1


def test_duplicate_event_key_rolls_back_unrelated_account_write(b22_pg):
    """Real UNIQUE constraint aborts the transaction, preserving all ledgers."""
    trade_id = _open(b22_pg)
    with connection(b22_pg) as conn:
        _trades, events, state, account = _ledger(conn)
        original = conn.execute(
            "SELECT event_ts,observed_ts,qty,price,side "
            "FROM paper_order_events WHERE event_key=%s",
            (events[0]["event_key"],),
        ).fetchone()
    assert len(events) == 1 and state["paper_trade_id"] == trade_id

    with pytest.raises(psycopg.errors.UniqueViolation):
        with connection(b22_pg) as conn:
            conn.execute(
                "UPDATE paper_account SET equity=equity-1 WHERE id=1"
            )
            record_fill(
                conn, trade_id=trade_id, symbol=b22_pg.symbol,
                event_id=events[0]["event_id"],
                at=original["event_ts"], observed_at=original["observed_ts"],
                side=original["side"], action="entry",
                qty=original["qty"], price=original["price"],
                fee=D("0"), slippage_cost=D("0"),
            )

    with connection(b22_pg) as conn:
        after_trades, after_events, after_state, after_account = _ledger(conn)
    assert len(after_trades) == len(after_events) == 1
    assert after_events == events and after_state == state
    assert after_account == account


def test_bounded_diagnostics_retention_preserves_connection_and_audit(b22_pg):
    """Actual SQL cleanup deletes old diagnostics, never source/incident rows."""
    old = datetime.now(UTC) - timedelta(days=210)
    with connection(b22_pg) as conn:
        conn.execute(
            "INSERT INTO feed_health(symbol,topic,ts,status) VALUES "
            "('ETHUSDT','connection',%s,'disconnected'), "
            "('ETHUSDT','publicTrade',%s,'disconnected')",
            (old, old),
        )
        conn.execute(
            "INSERT INTO bot_events(ts,severity,event,data) VALUES "
            "(%s,'info','expired_diagnostic','{}'::jsonb), "
            "(%s,'info','paper_trade_review_v1','{}'::jsonb), "
            "(%s,'warning','telegram_command',"
            "'{\"retention\":\"preserve\"}'::jsonb)",
            (old, old, old),
        )
    result = cleanup_diagnostics(b22_pg, health_days=90, event_days=180)
    assert result["feed_health_deleted"] == 1
    assert result["bot_events_deleted"] == 1
    assert result["raw_market_data_deleted"] == 0
    assert result["cleanup_complete"] is False
    with connection(b22_pg) as conn:
        topics = conn.execute("SELECT topic FROM feed_health").fetchall()
        records = conn.execute("SELECT event FROM bot_events ORDER BY event").fetchall()
    assert [row["topic"] for row in topics] == ["connection"]
    assert [row["event"] for row in records] == [
        "paper_trade_review_v1", "telegram_command",
    ]
