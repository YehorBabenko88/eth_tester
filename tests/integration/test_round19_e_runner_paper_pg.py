"""Disposable PostgreSQL Round19 integration: real E engine, position and ledger.

Synthetic D signal and B admission proof are explicitly test-local: this
proves runner -> actual open_paper -> actual PostgreSQL atomic insertion, NOT
authentic WS/OI/L2 evidence or exchange trading/economic certification.
"""
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4
import os

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester import paper, runner
from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.strategies import Parameters, strategy_identity

pytestmark = pytest.mark.postgres


@pytest.fixture
def disposable_e19_pg():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("Disposable PostgreSQL ETH_TEST_PG_DSN required")
    name = "eth_tester_e19_" + uuid4().hex[:12]
    with psycopg.connect(make_conninfo(admin_dsn, dbname="postgres"),
                         autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        settings = Settings(_env_file=None, database_url=make_conninfo(
            admin_dsn, dbname=name))
        init_database(settings)
        with connection(settings) as conn:
            conn.execute("UPDATE bot_control SET enabled=true,paused=false "
                         "WHERE singleton=true")
        yield settings
    finally:
        with psycopg.connect(make_conninfo(admin_dsn, dbname="postgres"),
                             autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(name)))


def test_real_runner_and_paper_insert_atomic_paper_ledger(disposable_e19_pg,
                                                          monkeypatch):
    """Do not mock runner.engine_tick, paper.open_paper, or PostgreSQL writes."""
    settings = disposable_e19_pg
    with connection(settings) as conn:
        now = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        bar = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        for offset in range(34, -1, -1):
            stamp = bar - timedelta(minutes=offset)
            conn.execute(
                """INSERT INTO candles(symbol,kind,tf,ts,open,high,low,close,
                     volume,closed,source,quality)
                   VALUES('ETHUSDT','trade','1',%s,100,101,99,100,
                          20,true,'test_synthetic','unverified')""",
                (stamp,),
            )
        conn.execute(
            """INSERT INTO feed_health(symbol,topic,ts,status,detail)
               VALUES('ETHUSDT','connection',%s,'connected',
                      '{"session_id":"qa_e19_unverified"}'::jsonb)""",
            (now,),
        )
        conn.execute(
            """INSERT INTO trades(symbol,ts,trade_id,price,qty,side,notional,
                                  source,raw)
               VALUES('ETHUSDT',%s,'e19-last',100,1,'Buy',100,
                      'websocket','{}'::jsonb)""",
            (now,),
        )

    # Only the independent D candidate and B source attestation are synthetic.
    # The full E runner, entry economic checks, transaction, and SQL ledger
    # remain REAL and run against a disposable PostgreSQL service.
    def deterministic_candidate(_bars, _fee, _slippage):
        return SimpleNamespace(
            direction="long", stop=Decimal("98"), take=Decimal("105"),
            reason={
                "strategy_version": strategy_identity(),
                "rules_version": Parameters().version,
                **Parameters().identity(),
                "source": "closed_candles",
                "signal_bar_ts": bar.isoformat(),
            },
        )

    def synthetic_d_confirmation(_conn, _settings, _signal, _bar,
                                 *, observed_at=None):
        assert observed_at is not None
        return True, {
            "admissible": True,
            "immutable_live_evidence": {
                "snapshot_key": "a" * 64,
                "source_checksum": "b" * 64,
                "contract": "eth_paper_live_evidence_v1",
                "quality": "live_observed_bounded",
                "paper_only": True,
                "ml_eligible": False,
                "bar_ts": bar.isoformat(),
                "observed_at": (bar + timedelta(minutes=1)).isoformat(),
                "computed_at": (bar + timedelta(minutes=1)).isoformat(),
                "expires_at": (bar + timedelta(minutes=2, seconds=30)).isoformat(),
            },
        }

    verified = []

    def test_only_b_gate(conn, *, symbol, reason, decision_at):
        # This synthetic source-gate mock is NOT market-data acceptance.
        assert symbol == settings.symbol
        assert reason["paper_only"] is True
        assert reason["ml_eligible"] is False
        assert "ml_snapshot_key" not in reason and "ml_label" not in reason
        assert reason["paper_live_decision_ms"] == int(
            decision_at.timestamp() * 1000)
        assert conn.execute("SELECT 1 AS witness").fetchone()["witness"] == 1
        verified.append(reason["paper_live_snapshot_key"])
        return {"snapshot_key": reason["paper_live_snapshot_key"]}

    monkeypatch.setattr(runner, "price_signal", deterministic_candidate)
    monkeypatch.setattr(runner, "confirm_rejection", synthetic_d_confirmation)
    monkeypatch.setattr(paper, "verify_live_paper_row", test_only_b_gate)
    from eth_tester import charts
    monkeypatch.setattr(charts, "render_trade", lambda *_args: None)

    outcome = runner.engine_tick(settings)
    assert outcome.startswith("opened_"), outcome
    assert verified == ["a" * 64]
    trade_id = int(outcome.removeprefix("opened_"))
    with connection(settings) as conn:
        trades = conn.execute(
            "SELECT id,state,notional,funding_verified,reason "
            "FROM paper_trades ORDER BY id"
        ).fetchall()
        assert len(trades) == 1
        assert trades[0]["id"] == trade_id
        assert trades[0]["state"] == "open"
        assert trades[0]["notional"] == Decimal(1000)
        assert trades[0]["funding_verified"] is False
        assert trades[0]["reason"]["ml_eligible"] is False
        assert trades[0]["reason"]["paper_only"] is True
        assert trades[0]["reason"]["paper_risk_budget"]["costs_certified"] is False
        assert trades[0]["reason"]["paper_risk_budget"]["funding_unknown_is_zero"] is False
        signals = conn.execute(
            "SELECT decision,reason FROM strategy_signals"
        ).fetchall()
        assert len(signals) == 1 and signals[0]["decision"] == "opened"
        assert signals[0]["reason"]["paper_trade_id"] == trade_id
        fills = conn.execute(
            "SELECT action,event_key,paper_trade_id FROM paper_order_events "
            "WHERE paper_trade_id=%s ORDER BY event_ts,event_key",
            (trade_id,),
        ).fetchall()
        assert len(fills) == 1 and fills[0]["action"] == "entry"
        assert fills[0]["paper_trade_id"] == trade_id
        assert fills[0]["event_key"]
        assert conn.execute(
            "SELECT paper_trade_id FROM paper_position_state "
            "WHERE singleton=true"
        ).fetchone()["paper_trade_id"] == trade_id

    # A duplicate loop cannot insert another position or entry event.
    runner.engine_tick(settings)
    with connection(settings) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades"
        ).fetchone()["n"] == 1
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_order_events WHERE action='entry'"
        ).fetchone()["n"] == 1
