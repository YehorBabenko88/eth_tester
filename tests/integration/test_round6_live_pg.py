"""Disposable real PostgreSQL 17/18 separation gates for paper live evidence.

Only synthetic *negative* evidence is constructed here; this is NOT exchange
coverage certification, nor a positive production-trading acceptance.
"""
from datetime import datetime, timedelta, timezone
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
from eth_tester.ml.extract import epoch_ms, load_causal_feature_examples
from eth_tester.paper import open_paper
from eth_tester.simulation.identity import rule_identity
from eth_tester.strategies import Parameters

pytestmark = pytest.mark.postgres
UTC = timezone.utc


@pytest.fixture
def live_qa_pg():
    admin_dsn = os.getenv("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("ETH_TEST_PG_DSN needed for disposable PostgreSQL")
    dbname = "eth_tester_h6_" + uuid4().hex[:12]
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    settings = Settings(_env_file=None,
                        database_url=make_conninfo(admin_dsn, dbname=dbname))
    try:
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(dbname))
            )


def test_bounded_live_paper_evidence_has_separate_explicit_sql_contract(live_qa_pg):
    with connection(live_qa_pg) as conn:
        assert conn.execute(
            "SELECT to_regclass('public.paper_live_evidence') AS table_name"
        ).fetchone()["table_name"] is not None
        columns = {
            row["column_name"] for row in conn.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema='public'
                     AND table_name='paper_live_evidence'"""
            ).fetchall()
        }
    assert {
        "snapshot_key", "source_checksum", "symbol", "tf", "bar_ts",
        "observed_at", "computed_at", "expires_at", "quality",
        "paper_only", "ml_eligible", "evidence",
    } <= columns
    assert "source_checksum" in columns and "snapshot_key" in columns


def test_forged_bounded_live_reference_cannot_open_paper(live_qa_pg):
    """Caller flags without B-issued immutable row never grant paper entry."""
    with connection(live_qa_pg) as conn:
        conn.execute(
            "UPDATE bot_control SET enabled=true,paused=false WHERE singleton=true"
        )
    now = datetime.now(UTC)
    bar = now.replace(second=0, microsecond=0) - timedelta(minutes=2)
    version, meta = rule_identity()
    fake = {
        "snapshot_key": "a"*64, "source_checksum": "b"*64,
        "contract": "eth_paper_live_evidence_v1",
        "quality": "live_observed_bounded",
        "bar_ts": bar.isoformat(),
        "observed_at": (bar + timedelta(minutes=1)).isoformat(),
        "computed_at": (bar + timedelta(minutes=1)).isoformat(),
        "expires_at": (bar + timedelta(minutes=2, seconds=30)).isoformat(),
        "paper_only": True, "ml_eligible": False,
    }
    reason = {
        "strategy_version": version, "rules_version": Parameters().version,
        **meta, "source": "closed_candles", "signal_bar_ts": bar.isoformat(),
        "orderflow": {"admissible": True, "immutable_live_evidence": fake},
    }
    with pytest.raises((ValueError, RuntimeError)):
        open_paper(
            live_qa_pg, "long", Decimal("100"), Decimal("98"),
            Decimal("105"), reason, observed_at=now,
        )
    with connection(live_qa_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades"
        ).fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_order_events"
        ).fetchone()["n"] == 0


def test_paper_only_closed_observation_never_enters_ml_training(live_qa_pg):
    """Manual negative SQL row has NO historical v2 snapshot or future label."""
    now = datetime.now(UTC)
    decision = now - timedelta(minutes=10)
    closed = now - timedelta(minutes=3)
    reason = {
        "orderflow": {
            "admissible": True,
            "immutable_live_evidence": {
                "snapshot_key": "c"*64, "source_checksum": "d"*64,
                "contract": "eth_paper_live_evidence_v1",
                "quality": "live_observed_bounded",
                "paper_only": True, "ml_eligible": False,
            },
        },
        "source_quality": "live_observed_bounded",
    }
    with connection(live_qa_pg) as conn:
        conn.execute(
            """INSERT INTO paper_trades(
                symbol,state,direction,signal_ts,opened_at,closed_at,
                entry,exit,qty,notional,stop,take,reason,
                gross_pnl,fee_paid,funding_paid,net_pnl,strategy_version)
               VALUES('ETHUSDT','closed','long',%s,%s,%s,
                100,101,10,1000,98,105,%s::jsonb,10,2,0,8,'qa-paper-only')""",
            (decision, decision, closed, json.dumps(reason)),
        )
    rows, rejected = load_causal_feature_examples(
        live_qa_pg, epoch_ms(now), tf="1",
    )
    assert rows == []
    assert rejected, "Paper-only negative case must remain visible in diagnostics"
