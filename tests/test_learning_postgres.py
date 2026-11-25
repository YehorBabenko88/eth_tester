"""Optional real PostgreSQL causal ML test in a unique disposable database."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from types import SimpleNamespace
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.database import connection, init_database
from eth_tester.ml import FEATURE_NAMES
from eth_tester.ml.extract import epoch_ms, load_causal_feature_examples


@pytest.fixture(scope="module")
def learning_postgres():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("ETH_TEST_PG_DSN required for isolated PostgreSQL ML evidence")
    name = f"eth_tester_ci_ml_{uuid4().hex[:12]}"
    settings = SimpleNamespace(
        database_url=make_conninfo(admin_dsn, dbname=name),
        initial_equity_usd=Decimal("1000"), symbol="ETHUSDT",
    )
    try:
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(make_conninfo(admin_dsn, dbname="postgres"),
                             autocommit=True) as admin:
            if admin.execute("SELECT 1 FROM pg_database WHERE datname=%s",
                             (name,)).fetchone():
                admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


def test_immutable_feature_snapshot_asof_join(learning_postgres):
    start = datetime(2026, 11, 20, 10, tzinfo=timezone.utc)
    decision = start + timedelta(minutes=4)
    opened = start + timedelta(minutes=5)
    closed = start + timedelta(minutes=10)
    feature_times = dict.fromkeys(FEATURE_NAMES, epoch_ms(start + timedelta(minutes=1)))
    values = dict.fromkeys(FEATURE_NAMES, 1.0)
    label = {
        "outcome": "bounce", "method": "fixed_horizon_barrier_v1",
        "horizon_end_ms": epoch_ms(closed),
        "issued_at_ms": epoch_ms(closed),
        "coverage_verified": True, "source_checksum": "b"*64,
    }
    with connection(learning_postgres) as conn:
        for suffix, computed in [
            ("good", start + timedelta(minutes=2)),
            ("late", start + timedelta(minutes=6)),
        ]:
            conn.execute(
                """INSERT INTO feature_snapshots(snapshot_key,symbol,tf,bar_ts,
                   available_at,observed_at,computed_at,source,source_checksum,
                   quality,is_causal,values_json,provenance)
                   VALUES(%s,'ETHUSDT','5',%s,%s,%s,%s,'test',%s,
                          'complete',true,%s::jsonb,%s::jsonb)""",
                ("ml-" + suffix, start, start+timedelta(minutes=1),
                 start+timedelta(minutes=2), computed, "a"*64,
                 json.dumps(values),
                 json.dumps({
                     "feature_available_ms": feature_times,
                     "source_coverage_verified": True,
                     "source_receipt_ms": epoch_ms(start+timedelta(minutes=2)),
                 })),
            )
        conn.execute(
            """INSERT INTO paper_trades(symbol,state,direction,signal_ts,
               opened_at,closed_at,entry,exit,qty,notional,stop,take,reason,
               gross_pnl,fee_paid,funding_paid,net_pnl,strategy_version)
               VALUES('ETHUSDT','closed','long',%s,%s,%s,3000,3010,0.333,
                      1000,2980,3040,%s::jsonb,12,2,1,9,'test-no-orders')""",
            (decision, opened, closed, json.dumps({
                "ml_outcome": "bounce", "ml_snapshot_key": "ml-good",
                "ml_source_checksum": "a"*64, "ml_label": label,
            })),
        )
    records, rejected = load_causal_feature_examples(
        learning_postgres, epoch_ms(closed)+1, tf="5"
    )
    assert len(records) == 1 and rejected == {}
    assert records[0].decision_ms == epoch_ms(decision)
    assert records[0].net_return == pytest.approx(.009)


def test_late_mutation_of_snapshot_is_forbidden(learning_postgres):
    with connection(learning_postgres) as conn:
        with pytest.raises(psycopg.Error, match="append-only"):
            with conn.transaction():
                conn.execute(
                    "UPDATE feature_snapshots SET quality='bad' WHERE snapshot_key='ml-good'"
                )
        assert conn.execute(
            "SELECT quality FROM feature_snapshots WHERE snapshot_key='ml-good'"
        ).fetchone()["quality"] == "complete"
