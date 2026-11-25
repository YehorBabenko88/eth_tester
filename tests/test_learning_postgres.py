"""Isolated real PostgreSQL B-writer -> E-binding -> F-ledger integration.

All source certificates are explicitly QA-only disposable fixtures, not proof
of real exchange print completeness or authorization for trading.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import os
import time
from types import SimpleNamespace
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.database import connection, init_database
from eth_tester.ml import FEATURE_NAMES
from eth_tester.ml.extract import epoch_ms, load_causal_feature_examples
from eth_tester.storage.snapshots import write_feature_snapshot
from eth_tester.storage.labels import record_paper_ml_label

pytestmark = pytest.mark.postgres
UTC = timezone.utc


@pytest.fixture(scope="module")
def learning_postgres():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("isolated PostgreSQL requires ETH_TEST_PG_DSN")
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


def _coverage(conn, *, source, interval, digest, start, end, checked, receipt):
    conn.execute(
        """INSERT INTO ingestion_coverage(
             source,symbol,instrument,interval,start_ts,end_ts,status,
             observed_rows,expected_rows,checksum,quality,evidence,checked_at)
           VALUES(%s,'ETHUSDT','linear',%s,%s,%s,'verified',
                  1,1,%s,'qa_fixture_verified',%s::jsonb,%s)""",
        (source, interval, start, end, digest,
         json.dumps({"source_receipt_ms": epoch_ms(receipt),
                     "fixture": "test_only_not_market_certification"}), checked),
    )


def _qa_paper(settings, *, reason, decision, closed):
    with connection(settings) as conn:
        return conn.execute(
            """INSERT INTO paper_trades(
                 symbol,state,direction,signal_ts,opened_at,closed_at,
                 entry,exit,qty,notional,stop,take,reason,
                 gross_pnl,fee_paid,funding_paid,net_pnl,strategy_version)
               VALUES('ETHUSDT','closed','long',%s,%s,%s,
                      3000,3010,0.333,1000,2980,3040,%s::jsonb,
                      12,2,1,9,'qa_shadow_only') RETURNING id""",
            (decision, decision, closed, json.dumps(reason)),
        ).fetchone()["id"]


def test_real_b_snapshot_and_append_only_horizon_ledger(learning_postgres):
    now = datetime.now(UTC)
    bar = now - timedelta(minutes=10)
    checked = now.replace(microsecond=0) - timedelta(seconds=2)
    receipt = checked - timedelta(seconds=1)
    feature_available = epoch_ms(checked) + 1000
    proof = {"source": "qa_ml_features", "instrument": "linear",
             "interval": "", "checksum": "c"*64,
             "source_event_ms": epoch_ms(bar),
             "source_receipt_ms": epoch_ms(receipt),
             "source_checked_at_ms": epoch_ms(checked),
             "coverage_status": "verified",
             "evidence_ref": "qa-only:feature-source"}
    with connection(learning_postgres) as conn:
        _coverage(conn, source="qa_ml_features", interval="", digest="c"*64,
                  start=bar-timedelta(minutes=1), end=now+timedelta(hours=1),
                  checked=checked, receipt=receipt)
        key = write_feature_snapshot(
            conn, symbol="ETHUSDT", tf="1", bar_ts=bar, observed_at=now,
            values=dict.fromkeys(FEATURE_NAMES, "1"),
            feature_available_ms=dict.fromkeys(FEATURE_NAMES, feature_available),
            source="qa_fixture", source_checksum="a"*64,
            feature_sources={name: dict(proof) for name in FEATURE_NAMES},
            certified=True,
        )
    with connection(learning_postgres) as conn:
        witness = conn.execute(
            "SELECT * FROM feature_snapshots WHERE snapshot_key=%s", (key,)
        ).fetchone()
    assert witness["quality"] == "complete"
    assert witness["provenance"]["contract"] == "eth_feature_snapshot_v2"
    assert witness["provenance"]["certified_by"] == "independent_source_proof"
    assert (witness["available_at"] == witness["observed_at"]
            == witness["computed_at"])

    decision = datetime.now(UTC)
    closed = decision + timedelta(milliseconds=40)
    horizon = decision + timedelta(milliseconds=100)
    reason = {
        "ml_snapshot_key": key, "ml_source_checksum": "a"*64,
        "ml_signal_decision_ms": epoch_ms(decision),
        "orderflow": {"immutable_feature_snapshot": {
            "snapshot_key": key, "source_checksum": "a"*64}},
    }
    trade_id = _qa_paper(
        learning_postgres, reason=reason, decision=decision, closed=closed)
    # A real DB clock must pass the predeclared, short QA-only horizon.
    time.sleep(0.35)
    label_clock = datetime.now(UTC)
    label_checked = label_clock.replace(
        microsecond=(label_clock.microsecond // 1000) * 1000)
    label_received = label_checked - timedelta(milliseconds=10)
    assert label_received >= horizon
    with connection(learning_postgres) as conn:
        _coverage(conn, source="qa_ml_label_bars", interval="1", digest="d"*64,
                  start=decision-timedelta(seconds=1),
                  end=decision+timedelta(seconds=5),
                  checked=label_checked, receipt=label_received)
        assert record_paper_ml_label(
            conn, paper_trade_id=trade_id, snapshot_key=key,
            source_checksum="a"*64, outcome="bounce",
            horizon_end_at=horizon, label_source="qa_ml_label_bars",
            label_interval="1", covered_from_at=decision,
            coverage_sha256="d"*64, source_receipt_at=label_received,
            coverage_checked_at=label_checked, label_instrument="linear",
            source_evidence={
                "barrier_evidence_sha256": "e"*64,
                "complete_closed_bar_grid": True,
                "expected_bars": 1, "verified_bars": 1,
                "fixture": "qa-only-synthetic-closed-horizon",
            },
        ) == trade_id
    cutoff = epoch_ms(datetime.now(UTC)) + 1000
    records, rejected = load_causal_feature_examples(
        learning_postgres, cutoff, tf="1")
    assert len(records) == 1 and not rejected
    assert records[0].event_id == f"paper:{trade_id}"
    assert records[0].outcome == "bounce"
    assert records[0].net_return == pytest.approx(.009)

    # Another closed paper record with no valid binding/label must be counted.
    _qa_paper(learning_postgres, reason={"qa_only": True},
              decision=datetime.now(UTC), closed=datetime.now(UTC)+timedelta(
                  milliseconds=40))
    records, rejected = load_causal_feature_examples(
        learning_postgres, epoch_ms(datetime.now(UTC))+1000, tf="1")
    assert len(records) == 1 and sum(rejected.values()) == 1


def test_real_ledger_and_snapshot_updates_are_forbidden(learning_postgres):
    with connection(learning_postgres) as conn:
        one = conn.execute("SELECT paper_trade_id,snapshot_key FROM paper_ml_labels"
                           ).fetchone()
    assert one is not None
    with connection(learning_postgres) as conn:
        with pytest.raises(psycopg.Error, match="append-only"):
            with conn.transaction():
                conn.execute(
                    "UPDATE paper_ml_labels SET outcome='breakout' "
                    "WHERE paper_trade_id=%s", (one["paper_trade_id"],))
        with pytest.raises(psycopg.Error, match="append-only"):
            with conn.transaction():
                conn.execute(
                    "UPDATE feature_snapshots SET quality='bad' "
                    "WHERE snapshot_key=%s", (one["snapshot_key"],))
