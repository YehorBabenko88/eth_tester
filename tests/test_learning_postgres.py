"""Disposable PostgreSQL B-v2/F failure-closed integration.

Synthetic fixture receipts are DATABASE API tests only. They are never proof of
real exchange coverage, legitimate market history, or production ML readiness.
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
from eth_tester.ml.label_worker import run_label_cycle

pytestmark = pytest.mark.postgres
UTC = timezone.utc


@pytest.fixture(scope="module")
def learning_postgres():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("isolated PostgreSQL requires ETH_TEST_PG_DSN")
    name = "eth_tester_ci_ml_" + uuid4().hex[:12]
    settings = SimpleNamespace(
        database_url=make_conninfo(admin_dsn, dbname=name),
        initial_equity_usd=Decimal("1000"), symbol="ETHUSDT")
    try:
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(make_conninfo(admin_dsn, dbname="postgres"),
                             autocommit=True) as admin:
            if admin.execute("SELECT 1 FROM pg_database WHERE datname=%s",
                             (name,)).fetchone():
                admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(name)))


def _receipt(conn, *, source, ref, digest, bar, checked, receipt, valid=True):
    """Independent *synthetic* B storage contract witness, never A provenance."""
    payload={"fixture":"disposable_sql_contract_only", "source_receipt_ms":epoch_ms(receipt)}
    if valid:
        payload.update({"evidence_ref":ref, "receipt_is_independently_recorded":True})
    conn.execute("""INSERT INTO ingestion_coverage(
      source,symbol,instrument,interval,start_ts,end_ts,status,
      observed_rows,expected_rows,checksum,quality,evidence,checked_at)
      VALUES(%s,'ETHUSDT','linear','1',%s,%s,'verified',1,1,%s,
             'synthetic_disposable_sql_fixture',%s::jsonb,%s)""",
      (source,bar-timedelta(minutes=1),bar+timedelta(hours=1),digest,
       json.dumps(payload),checked))


def _snapshot(settings, *, valid=True):
    now=datetime.now(UTC)
    bar=now-timedelta(minutes=10)
    checked=now.replace(microsecond=0)-timedelta(seconds=2)
    receipt=checked-timedelta(seconds=1)
    ref="qa-only:independent-feature-source"
    digest=("a" if valid else "b")*64
    source="qa_ml_features_valid" if valid else "qa_ml_features_missing_proof"
    with connection(settings) as conn:
        _receipt(conn,source=source,ref=ref,digest=digest,bar=bar,
                 checked=checked,receipt=receipt,valid=valid)
        proof={"source":source,"instrument":"linear","interval":"1",
            "checksum":digest,"source_event_ms":epoch_ms(bar),
            "source_receipt_ms":epoch_ms(receipt),
            "source_checked_at_ms":epoch_ms(checked),
            "coverage_status":"verified","evidence_ref":ref}
        key=write_feature_snapshot(
            conn,symbol="ETHUSDT",tf="1",bar_ts=bar,observed_at=now,
            values=dict.fromkeys(FEATURE_NAMES,"1"),
            feature_available_ms=dict.fromkeys(FEATURE_NAMES,epoch_ms(checked)+1000),
            source="qa_disposable_fixture",source_checksum="c"*64,
            feature_sources={name:dict(proof) for name in FEATURE_NAMES},
            certified=True)
        return key


def _closed_qa_paper(settings,key):
    with connection(settings) as conn:
        decision=conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        reason={"ml_snapshot_key":key,"ml_source_checksum":"c"*64,
                "ml_signal_decision_ms":epoch_ms(decision),
                "orderflow":{"immutable_feature_snapshot":{
                     "snapshot_key":key,"source_checksum":"c"*64}}}
        trade_id=conn.execute("""INSERT INTO paper_trades(
             symbol,state,direction,signal_ts,opened_at,closed_at,
             entry,exit,qty,notional,stop,take,reason,
             gross_pnl,fee_paid,funding_paid,net_pnl,strategy_version)
             VALUES('ETHUSDT','closed','long',%s,%s,%s,
                    3000,3010,0.333,1000,2980,3040,%s::jsonb,
                    12,2,1,9,'qa_shadow_only') RETURNING id""",
             (decision,decision,decision+timedelta(milliseconds=1),
              json.dumps(reason))).fetchone()["id"]
    return trade_id,decision


def test_verified_sql_fixture_snapshot_but_no_early_horizon_label(learning_postgres):
    settings=learning_postgres
    with pytest.raises(ValueError, match="Source receipt not independently persisted"):
        _snapshot(settings,valid=False)
    key=_snapshot(settings,valid=True)
    with connection(settings) as conn:
        snap=conn.execute("SELECT quality,is_causal,provenance FROM feature_snapshots "
                          "WHERE snapshot_key=%s",(key,)).fetchone()
    assert snap["quality"]=="complete" and snap["is_causal"] is True
    assert snap["provenance"]["contract"]=="eth_feature_snapshot_v2"
    trade_id,decision=_closed_qa_paper(settings,key)
    # Deliberately old 100ms-horizon synthetic label has NO precommitted
    # 3-full-future-bar policy; 0007 must reject it even after source receipt.
    horizon=decision+timedelta(milliseconds=100)
    time.sleep(0.2)
    with connection(settings) as conn:
        observed=conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        receipt=observed-timedelta(milliseconds=1)
        _receipt(conn,source="qa_label_horizon_invalid",ref="qa:not_really_horizon",
                 digest="d"*64,bar=decision,checked=observed,receipt=receipt)
        with pytest.raises(psycopg.Error):
            with conn.transaction():
                record_paper_ml_label(
                    conn,paper_trade_id=trade_id,snapshot_key=key,
                    source_checksum="c"*64,outcome="bounce",
                    horizon_end_at=horizon,label_source="qa_label_horizon_invalid",
                    label_interval="1",label_instrument="linear",
                    covered_from_at=decision,coverage_sha256="d"*64,
                    source_receipt_at=receipt,coverage_checked_at=observed,
                    source_evidence={"barrier_evidence_sha256":"e"*64,
                        "complete_closed_bar_grid":True,"expected_bars":3,
                        "verified_bars":3,"covered_from_decision":True,
                        "decision_ms":epoch_ms(decision),
                        "first_full_bar_start_ms":((epoch_ms(decision)//60000)+1)*60000,
                        "receipt_is_independently_recorded":True})
    with connection(settings) as conn:
        assert conn.execute("SELECT count(*) AS n FROM paper_ml_labels").fetchone()["n"]==0
    examples,_=load_causal_feature_examples(settings,epoch_ms(datetime.now(UTC))+1000,tf="1")
    assert examples==[]


def test_snapshot_immutability_and_label_worker_no_fixture_upgrade(learning_postgres):
    settings=learning_postgres
    with connection(settings) as conn:
        key=conn.execute("SELECT snapshot_key FROM feature_snapshots LIMIT 1").fetchone()["snapshot_key"]
        with pytest.raises(psycopg.Error,match="append-only"):
            with conn.transaction():
                conn.execute("UPDATE feature_snapshots SET quality='bad' WHERE snapshot_key=%s",(key,))
        before=conn.execute("SELECT count(*) AS n FROM paper_ml_labels").fetchone()["n"]
    report=run_label_cycle(settings,limit=25)
    with connection(settings) as conn:
        after=conn.execute("SELECT count(*) AS n FROM paper_ml_labels").fetchone()["n"]
    assert before==after==0
    assert report["created"]==0 and report["approved"] is False
    assert report["status"]=="insufficient_verified_horizon"
    assert report["rejection_counts"]
