"""Release-gate integration across B/C/D/E/F on a disposable PostgreSQL DB.

Synthetic source rows in this suite verify interface compatibility only.
They are NOT independent evidence that all exchange trade IDs exist, and they
must not be used as production certification or to enable real trading.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.ml import FEATURE_NAMES
from eth_tester.ml.extract import epoch_ms, load_causal_feature_examples
from eth_tester.orderflow import _certified_snapshot
from eth_tester.paper import open_paper
from eth_tester.simulation.identity import rule_identity
from eth_tester.snapshot_producer import certify_snapshot
from eth_tester.storage.snapshots import write_feature_snapshot
from eth_tester.strategies import Parameters


pytestmark = pytest.mark.postgres
UTC = timezone.utc


@pytest.fixture
def qa_round4_pg():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("Real disposable PostgreSQL requires ETH_TEST_PG_DSN")
    name = "eth_tester_qa_round4_" + uuid4().hex[:12]
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        settings = Settings(
            _env_file=None, database_url=make_conninfo(admin_dsn, dbname=name),
        )
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


def _strategy_reason(snapshot=None):
    version, identity = rule_identity()
    reason = {
        "strategy_version": version,
        "rules_version": Parameters().version,
        **identity,
    }
    if snapshot is not None:
        reason["orderflow"] = {
            "admissible": True,
            "immutable_feature_snapshot": snapshot,
        }
    return reason


def test_missing_market_proof_fails_closed_across_c_b_d_e_f(qa_round4_pg):
    """No fake certification, no paper entry, no training observation."""
    settings = qa_round4_pg
    assert certify_snapshot(settings, "1")["status"] == "rejected"
    bar = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=4)
    bogus_sha = "0" * 64
    event_ms = epoch_ms(bar + timedelta(seconds=15))
    bogus_sources = {
        name: {
            "source": "unobserved_qa", "instrument": "linear",
            "interval": "1", "checksum": bogus_sha,
            "source_event_ms": event_ms,
            "source_receipt_ms": event_ms,
            "source_checked_at_ms": event_ms,
            "coverage_status": "verified",
            "evidence_ref": "missing",
        }
        for name in FEATURE_NAMES
    }
    with connection(settings) as conn:
        with pytest.raises(ValueError):
            write_feature_snapshot(
                conn, symbol=settings.symbol, tf="1", bar_ts=bar,
                observed_at=datetime.now(UTC),
                values={name: Decimal("1") for name in FEATURE_NAMES},
                feature_available_ms={
                    name: epoch_ms(datetime.now(UTC)) for name in FEATURE_NAMES
                },
                source="unobserved_qa", source_checksum=bogus_sha,
                feature_sources=bogus_sources, certified=True,
            )
    with connection(settings) as conn:
        current = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        assert _certified_snapshot(
            conn, settings.symbol, bar, bar + timedelta(minutes=1), current
        ) is None
        conn.execute(
            "UPDATE bot_control SET enabled=true,paused=false WHERE singleton=true"
        )
    forged = {
        "snapshot_key": "forged",
        "source_checksum": bogus_sha,
        "available_at": current.isoformat(),
        "computed_at": current.isoformat(),
        "feature_available_ms": {name: epoch_ms(current) for name in FEATURE_NAMES},
    }
    with pytest.raises((ValueError, RuntimeError)):
        open_paper(
            settings, "long", Decimal(100), Decimal(98), Decimal(105),
            _strategy_reason(forged), observed_at=current,
        )
    with connection(settings) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM feature_snapshots"
        ).fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_trades"
        ).fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_order_events"
        ).fetchone()["n"] == 0
    examples, _ = load_causal_feature_examples(
        settings, epoch_ms(datetime.now(UTC)), tf="1",
    )
    assert examples == []


def test_verified_snapshot_paper_binding_is_exact_and_late_ml_stays_closed(
    qa_round4_pg,
):
    """Synthetic verified coverage tests B→D→E→F wiring, NOT exchange truth."""
    settings = qa_round4_pg
    bar = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=4)
    event_ms = epoch_ms(bar + timedelta(seconds=15))
    sources = {}
    with connection(settings) as conn:
        receipt = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        receipt_ms = epoch_ms(receipt)
        for name in FEATURE_NAMES:
            digest = sha256(("qa:" + name).encode()).hexdigest()
            source = "qa_" + name
            evidence = {
                "scope": "disposable_sql_contract_only",
                "source_receipt_ms": receipt_ms,
                "source_checked_at_ms": receipt_ms,
            }
            conn.execute(
                """INSERT INTO ingestion_coverage(
                    source,symbol,instrument,interval,start_ts,end_ts,status,
                    observed_rows,expected_rows,checksum,checked_at,quality,evidence
                ) VALUES(%s,%s,'linear','1',%s,%s,'verified',1,1,%s,%s,
                    'synthetic_disposable_sql_fixture',%s::jsonb)""",
                (
                    source, settings.symbol, bar, bar + timedelta(minutes=1),
                    digest, receipt, json.dumps(evidence),
                ),
            )
            sources[name] = {
                "source": source, "instrument": "linear", "interval": "1",
                "checksum": digest, "source_event_ms": event_ms,
                "source_checked_at_ms": receipt_ms,
                "source_receipt_ms": receipt_ms,
                "coverage_status": "verified",
                "evidence_ref": source + ":" + digest,
            }

    values = {name: Decimal("1.5") for name in FEATURE_NAMES}
    with connection(settings) as conn:
        published = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        availability = {name: epoch_ms(published) for name in FEATURE_NAMES}
        key = write_feature_snapshot(
            conn, symbol=settings.symbol, tf="1", bar_ts=bar,
            observed_at=published, values=values,
            feature_available_ms=availability, source="qa_aggregated",
            source_checksum=sha256(b"qa_aggregated").hexdigest(),
            instrument="linear", feature_sources=sources, certified=True,
        )
        stored = conn.execute(
            """SELECT snapshot_key,source_checksum,available_at,observed_at,
                computed_at,quality,is_causal,provenance FROM feature_snapshots
                WHERE snapshot_key=%s""", (key,),
        ).fetchone()
        assert stored["quality"] == "complete" and stored["is_causal"]
        proof = stored["provenance"]
        assert proof["contract"] == "eth_feature_snapshot_v2"
        assert proof["source_coverage_verified"] is True
        assert set(proof["feature_sources"]) == set(FEATURE_NAMES)
        assert stored["computed_at"] >= stored["observed_at"] >= stored["available_at"]

    with connection(settings) as conn:
        decision = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        verified = _certified_snapshot(
            conn, settings.symbol, bar, bar + timedelta(minutes=1), decision
        )
        assert verified is not None, "D rejected the B canonical v2 snapshot"
        assert verified["snapshot_key"] == key
        assert verified["source_checksum"] == stored["source_checksum"]
        conn.execute(
            "UPDATE bot_control SET enabled=true,paused=false WHERE singleton=true"
        )

    opened_id = open_paper(
        settings, "long", Decimal(100), Decimal(98), Decimal(105),
        _strategy_reason(verified), observed_at=decision,
    )
    assert opened_id > 0
    with connection(settings) as conn:
        trade = conn.execute(
            """SELECT signal_ts,reason FROM paper_trades WHERE id=%s""",
            (opened_id,),
        ).fetchone()
    assert trade["reason"]["ml_snapshot_key"] == key
    assert trade["reason"]["ml_source_checksum"] == stored["source_checksum"]
    assert trade["signal_ts"] >= stored["computed_at"]
    # F must not fabricate a future structural label at paper-entry time.
    examples, _ = load_causal_feature_examples(
        settings, epoch_ms(datetime.now(UTC)), tf="1",
    )
    assert examples == []
