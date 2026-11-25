"""Optional real PostgreSQL integration suite.

Set ETH_TEST_PG_DSN to an administrative PostgreSQL connection URL for a
disposable PostgreSQL instance. The fixture creates and cleans ONLY a uniquely
named ETH Tester test database. Without that variable all tests SKIP.
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json
import os
from pathlib import Path
import shutil
from types import SimpleNamespace
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.database import init_database, connection
from eth_tester.storage import (
    backup_database, backup_sha256, deletion_manifest,
    drop_owned_database, restore_database, verify_backup,
)
from eth_tester.storage.bulk import copy_trades, merge_staged_trades
from eth_tester.storage.partitions import ensure_month_partition


@pytest.fixture(scope="module")
def postgres_lab():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("Real PostgreSQL suite requires ETH_TEST_PG_DSN")
    name = f"eth_tester_ci_{uuid4().hex[:12]}"
    dsn = make_conninfo(admin_dsn, dbname=name)
    settings = SimpleNamespace(database_url=dsn, initial_equity_usd=Decimal("10000"))
    try:
        init_database(settings)
        yield settings
    finally:
        # Only delete the fresh, randomly named test DB we created.
        with psycopg.connect(make_conninfo(admin_dsn, dbname="postgres"),
                             autocommit=True) as admin:
            present = admin.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s", (name,)
            ).fetchone()
            if present:
                admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


def test_migrations_and_reinitialization_preserve_rows(postgres_lab):
    with connection(postgres_lab) as conn:
        versions = conn.execute(
            "SELECT version FROM eth_schema_migrations ORDER BY version"
        ).fetchall()
        assert [r["version"] for r in versions] == [
            "0001_partitioned_archive.sql", "0002_quality_and_provenance.sql",
            "0003_retention_indexes.sql", "0004_trade_identity_guard.sql",
            "0005_paper_settlement_ledger.sql", "0006_paper_ml_labels.sql",
            "0007_bounded_live_and_archive_bins.sql", "0008_provisional_cost_observations.sql"
        ]
        conn.execute(
            """INSERT INTO funding(symbol,ts,rate) VALUES(%s,%s,%s)
               ON CONFLICT DO NOTHING""",
            ("ETHUSDT", datetime(2024, 1, 1, tzinfo=timezone.utc), Decimal("0.0001")),
        )
    init_database(postgres_lab)
    with connection(postgres_lab) as conn:
        assert conn.execute("SELECT count(*) AS n FROM funding").fetchone()["n"] == 1
        assert conn.execute("SELECT owner_tag FROM eth_storage_identity").fetchone()[
            "owner_tag"
        ] == "eth_tester"
        assert conn.execute(
            """SELECT count(*) AS n FROM pg_inherits
               WHERE inhparent=to_regclass('trade_archive_batches')"""
        ).fetchone()["n"] >= 2


def sample_trade(identifier, *, price="3000", minute=0):
    ts = datetime(2026, 11, 20, tzinfo=timezone.utc) + timedelta(minutes=minute)
    p = Decimal(price)
    quantity = Decimal("0.02")
    return ("ETHUSDT", ts, identifier, p, quantity, "Buy", p * quantity,
            "archive", {"trade_id": identifier})


def test_concurrent_database_init_is_idempotent(postgres_lab):
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=3) as pool:
        assert all(result is None for result in
                   pool.map(init_database, [postgres_lab] * 3))
    with connection(postgres_lab) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM eth_schema_migrations"
        ).fetchone()["n"] == 8


def test_copy_replay_and_conflict_rejection(postgres_lab):
    with connection(postgres_lab) as conn:
        records = [sample_trade("BPG-001"), sample_trade("BPG-002", minute=1)]
        first = copy_trades(conn, records)
        assert first["inserted_rows"] == 2
        repeated = copy_trades(conn, records)
        assert repeated["inserted_rows"] == 0
        assert repeated["deduplicated_rows"] == 2
        assert copy_trades(conn, [])["input_rows"] == 0
        assert copy_trades(conn, [])["input_rows"] == 0
    with connection(postgres_lab) as conn:
        with pytest.raises(ValueError, match="Conflicting"):
            with conn.transaction():
                copy_trades(conn, [sample_trade("BPG-001", price="3100")])
        assert conn.execute(
            "SELECT count(*) AS n FROM trades WHERE trade_id LIKE 'BPG-%'"
        ).fetchone()["n"] == 2
        with pytest.raises(ValueError, match="Conflicting"):
            with conn.transaction():
                copy_trades(conn, [sample_trade("BPG-003"),
                                   sample_trade("BPG-003", price="3100")])
        assert conn.execute(
            "SELECT count(*) AS n FROM trades WHERE trade_id='BPG-003'"
        ).fetchone()["n"] == 0


def test_two_writers_conflicting_trade_identity(postgres_lab):
    from concurrent.futures import ThreadPoolExecutor

    def insert_one(price):
        try:
            with connection(postgres_lab) as conn:
                return copy_trades(conn, [sample_trade("RACE-001", price=price)])
        except (ValueError, psycopg.Error):
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(insert_one, ("3050", "3060")))
    assert sum(isinstance(x, dict) and x["inserted_rows"] == 1
               for x in outcomes) == 1
    assert outcomes.count("rejected") == 1
    with connection(postgres_lab) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM trades WHERE trade_id='RACE-001'"
        ).fetchone()["n"] == 1


def test_append_only_feature_snapshots(postgres_lab):
    timestamp = datetime(2026, 11, 20, tzinfo=timezone.utc)
    with connection(postgres_lab) as conn:
        conn.execute(
            """INSERT INTO feature_snapshots(
                snapshot_key,symbol,tf,bar_ts,available_at,observed_at,
                source,quality,is_causal,values_json)
               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
            ("test-snap-001", "ETHUSDT", "1", timestamp,
             timestamp + timedelta(minutes=1), timestamp + timedelta(minutes=2),
             "test", "complete", True, '{"delta":"8"}'),
        )
        with pytest.raises(psycopg.Error, match="append-only"):
            with conn.transaction():
                conn.execute(
                    "UPDATE feature_snapshots SET quality='bad' WHERE snapshot_key=%s",
                    ("test-snap-001",),
                )
        assert conn.execute(
            "SELECT quality FROM feature_snapshots WHERE snapshot_key=%s",
            ("test-snap-001",),
        ).fetchone()["quality"] == "complete"


def test_monthly_partitions(postgres_lab):
    with connection(postgres_lab) as conn:
        child = ensure_month_partition(conn, "eth_storage_events", 2026, 12)
        assert child == "eth_storage_events_2026_12"
        assert conn.execute(
            """SELECT count(*) AS n FROM pg_inherits
               WHERE inhrelid=to_regclass(%s)
                 AND inhparent=to_regclass('eth_storage_events')""",
            (child,),
        ).fetchone()["n"] == 1


def test_recovery_is_explicit_and_non_destructive(postgres_lab):
    from eth_tester.storage.recovery import recover_abandoned_ingestion

    with connection(postgres_lab) as conn:
        conn.execute(
            """INSERT INTO ingestion_job(source,symbol,day,state,updated_at)
               VALUES('test_resume','ETHUSDT','2026-11-19','running',
                      now() - interval '1 day')
               ON CONFLICT(source,symbol,day)
               DO UPDATE SET state='running',updated_at=EXCLUDED.updated_at"""
        )
        with pytest.raises(RuntimeError, match="Stop all"):
            recover_abandoned_ingestion(conn)
        report = recover_abandoned_ingestion(
            conn, workers_stopped=True, stale_minutes=10
        )
        assert report["requeued"] >= 1
        assert conn.execute(
            """SELECT state FROM ingestion_job
               WHERE source='test_resume' AND symbol='ETHUSDT'
               AND day='2026-11-19'"""
        ).fetchone()["state"] == "pending"


def test_bounded_cleanup_preserves_market_data(postgres_lab):
    from eth_tester.retention import cleanup_diagnostics, storage_report

    old_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
    with connection(postgres_lab) as conn:
        conn.execute(
            """INSERT INTO feed_health(symbol,topic,ts,status)
               VALUES('ETHUSDT','test-retention',%s,'test')""",
            (old_time,),
        )
        conn.execute(
            """INSERT INTO bot_events(ts,severity,event)
               VALUES(%s,'info','test-retention')""",
            (old_time,),
        )
    report = storage_report(postgres_lab)
    assert report["bytes"] > 0
    assert report["row_counts_estimated"] is True
    assert "cluster_wal_bytes_generated" in report
    removed = cleanup_diagnostics(
        postgres_lab, health_days=90, event_days=180, max_rows_per_table=1,
    )
    assert removed["feed_health_deleted"] == 1
    assert removed["bot_events_deleted"] == 1
    assert removed["raw_market_data_deleted"] == 0
    with connection(postgres_lab) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM trades WHERE trade_id LIKE 'BPG-%'"
        ).fetchone()["n"] == 2


def test_paper_event_idempotency_and_monotonic_cursor(postgres_lab):
    signal = datetime(2026, 11, 20, tzinfo=timezone.utc)
    with connection(postgres_lab) as conn:
        trade = conn.execute(
            """INSERT INTO paper_trades(
                symbol,state,direction,signal_ts,reason,strategy_version)
               VALUES('ETHUSDT','closed','long',%s,'{}'::jsonb,'test')
               RETURNING id""",
            (signal,),
        ).fetchone()["id"]
        event_args = (
            "test-event-001", "paper_test", "ETHUSDT", "event-001",
            trade, "order-001", "entry", "Buy", Decimal("0.1"),
            Decimal("3100"), False, Decimal("1"), Decimal("0"),
            Decimal("0.5"), signal, signal + timedelta(minutes=1),
        )
        for _ in range(2):
            conn.execute(
                """INSERT INTO paper_order_events(
                   event_key,source,symbol,event_id,paper_trade_id,
                   order_id,action,side,qty,price,maker,fee,
                   spread_cost,slippage_cost,event_ts,observed_ts)
                   VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(event_key) DO NOTHING""",
                event_args,
            )
        assert conn.execute(
            "SELECT count(*) AS n FROM paper_order_events WHERE event_key='test-event-001'"
        ).fetchone()["n"] == 1
        conn.execute(
            """INSERT INTO paper_funding_events(
                paper_trade_id,funding_ts,rate,mark_price,position_qty,amount)
                VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (trade, signal, Decimal("0.0001"), Decimal("3000"),
             Decimal("0.1"), Decimal("-0.03")),
        )
        conn.execute(
            """UPDATE paper_position_state
               SET paper_trade_id=%s, version=version+1,
                   processed_cursor=%s WHERE singleton=true""",
            (trade, signal),
        )
        with pytest.raises(psycopg.Error, match="version must increase"):
            with conn.transaction():
                conn.execute(
                    "UPDATE paper_position_state SET pending_exit=false WHERE singleton=true"
                )
        with pytest.raises(psycopg.Error, match="cannot move backwards"):
            with conn.transaction():
                conn.execute(
                    """UPDATE paper_position_state SET version=version+1,
                       processed_cursor=%s WHERE singleton=true""",
                    (signal - timedelta(minutes=1),),
                )
        conn.execute(
            """UPDATE paper_position_state SET version=version+1,
               processed_cursor=%s WHERE singleton=true""",
            (signal + timedelta(minutes=1),),
        )


def test_verified_backup_restore_and_scoped_delete(postgres_lab, tmp_path, monkeypatch):
    # File-descriptor pinning is POSIX /proc-only. Windows fails CLOSED, not
    # through an unsafe PATH/native-handle fallback.
    if os.name != "posix" or not Path("/proc/self/fd").is_dir():
        pytest.skip("Round14 secure backup needs POSIX /proc FD support")
    from eth_tester.storage.secure_backup import trusted_tool
    selected = os.environ.get("ETH_TESTER_PG_BIN_DIR", "")
    if not selected:
        cli = shutil.which("pg_dump")
        if not cli:
            pytest.fail("Real PG backup test cannot find approved pg_dump/pg_restore")
        candidate = Path(os.path.realpath(cli)).parent
        if not (candidate / "pg_restore").is_file():
            pytest.fail("Real PG backup test requires ETH_TESTER_PG_BIN_DIR with native tools")
        monkeypatch.setenv("ETH_TESTER_PG_BIN_DIR", str(candidate))
    try:
        trusted_tool("pg_dump")
        trusted_tool("pg_restore")
    except (RuntimeError, OSError) as exc:
        pytest.fail("Real PG backup test lacks trusted executable: " + str(exc))
    archive = Path(tmp_path) / "owned.dump"
    backup_database(postgres_lab.database_url, archive)
    checksum = backup_sha256(archive)
    assert verify_backup(archive, expected_sha256=checksum)
    with pytest.raises(RuntimeError, match="checksum"):
        verify_backup(archive, expected_sha256="0" * 64)
    source_name = postgres_lab.database_url
    restored_name = f"eth_tester_ci_restored_{uuid4().hex[:12]}"
    restored = make_conninfo(source_name, dbname=restored_name)
    admin = make_conninfo(source_name, dbname="postgres")
    try:
        restore_database(restored, archive, expected_sha256=checksum)
        with psycopg.connect(restored) as conn:
            assert conn.execute(
                "SELECT count(*) FROM trades WHERE trade_id LIKE 'BPG-%'"
            ).fetchone()[0] == 2
        restored_backup = Path(tmp_path) / "restored-owned.dump"
        backup_database(restored, restored_backup)
        restored_sha = backup_sha256(restored_backup)
        manifest = deletion_manifest(restored, restored_backup)
        with pytest.raises(ValueError):
            drop_owned_database(
                restored, confirmation="wrong", backup_path=restored_backup,
                backup_sha=restored_sha, expected_database_oid=manifest["database_oid"],
            )
        assert drop_owned_database(
            restored, confirmation=manifest["confirmation_phrase"],
            backup_path=restored_backup, backup_sha=restored_sha,
            expected_database_oid=manifest["database_oid"],
        ) == restored_name
    finally:
        # In case an assertion above fails, never leave the test DB behind.
        with psycopg.connect(admin, autocommit=True) as conn:
            exists = conn.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s", (restored_name,)
            ).fetchone()
            if exists:
                conn.execute(
                    sql.SQL("DROP DATABASE {}").format(sql.Identifier(restored_name))
                )


@pytest.mark.skipif(
    os.environ.get("ETH_TEST_STORAGE_STRESS") != "1",
    reason="Set ETH_TEST_STORAGE_STRESS=1 for 10k-trade COPY stress",
)
def test_bulk_copy_ten_thousand(postgres_lab):
    with connection(postgres_lab) as conn:
        result = copy_trades(
            conn, (sample_trade(f"STRESS-{i:07d}", minute=i % 1440)
                   for i in range(10_000))
        )
        assert result["inserted_rows"] == 10_000
        assert copy_trades(
            conn, (sample_trade(f"STRESS-{i:07d}", minute=i % 1440)
                   for i in range(10_000))
        )["deduplicated_rows"] == 10_000



def test_round2_read_only_schema_startup(postgres_lab):
    from eth_tester.database import verify_existing_schema

    report = verify_existing_schema(postgres_lab)
    assert report["verified"] is True
    assert report["schema_version"] == "0007_bounded_live_and_archive_bins.sql"


def _synthetic_verified_round4_snapshot(conn, *, tf="5"):
    """Isolated source fixtures only; never a real-market coverage assertion."""
    from eth_tester.ml import FEATURE_NAMES
    from eth_tester.storage.snapshots import write_feature_snapshot

    bar = datetime.now(timezone.utc).replace(
        second=0, microsecond=0
    ) - timedelta(minutes=4)
    event = bar + timedelta(seconds=15)
    receipt = datetime.now(timezone.utc) - timedelta(seconds=3)
    receipt_ms = int(receipt.timestamp() * 1000)
    nonce = uuid4().hex
    sources = {}
    for name in FEATURE_NAMES:
        name_source = f"round4_fixture_{nonce}_{name}"
        checksum = ("b" * 63) + hex(len(name))[2:][-1]
        recorded = conn.execute(
            """INSERT INTO ingestion_coverage(
                 source,symbol,instrument,interval,start_ts,end_ts,status,
                 observed_rows,checksum,quality,evidence)
               VALUES(%s,'ETHUSDT','linear',%s,%s,%s,'verified',
                      1,%s,'synthetic_contract_fixture',%s::jsonb)
               RETURNING checked_at""",
            (name_source, tf, bar, bar + timedelta(minutes=1), checksum,
             json.dumps({"source_receipt_ms": receipt_ms,
                         "receipt_is_independently_recorded": True,
                         "evidence_ref": nonce + ":" + name,
                         "scope": "isolated_real_pg_synthetic_fixture"})),
        ).fetchone()
        sources[name] = {
            "source": name_source, "instrument": "linear", "interval": tf,
            "checksum": checksum,
            "source_event_ms": int(event.timestamp() * 1000),
            "source_receipt_ms": receipt_ms,
            "source_checked_at_ms": int(recorded["checked_at"].timestamp() * 1000),
            "coverage_status": "verified", "evidence_ref": nonce + ":" + name,
        }
    observation = conn.execute("SELECT clock_timestamp() AS stamp").fetchone()["stamp"]
    values = {name: Decimal("1.25") for name in FEATURE_NAMES}
    available_ms = int(observation.timestamp() * 1000)
    features = {name: available_ms for name in FEATURE_NAMES}
    args = dict(
        symbol="ETHUSDT", tf=tf, bar_ts=bar, observed_at=observation,
        values=values, feature_available_ms=features, source="round4_fixture",
        source_checksum="a" * 64, feature_sources=sources,
    )
    key = write_feature_snapshot(conn, certified=True, **args)
    return key, args


def test_round2_snapshot_writer_no_backdating_or_unverified_coverage(postgres_lab):
    from eth_tester.ml import FEATURE_NAMES
    from eth_tester.storage.snapshots import write_feature_snapshot

    bar = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=3)
    observed = datetime.now(timezone.utc)
    timestamp = int(observed.timestamp() * 1000)
    base = dict(
        symbol="ETHUSDT", tf="5", bar_ts=bar, observed_at=observed,
        values={name: Decimal("1.5") for name in FEATURE_NAMES},
        feature_available_ms={name: timestamp for name in FEATURE_NAMES},
        source="round4_provisional", source_checksum="a" * 64,
    )
    with connection(postgres_lab) as conn:
        with pytest.raises(ValueError, match="nine per-feature"):
            write_feature_snapshot(conn, certified=True, **base)
        provisional = write_feature_snapshot(conn, certified=False, **base)
        row = conn.execute(
            "SELECT quality,is_causal FROM feature_snapshots WHERE snapshot_key=%s",
            (provisional,),
        ).fetchone()
        assert row["quality"] == "partial_unverified" and not row["is_causal"]
        key, args = _synthetic_verified_round4_snapshot(conn)
        assert write_feature_snapshot(conn, certified=True, **args) == key
        certified = conn.execute(
            """SELECT quality,is_causal,available_at,observed_at,
                      computed_at,provenance
               FROM feature_snapshots WHERE snapshot_key=%s""",
            (key,),
        ).fetchone()
        assert certified["quality"] == "complete" and certified["is_causal"]
        assert certified["available_at"] == certified["observed_at"]
        assert certified["observed_at"] == certified["computed_at"]
        proof = certified["provenance"]
        assert proof["contract"] == "eth_feature_snapshot_v2"
        assert proof["source_coverage_verified"] is True
        assert proof["coverage_status"] == "verified"
        assert proof["certified_by"] == "independent_source_proof"
        assert len(proof["feature_sources"]) == len(FEATURE_NAMES) == 9
        assert proof["source_receipt_ms"] <= proof["source_checked_at_ms"]
        assert all(proof["feature_available_ms"][name] <=
                   int(certified["available_at"].timestamp() * 1000)
                   for name in FEATURE_NAMES)


def test_round4_verified_snapshot_rejects_forged_source_receipt(postgres_lab):
    from eth_tester.storage.snapshots import write_feature_snapshot

    with connection(postgres_lab) as conn:
        key, args = _synthetic_verified_round4_snapshot(conn)
        tampered = dict(args)
        tampered["feature_sources"] = {
            k: dict(v) for k, v in args["feature_sources"].items()
        }
        tampered["feature_sources"]["oi"]["source_receipt_ms"] += 100
        with pytest.raises(ValueError, match="independently persisted"):
            write_feature_snapshot(conn, certified=True, **tampered)
        tampered["feature_sources"]["oi"]["source_receipt_ms"] -= 100
        tampered["feature_sources"]["oi"]["source_event_ms"] += 86400000
        with pytest.raises(ValueError, match="Unverified|retrospective"):
            write_feature_snapshot(conn, certified=True, **tampered)
        assert conn.execute(
            "SELECT count(*) AS n FROM feature_snapshots WHERE snapshot_key=%s", (key,)
        ).fetchone()["n"] == 1


def test_round4_copy_merge_uses_dict_row_on_real_pg(postgres_lab):
    with connection(postgres_lab) as conn:
        ids = [f"STAGED-R4-{uuid4().hex[:12]}" for _ in range(2)]
        conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
        with conn.cursor() as cur:
            with cur.copy(
                "COPY eth_stage "
                "(symbol,ts,trade_id,price,qty,side,notional,source,raw) FROM STDIN"
            ) as copy:
                for ident in ids:
                    item = list(sample_trade(ident))
                    item[-1] = json.dumps(item[-1])
                    copy.write_row(item)
        merged = merge_staged_trades(conn, "eth_stage")
        assert merged == {
            "input_rows": 2, "inserted_rows": 2, "deduplicated_rows": 0
        }
        replay = merge_staged_trades(conn, "eth_stage")
        assert replay["input_rows"] == 2
        assert replay["inserted_rows"] == 0
        with pytest.raises(ValueError, match="Conflicting"):
            with conn.transaction():
                conn.execute(
                    "UPDATE eth_stage SET price=price+1 WHERE trade_id=%s", (ids[0],)
                )
                merge_staged_trades(conn, "eth_stage")
        assert conn.execute(
            "SELECT count(*) AS n FROM trades WHERE trade_id=ANY(%s)", (ids,)
        ).fetchone()["n"] == 2


def test_round4_post_horizon_label_ledger_rejects_hindsight(postgres_lab):
    """SQL-level immutable evidence tests; fixtures are not live certification."""
    from eth_tester.storage.labels import record_paper_ml_label

    bar = datetime(2026, 1, 1, 8, tzinfo=timezone.utc)
    decision = bar + timedelta(minutes=1)
    closed = bar + timedelta(minutes=2)
    horizon = bar + timedelta(minutes=5)
    first_full_start = decision + timedelta(minutes=1)
    key = f"round4-label-{uuid4().hex}"
    snapshot_sha = "a" * 64
    horizon_sha = "c" * 64
    with connection(postgres_lab) as conn:
        conn.execute(
            """INSERT INTO feature_snapshots(
               snapshot_key,symbol,tf,bar_ts,available_at,observed_at,
               computed_at,source,source_checksum,quality,is_causal,
               values_json,provenance)
               VALUES(%s,'ETHUSDT','1',%s,%s,%s,%s,'label_fixture',
                      %s,'complete',true,'{}'::jsonb,%s::jsonb)""",
            (key, bar, bar + timedelta(seconds=40),
             bar + timedelta(seconds=42), bar + timedelta(seconds=45),
             snapshot_sha, json.dumps({
                 "contract": "eth_feature_snapshot_v2",
                 "source_coverage_verified": True,
                 "scope": "synthetic_old_clock_trigger_fixture",
             })),
        )
        trade_id = conn.execute(
            """INSERT INTO paper_trades(
                 symbol,state,direction,signal_ts,opened_at,closed_at,
                 reason,strategy_version)
               VALUES('ETHUSDT','closed','long',%s,%s,%s,%s::jsonb,'fixture')
               RETURNING id""",
            (decision, decision, closed,
             json.dumps({"ml_snapshot_key": key,
                         "ml_source_checksum": snapshot_sha,
                         "side": "resistance", "level": "100",
                         "ml_label_policy": {
                             "version": "utc_full_bars_v1",
                             "method": "fixed_horizon_barrier_v1",
                             "bar_interval_ms": 60000,
                             "horizon_bars": 3, "barrier_bps": 20,
                             "anchor": "first_full_bar_strictly_after_decision",
                         }})),
        ).fetchone()["id"]
        for minute in (2, 3, 4):
            conn.execute(
                """INSERT INTO candles(
                   symbol,kind,tf,ts,open,high,low,close,volume,
                   turnover,closed,source)
                   VALUES('ETHUSDT','trade','1',%s,100,100,99.5,99.7,
                          1,100,true,'bybit_v5')
                   ON CONFLICT(symbol,kind,tf,ts) DO NOTHING""",
                (bar+timedelta(minutes=minute),),
            )
        receipt = datetime.now(timezone.utc) - timedelta(seconds=2)
        coverage = conn.execute(
            """INSERT INTO ingestion_coverage(
                 source,symbol,instrument,interval,start_ts,end_ts,status,
                 observed_rows,checksum,quality,evidence)
               VALUES('kline:trade','ETHUSDT','linear','1',%s,%s,
                      'verified',3,%s,'synthetic_closed_grid',
                      %s::jsonb) RETURNING checked_at""",
            (bar, horizon + timedelta(minutes=1), horizon_sha,
             json.dumps({"source_receipt_ms": int(receipt.timestamp() * 1000)})),
        ).fetchone()
        label_args = dict(
            paper_trade_id=trade_id, snapshot_key=key,
            source_checksum=snapshot_sha, outcome="bounce",
            horizon_end_at=horizon, label_source="kline:trade",
            label_interval="1", covered_from_at=decision,
            coverage_sha256=horizon_sha,
            source_receipt_at=receipt.replace(
                microsecond=receipt.microsecond // 1000 * 1000
            ),
            coverage_checked_at=coverage["checked_at"],
            source_evidence={
                "barrier_evidence_sha256": "d" * 64,
                "first_full_bar_start_ms": int(first_full_start.timestamp()*1000),
                "decision_ms": int(decision.timestamp()*1000),
                "expected_bars": 3, "verified_bars": 3,
                "complete_closed_bar_grid": True,
                "receipt_is_independently_recorded": True,
                "covered_from_decision": True,
            },
        )
        assert record_paper_ml_label(conn, **label_args) == trade_id
        assert record_paper_ml_label(conn, **label_args) == trade_id
        with pytest.raises(ValueError, match="Conflicting"):
            with conn.transaction():
                record_paper_ml_label(
                    conn, **{**label_args, "outcome": "breakout"}
                )
        with pytest.raises(psycopg.Error, match="append-only"):
            with conn.transaction():
                conn.execute(
                    "UPDATE paper_ml_labels SET outcome='uncertain' "
                    "WHERE paper_trade_id=%s", (trade_id,)
                )
        with pytest.raises(psycopg.Error, match="append-only"):
            with conn.transaction():
                conn.execute(
                    "DELETE FROM paper_ml_labels WHERE paper_trade_id=%s", (trade_id,)
                )
        saved = conn.execute(
            "SELECT outcome,issued_at FROM paper_ml_labels WHERE paper_trade_id=%s",
            (trade_id,),
        ).fetchone()
        assert saved["outcome"] == "bounce"
        assert saved["issued_at"] >= coverage["checked_at"]


def test_round4_label_rejects_open_paper_and_future_horizon(postgres_lab):
    from eth_tester.storage.labels import record_paper_ml_label

    key = f"round4-label-negative-{uuid4().hex}"
    snapshot_sha = "e" * 64
    old = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with connection(postgres_lab) as conn:
        conn.execute(
            """INSERT INTO feature_snapshots(snapshot_key,symbol,tf,bar_ts,
               available_at,observed_at,computed_at,source,source_checksum,
               quality,is_causal,values_json,provenance)
               VALUES(%s,'ETHUSDT','1',%s,%s,%s,%s,'fixture',%s,
                      'complete',true,'{}'::jsonb,%s::jsonb)""",
            (key, old, old + timedelta(seconds=1),
             old + timedelta(seconds=2), old + timedelta(seconds=3),
             snapshot_sha, json.dumps({
                 "contract": "eth_feature_snapshot_v2",
                 "source_coverage_verified": True,
             })),
        )
        trade = conn.execute(
            """INSERT INTO paper_trades(
                 symbol,state,direction,signal_ts,reason,strategy_version)
               VALUES('ETHUSDT','closed','long',%s,%s::jsonb,'test')
               RETURNING id""",
            (old + timedelta(minutes=1),
             json.dumps({"ml_snapshot_key": key,
                         "ml_source_checksum": snapshot_sha})),
        ).fetchone()["id"]
        now = datetime.now(timezone.utc)
        args = dict(
            paper_trade_id=trade, snapshot_key=key,
            source_checksum=snapshot_sha, outcome="bounce",
            horizon_end_at=now + timedelta(days=1),
            label_source="kline:trade", label_interval="1",
            covered_from_at=old, coverage_sha256="f" * 64,
            source_receipt_at=now - timedelta(seconds=2),
            coverage_checked_at=now - timedelta(seconds=1),
            source_evidence={"barrier_evidence_sha256": "d" * 64},
        )
        with pytest.raises(psycopg.Error, match="label must bind"):
            with conn.transaction():
                record_paper_ml_label(conn, **args)
        conn.execute(
            "UPDATE paper_trades SET closed_at=%s WHERE id=%s",
            (old + timedelta(minutes=2), trade),
        )
        with pytest.raises(psycopg.Error, match="post-horizon"):
            with conn.transaction():
                record_paper_ml_label(conn, **args)
