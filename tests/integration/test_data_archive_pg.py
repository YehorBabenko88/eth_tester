"""Real PostgreSQL regression evidence for DATA #1 / QA #10.

Requires ETH_TEST_PG_DSN pointing ONLY to disposable local PostgreSQL admin.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.archive_quality import persist_minute_evidence
from eth_tester.config import Settings
from eth_tester.data_quality import DataIntegrityError
from eth_tester.database import connection, init_database
from eth_tester.history_integrity import store_candle
from eth_tester.live import ingest_ws
from eth_tester.storage.bulk import merge_staged_trades

pytestmark = pytest.mark.postgres
UTC = timezone.utc


@pytest.fixture
def data_settings():
    admin_dsn = os.environ.get("ETH_TEST_PG_DSN")
    if not admin_dsn:
        pytest.skip("Set ETH_TEST_PG_DSN for isolated PostgreSQL data tests")
    dbname = "eth_tester_data_" + uuid4().hex[:12]
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    try:
        config = Settings(_env_file=None, database_url=make_conninfo(admin_dsn, dbname=dbname))
        init_database(config)
        yield config
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(dbname)))


def _utc_ms():
    return int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)


def test_live_trade_identity_conflict_rollback(data_settings):
    stamp = _utc_ms()
    def message(qty):
        return {"topic": "publicTrade.ETHUSDT", "data": [
            {"i": "same-id", "T": stamp, "p": "100", "v": str(qty), "S": "Buy"}]}
    with connection(data_settings) as conn:
        ingest_ws(conn, "ETHUSDT", message("1"))
    with pytest.raises(DataIntegrityError, match="Conflicting WS trade ID"):
        with connection(data_settings) as conn:
            ingest_ws(conn, "ETHUSDT", message("2"))
    with connection(data_settings) as conn:
        saved = conn.execute("SELECT qty FROM trades WHERE trade_id='same-id'").fetchone()
        assert saved["qty"] == Decimal("1")


def test_closed_candle_conflict_preserves_original(data_settings):
    stamp = _utc_ms()
    with connection(data_settings) as conn:
        store_candle(conn, "ETHUSDT", "trade", "1", stamp, (
            Decimal("100"), Decimal("102"), Decimal("99"), Decimal("101"),
            Decimal("3"), Decimal("300")))
        store_candle(conn, "ETHUSDT", "trade", "1", stamp, (
            Decimal("100"), Decimal("102"), Decimal("99"), Decimal("101"),
            Decimal("3"), Decimal("300")))
    with pytest.raises(DataIntegrityError, match="Stored candle"):
        with connection(data_settings) as conn:
            store_candle(conn, "ETHUSDT", "trade", "1", stamp, (
                Decimal("100"), Decimal("102"), Decimal("99"), Decimal("101"),
                Decimal("4"), Decimal("400")))
    with connection(data_settings) as conn:
        found = conn.execute(
            "SELECT volume FROM candles WHERE symbol='ETHUSDT' AND kind='trade'"
        ).fetchone()
        assert found["volume"] == Decimal("3")


def test_archive_stage_atomic_conflict_and_preservation(data_settings):
    when = datetime(2026, 1, 1, tzinfo=UTC)
    with connection(data_settings) as conn:
        conn.execute("""INSERT INTO trades
            (symbol,ts,trade_id,price,qty,side,notional,source)
            VALUES('ETHUSDT',%s,'archive-identity',100,1,'Buy',100,'websocket')""",
                     (when,))
    with pytest.raises((ValueError, psycopg.Error)):
        with connection(data_settings) as conn:
            conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
            conn.execute("""INSERT INTO eth_stage
                (symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,'archive-identity',100,2,'Buy',200,'archive')""",
                         (when,))
            merge_staged_trades(conn, "eth_stage")
    with connection(data_settings) as conn:
        assert conn.execute(
            "SELECT qty FROM trades WHERE trade_id='archive-identity'"
        ).fetchone()["qty"] == Decimal("1")


def test_archive_stage_identical_replay_idempotent(data_settings):
    when = datetime(2026, 1, 1, tzinfo=UTC)
    with connection(data_settings) as conn:
        conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
        for _ in range(2):
            conn.execute("""INSERT INTO eth_stage
                (symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,'archive-repeat',100,2,'Buy',200,'archive')""",
                         (when,))
        result = merge_staged_trades(conn, "eth_stage")
        assert result["input_rows"] == 2
    with connection(data_settings) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM trades WHERE trade_id='archive-repeat'"
        ).fetchone()["n"] == 1


def test_quality_coverage_and_1440_minute_evidence(data_settings):
    day = date(2026, 1, 1)
    base = datetime.combine(day, datetime.min.time(), UTC)
    with connection(data_settings) as conn:
        for minute in range(1440):
            ts = base + timedelta(minutes=minute)
            conn.execute("""INSERT INTO candles(
                symbol,kind,tf,ts,open,high,low,close,volume,turnover,source)
                VALUES('ETHUSDT','trade','1',%s,100,100,100,100,1,100,'qa')""",
                (ts,))
            conn.execute("""INSERT INTO trades(
                symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,%s,100,1,'Buy',100,'archive')""",
                (ts, "tick-" + str(minute)))
    with connection(data_settings) as conn:
        report = persist_minute_evidence(conn, "ETHUSDT", day, "a" * 64)
        assert report["status"] == "verified"
        assert report["match"] == 1440
    with connection(data_settings) as conn:
        rows = conn.execute("""SELECT count(*) AS n FROM trade_candle_volume_checks
            WHERE symbol='ETHUSDT' AND status='match'""").fetchone()
        assert rows["n"] == 1440


def test_quality_evidence_missing_candle_never_match(data_settings):
    day = date(2026, 1, 1)
    with connection(data_settings) as conn:
        assert persist_minute_evidence(conn, "ETHUSDT", day, "b" * 64
                                       )["status"] == "incomplete_candles"
        counts = conn.execute("""SELECT status,count(*) AS n
            FROM trade_candle_volume_checks GROUP BY status""").fetchall()
        assert len(counts) == 1
        assert counts[0]["status"] == "incomplete"
        assert counts[0]["n"] == 1440


def test_canonical_archive_timestamp_matches_live_utc_milliseconds():
    from eth_tester.archives import parse_trade
    row = {"timestamp": "1767225600.123999", "trdMatchID": "fixed",
           "side": "Buy", "price": "100", "size": "1"}
    stamp,*_ = parse_trade(row)
    assert stamp == datetime.fromtimestamp(1767225600123 / 1000, UTC)


def test_archive_evidence_certificate_observed_after_source(data_settings):
    day = date(2026, 1, 2)
    start = datetime.combine(day, datetime.min.time(), UTC)
    with connection(data_settings) as conn:
        for i in range(1440):
            ts = start + timedelta(minutes=i)
            conn.execute("""INSERT INTO candles
                (symbol,kind,tf,ts,open,high,low,close,volume,turnover,source)
                VALUES ('ETHUSDT','trade','1',%s,100,100,100,100,1,100,'qa')""",
                (ts,))
            conn.execute("""INSERT INTO trades
                (symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES ('ETHUSDT',%s,%s,100,1,'Buy',100,'archive')""",
                (ts, 'cert-'+str(i)))
    before = datetime.now(UTC)
    with connection(data_settings) as conn:
        report = persist_minute_evidence(conn,"ETHUSDT",day,"c"*64)
    assert report["status"] == "verified"
    receipt = datetime.fromisoformat(report["certificate"]["observed_at"])
    assert receipt >= before - timedelta(seconds=1)
    assert report["certificate"]["archive_sha256"] == "c"*64
    assert report["certificate"]["actual_base_volume"] == "1440"
    assert report["certificate"]["expected_base_volume"] == "1440"
    assert report["certificate"]["proves_all_trade_ids"] is False
    with connection(data_settings) as conn:
        row = conn.execute("""SELECT evidence,checked_at
            FROM trade_candle_volume_checks WHERE symbol='ETHUSDT'
              AND archive_sha256=%s LIMIT 1""", ("c"*64,)).fetchone()
    assert row["evidence"]["observed_at"] == receipt.isoformat()
    assert row["checked_at"] == receipt

def test_stage_volume_includes_identical_websocket_first_trade(data_settings):
    """Archive COPY must count its own prints, not global trades.source."""
    from eth_tester.archives import certify_archive_day

    day = date(2026, 1, 3)
    stamp = datetime.combine(day, datetime.min.time(), UTC) + timedelta(minutes=12)
    when_ms = int(stamp.timestamp() * 1000)
    with connection(data_settings) as conn:
        conn.execute("""INSERT INTO candles(
            symbol,kind,tf,ts,open,high,low,close,volume,turnover,source)
            VALUES('ETHUSDT','trade','1',%s,100,100,100,100,0.5,50,'qa')""",
            (stamp,))
        ingest_ws(conn, "ETHUSDT", {"topic": "publicTrade.ETHUSDT",
            "data": [{"i": "archive-ws-overlap", "T": when_ms, "p": "100",
                      "v": "0.2", "S": "Buy"}]})
        conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
        for trade_id, qty in (("archive-ws-overlap", Decimal("0.2")),
                              ("archive-only-print", Decimal("0.3"))):
            conn.execute("""INSERT INTO eth_stage(
                symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,%s,100,%s,'Buy',%s,'archive')""",
                (stamp, trade_id, qty, qty * 100))
        merged = merge_staged_trades(conn, "eth_stage")
        assert merged["input_rows"] == 2
        report = certify_archive_day(
            conn, data_settings, day, "e" * 64,
            stage_name="eth_stage", row_count=2)
        assert report["match"] == 1
        assert report["status"] == "incomplete_candles"
        assert report["certificate"]["archive_stage_captured"] is True
        assert report["certificate"]["proves_all_trade_ids"] is False
        overlap = conn.execute(
            "SELECT source FROM trades WHERE trade_id='archive-ws-overlap'"
        ).fetchone()
        assert overlap["source"] == "websocket"
        minute = conn.execute("""SELECT trade_count,trade_base_qty,
            trade_quote_notional,status,evidence FROM trade_candle_volume_checks
            WHERE symbol='ETHUSDT' AND archive_sha256=%s AND minute_ts=%s""",
            ("e" * 64, stamp)).fetchone()
        assert minute["trade_count"] == 2
        assert minute["trade_base_qty"] == Decimal("0.5")
        assert minute["trade_quote_notional"] == Decimal("50")
        assert minute["status"] == "match"
        assert minute["evidence"]["archive_stage_captured"] is True
        coverage = conn.execute("""SELECT status,checksum,evidence
            FROM ingestion_coverage WHERE source='trades_archive'
            AND symbol='ETHUSDT' AND start_ts=%s""",
            (datetime.combine(day, datetime.min.time(), UTC),)).fetchone()
        assert coverage["status"] == "partial"
        assert coverage["checksum"] == "e" * 64
        assert coverage["evidence"]["source_receipt_ms"] > when_ms


def test_archive_first_live_replay_marks_websocket_observation(data_settings):
    """Live receipt must not be invisible to the paper engine after archive."""
    stamp = datetime(2026, 1, 4, 0, 3, tzinfo=UTC)
    when_ms = int(stamp.timestamp() * 1000)
    with connection(data_settings) as conn:
        conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
        conn.execute("""INSERT INTO eth_stage(
            symbol,ts,trade_id,price,qty,side,notional,source)
            VALUES('ETHUSDT',%s,'archive-before-live',100,0.5,
                   'Buy',50,'archive')""", (stamp,))
        merge_staged_trades(conn, "eth_stage")
        ingest_ws(conn, "ETHUSDT", {"topic": "publicTrade.ETHUSDT",
            "data": [{"i": "archive-before-live", "T": when_ms,
                      "p": "100", "v": "0.5", "S": "Buy"}]})
        found = conn.execute("""SELECT source,qty,notional FROM trades
            WHERE trade_id='archive-before-live'""").fetchone()
        assert found["source"] == "websocket"
        assert found["qty"] == Decimal("0.5")
        assert found["notional"] == Decimal("50")


def test_stage_same_sha_replay_is_immutable(data_settings):
    day = date(2026, 1, 5)
    stamp = datetime.combine(day, datetime.min.time(), UTC)
    with connection(data_settings) as conn:
        conn.execute("""INSERT INTO candles(
            symbol,kind,tf,ts,open,high,low,close,volume,turnover,source)
            VALUES('ETHUSDT','trade','1',%s,100,100,100,100,1,100,'qa')""",
            (stamp,))
        conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
        conn.execute("""INSERT INTO eth_stage(
            symbol,ts,trade_id,price,qty,side,notional,source)
            VALUES('ETHUSDT',%s,'stable-archive-print',100,1,
                   'Buy',100,'archive')""", (stamp,))
        sha = "d" * 64
        first = persist_minute_evidence(
            conn, "ETHUSDT", day, sha, stage_name="eth_stage")
        second = persist_minute_evidence(
            conn, "ETHUSDT", day, sha, stage_name="eth_stage")
        assert first["certificate"]["observed_at"] == (
            second["certificate"]["observed_at"])
        assert first["certificate"]["archive_minute_aggregate_sha256"] == (
            second["certificate"]["archive_minute_aggregate_sha256"])
        conn.execute("""UPDATE eth_stage SET qty=2,notional=200
            WHERE trade_id='stable-archive-print'""")
        with pytest.raises(DataIntegrityError, match="Conflicting immutable"):
            with conn.transaction():
                persist_minute_evidence(
                    conn, "ETHUSDT", day, sha, stage_name="eth_stage")


def test_archive_stage_conflict_does_not_publish_receipts(data_settings):
    stamp = datetime(2026, 1, 6, tzinfo=UTC)
    with connection(data_settings) as conn:
        conn.execute("""INSERT INTO trades(
            symbol,ts,trade_id,price,qty,side,notional,source)
            VALUES('ETHUSDT',%s,'global-conflict',100,1,'Buy',100,
                   'websocket')""", (stamp,))
    with pytest.raises((ValueError, psycopg.Error)):
        with connection(data_settings) as conn:
            conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
            conn.execute("""INSERT INTO eth_stage(
                symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,'global-conflict',100,2,'Buy',200,
                       'archive')""", (stamp,))
            merge_staged_trades(conn, "eth_stage")
            persist_minute_evidence(
                conn, "ETHUSDT", stamp.date(), "a" * 64,
                stage_name="eth_stage")
    with connection(data_settings) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM trade_candle_volume_checks"
        ).fetchone()["n"] == 0
        original = conn.execute(
            "SELECT qty FROM trades WHERE trade_id='global-conflict'"
        ).fetchone()
        assert original["qty"] == Decimal("1")


def test_volume_match_cannot_auto_resolve_publictrade_gap(data_settings):
    from eth_tester.archives import resolve_archived_gaps
    stamp = datetime(2026, 1, 7, tzinfo=UTC)
    with connection(data_settings) as conn:
        conn.execute("""INSERT INTO data_gaps(
            source,symbol,start_ts,end_ts,explanation)
            VALUES('publicTrade','ETHUSDT',%s,%s,'missing WS event stream')""",
            (stamp, stamp + timedelta(minutes=1)))
    assert resolve_archived_gaps(data_settings) == 0
    with connection(data_settings) as conn:
        row = conn.execute("""SELECT resolved_at FROM data_gaps
            WHERE source='publicTrade' AND symbol='ETHUSDT'""").fetchone()
        assert row["resolved_at"] is None
