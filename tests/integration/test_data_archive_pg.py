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


def _create_archive_stage(conn):
    """Stage synthetic archive prints with their original-shaped raw JSON.

    The production COPY always provides raw. Synthetic SQL test inserts did
    not, causing NOT NULL failures before the intended identity assertions.
    Preserve the strict schema; derive JSON from each inserted fixture row.
    """
    conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
    conn.execute("""
        CREATE FUNCTION pg_temp.stage_fixture_raw() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.raw IS NULL THEN
                NEW.raw = jsonb_build_object(
                    'timestamp', extract(epoch FROM NEW.ts)::text,
                    'trdMatchID', NEW.trade_id,
                    'symbol', NEW.symbol,
                    'side', NEW.side,
                    'price', NEW.price::text,
                    'size', NEW.qty::text
                );
            END IF;
            RETURN NEW;
        END $$""")
    conn.execute("""
        CREATE TRIGGER stage_fixture_raw_before_insert
        BEFORE INSERT ON eth_stage FOR EACH ROW
        EXECUTE FUNCTION pg_temp.stage_fixture_raw()""")


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
            _create_archive_stage(conn)
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
        _create_archive_stage(conn)
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
        _create_archive_stage(conn)
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
        _create_archive_stage(conn)
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
        _create_archive_stage(conn)
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
            _create_archive_stage(conn)
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

def test_sha_archive_membership_excludes_ws_only_not_ws_overlap(data_settings):
    """Exact tape members survive source=websocket; extra WS prints stay out."""
    from eth_tester.storage.archive_receipts import persist_archive_stage_bins
    from eth_tester.archive_quality import compare_persisted_archive_bins

    day = date(2026, 1, 8)
    at = datetime.combine(day, datetime.min.time(), UTC)
    sha = "f" * 64
    with connection(data_settings) as conn:
        for identity, qty in (("ws-overlap", Decimal("0.2")),
                              ("ws-extra", Decimal("99"))):
            conn.execute("""INSERT INTO trades(
                symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,%s,100,%s,'Buy',%s,'websocket')""",
                (at, identity, qty, qty*100))
        _create_archive_stage(conn)
        for identity, qty in (("ws-overlap", Decimal("0.2")),
                              ("archive-only", Decimal("0.3"))):
            conn.execute("""INSERT INTO eth_stage(
                symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,%s,100,%s,'Buy',%s,'archive')""",
                (at, identity, qty, qty*100))
        result = merge_staged_trades(conn, "eth_stage")
        assert result["input_rows"] == 2
        persist_archive_stage_bins(
            conn, "ETHUSDT", day, sha, 2,
            stage_name="eth_stage", instrument="linear")
        rows = conn.execute("""SELECT sum(trade_count) AS n,
                sum(base_qty) AS qty,sum(quote_notional) AS notional
            FROM archive_source_price_bins
            WHERE symbol='ETHUSDT' AND archive_day=%s
              AND archive_sha256=%s""",
            (day,sha)).fetchone()
        assert rows["n"] == 2
        assert rows["qty"] == Decimal("0.5")
        assert rows["notional"] == Decimal("50")
        assert conn.execute(
            "SELECT source FROM trades WHERE trade_id='ws-overlap'"
        ).fetchone()["source"] == "websocket"
        first = compare_persisted_archive_bins(conn, "ETHUSDT", day, sha)
        assert first["status"] == "incomplete_candles"
        assert first["receipt"]["ml_snapshot_eligible"] is False
        assert first["receipt"]["proves_all_trade_ids"] is False


def test_archive_sha_pinned_recheck_after_official_candles(data_settings):
    """Late official bars improve a NEW report, never retroactively change source."""
    from eth_tester.storage.archive_receipts import persist_archive_stage_bins
    from eth_tester.archive_quality import compare_persisted_archive_bins

    day = date(2026, 1, 9)
    at = datetime.combine(day, datetime.min.time(), UTC)
    sha = "7" * 64
    with connection(data_settings) as conn:
        _create_archive_stage(conn)
        conn.execute("""INSERT INTO eth_stage(
            symbol,ts,trade_id,price,qty,side,notional,source)
            VALUES('ETHUSDT',%s,'late-candle-print',100,2,
                   'Sell',200,'archive')""", (at,))
        merge_staged_trades(conn, "eth_stage")
        persist_archive_stage_bins(
            conn, "ETHUSDT", day, sha, 1,
            stage_name="eth_stage", instrument="linear")
        earlier = compare_persisted_archive_bins(conn, "ETHUSDT", day, sha)
        assert earlier["status"] == "incomplete_candles"
        candle_rows = [
            (at+timedelta(minutes=i), Decimal("2") if i==0 else Decimal(0),
             Decimal("200") if i==0 else Decimal(0))
            for i in range(1440)
        ]
        with conn.cursor() as cursor:
            cursor.executemany("""INSERT INTO candles(
                symbol,kind,tf,ts,open,high,low,close,volume,turnover,source)
                VALUES('ETHUSDT','trade','1',%s,100,100,100,100,%s,%s,'qa')""",
                candle_rows)
        latest = compare_persisted_archive_bins(conn, "ETHUSDT", day, sha)
        assert latest["status"] == "verified"
        assert latest["match"] == 1440
        assert latest["receipt"]["archive_sha256"] == sha
        assert latest["receipt"]["ml_snapshot_eligible"] is False
        assert latest["receipt"]["checked_at"] >= earlier["receipt"]["checked_at"]
        assert earlier["receipt"]["status"] == "incomplete_candles"
        assert latest["receipt"]["archive_bins_sha256"] == (
            earlier["receipt"]["archive_bins_sha256"])


def test_completed_archive_rehydrates_missing_membership_without_sha_change(data_settings):
    """Old complete acquisition can be SHA-replayed; no retrospective ML proof."""
    import asyncio
    import gzip
    from hashlib import sha256
    import httpx
    from eth_tester.archives import download_archive

    day = date(2026, 1, 10)
    csv_text = (
        "timestamp,trdMatchID,side,price,size,symbol\\n"
        "1768003200,rehydrate-id,Buy,100,1,ETHUSDT\\n"
    ).replace("\\n", "\n")
    payload = gzip.compress(csv_text.encode("utf8"))
    digest = sha256(payload).hexdigest()
    with connection(data_settings) as conn:
        conn.execute("""INSERT INTO ingestion_job(
            source,symbol,day,state,attempts,bytes_downloaded,row_count,sha256)
            VALUES('trades_archive','ETHUSDT',%s,'complete',1,%s,1,%s)""",
            (day, len(payload), digest))
    count = {"requests": 0}

    def handler(request):
        count["requests"] += 1
        return httpx.Response(200, content=payload, request=request)

    settings = data_settings.model_copy(update={"minimum_free_gb": 0})
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            assert await download_archive(settings, day, client) == "complete"
            assert await download_archive(settings, day, client) == "cached"
    asyncio.run(run())
    assert count["requests"] == 1
    with connection(settings) as conn:
        stored = conn.execute("""SELECT state,sha256 FROM ingestion_job
            WHERE source='trades_archive' AND symbol='ETHUSDT' AND day=%s""",
            (day,)).fetchone()
        assert stored["state"] == "complete"
        assert stored["sha256"] == digest
        member = conn.execute("""SELECT sum(trade_count) AS n
            FROM archive_source_price_bins WHERE archive_day=%s
              AND archive_sha256=%s""", (day,digest)).fetchone()
        assert member["n"] == 1
        assert conn.execute("""SELECT count(*) AS n FROM feature_snapshots
            WHERE quality='complete'""").fetchone()["n"] == 0

def test_real_pg_fast_closed_minute_receipt_is_db_clock_not_bar_close(data_settings):
    """Fast real PG producer with mock Bybit payload: no historical backdate."""
    import asyncio
    from eth_tester.history_integrity import collect_latest_closed_trade_minute

    class ClosedResponse:
        async def get(self, path, **params):
            assert path == "kline" and params["interval"] == "1"
            assert params["start"] == params["end"]
            return {"list": [[str(params["start"]), "100", "101", "99",
                              "100", "1.25", "125"]]}
    report = asyncio.run(
        collect_latest_closed_trade_minute(data_settings, ClosedResponse()))
    assert report["status"] == "closed_grid_observed"
    assert report["bar_end_ms"] <= report["source_receipt_ms"]
    assert report["proves_all_trade_ids"] is False
    stamp = datetime.fromtimestamp(report["bar_start_ms"] / 1000, UTC)
    with connection(data_settings) as conn:
        candle = conn.execute("""SELECT volume,turnover,closed FROM candles
            WHERE symbol='ETHUSDT' AND kind='trade' AND tf='1' AND ts=%s""",
            (stamp,)).fetchone()
        assert candle["closed"] is True
        assert candle["volume"] == Decimal("1.25")
        assert candle["turnover"] == Decimal("125")
        evidence = conn.execute("""SELECT status,quality,checked_at,evidence
            FROM ingestion_coverage
            WHERE source='kline:trade' AND symbol='ETHUSDT'
              AND interval='1' AND start_ts=%s""",
            (stamp,)).fetchone()
    assert evidence["status"] == "verified"
    assert evidence["quality"] == "exchange_closed_grid_checked"
    assert evidence["evidence"]["proves_all_trade_ids"] is False
    assert evidence["evidence"]["source_receipt_ms"] >= report["bar_end_ms"]
    assert int(evidence["checked_at"].timestamp()*1000) >= (
        report["source_receipt_ms"])


def test_real_pg_recent_oi_receipt_stays_partial(data_settings):
    """Actual PostgreSQL B OI witness, never fabricated full 5m historical grid."""
    import asyncio
    from eth_tester.history_series import collect_recent_oi_receipt

    class OIResponse:
        async def get(self, path, **params):
            assert path == "open-interest" and params["intervalTime"] == "5min"
            return {"list": [{"timestamp": str(params["endTime"]),
                              "openInterest": "317.25"}]}
    result = asyncio.run(collect_recent_oi_receipt(data_settings, OIResponse()))
    assert result["status"] == "oi_observed_partial"
    assert result["ml_snapshot_eligible"] is False
    assert result["source_event_ms"] <= result["source_receipt_ms"]
    stamp = datetime.fromtimestamp(result["source_event_ms"] / 1000, UTC)
    with connection(data_settings) as conn:
        stored = conn.execute("""SELECT value FROM open_interest
            WHERE symbol='ETHUSDT' AND interval='5min' AND ts=%s""",
            (stamp,)).fetchone()
        assert stored["value"] == Decimal("317.25")
        witness = conn.execute("""SELECT status,quality,evidence,checked_at
            FROM ingestion_coverage
            WHERE source='oi' AND symbol='ETHUSDT'
              AND interval='5min' AND start_ts<=%s AND end_ts>%s
            ORDER BY checked_at DESC LIMIT 1""",
            (stamp, stamp)).fetchone()
    assert witness["status"] == "partial"
    assert witness["evidence"]["oi_receipt_and_grid_verified"] is False
    assert witness["evidence"]["source_receipt_ms"] == (
        result["source_receipt_ms"])
    assert witness["checked_at"] >= stamp

def test_old_complete_archive_changed_remote_bytes_cannot_mutate_job(data_settings):
    """Preexisting SHA is immutable even if the public host replaces a gz."""
    import asyncio
    import gzip
    import httpx
    from eth_tester.archives import download_archive

    day = date(2026, 1, 11)
    old_sha = "1" * 64
    with connection(data_settings) as conn:
        conn.execute("""INSERT INTO ingestion_job(
            source,symbol,day,state,attempts,sha256,row_count)
            VALUES('trades_archive','ETHUSDT',%s,'complete',3,%s,1)""",
            (day,old_sha))
    payload = gzip.compress(b"timestamp,trdMatchID,side,price,size,symbol\n"
                            b"1768089600,x,Buy,100,1,ETHUSDT\n")
    def response(request):
        return httpx.Response(200, content=payload, request=request)
    async def attempt():
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
            await download_archive(
                data_settings.model_copy(update={"minimum_free_gb":0}), day, client)
    with pytest.raises(DataIntegrityError, match="SHA256 changed"):
        asyncio.run(attempt())
    with connection(data_settings) as conn:
        original = conn.execute("""SELECT state,sha256,attempts
            FROM ingestion_job WHERE source='trades_archive'
              AND symbol='ETHUSDT' AND day=%s""", (day,)).fetchone()
        assert original["state"] == "complete"
        assert original["sha256"] == old_sha
        assert original["attempts"] == 3
        assert conn.execute("""SELECT count(*) AS n FROM trades
            WHERE symbol='ETHUSDT'""").fetchone()["n"] == 0


def test_identical_ws_replay_inside_one_frame_db_source_identity(data_settings):
    """Identical on-wire repeats merge to one trade + one B receipt, never x2."""
    at = datetime.now(UTC) - timedelta(seconds=2)
    stamp = int(at.timestamp()*1000)
    at = datetime.fromtimestamp(stamp/1000,UTC)
    row = {"i": "one-wire-replay", "T": stamp, "p": "100",
           "v": "0.5", "S": "Buy"}
    wire = {"topic": "publicTrade.ETHUSDT", "data": [
        row, dict(row, p="100.000", v="0.5000")]}
    with connection(data_settings) as conn:
        ingest_ws(conn, "ETHUSDT", wire, session_id="real-pg-duplicate")
    with connection(data_settings) as conn:
        assert conn.execute("""SELECT count(*) AS n FROM trades
            WHERE symbol='ETHUSDT' AND trade_id='one-wire-replay'"""
        ).fetchone()["n"] == 1
        receipt = conn.execute("""SELECT session_id,qty,exchange_complete
            FROM live_trade_receipts WHERE symbol='ETHUSDT'
              AND trade_id='one-wire-replay'""").fetchone()
        assert receipt["session_id"] == "real-pg-duplicate"
        assert receipt["qty"] == Decimal("0.5")
        assert receipt["exchange_complete"] is False
        detail = conn.execute("""SELECT detail FROM feed_health
            WHERE symbol='ETHUSDT' AND topic='publicTrade'
            ORDER BY ts DESC LIMIT 1""").fetchone()["detail"]
        assert detail["observed_rows"] == 2
        assert detail["distinct_trade_ids"] == 1


def test_archive_recheck_late_official_candles_appends_never_rewrites(data_settings):
    """Actual B 0007 versioned receipts: missing -> full original 1440 grid."""
    from eth_tester.archives import revalidate_completed_archive
    from eth_tester.storage.archive_receipts import persist_archive_stage_bins

    day = date(2026, 1, 12)
    start = datetime.combine(day, datetime.min.time(), UTC)
    digest = "a" * 64
    with connection(data_settings) as conn:
        _create_archive_stage(conn)
        conn.execute("""INSERT INTO eth_stage(
            symbol,ts,trade_id,price,qty,side,notional,source)
            VALUES('ETHUSDT',%s,'source-tape-id',100,2,
                   'Buy',200,'archive')""", (start,))
        merge_staged_trades(conn, "eth_stage")
        persist_archive_stage_bins(
            conn, "ETHUSDT", day, digest, 1, instrument="linear")
        conn.execute("""INSERT INTO ingestion_job(
            source,symbol,day,state,attempts,sha256,row_count)
            VALUES('trades_archive','ETHUSDT',%s,'complete',1,%s,1)""",
            (day, digest))
        conn.execute("""INSERT INTO candles(
            symbol,kind,tf,ts,open,high,low,close,volume,turnover,source)
            VALUES('ETHUSDT','trade','1',%s,100,100,100,100,
                   2,200,'bybit_v5')""", (start,))
    first = revalidate_completed_archive(data_settings, day)
    assert first["status"] == "incomplete"
    assert first["exchange_complete"] is False
    with connection(data_settings) as conn:
        with conn.cursor() as cursor:
            cursor.executemany("""INSERT INTO candles(
                symbol,kind,tf,ts,open,high,low,close,volume,turnover,source)
                VALUES('ETHUSDT','trade','1',%s,100,100,100,100,
                       0,0,'bybit_v5')""",
                [(start+timedelta(minutes=i),) for i in range(1,1440)])
    final = revalidate_completed_archive(data_settings, day)
    assert final["status"] == "volume_only_reconciled"
    assert final["minutes_checked"] == final["minutes_matched"] == 1440
    assert final["validation_key"] != first["validation_key"]
    again = revalidate_completed_archive(data_settings, day)
    assert again["validation_key"] == final["validation_key"]
    with connection(data_settings) as conn:
        rows = conn.execute("""SELECT validation_key,status,minutes_checked
            FROM archive_validation_receipts WHERE symbol='ETHUSDT'
              AND archive_day=%s ORDER BY checked_at""",
            (day,)).fetchall()
        assert len(rows) == 2
        assert {row["status"] for row in rows} == {
            "incomplete", "volume_only_reconciled"}
        saved = conn.execute("""SELECT state,sha256 FROM ingestion_job
            WHERE source='trades_archive' AND symbol='ETHUSDT' AND day=%s""",
            (day,)).fetchone()
        assert saved["state"] == "complete" and saved["sha256"] == digest
        assert conn.execute("""SELECT count(*) AS n FROM feature_snapshots
            WHERE quality='complete'""").fetchone()["n"] == 0


def test_old_complete_archive_404_cannot_erase_source_sha(data_settings):
    """Post-acquisition HTTP 404 is failure of recovery, not prior data."""
    import asyncio
    import httpx
    from eth_tester.archives import download_archive

    day = date(2026, 1, 13)
    digest = "b" * 64
    with connection(data_settings) as conn:
        conn.execute("""INSERT INTO ingestion_job(
            source,symbol,day,state,attempts,sha256,row_count)
            VALUES('trades_archive','ETHUSDT',%s,'complete',4,%s,1)""",
            (day,digest))
    def failure(request):
        return httpx.Response(404,request=request)
    async def run():
        async with httpx.AsyncClient(
                transport=httpx.MockTransport(failure)) as client:
            await download_archive(
                data_settings.model_copy(update={"minimum_free_gb":0}),
                day, client)
    with pytest.raises(DataIntegrityError, match="Previously completed archive"):
        asyncio.run(run())
    with connection(data_settings) as conn:
        job = conn.execute("""SELECT state,sha256,attempts FROM ingestion_job
            WHERE source='trades_archive' AND symbol='ETHUSDT' AND day=%s""",
            (day,)).fetchone()
        assert job["state"] == "complete"
        assert job["sha256"] == digest
        assert job["attempts"] == 4
        assert conn.execute("""SELECT count(*) AS n FROM
            archive_source_price_bins WHERE archive_day=%s""",
            (day,)).fetchone()["n"] == 0


def test_round9_pg_unclean_process_restart_serializes_real_source_gap(data_settings):
    """Two DB clients: A outage writer owns exact E/B admission lock.

    Only disposable test DB, no exchange API or real orders. The pending gap is
    deliberately unresolved and the original trade-ID timeline not fabricated.
    """
    from psycopg.rows import dict_row
    from eth_tester.live import reconcile_unclean_ws_start

    lock_key = "eth_paper_admission_source:ETHUSDT"
    with connection(data_settings) as conn:
        conn.execute("""INSERT INTO feed_health(symbol,topic,status,detail)
            VALUES('ETHUSDT','connection','connected',
                   '{"session_id":"crashed","ml_eligible":false}'::jsonb)""")
    with connection(data_settings) as writer:
        pending = reconcile_unclean_ws_start(writer, "ETHUSDT")
        assert pending is not None
        with psycopg.connect(
                data_settings.database_url, row_factory=dict_row) as reader:
            reader.execute("SET search_path TO public")
            blocked = reader.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS acquired",
                (lock_key,)).fetchone()
            assert blocked["acquired"] is False
            # Transaction snapshot under default READ COMMITTED cannot see the
            # uncommitted A disconnected state or its new unresolved gap.
            assert reader.execute("""SELECT count(*) AS n FROM feed_health
                WHERE symbol='ETHUSDT' AND topic='connection'
                AND status='disconnected'""").fetchone()["n"] == 0
        # writer's context commits while preserving the session fence.
    with connection(data_settings) as reader:
        assert reader.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS acquired",
            (lock_key,)).fetchone()["acquired"] is True
        state = reader.execute("""SELECT status FROM feed_health WHERE
            symbol='ETHUSDT' AND topic='connection'
            ORDER BY ts DESC LIMIT 1""").fetchone()
        assert state["status"] == "disconnected"
        original_gap = reader.execute("""SELECT id,source,start_ts,end_ts,resolved_at
            FROM data_gaps WHERE symbol='ETHUSDT' AND source='publicTrade'
            ORDER BY id DESC LIMIT 1""").fetchone()
        assert original_gap["id"] == pending
        assert original_gap["end_ts"] is None
        assert original_gap["resolved_at"] is None


def test_round9_pg_shutdown_outage_atomic_source_fence(data_settings):
    """An A orderly cancel also records source loss under the exact B/E fence."""
    from psycopg.rows import dict_row
    from eth_tester.live import persist_ws_disconnect

    stamp = datetime.now(UTC) - timedelta(seconds=5)
    lock_key = "eth_paper_admission_source:ETHUSDT"
    with connection(data_settings) as writer:
        new_gap = persist_ws_disconnect(
            writer, "ETHUSDT", "test-shutdown", "worker_cancelled",
            last_trade=stamp, connected_at=stamp-timedelta(minutes=1))
        with psycopg.connect(
                data_settings.database_url, row_factory=dict_row) as reader:
            assert reader.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS acquired",
                (lock_key,)).fetchone()["acquired"] is False
    with connection(data_settings) as conn:
        assert conn.execute("""SELECT count(*) AS n FROM feed_health
            WHERE symbol='ETHUSDT' AND topic='connection'
              AND status='disconnected'""").fetchone()["n"] == 1
        row = conn.execute("""SELECT end_ts,resolved_at FROM data_gaps
            WHERE id=%s""", (new_gap,)).fetchone()
        assert row["end_ts"] is None and row["resolved_at"] is None
