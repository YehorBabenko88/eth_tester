"""Actual PostgreSQL 17/18 test for causal levels and rollup corrections."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
from uuid import uuid4
import pytest
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.levels_incremental import process_levels
from eth_tester.rollups import rollup_day

pytestmark = pytest.mark.postgres
UTC = timezone.utc

@pytest.fixture
def c_pg():
    admin = os.environ.get("ETH_TEST_PG_DSN")
    if not admin:
        pytest.skip("PostgreSQL integration requires ETH_TEST_PG_DSN")
    name = "eth_tester_c_" + uuid4().hex[:12]
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        settings = Settings(_env_file=None, database_url=make_conninfo(admin, dbname=name))
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))

def test_daily_levels_closed_confirmation_retest_and_resume(c_pg):
    start = datetime(2026,1,1,tzinfo=UTC)
    candles = [(10,6,8),(12,7,10),(11,8,9),(12,9,11),
               (14,11,13),(14,12,13),(13,10,11)]
    with connection(c_pg) as conn:
        for i,(high,low,close) in enumerate(candles[:6]):
            conn.execute("""INSERT INTO candles(symbol,kind,tf,ts,open,high,low,
                close,volume,source)
                VALUES('ETHUSDT','trade','D',%s,%s,%s,%s,%s,1,'qa')""",
                (start+timedelta(days=i),close,high,low,close))
    assert process_levels(c_pg,"D",flank=1)==6
    assert process_levels(c_pg,"D",flank=1)==0
    with connection(c_pg) as conn:
        events=conn.execute("""SELECT e.event_type,e.event_ts
            FROM level_events e JOIN levels l ON e.level_id=l.id
            WHERE l.symbol='ETHUSDT' AND l.tf='D' AND
                  l.side='resistance' AND l.origin_ts=%s
            ORDER BY e.event_ts""",(start+timedelta(days=1),)).fetchall()
    assert [v["event_type"] for v in events]==[
        "confirmed","touched","broken","retested"]
    assert [v["event_ts"] for v in events]==[
        start+timedelta(days=k) for k in (3,4,5,6)]
    with connection(c_pg) as conn:
        conn.execute("""INSERT INTO candles(symbol,kind,tf,ts,open,high,low,
            close,volume,source)
            VALUES('ETHUSDT','trade','D',%s,11,13,10,11,1,'qa')""",
            (start+timedelta(days=6),))
    assert process_levels(c_pg,"D",flank=1)==1
    with connection(c_pg) as conn:
        row=conn.execute("""SELECT status,invalidated_at FROM levels
            WHERE symbol='ETHUSDT' AND tf='D' AND side='resistance'
              AND origin_ts=%s""",(start+timedelta(days=1),)).fetchone()
    assert row["status"]=="invalidated"
    assert row["invalidated_at"]==start+timedelta(days=7)

def _seed_sha_archive_stage(settings, day, digest, rows):
    """Actual B stage/COPY membership in a disposable PostgreSQL test DB.

    Rows represent a small downloaded-archive test fixture, NOT an assertion
    that the exchange published every trade or that ML is certified.
    """
    import json
    from eth_tester.storage.bulk import merge_staged_trades
    from eth_tester.storage.archive_receipts import persist_archive_stage_bins

    with connection(settings) as conn:
        conn.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
        with conn.cursor() as cursor:
            with cursor.copy("""COPY eth_stage (
                symbol,ts,trade_id,price,qty,side,notional,source,raw)
                FROM STDIN""") as copy:
                for identity, stamp, price, quantity, side in rows:
                    price, quantity = Decimal(price), Decimal(quantity)
                    copy.write_row((
                        settings.symbol, stamp, identity, price, quantity,
                        side, price*quantity, "archive",
                        json.dumps({"trade_id": identity}),
                    ))
        merged = merge_staged_trades(conn, "eth_stage")
        assert merged["input_rows"] == len(rows)
        witness = persist_archive_stage_bins(
            conn, settings.symbol, day, digest, len(rows),
            instrument=settings.category)
        assert witness["distinct_trade_ids"] == len(rows)
        assert witness["exchange_complete"] is False
        conn.execute("""INSERT INTO ingestion_job(
            source,symbol,day,state,row_count,sha256)
            VALUES('trades_archive',%s,%s,'complete',%s,%s)
            ON CONFLICT(source,symbol,day) DO UPDATE SET
              state='complete',row_count=EXCLUDED.row_count,
              sha256=EXCLUDED.sha256,updated_at=clock_timestamp()""",
            (settings.symbol, day, len(rows), digest))
    return witness


def test_weekly_rollup_daily_archive_correction(c_pg):
    """Rebuild an archive-day correction without consulting global WS rows."""
    t = datetime(2026, 1, 1, tzinfo=UTC)
    _seed_sha_archive_stage(
        c_pg, t.date(), "a"*64,
        [("c1", t, 100, 2, "Buy")])
    _seed_sha_archive_stage(
        c_pg, (t+timedelta(days=1)).date(), "b"*64,
        [("c2", t+timedelta(days=1), 100, 1, "Sell"),
         ("c3", t+timedelta(days=1), 101, 3, "Buy")])
    assert rollup_day(c_pg, t.date(), "W", "a"*64) == 1
    assert rollup_day(c_pg, (t+timedelta(days=1)).date(), "W", "b"*64) == 2
    with connection(c_pg) as conn:
        f = conn.execute(
            "SELECT delta,volume,metadata FROM features WHERE tf='W'"
        ).fetchone()
    assert (f["delta"], f["volume"]) == (Decimal(4), Decimal(6))
    assert f["metadata"]["val"] is not None
    # The previous global c3 remains stored; the corrected gzip SHA omits
    # that ID and must withdraw its price bin without erasing global history.
    _seed_sha_archive_stage(
        c_pg, (t+timedelta(days=1)).date(), "c"*64,
        [("c2", t+timedelta(days=1), 100, 1, "Sell")])
    assert rollup_day(c_pg, (t+timedelta(days=1)).date(), "W", "c"*64) == 1
    with connection(c_pg) as conn:
        bins = conn.execute(
            "SELECT price FROM footprint WHERE tf='W' ORDER BY price"
        ).fetchall()
        still_global = conn.execute(
            "SELECT 1 FROM trades WHERE symbol='ETHUSDT' AND trade_id='c3'"
        ).fetchone()
    assert [item["price"] for item in bins] == [Decimal(100)]
    assert still_global is not None



def test_indicator_asof_and_trade_vwap(c_pg):
    from eth_tester.indicator_incremental import indicator_worker_step

    start = datetime(2026, 1, 1, tzinfo=UTC)
    with connection(c_pg) as conn:
        for i, hi, lo, close in [(0, 101, 99, 100),
                                  (1, 103, 99, 102),
                                  (2, 105, 101, 104)]:
            conn.execute("""INSERT INTO candles(
                symbol,kind,tf,ts,open,high,low,close,volume,source)
                VALUES('ETHUSDT','trade','1',%s,%s,%s,%s,%s,3,'qa')""",
                (start + timedelta(minutes=i), close, hi, lo, close))
        conn.execute("""INSERT INTO footprint(
            symbol,tf,bucket,price,buy_qty,sell_qty,buy_notional,
            sell_notional,trades)
            VALUES('ETHUSDT','1',%s,100,2,1,200,100,3)""", (start,))
    assert indicator_worker_step(c_pg, "1", period=2) == 3
    assert indicator_worker_step(c_pg, "1", period=2) == 0
    with connection(c_pg) as conn:
        items = conn.execute("""SELECT ts,metadata FROM features
            WHERE symbol='ETHUSDT' AND tf='1' ORDER BY ts""").fetchall()
    assert items[0]["metadata"]["trade_vwap"] == "100"
    assert items[0]["metadata"]["trade_vwap_quality"] == "observed_volume_unverified"
    assert items[0]["metadata"]["causally_certified"] is False
    assert items[1]["metadata"]["atr"] is not None
    assert items[2]["metadata"]["realized_volatility"] is not None



def test_round4_synthetic_archive_cannot_issue_causal_snapshot(c_pg):
    """Synthetic archive proof; production A currently emits partial only."""
    import json
    from eth_tester.snapshot_producer import certify_snapshot, coverage_certificate

    observed = datetime.now(UTC)
    day_end = observed.replace(hour=0, minute=0, second=0, microsecond=0)
    start = day_end - timedelta(minutes=1)
    day_start = day_end - timedelta(days=1)
    digest = "a" * 64
    evidence = {"gzip_crc_checked": True, "trade_identity_checked": True,
                "volume_verified": True, "download_sha256": digest}
    coverage = [{"start_ts": day_start, "end_ts": day_end, "status": "verified",
                 "checksum": digest, "observed_rows": 3, "expected_rows": 3,
                 "checked_at": observed, "evidence": evidence}]
    jobs = {day_start.date(): {"state": "complete", "sha256": digest}}
    checks = {(day_start.date(), digest):
              {"total": 1, "matched": 1, "unique_minutes": 1,
               "latest_checked": observed}}
    source_sha, _ = coverage_certificate(
        start, day_end, observed + timedelta(seconds=1), coverage, jobs, checks)
    metadata = {
        "asof_close": day_end.isoformat(),
        "realized_volatility": "0.02", "cvd_scope": "verified_window",
        "cvd_completeness": "verified", "cvd_source_sha256": source_sha,
    }
    latest_oi = observed - timedelta(minutes=2)
    earlier_oi = latest_oi - timedelta(minutes=5)
    with connection(c_pg) as conn:
        conn.execute("""INSERT INTO ingestion_job(source,symbol,day,state,
            row_count,sha256) VALUES('trades_archive','ETHUSDT',%s,'complete',3,%s)""",
            (day_start.date(), digest))
        conn.execute("""INSERT INTO ingestion_coverage(
            source,symbol,instrument,interval,start_ts,end_ts,status,
            observed_rows,expected_rows,checksum,quality,evidence,checked_at)
            VALUES('trades_archive','ETHUSDT','linear','',%s,%s,
                   'verified',3,3,%s,'synthetic_independent_proof',%s::jsonb,%s)""",
            (day_start, day_end, digest, json.dumps(evidence), observed))
        conn.execute("""INSERT INTO trade_candle_volume_checks(
            symbol,instrument,minute_ts,archive_source,archive_sha256,
            status,checked_at)
            VALUES('ETHUSDT','linear',%s,'trades_archive',%s,'match',%s)""",
            (start, digest, observed))
        conn.execute("""INSERT INTO aggregation_job(
            symbol,tf,day,archive_sha256,rows)
            VALUES('ETHUSDT','1',%s,%s,1)""", (day_start.date(), digest))
        conn.execute("""INSERT INTO candles(
            symbol,kind,tf,ts,open,high,low,close,volume,source)
            VALUES('ETHUSDT','trade','1',%s,100,101,99,100,3,'qa')""",
            (start,))
        conn.execute("""INSERT INTO footprint(
            symbol,tf,bucket,price,buy_qty,sell_qty,
            buy_notional,sell_notional,trades)
            VALUES('ETHUSDT','1',%s,100,2,1,200,100,3)""", (start,))
        conn.execute("""INSERT INTO features(
            symbol,tf,ts,poc,delta,cvd,volume,tape_tps,metadata,quality)
            VALUES('ETHUSDT','1',%s,100,1,5,3,0.05,%s::jsonb,
                   'archive_rollup_partial')""", (start, json.dumps(metadata)))
        conn.execute("""INSERT INTO levels(
            symbol,tf,side,origin_ts,level_price,source,confirmed_at)
            VALUES('ETHUSDT','1','resistance',%s,102,
                   'causal_closed_pivot',%s)""",
            (start - timedelta(minutes=4), start - timedelta(minutes=1)))
        conn.execute("""INSERT INTO market_book_state(
            symbol,last_event_id,bids,asks,last_update_id,valid,updated_at)
            VALUES('ETHUSDT',1,%s::jsonb,%s::jsonb,1,true,%s)""",
            (json.dumps({"99": "10"}), json.dumps({"101": "5"}), observed))
        for stamp, value in ((latest_oi, 1010), (earlier_oi, 1000)):
            conn.execute("""INSERT INTO open_interest(
                symbol,interval,ts,value)
                VALUES('ETHUSDT','5min',%s,%s)""", (stamp, value))
    # These are synthetic flags, not independently witnessed source receipts.
    # The canonical writer is the ONLY permitted certified insertion path.
    # They MUST NOT create a causal snapshot, even with matching volumes.
    result = certify_snapshot(c_pg, "1")
    assert result["status"] == "rejected", result
    assert result["reason"] == "independent_nine_feature_witnesses_missing"
    with connection(c_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM feature_snapshots").fetchone()["n"] == 0


def test_round2_partial_coverage_never_materializes_certified_snapshot(c_pg):
    from eth_tester.snapshot_producer import certify_snapshot

    start = datetime.now(UTC).replace(second=0, microsecond=0)
    start -= timedelta(minutes=3)
    with connection(c_pg) as conn:
        conn.execute("""INSERT INTO candles(
            symbol,kind,tf,ts,open,high,low,close,volume,source)
            VALUES('ETHUSDT','trade','1',%s,100,101,99,100,3,'qa')""",
            (start,))
        conn.execute("""INSERT INTO features(
            symbol,tf,ts,poc,delta,cvd,volume,tape_tps)
            VALUES('ETHUSDT','1',%s,100,1,1,3,0.05)""", (start,))
    result = certify_snapshot(c_pg, "1")
    assert result["status"] == "rejected"
    with connection(c_pg) as conn:
        assert conn.execute("SELECT count(*) AS n FROM feature_snapshots").fetchone()["n"] == 0


def test_round2_archive_live_overlap_preserves_rollup_and_all_frames(c_pg):
    """WS-first archive members count, WS-only trades do not contaminate."""
    from eth_tester.analytics import build_footprint
    from eth_tester.analytics_core import FRAMES

    now = datetime.now(UTC).replace(second=0, microsecond=0)
    stamp = now - timedelta(minutes=2)
    with connection(c_pg) as conn:
        for identity, price, qty in (
                ("overlap-buy", 100, 2),
                ("ws-only-extra", 999, 8)):
            conn.execute("""INSERT INTO trades(
                symbol,ts,trade_id,price,qty,side,notional,source,raw)
                VALUES('ETHUSDT',%s,%s,%s,%s,'Buy',%s,
                       'websocket','{}'::jsonb)""",
                (stamp, identity, price, qty, price*qty))
    _seed_sha_archive_stage(
        c_pg, stamp.date(), "d"*64,
        [("overlap-buy", stamp, 100, 2, "Buy"),
         ("overlap-sell", stamp, 101, 1, "Sell")])
    assert len(FRAMES) == 13
    for tf in FRAMES:
        assert rollup_day(c_pg, stamp.date(), tf, "d"*64) == 2
        with connection(c_pg) as conn:
            result = conn.execute("""SELECT sum(volume) AS total
                FROM features WHERE symbol='ETHUSDT' AND tf=%s""",
                (tf,)).fetchone()
        assert result["total"] == Decimal(3), (tf, result)
    assert build_footprint(c_pg, "1", hours=2) == 0
    with connection(c_pg) as conn:
        rows = conn.execute("""SELECT price,sum(buy_qty+sell_qty) AS qty
            FROM footprint WHERE symbol='ETHUSDT' AND tf='1'
            GROUP BY price ORDER BY price""").fetchall()
    assert [(row["price"], row["qty"]) for row in rows] == [
        (Decimal(100), Decimal(2)), (Decimal(101), Decimal(1))]



def test_round4_partial_history_and_l2_cannot_bypass_b_writer(c_pg, monkeypatch):
    from eth_tester import snapshot_producer as producer

    start = datetime.now(UTC).replace(second=0, microsecond=0)
    start -= timedelta(minutes=4)
    with connection(c_pg) as conn:
        conn.execute("""INSERT INTO candles(
            symbol,kind,tf,ts,open,high,low,close,volume,source)
            VALUES('ETHUSDT','trade','1',%s,100,101,99,100,3,'qa')""",
            (start,))
        conn.execute("""INSERT INTO features(
            symbol,tf,ts,poc,delta,cvd,volume,tape_tps,quality)
            VALUES('ETHUSDT','1',%s,100,1,1,3,0.05,'partial_unverified')""",
            (start,))
    attempts = []
    def forbidden_writer(*args, **kwargs):
        attempts.append(kwargs)
        raise AssertionError("No certification without original source evidence")
    monkeypatch.setattr(producer, "write_feature_snapshot", forbidden_writer)
    for _ in range(2):
        result = producer.certify_snapshot(c_pg, "1")
        assert result["status"] == "rejected", result
    assert attempts == []
    with connection(c_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM feature_snapshots").fetchone()["n"] == 0


def test_round4_storage_v1_direct_insert_record_not_reusable(c_pg):
    """An immutable legacy row cannot be mistaken for a B-certified v2 proof."""
    from eth_tester.snapshot_producer import CertificationRejected, _verify_b_writer_v2

    start = datetime(2026, 1, 1, tzinfo=UTC)
    source = "a" * 64
    with connection(c_pg) as conn:
        conn.execute("""INSERT INTO feature_snapshots(
            snapshot_key,symbol,tf,bar_ts,available_at,observed_at,
            computed_at,source,source_checksum,quality,is_causal,values_json,provenance)
            VALUES('legacy-c-direct','ETHUSDT','1',%s,%s,%s,%s,
                   'old_direct_writer',%s,'complete',true,
                   '{}'::jsonb,'{"source_coverage_verified":true}'::jsonb)""",
            (start, start+timedelta(minutes=1),
             start+timedelta(minutes=2), start+timedelta(minutes=3), source))
        with pytest.raises(CertificationRejected, match="canonical_storage_v2"):
            _verify_b_writer_v2(conn, "legacy-c-direct", source)


def test_round6_live_window_never_promotes_partial_archive_or_creates_ml_snapshot(c_pg):
    """Real PostgreSQL, genuine closed-bar clock; absence of B witness stays pending."""
    from eth_tester.live_paper_window import inspect_latest_live_bar
    from eth_tester.snapshot_producer import certify_snapshot

    now = datetime.now(UTC)
    closed = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
    with connection(c_pg) as conn:
        conn.execute("""INSERT INTO candles(
            symbol,kind,tf,ts,open,high,low,close,volume,source,closed)
            VALUES('ETHUSDT','trade','1',%s,100,101,99,100,3,'public',true)""",
            (closed,))
    pending = inspect_latest_live_bar(c_pg)
    assert pending["status"] in ("pending", "expired"), pending
    if pending["status"] == "pending":
        assert pending["reason"] in (
            "b_authoritative_source_witness_missing", "candle_not_closed")
        assert pending.get("ml_eligible") is False or (
            pending["reason"] == "candle_not_closed")
    historic = certify_snapshot(c_pg, "1")
    assert historic["status"] == "rejected", historic
    with connection(c_pg) as conn:
        assert conn.execute("""SELECT count(*) AS n FROM feature_snapshots
            WHERE symbol='ETHUSDT' AND is_causal""").fetchone()["n"] == 0


def test_round7_missing_archive_stage_refuses_global_trades_without_mutation(c_pg):
    """A completed gzip job alone is not SHA membership of the global tape."""
    with connection(c_pg) as conn:
        moment = datetime(2026, 3, 1, tzinfo=UTC)
        conn.execute("""INSERT INTO trades(
            symbol,ts,trade_id,price,qty,side,notional,source,raw)
            VALUES('ETHUSDT',%s,'orphan-ws',100,5,'Buy',500,
                   'websocket','{}'::jsonb)""", (moment,))
        conn.execute("""INSERT INTO ingestion_job(
            source,symbol,day,state,row_count,sha256)
            VALUES('trades_archive','ETHUSDT',%s,'complete',1,%s)""",
            (moment.date(), "e"*64))
    with pytest.raises(ValueError, match="sha_pinned_archive_stage_receipt_missing"):
        rollup_day(c_pg, moment.date(), "D", "e"*64)
    with connection(c_pg) as conn:
        assert conn.execute(
            "SELECT count(*) AS n FROM footprint_daily").fetchone()["n"] == 0
        assert conn.execute(
            "SELECT count(*) AS n FROM aggregation_job").fetchone()["n"] == 0


def test_round7_repair_old_global_trades_contamination_on_matching_job_sha(c_pg):
    """Legacy completed jobs must be rechecked against actual archive bins."""
    moment = datetime(2026, 3, 2, tzinfo=UTC)
    digest = "f"*64
    _seed_sha_archive_stage(
        c_pg, moment.date(), digest,
        [("real-member", moment+timedelta(minutes=1), 100, 2, "Buy")])
    with connection(c_pg) as conn:
        conn.execute("""INSERT INTO footprint_daily(
            symbol,tf,day,bucket,price,buy_qty,sell_qty,trades)
            VALUES('ETHUSDT','1',%s,%s,999,9,0,1)""",
            (moment.date(), moment+timedelta(minutes=1)))
        conn.execute("""INSERT INTO aggregation_job(
            symbol,tf,day,archive_sha256,rows)
            VALUES('ETHUSDT','1',%s,%s,1)""",
            (moment.date(), digest))
    assert rollup_day(c_pg, moment.date(), "1", digest) == 1
    with connection(c_pg) as conn:
        fixed = conn.execute("""SELECT price,buy_qty FROM footprint_daily
            WHERE symbol='ETHUSDT' AND tf='1'""").fetchall()
    assert fixed == [{"price": Decimal(100), "buy_qty": Decimal(2)}]
    assert rollup_day(c_pg, moment.date(), "1", digest) == 0


def test_round7_fast_c_actual_b_publisher_source_receipts_l2_and_oi(c_pg):
    """C actual runtime caller + B immutable PG receipts, never a ML snapshot.

    This is a controlled source-contract fixture, not an actual Bybit feed
    capture or a claim that observed prints prove exchange completeness.
    """
    import json
    from eth_tester.live_paper_window import publish_latest_bounded_live
    from eth_tester.storage.live_evidence import (
        record_trade_receipts, record_book_lineage,
        record_oi_observation, require_bounded_paper_evidence,
    )

    session = "round7-source-session"
    with connection(c_pg) as conn:
        wall = conn.execute(
            "SELECT clock_timestamp() AS now").fetchone()["now"]
        bar_start = wall.replace(second=0, microsecond=0)-timedelta(minutes=1)
        trade_event = bar_start + timedelta(seconds=59)
        conn.execute("""INSERT INTO candles(
            symbol,kind,tf,ts,open,high,low,close,volume,turnover,
            closed,source)
            VALUES('ETHUSDT','trade','1',%s,3000,3000,3000,3000,
                   0.02,60,true,'bybit_v5')""", (bar_start,))
        # Use A's actual closed-grid receipt writer in this disposable test.
        # The quality witnesses ONLY a closed OHLCV candle, not all trade IDs.
        from eth_tester.data_coverage import write_coverage
        write_coverage(
            conn, source="kline:trade", symbol="ETHUSDT", interval="1",
            start_ms=int(bar_start.timestamp()*1000),
            end_exclusive_ms=int(
                (bar_start+timedelta(minutes=1)).timestamp()*1000),
            status="verified", observed_rows=1, expected_rows=1,
            checksum="a"*64, quality="exchange_closed_grid_checked",
            evidence={"closed_bar_only": True, "ml_snapshot_eligible": False,
                      "proves_all_trade_ids": False},
            instrument="linear")
        conn.execute("""INSERT INTO trades(
            symbol,ts,trade_id,price,qty,side,notional,source,raw)
            VALUES('ETHUSDT',%s,'R7-PUBLIC-1',3000,0.02,'Buy',60,
                   'websocket',%s::jsonb)""",
            (trade_event, json.dumps({"i": "R7-PUBLIC-1"})))
        record_trade_receipts(conn, "ETHUSDT", session, ["R7-PUBLIC-1"])
        # B's prospective decision requires a genuinely current public
        # trade, not a late receipt of an older bar's print. This current
        # print is deliberately OUTSIDE the original closed minute and
        # therefore cannot inflate its official base/quote reconciliation.
        newest_trade = conn.execute(
            "SELECT clock_timestamp() AS now").fetchone()["now"]
        conn.execute("""INSERT INTO trades(
            symbol,ts,trade_id,price,qty,side,notional,source,raw)
            VALUES('ETHUSDT',%s,'R7-CURRENT-PRINT',3000,0.001,
                   'Buy',3,'websocket',%s::jsonb)""",
            (newest_trade, json.dumps({"i":"R7-CURRENT-PRINT"})))
        record_trade_receipts(conn, "ETHUSDT", session, ["R7-CURRENT-PRINT"])
        conn.execute("""INSERT INTO feed_health(
            symbol,topic,ts,status,detail)
            VALUES('ETHUSDT','connection',clock_timestamp(),'connected',
                   %s::jsonb)""",
            (json.dumps({"session_id": session}),))
    # Trade only: actual C fast pipeline MUST stay pending without L2.
    before = publish_latest_bounded_live(c_pg)
    assert before["status"] == "pending", before
    assert before["reason"] == "l2_replay_backlog_or_invalid"

    with connection(c_pg) as conn:
        snapshot = conn.execute("""INSERT INTO book_events(
            symbol,exchange_ts,recv_ts,update_id,sequence_no,
            event_type,bids,asks,valid)
            VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),
                   10,10,'snapshot','[["2999","1"]]'::jsonb,
                   '[["3001","1"]]'::jsonb,true) RETURNING id""").fetchone()["id"]
        record_book_lineage(conn, "ETHUSDT", session, snapshot, snapshot)
    # Fast path now actually replays the new B-receipted book event.
    without_oi = publish_latest_bounded_live(c_pg)
    assert without_oi["status"] == "pending", without_oi
    assert without_oi["reason"] == "independent_recent_oi_receipt_missing"
    with connection(c_pg) as conn:
        wall = conn.execute(
            "SELECT clock_timestamp() AS now").fetchone()["now"]
        minute = wall.replace(second=0, microsecond=0)
        observed_oi = minute-timedelta(minutes=minute.minute % 5)
        conn.execute("""INSERT INTO open_interest(symbol,interval,ts,value)
            VALUES('ETHUSDT','5min',%s,1000)""", (observed_oi,))
        record_oi_observation(
            conn, "ETHUSDT", "5min", observed_oi, "b"*64)
    published = publish_latest_bounded_live(c_pg)
    assert published["status"] == "published", published
    assert published["paper_only"] is True
    assert published["ml_eligible"] is False
    assert published["l2_replayed"] >= 0
    assert 0 <= published["publication_latency_ms"] <= 90000
    assert len(published["snapshot_key"]) == 64
    again = publish_latest_bounded_live(c_pg)
    assert again["status"] == "published", again
    assert again["snapshot_key"] == published["snapshot_key"]
    with connection(c_pg) as conn:
        proof = require_bounded_paper_evidence(
            conn, symbol="ETHUSDT", bar_ts=bar_start,
            evidence_key=published["snapshot_key"],
            decision_at=conn.execute(
                "SELECT clock_timestamp() AS now").fetchone()["now"])
        assert proof["ml_eligible"] is False
        assert proof["paper_only"] is True
        assert proof["exchange_complete"] is False
        from eth_tester.strategies.paper_live import read_live_evidence
        decision = conn.execute(
            "SELECT clock_timestamp() AS now").fetchone()["now"]
        d_flow = read_live_evidence(
            conn, "ETHUSDT", bar_start,
            bar_start+timedelta(minutes=1), decision)
        assert d_flow is not None
        assert d_flow["snapshot_key"] == published["snapshot_key"]
        assert d_flow["paper_only"] is True
        assert d_flow["ml_eligible"] is False
        assert conn.execute("""SELECT count(*) AS n FROM paper_live_evidence
            WHERE symbol='ETHUSDT'""").fetchone()["n"] == 1
        assert conn.execute("""SELECT count(*) AS n FROM feature_snapshots
            WHERE symbol='ETHUSDT' AND is_causal""").fetchone()["n"] == 0

    # A new stream interruption cannot make an old B row newly admissible.
    with connection(c_pg) as conn:
        conn.execute("""INSERT INTO data_gaps(
            source,symbol,start_ts,end_ts,explanation)
            VALUES('publicTrade','ETHUSDT',%s,%s,
                   'controlled_test_disconnect')""",
            (bar_start, bar_start+timedelta(minutes=1)))
    with pytest.raises(ValueError, match="interruption|overlaps"):
        publish_latest_bounded_live(c_pg)


def test_round7_aggregate_pending_rechecks_already_complete_legacy_jobs(c_pg):
    """The autonomous archive worker revisits OLD completed-but-wrong rollups."""
    from eth_tester.rollups import aggregate_pending
    from eth_tester.analytics_core import FRAMES
    t = datetime(2026, 4, 3, tzinfo=UTC)
    stamp = t+timedelta(minutes=1)
    sha = "9"*64
    _seed_sha_archive_stage(
        c_pg, t.date(), sha, [("source-only", stamp, 100, 2, "Buy")])
    with connection(c_pg) as conn:
        # A global websocket trade not represented by this archive SHA.
        conn.execute("""INSERT INTO trades(
            symbol,ts,trade_id,price,qty,side,notional,source)
            VALUES('ETHUSDT',%s,'ws-not-archived',999,9,
                   'Buy',8991,'websocket')""", (stamp,))
        for tf in FRAMES:
            conn.execute("""INSERT INTO aggregation_job(
                symbol,tf,day,archive_sha256,rows)
                VALUES('ETHUSDT',%s,%s,%s,1)""", (tf, t.date(), sha))
        # This preexisting completed one-minute rollup is WRONG.
        conn.execute("""INSERT INTO footprint_daily(
            symbol,tf,day,bucket,price,buy_qty,sell_qty,trades)
            VALUES('ETHUSDT','1',%s,%s,999,9,0,1)""",
            (t.date(), stamp))
    assert aggregate_pending(c_pg, max_days=1) == len(FRAMES)
    with connection(c_pg) as conn:
        bins = conn.execute("""SELECT price,buy_qty FROM footprint_daily
            WHERE symbol='ETHUSDT' AND tf='1' ORDER BY price""").fetchall()
    assert bins == [{"price": Decimal(100), "buy_qty": Decimal(2)}]


def test_round9_concurrent_first_start_l2_replay_never_regresses_cursor(c_pg):
    """Two real PostgreSQL connections race from *no* market_book_state row."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from eth_tester.book_analysis import replay_orderbook
    gate = Barrier(3)
    with connection(c_pg) as conn:
        snap = conn.execute("""INSERT INTO book_events(
            symbol,exchange_ts,recv_ts,update_id,sequence_no,event_type,
            bids,asks,valid)
            VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),20,20,
                   'snapshot','[["100","5"]]'::jsonb,
                   '[["101","5"]]'::jsonb,true)
            RETURNING id""").fetchone()["id"]
        delta = conn.execute("""INSERT INTO book_events(
            symbol,exchange_ts,recv_ts,update_id,sequence_no,event_type,
            bids,asks,valid)
            VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),21,21,
                   'delta','[["100","7"]]'::jsonb,'[]'::jsonb,true)
            RETURNING id""").fetchone()["id"]
    def one():
        gate.wait(timeout=20)
        return replay_orderbook(c_pg, batch=1, max_batches=1)
    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = [pool.submit(one), pool.submit(one)]
        gate.wait(timeout=20)
        assert sum(f.result(timeout=30) for f in jobs) == 2
    with connection(c_pg) as conn:
        state = conn.execute("""SELECT last_event_id,bids,asks,valid
            FROM market_book_state WHERE symbol='ETHUSDT'""").fetchone()
        gaps = conn.execute("""SELECT count(*) AS n FROM data_gaps
            WHERE symbol='ETHUSDT' AND source='orderbook'""").fetchone()["n"]
    assert state["last_event_id"] == delta > snap
    assert state["bids"]["100"] == "7"
    assert state["asks"]["101"] == "5"
    assert state["valid"] is True
    assert gaps == 0
    assert replay_orderbook(c_pg,batch=1,max_batches=1) == 0


def test_round9_corrupt_l2_writes_gap_and_recovers_only_from_new_snapshot(c_pg):
    from eth_tester.book_analysis import replay_orderbook
    with connection(c_pg) as conn:
        first = conn.execute("""INSERT INTO book_events(
            symbol,exchange_ts,recv_ts,update_id,sequence_no,event_type,
            bids,asks,valid)
            VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),10,10,
                   'snapshot','[["100","3"]]'::jsonb,
                   '[["101","5"]]'::jsonb,true)
            RETURNING id""").fetchone()["id"]
        poison = conn.execute("""INSERT INTO book_events(
            symbol,exchange_ts,recv_ts,update_id,sequence_no,event_type,
            bids,asks,valid)
            VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),11,11,
                   'delta','[["100","-10"]]'::jsonb,'[]'::jsonb,true)
            RETURNING id""").fetchone()["id"]
        recovered = conn.execute("""INSERT INTO book_events(
            symbol,exchange_ts,recv_ts,update_id,sequence_no,event_type,
            bids,asks,valid)
            VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),20,20,
                   'snapshot','[["100","2"]]'::jsonb,
                   '[["101","6"]]'::jsonb,true)
            RETURNING id""").fetchone()["id"]
    assert replay_orderbook(c_pg,batch=1,max_batches=1) == 1
    with connection(c_pg) as conn:
        assert conn.execute("""SELECT valid FROM market_book_state
            WHERE symbol='ETHUSDT'""").fetchone()["valid"] is True
    assert replay_orderbook(c_pg,batch=1,max_batches=1) == 1
    with connection(c_pg) as conn:
        state = conn.execute("""SELECT last_event_id,valid,bids FROM
            market_book_state WHERE symbol='ETHUSDT'""").fetchone()
        gap = conn.execute("""SELECT count(*) AS n FROM data_gaps
            WHERE symbol='ETHUSDT' AND source='orderbook'
              AND explanation LIKE 'invalid L2 source event:%'""").fetchone()["n"]
    assert state["last_event_id"] == poison > first
    assert state["valid"] is False
    assert state["bids"] == {}
    assert gap == 1
    assert replay_orderbook(c_pg,batch=1,max_batches=1) == 1
    with connection(c_pg) as conn:
        state = conn.execute("""SELECT last_event_id,valid,bids FROM
            market_book_state WHERE symbol='ETHUSDT'""").fetchone()
        gap = conn.execute("""SELECT count(*) AS n FROM data_gaps
            WHERE symbol='ETHUSDT' AND source='orderbook'""").fetchone()["n"]
    assert state["last_event_id"] == recovered
    assert state["valid"] is True
    assert state["bids"]["100"] == "2"
    assert gap >= 1  # The real interruption is not silently erased.


def test_round9_live_footprint_server_side_aggregates_many_small_ticks(c_pg):
    """No Python materialization of every trade in the real PG aggregate path."""
    from eth_tester.analytics import build_footprint
    moment = datetime.now(UTC).replace(second=0, microsecond=0)
    stamp = moment-timedelta(minutes=1)
    with connection(c_pg) as conn:
        conn.execute("""INSERT INTO trades(
            symbol,ts,trade_id,price,qty,side,notional,source)
            SELECT 'ETHUSDT',%s, 'dense-'||g::text,100,0.00000001,
              CASE WHEN g%%2=0 THEN 'Buy' ELSE 'Sell' END,
              0.000001,'websocket'
            FROM generate_series(1,5000) g""", (stamp,))
    assert build_footprint(c_pg,tf="1",hours=1) == 1
    with connection(c_pg) as conn:
        result = conn.execute("""SELECT buy_qty,sell_qty,trades FROM footprint
            WHERE symbol='ETHUSDT' AND tf='1' AND bucket=%s""",
            (stamp,)).fetchone()
        feature = conn.execute("""SELECT volume,delta,quality
            FROM features WHERE symbol='ETHUSDT' AND tf='1' AND ts=%s""",
            (stamp,)).fetchone()
    assert result["buy_qty"] == Decimal("0.00002500")
    assert result["sell_qty"] == Decimal("0.00002500")
    assert result["trades"] == 5000
    assert feature["volume"] == Decimal("0.00005000")
    assert feature["delta"] == 0
    assert feature["quality"] == "partial_unverified"



def test_round11_paper_admission_fences_poison_l2_gap_state(c_pg):
    """Real 2-connection PG admission gate vs C invalidating market book.

    No paper trade is created. Simulates E holding the exact shared lease,
    while C sees an invalid original raw delta. All effects are DB-atomic.
    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
    from threading import Event
    from eth_tester.book_analysis import replay_orderbook

    with connection(c_pg) as conn:
        base = conn.execute("""INSERT INTO book_events(
            symbol,exchange_ts,recv_ts,update_id,sequence_no,event_type,
            bids,asks,valid)
            VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),10,10,
                   'snapshot','[["100","4"]]'::jsonb,
                   '[["101","5"]]'::jsonb,true)
            RETURNING id""").fetchone()["id"]
    assert replay_orderbook(c_pg,batch=1,max_batches=1) == 1
    with connection(c_pg) as conn:
        poison = conn.execute("""INSERT INTO book_events(
            symbol,exchange_ts,recv_ts,update_id,sequence_no,event_type,
            bids,asks,valid)
            VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),11,11,
                   'delta','[["100","-1"]]'::jsonb,
                   '[]'::jsonb,true)
            RETURNING id""").fetchone()["id"]
    started = Event()
    def reader():
        started.set()
        return replay_orderbook(c_pg,batch=1,max_batches=1)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with connection(c_pg) as admission:
            admission.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                              ("eth_paper_admission_source:ETHUSDT",))
            future = pool.submit(reader)
            assert started.wait(timeout=2)
            with pytest.raises(FutureTimeout):
                future.result(timeout=0.15)
            # Independent connection sees no uncommitted or unsafe state.
            with connection(c_pg) as observer:
                state = observer.execute("""SELECT last_event_id,valid
                    FROM market_book_state WHERE symbol='ETHUSDT'""").fetchone()
                gap = observer.execute("""SELECT count(*) AS n FROM data_gaps
                    WHERE symbol='ETHUSDT' AND source='orderbook'""").fetchone()["n"]
                assert state["last_event_id"] == base
                assert state["valid"] is True
                assert gap == 0
        # Source lock was released by admission transaction COMMIT.
        assert future.result(timeout=3) == 1
    with connection(c_pg) as conn:
        state = conn.execute("""SELECT last_event_id,valid,bids FROM
            market_book_state WHERE symbol='ETHUSDT'""").fetchone()
        gap = conn.execute("""SELECT count(*) AS n FROM data_gaps
            WHERE symbol='ETHUSDT' AND source='orderbook'""").fetchone()["n"]
    assert poison > base
    assert state["last_event_id"] == poison
    assert state["valid"] is False
    assert state["bids"] == {}
    assert gap == 1


def test_round11_source_lock_timeout_preserves_cursor_and_gap(c_pg):
    """2-second acquisition timeout fails closed and resumes exactly once."""
    from eth_tester.book_analysis import replay_orderbook
    with connection(c_pg) as conn:
        first = conn.execute("""INSERT INTO book_events(
            symbol,exchange_ts,recv_ts,update_id,sequence_no,event_type,
            bids,asks,valid)
            VALUES('ETHUSDT',clock_timestamp(),clock_timestamp(),10,10,
                   'snapshot','[["100","4"]]'::jsonb,
                   '[["101","5"]]'::jsonb,true)
            RETURNING id""").fetchone()["id"]
    with connection(c_pg) as admission:
        admission.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                          ("eth_paper_admission_source:ETHUSDT",))
        with pytest.raises(psycopg.errors.LockNotAvailable):
            replay_orderbook(c_pg,batch=1,max_batches=1)
    with connection(c_pg) as conn:
        state = conn.execute("""SELECT last_event_id FROM market_book_state
            WHERE symbol='ETHUSDT'""").fetchone()
        gap = conn.execute("""SELECT count(*) AS n FROM data_gaps
            WHERE symbol='ETHUSDT' AND source='orderbook'""").fetchone()["n"]
        assert state is None
        assert gap == 0
    assert replay_orderbook(c_pg,batch=1,max_batches=1) == 1
    assert replay_orderbook(c_pg,batch=1,max_batches=1) == 0
    with connection(c_pg) as conn:
        state = conn.execute("""SELECT last_event_id,valid FROM market_book_state
            WHERE symbol='ETHUSDT'""").fetchone()
        assert state["last_event_id"] == first
        assert state["valid"] is True
