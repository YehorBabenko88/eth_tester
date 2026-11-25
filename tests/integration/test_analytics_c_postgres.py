"""Actual PostgreSQL 17/18 test for causal levels and rollup corrections."""
from datetime import datetime, date, timedelta, timezone
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

def test_weekly_rollup_daily_archive_correction(c_pg):
    t=datetime(2026,1,1,tzinfo=UTC)
    with connection(c_pg) as conn:
        for n,dt,price,qty,side in [
            ("c1",t,100,2,"Buy"),("c2",t+timedelta(days=1),100,1,"Sell"),
            ("c3",t+timedelta(days=1),101,3,"Buy")]:
            conn.execute("""INSERT INTO trades(symbol,ts,trade_id,price,
                qty,side,notional,source)
                VALUES('ETHUSDT',%s,%s,%s,%s,%s,%s,'qa')""",
                (dt,n,price,qty,side,price*qty))
    assert rollup_day(c_pg,date(2026,1,1),"W","s1")==1
    assert rollup_day(c_pg,date(2026,1,2),"W","s2")==2
    with connection(c_pg) as conn:
        f=conn.execute("SELECT delta,volume,metadata FROM features WHERE tf='W'").fetchone()
    assert (f["delta"],f["volume"])==(Decimal(4),Decimal(6))
    assert f["metadata"]["val"] is not None
    with connection(c_pg) as conn:
        conn.execute("DELETE FROM trades WHERE trade_id='c3'")
    assert rollup_day(c_pg,date(2026,1,2),"W","s2-replaced")==1
    with connection(c_pg) as conn:
        bins=conn.execute("SELECT price FROM footprint WHERE tf='W' ORDER BY price").fetchall()
    assert [x["price"] for x in bins]==[Decimal(100)]


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



def test_round2_certified_snapshot_immutable_and_actual_materialization_time(c_pg):
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
    published = certify_snapshot(c_pg, "1")
    assert published["status"] == "published", published
    again = certify_snapshot(c_pg, "1")
    assert again["status"] == "already_published", again
    with connection(c_pg) as conn:
        rows = conn.execute("""SELECT bar_ts,available_at,observed_at,computed_at,
            quality,is_causal,source_checksum,values_json,provenance
            FROM feature_snapshots WHERE symbol='ETHUSDT'""").fetchall()
    assert len(rows) == 1
    one = rows[0]
    assert one["bar_ts"] == start
    assert one["source_checksum"] == source_sha
    assert one["quality"] == "complete" and one["is_causal"]
    assert one["available_at"] == one["observed_at"] == one["computed_at"]
    assert one["computed_at"] >= observed > day_end
    assert one["values_json"]["oi"] == "10"
    assert one["provenance"]["availability_convention"] == (
        "DB_materialization_time_not_bar_close")
    assert all(stamp >= int(observed.timestamp() * 1000)
               for stamp in one["provenance"]["feature_available_ms"].values())
    with pytest.raises(psycopg.errors.RaiseException):
        with connection(c_pg) as conn:
            conn.execute("""UPDATE feature_snapshots SET is_causal=false
                WHERE source_checksum=%s""", (source_sha,))


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
    from eth_tester.analytics import build_footprint
    from eth_tester.analytics_core import FRAMES

    now = datetime.now(UTC).replace(second=0, microsecond=0)
    stamp = now - timedelta(minutes=2)
    with connection(c_pg) as conn:
        for ident,price,qty,side in (
                ("overlap-buy", 100, 2, "Buy"),
                ("overlap-sell", 101, 1, "Sell")):
            conn.execute("""INSERT INTO trades(
                symbol,ts,trade_id,price,qty,side,notional,source)
                VALUES('ETHUSDT',%s,%s,%s,%s,%s,%s,'qa_archive')""",
                (stamp, ident, price, qty, side, price*qty))
    assert len(FRAMES) == 13
    for tf in FRAMES:
        assert rollup_day(c_pg, stamp.date(), tf, "overlap-sha") == 2
        with connection(c_pg) as conn:
            result = conn.execute("""SELECT sum(volume) AS total
                FROM features WHERE symbol='ETHUSDT' AND tf=%s""",
                (tf,)).fetchone()
        assert result["total"] == Decimal(3), (tf, result)
    assert build_footprint(c_pg, "1", hours=2) == 0
    with connection(c_pg) as conn:
        result = conn.execute("""SELECT sum(buy_qty+sell_qty) AS total
            FROM footprint WHERE symbol='ETHUSDT' AND tf='1'""").fetchone()
    assert result["total"] == Decimal(3)
