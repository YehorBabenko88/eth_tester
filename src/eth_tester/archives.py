import asyncio
import csv
from datetime import datetime,timedelta,timezone
from decimal import Decimal, ROUND_FLOOR
import gzip
import hashlib
import json
import re
from pathlib import Path
import tempfile
from shutil import disk_usage
import httpx
from psycopg.errors import UniqueViolation
from .database import connection
from .data_quality import DataIntegrityError, reconcile_minute_volume
from .data_coverage import write_coverage
from .archive_quality import persist_minute_evidence
from .storage.bulk import merge_staged_trades

def parse_trade(row):
    timestamp=row.get("timestamp") or row.get("timestamp_ms") or row.get("time")
    identifier=row.get("trdMatchID") or row.get("trade_id") or row.get("id")
    side=row.get("side")
    price=row.get("price")
    qty=row.get("size") or row.get("qty") or row.get("quantity")
    if not all((timestamp,identifier,side,price,qty)):
        raise ValueError(f"Unknown trade CSV columns: {sorted(row)}")
    if side.lower() not in ("buy","sell"):
        raise ValueError("Invalid aggressor side")
    numeric_time=Decimal(str(timestamp))
    if not numeric_time.is_finite() or numeric_time <= 0:
        raise ValueError("Invalid historical trade timestamp")
    # Archive CSV may contain sub-millisecond fractions in epoch seconds;
    # V5 WebSocket / recent REST trade IDs use epoch milliseconds.
    # Normalize indexed ts to integer UTC milliseconds to make identities
    # comparable, while retaining the original full precision in raw JSON.
    epoch_ms = numeric_time if numeric_time > 100000000000 else numeric_time*1000
    epoch_ms = int(epoch_ms.to_integral_value(rounding=ROUND_FLOOR))
    stamp=(datetime.fromtimestamp(epoch_ms//1000,timezone.utc)
           +timedelta(milliseconds=epoch_ms%1000))
    p,q=Decimal(price),Decimal(qty)
    if not p.is_finite() or not q.is_finite() or p<=0 or q<=0:
        raise ValueError("Nonpositive or non-finite trade price/size")
    if not str(identifier).strip():
        raise ValueError("Empty trade identity")
    return stamp,str(identifier),p,q,side.title()

async def download_archive(settings,day,client):
    symbol=settings.symbol
    url=f"https://public.bybit.com/trading/{symbol}/{symbol}{day.isoformat()}.csv.gz"
    with connection(settings) as conn:
        state=conn.execute("""SELECT state,sha256 FROM ingestion_job
         WHERE source='trades_archive' AND symbol=%s AND day=%s""",
          (symbol,day)).fetchone()
        replay_complete = bool(state and state["state"] == "complete")
        original_sha = state["sha256"] if replay_complete else None
        if replay_complete:
            # Completed archives must retain their immutable original SHA.
            if not isinstance(original_sha,str) or not re.fullmatch(
                    r"[0-9a-f]{64}",original_sha):
                raise DataIntegrityError("Completed archive missing original SHA")
            membership = conn.execute(
                """SELECT 1 FROM archive_source_price_bins
                   WHERE symbol=%s AND instrument=%s
                     AND archive_day=%s AND archive_sha256=%s LIMIT 1""",
                (symbol,settings.category,day,original_sha)).fetchone()
            receipt = conn.execute(
                """SELECT 1 FROM archive_stage_receipts
                   WHERE symbol=%s AND instrument=%s
                     AND archive_day=%s AND archive_sha256=%s LIMIT 1""",
                (symbol,settings.category,day,original_sha)).fetchone()
            if membership and receipt:
                return "cached"
        else:
            conn.execute("""INSERT INTO ingestion_job(source,symbol,day,state,attempts)
              VALUES('trades_archive',%s,%s,'running',1)
              ON CONFLICT(source,symbol,day) DO UPDATE SET
              state='running',attempts=ingestion_job.attempts+1,updated_at=now()""",
              (symbol,day))
    free=disk_usage(tempfile.gettempdir()).free
    if free < settings.minimum_free_gb*1024**3:
        raise RuntimeError(f"Disk space too low: {free/1024**3:.1f} GiB")
    with tempfile.TemporaryDirectory() as directory:
        path=Path(directory)/"trades.csv.gz"
        checksum=hashlib.sha256()
        size=0
        async with client.stream("GET",url,timeout=httpx.Timeout(120,read=120)) as resp:
            if resp.status_code==404:
                if replay_complete:
                    # An already acquired source SHA must outlive host
                    # unavailability. Rehydration fails, original receipt stays.
                    raise DataIntegrityError(
                        "Previously completed archive unavailable during "
                        "same-SHA membership rehydration (HTTP 404)")
                with connection(settings) as conn:
                    conn.execute("""UPDATE ingestion_job SET state='unavailable',error='HTTP 404',
                    updated_at=now() WHERE source='trades_archive' AND symbol=%s AND day=%s""",
                                 (symbol,day))
                return "unavailable"
            resp.raise_for_status()
            with path.open("wb") as writer:
                async for chunk in resp.aiter_bytes():
                    size+=len(chunk)
                    checksum.update(chunk)
                    writer.write(chunk)
        with path.open("rb") as reader:
            if hashlib.file_digest(reader, "sha256").hexdigest() != checksum.hexdigest():
                raise DataIntegrityError("Downloaded archive SHA256 mismatch on disk")
        if replay_complete and checksum.hexdigest() != original_sha:
            # Preserve the original completed job and its immutable receipts.
            # An exchange archive changed on the server; do not overwrite
            # provenance or retroactively assert the newer bytes are the same.
            raise DataIntegrityError("Previously completed archive SHA256 changed")
        count=0
        # Atomic COPY staging: never mark partial or damaged gzip archive complete.
        with connection(settings) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
                with cur.copy("COPY eth_stage (symbol,ts,trade_id,price,qty,side,notional,source,raw) FROM STDIN") as writer:
                    with gzip.open(path,"rt",newline="",encoding="utf-8-sig") as file:
                        for row in csv.DictReader(file):
                            if row.get("symbol") and row["symbol"] != symbol:
                                raise DataIntegrityError("Archive contains unexpected symbol")
                            stamp,identifier,price,qty,side=parse_trade(row)
                            if stamp.date()!=day:
                                raise ValueError(f"Archive UTC day mismatch {stamp} {day}")
                            writer.write_row((symbol,stamp,identifier,price,qty,side,price*qty,
                                              "archive",json.dumps(row)))
                            count+=1
                if not count:
                    raise DataIntegrityError("Empty archive cannot be complete")
                # Storage B owns the identity conflict guard. It validates staged
                # and existing trade IDs, including the atomic conflict path.
                merged = merge_staged_trades(conn, "eth_stage")
                if merged["input_rows"] != count:
                    raise DataIntegrityError("Archive COPY row count disagrees with stage")
                # B-owned append-only membership is derived exclusively from
                # THIS validated gzip COPY stage, not global trades.source.
                from .storage.archive_receipts import persist_archive_stage_bins
                persist_archive_stage_bins(
                    conn, symbol, day, checksum.hexdigest(), count,
                    stage_name="eth_stage", instrument=settings.category)
            if not replay_complete:
                certify_archive_day(
                    conn, settings, day, checksum.hexdigest(), stage_name="eth_stage",
                    row_count=count, byte_count=size, url=url)
            # Rehydrating an old successfully downloaded day must not rewrite
            # any original minute volume receipt if later candles differ.
            # B's separately versioned comparison ledger will re-evaluate it.
            if not replay_complete:
                conn.execute("""UPDATE ingestion_job SET state='complete',bytes_downloaded=%s,
                  row_count=%s,sha256=%s,error=NULL,updated_at=now()
                  WHERE source='trades_archive' AND symbol=%s AND day=%s""",
                  (size,count,checksum.hexdigest(),symbol,day))
            # Original completed source metadata is deliberately immutable
            # during same-SHA membership recovery. Only B's new append-only
            # SHA bins and comparison receipts may be written.
    return "complete"


def certify_archive_day(conn, settings, day, checksum, *,
                        stage_name, row_count=None, byte_count=None, url=None):
    """Write time-honest aggregate evidence without claiming individual tick coverage."""
    if stage_name != "eth_stage":
        raise ValueError("Archive attestation requires verified COPY stage")
    report = persist_minute_evidence(
        conn, settings.symbol, day, checksum, settings.category,
        stage_name=stage_name)
    begin = datetime.combine(day, datetime.min.time(), timezone.utc)
    state = report["status"]
    write_coverage(
        conn, source="trades_archive", symbol=settings.symbol, interval="",
        start_ms=int(begin.timestamp()*1000),
        end_exclusive_ms=int((begin+timedelta(days=1)).timestamp()*1000),
        status="gap" if state=="volume_mismatch" else "partial",
        observed_rows=row_count if row_count is not None else sum(
            row["n"] for row in conn.execute(
                """SELECT count(*) AS n FROM trades WHERE symbol=%s
                   AND source='archive' AND ts >= %s AND ts < %s""",
                (settings.symbol, begin, begin+timedelta(days=1))).fetchall()),
        expected_rows=None, checksum=checksum,
        quality=("volume_reconciled_not_trade_id_complete"
                 if state=="verified" else "archive_volume_"+state),
        evidence={"certificate": report["certificate"],
                  "compression_bytes": byte_count, "origin": url,
                  "gzip_crc_read_to_eof": True,
                  "trade_identity_guarded": True,
                  "source_event_start_ms":
                      report["certificate"]["source_event_start_ms"],
                  "source_event_end_exclusive_ms":
                      report["certificate"]["source_event_end_exclusive_ms"],
                  "source_receipt_ms":
                      report["certificate"]["source_receipt_ms"],
                  "can_certify_historical_asof": False},
        instrument=settings.category)
    return report


def reconcile_archive_day(conn, symbol, day):
    """Verify archive tape against closed 1m BASE-volume candles for UTC day.

    A volume match is necessary but cannot prove individual tick completeness.
    """
    start = datetime.combine(day, datetime.min.time(), timezone.utc)
    end = start + timedelta(days=1)
    candles = conn.execute(
        """SELECT ts,volume FROM candles WHERE symbol=%s AND kind='trade'
           AND tf='1' AND closed=true AND ts >= %s AND ts < %s
           ORDER BY ts""", (symbol, start, end)).fetchall()
    trades = conn.execute(
        """SELECT date_trunc('minute',ts) AS ts,sum(qty) AS qty FROM trades
           WHERE symbol=%s AND ts >= %s AND ts < %s GROUP BY 1 ORDER BY 1""",
        (symbol, start, end)).fetchall()
    result = reconcile_minute_volume(
        [(row["ts"], row["qty"]) for row in trades],
        [(row["ts"], row["volume"]) for row in candles])
    return result


def resolve_archived_gaps(settings):
    """Fail closed: volume parity cannot prove every missing WS trade identity.

    The complete gzip checksum and 1440 candle totals show consistency with
    the downloaded archive, not authoritative exchange-wide print coverage.
    Future independently attested identity recovery needs its own proof.
    Do not mutate data_gaps.resolved_at merely from sum equality.
    """
    return 0

def revalidate_completed_archive(settings, day):
    """Append-only volume recheck when later original Bybit 1m bars arrive.

    Operates exclusively on previously downloaded exact-SHA archive bins;
    never modifies original acquisition state, resolves WS gaps, or issues
    full-trade-ID / ML certificates. A missing official grid remains partial.
    """
    from .storage.archive_receipts import revalidate_archive_bins
    start = datetime.combine(day, datetime.min.time(), timezone.utc)
    end = start + timedelta(days=1)
    with connection(settings) as conn:
        original = conn.execute(
            """SELECT state,sha256 FROM ingestion_job WHERE
               source='trades_archive' AND symbol=%s AND day=%s""",
            (settings.symbol, day)).fetchone()
        if (original is None or original["state"] != "complete"
                or not original["sha256"]):
            return {"status": "pending_acquisition"}
        stage = conn.execute(
            """SELECT 1 FROM archive_stage_receipts WHERE
               symbol=%s AND instrument=%s AND archive_day=%s
               AND archive_sha256=%s""",
            (settings.symbol, settings.category, day,
             original["sha256"])).fetchone()
        if not stage:
            return {"status": "missing_sha_stage"}
        candles = conn.execute(
            """SELECT count(*) AS total,
                      count(*) FILTER (WHERE source='bybit_v5') AS official
               FROM candles WHERE symbol=%s AND kind='trade' AND tf='1'
                 AND closed=true AND ts >= %s AND ts < %s""",
            (settings.symbol, start, end)).fetchone()
        if candles["total"] == 0:
            return {"status": "pending_official_1m"}
        if candles["total"] != candles["official"]:
            # A synthetic QA candle must NEVER become a new positive source
            # witness merely by being included in B's comparison query.
            return {"status": "non_official_1m_source"}
        proof = revalidate_archive_bins(
            conn, settings.symbol, day, original["sha256"],
            instrument=settings.category)
        if proof.get("exchange_complete") is not False:
            raise DataIntegrityError("Archive volume recheck claimed all trade IDs")
        return proof


def plan_archive_days(start, end, cursor, retries, *, limit=24):
    """Compatibility planner: bounded fresh days plus due failed-day retries."""
    if end < start or not 1 <= limit <= 64:
        raise ValueError("Invalid archive start or unbounded cycle")
    cursor = max(start, min(end, cursor or start))
    days = []
    for day in list(retries)[:8]:
        if start <= day < end and day not in days:
            days.append(day)
    for offset in range(min(limit, (end-cursor).days)):
        day = cursor + timedelta(days=offset)
        if day not in days:
            days.append(day)
    return days


def plan_archive_work(start, end, fresh_cursor, retries, recheck_cursor,
                      pending, *, rehydrate=(), fresh_limit=24, recheck_limit=8):
    """One bounded cycle; old retries NEVER advance the fresh UTC cursor.

    A second rotating cursor revisits earlier official-candle comparisons.
    Pending comparisons are retried independently of both scan positions.
    """
    if end < start or not 1 <= fresh_limit <= 64 or not 1 <= recheck_limit <= 32:
        raise ValueError("Invalid or unbounded archive cycle")
    fresh_cursor = max(start, min(end, fresh_cursor or start))
    tasks, seen = [], set()

    def add(mode, day):
        if start <= day < end and day not in seen:
            tasks.append((mode, day))
            seen.add(day)

    for day in list(rehydrate)[:4]:
        add("rehydrate", day)
    for day in retries[:8]:
        add("retry", day)
    for offset in range(min(fresh_limit, (end-fresh_cursor).days)):
        add("fresh", fresh_cursor + timedelta(days=offset))
    for day in pending[:8]:
        add("recheck", day)
    if start < end:
        span = (end - start).days
        if not recheck_cursor or not start <= recheck_cursor < end:
            recheck_cursor = start
        for offset in range(min(recheck_limit, span)):
            delta = ((recheck_cursor-start).days + offset) % span
            add("recheck", start + timedelta(days=delta))
    return tasks


def archive_volume_reconciled(proof):
    """B's 1440-minute *volume-only* result, NEVER full print completeness."""
    return (isinstance(proof, dict)
            and proof.get("status") == "volume_only_reconciled"
            and proof.get("minutes_checked") == 1440
            and proof.get("minutes_matched") == 1440
            and proof.get("exchange_complete") is False)


def archive_cycle_plan(settings):
    """Read independent fresh, failed-day and past-day recheck cursors."""
    start = settings.archive_start
    end = datetime.now(timezone.utc).date()
    if end < start:
        raise ValueError("ARCHIVE_START in future")
    with connection(settings) as conn:
        stored = conn.execute("""SELECT source,cursor_ts FROM ingestion_cursor
            WHERE symbol=%s AND instrument=%s AND interval=''
              AND source IN ('trades_archive_scan','trades_archive_recheck_scan')""",
            (settings.symbol, settings.category)).fetchall()
        cursors = {row["source"]: (row["cursor_ts"].date()
                    if row["cursor_ts"] else None) for row in stored}
        retry = conn.execute("""SELECT day FROM ingestion_job
            WHERE source='trades_archive' AND symbol=%s
              AND day >= %s AND day < %s
              AND ((state IN ('failed','running','pending')
                    AND updated_at < clock_timestamp()-interval '6 hours')
                 OR (state='unavailable'
                    AND updated_at < clock_timestamp()-interval '24 hours'))
            ORDER BY updated_at,day LIMIT 8""",
            (settings.symbol, start, end)).fetchall()
        recover = conn.execute("""SELECT j.day FROM ingestion_job j
            WHERE j.source='trades_archive' AND j.symbol=%s
              AND j.state='complete' AND j.day >= %s AND j.day < %s
              AND j.sha256 ~ '^[0-9a-f]{64}$'
              AND (
                NOT EXISTS (SELECT 1 FROM archive_stage_receipts s
                  WHERE s.symbol=j.symbol AND s.instrument=%s
                    AND s.archive_day=j.day AND s.archive_sha256=j.sha256)
                OR NOT EXISTS (SELECT 1 FROM archive_source_price_bins b
                  WHERE b.symbol=j.symbol AND b.instrument=%s
                    AND b.archive_day=j.day AND b.archive_sha256=j.sha256))
              AND NOT EXISTS (SELECT 1 FROM ingestion_cursor r
                WHERE r.source='trades_archive_rehydrate'
                  AND r.symbol=j.symbol AND r.instrument=%s
                  AND r.interval=to_char(j.day,'YYYY-MM-DD')
                  AND (r.status='blocked_original_sha'
                       OR r.last_success > clock_timestamp()-interval '24 hours'))
            ORDER BY j.day LIMIT 4""",
            (settings.symbol,start,end,settings.category,
             settings.category,settings.category)).fetchall()
        pending = conn.execute("""SELECT interval FROM ingestion_cursor
            WHERE source='trades_archive_day_recheck' AND symbol=%s
              AND instrument=%s AND interval >= %s AND interval < %s
              AND status='pending'
              AND last_success < clock_timestamp()-interval '6 hours'
            ORDER BY last_success,interval LIMIT 8""",
            (settings.symbol, settings.category,
             start.isoformat(), end.isoformat())).fetchall()
    return plan_archive_work(
        start, end, cursors.get("trades_archive_scan"),
        [r["day"] for r in retry], cursors.get("trades_archive_recheck_scan"),
        [datetime.fromisoformat(r["interval"]).date() for r in pending],
        rehydrate=[r["day"] for r in recover])


def advance_archive_cursor(settings, day, *, mode="fresh"):
    """Only sequential fresh work advances ingestion; retry cannot skip days."""
    if mode not in ("fresh", "recheck"):
        raise ValueError("Invalid scan cursor mode")
    source = ("trades_archive_scan" if mode == "fresh"
              else "trades_archive_recheck_scan")
    stamp = datetime.combine(day+timedelta(days=1), datetime.min.time(), timezone.utc)
    with connection(settings) as conn:
        conn.execute("""INSERT INTO ingestion_cursor
             (source,symbol,instrument,interval,cursor_ts,last_success,status,meta)
             VALUES(%s,%s,%s,'',%s,clock_timestamp(),'partial',%s::jsonb)
             ON CONFLICT(source,symbol,instrument,interval) DO UPDATE SET
             cursor_ts=CASE WHEN excluded.source='trades_archive_scan'
                 THEN greatest(ingestion_cursor.cursor_ts,excluded.cursor_ts)
                 ELSE excluded.cursor_ts END,
             last_success=clock_timestamp(),status='partial',meta=excluded.meta""",
             (source, settings.symbol, settings.category, stamp,
              json.dumps({"contract":"bounded_archive_cycle_v2",
                          "source_quality":"partial",
                          "exchange_complete":False,"ml_eligible":False})))


def record_archive_recheck(settings, day, comparison):
    """Durable per-day status so future cycles cannot silently forget pending."""
    if not isinstance(comparison, dict):
        comparison = {"status":"invalid_comparison"}
    ok = archive_volume_reconciled(comparison)
    state = "volume_only_reconciled" if ok else "pending"
    meta = {"contract":"archive_recheck_v1",
            "comparison_status":str(comparison.get("status","unknown"))[:80],
            "minutes_checked":comparison.get("minutes_checked"),
            "minutes_matched":comparison.get("minutes_matched"),
            "exchange_complete":False,"ml_eligible":False}
    with connection(settings) as conn:
        conn.execute("""INSERT INTO ingestion_cursor
            (source,symbol,instrument,interval,last_success,status,meta)
            VALUES('trades_archive_day_recheck',%s,%s,%s,
                   clock_timestamp(),%s,%s::jsonb)
            ON CONFLICT(source,symbol,instrument,interval) DO UPDATE SET
            last_success=clock_timestamp(),status=excluded.status,
            meta=excluded.meta""",
            (settings.symbol, settings.category, day.isoformat(),
             state, json.dumps(meta, sort_keys=True)))
    return ok


def record_archive_rehydration(settings, day, state, error=None):
    """Durably record same-SHA recovery without rewriting source identity."""
    with connection(settings) as conn:
        original = conn.execute("""SELECT state,sha256 FROM ingestion_job
            WHERE source='trades_archive' AND symbol=%s AND day=%s""",
            (settings.symbol,day)).fetchone()
        if (not original or original["state"] != "complete"
                or not isinstance(original["sha256"],str)
                or not re.fullmatch(r"[0-9a-f]{64}",original["sha256"])):
            raise DataIntegrityError("Missing immutable original SHA")
        status = ("recovered_same_sha" if state in ("complete","cached") else
                  "blocked_original_sha" if error is not None and
                  "Previously completed archive SHA256 changed" in str(error)
                  else "pending")
        conn.execute("""INSERT INTO ingestion_cursor
            (source,symbol,instrument,interval,last_success,status,meta)
            VALUES('trades_archive_rehydrate',%s,%s,%s,
                   clock_timestamp(),%s,%s::jsonb)
            ON CONFLICT(source,symbol,instrument,interval) DO UPDATE SET
            last_success=clock_timestamp(),status=excluded.status,
            meta=excluded.meta""",
            (settings.symbol,settings.category,day.isoformat(),status,
             json.dumps({"contract":"archive_same_sha_rehydration_v1",
                         "original_sha256":original["sha256"],
                         "outcome":status,"reason":str(error)[:160] if error else "",
                         "exchange_complete":False,"ml_eligible":False})))
    return status


def recheck_archive_with_receipt(settings, day):
    """Never let a failed comparison create a positive archive witness."""
    try:
        comparison = revalidate_completed_archive(settings, day)
    except Exception as exc:
        comparison = {"status":"recheck_error","error":str(exc)[:250]}
        print(f"Archive SHA recheck pending for {day}: {exc}", flush=True)
    # A failed DB write here aborts the cycle; no false green cursor.
    return record_archive_recheck(settings, day, comparison)


def quarantine_archive_day(settings, day, error, permanent):
    """An unrelated failed UTC day must not erase an acquired immutable SHA."""
    begin = datetime.combine(day,datetime.min.time(),timezone.utc)
    with connection(settings) as conn:
        original = conn.execute("""SELECT state,sha256 FROM ingestion_job
           WHERE source='trades_archive' AND symbol=%s AND day=%s FOR UPDATE""",
           (settings.symbol,day)).fetchone()
        if original and original["state"] == "complete":
            conn.execute("""INSERT INTO bot_events(severity,event,data)
                VALUES('error','archive_rehydration_failed',%s::jsonb)""",
                (json.dumps({"day":day.isoformat(),"original_sha256":original["sha256"],
                             "error":str(error)[:300],"verified_new_bins":False}),))
            return
        conn.execute("""INSERT INTO ingestion_job
          (source,symbol,day,state,attempts,error,updated_at)
          VALUES('trades_archive',%s,%s,'failed',1,%s,clock_timestamp())
          ON CONFLICT(source,symbol,day) DO UPDATE
          SET state='failed',error=excluded.error,updated_at=clock_timestamp()""",
          (settings.symbol,day,str(error)[:2000]))
        write_coverage(conn,source="trades_archive",symbol=settings.symbol,
            interval="",start_ms=int(begin.timestamp()*1000),
            end_exclusive_ms=int((begin+timedelta(days=1)).timestamp()*1000),
            status="gap",observed_rows=0,quality="archive_import_failed",
            evidence={"error":str(error)[:1500],"permanent":bool(permanent),
                      "retry_after_hours":6,"ml_eligible":False},
            instrument=settings.category)


async def backfill_archives(settings):
    """Bounded restartable cycle, with durable prior-day comparison backlog."""
    tasks = archive_cycle_plan(settings)
    result = {"attempted":0,"failed":0,"unavailable":0,"completed":0,
              "recheck_pending":0,"rehydrate_pending":0,
              "exchange_complete":False,"ml_eligible":False}
    async with httpx.AsyncClient(follow_redirects=False) as client:
        for mode, day in tasks:
            result["attempted"] += 1
            if mode == "recheck":
                if not recheck_archive_with_receipt(settings, day):
                    result["recheck_pending"] += 1
                advance_archive_cursor(settings, day, mode="recheck")
                continue
            state = "failed"
            final_error = None
            for attempt in range(5):
                try:
                    state = await download_archive(settings,day,client)
                    break
                except Exception as exc:
                    if (isinstance(exc,OSError) and getattr(exc,"errno",None)==28):
                        raise  # ENOSPC is a global fatal error.
                    if isinstance(exc,RuntimeError) and "Disk space too low" in str(exc):
                        raise
                    permanent = isinstance(exc,(DataIntegrityError,ValueError,UniqueViolation))
                    if isinstance(exc,httpx.HTTPStatusError):
                        permanent = exc.response.status_code in (400,401,403,410)
                    if permanent or attempt == 4:
                        final_error = exc
                        # Durable failure ledger is required before any progress.
                        quarantine_archive_day(settings,day,exc,permanent)
                        print(f"Archive quarantined {day}: {exc}",flush=True)
                        break
                    await asyncio.sleep(min(90,2**attempt*3))
            if mode == "rehydrate":
                if record_archive_rehydration(settings,day,state,final_error) != "recovered_same_sha":
                    result["rehydrate_pending"] += 1
            if state in ("cached","complete"):
                result["completed"] += 1
                if not recheck_archive_with_receipt(settings, day):
                    result["recheck_pending"] += 1
            else:
                result["unavailable" if state=="unavailable" else "failed"] += 1
            if mode == "fresh":
                advance_archive_cursor(settings, day, mode="fresh")
            if state != "cached":
                await asyncio.sleep(1/max(0.05,settings.archive_downloads_per_second))
    end = datetime.now(timezone.utc).date()
    with connection(settings) as conn:
        unresolved = conn.execute("""SELECT count(*) AS n FROM ingestion_job
            WHERE source='trades_archive' AND symbol=%s
              AND day >= %s AND day < %s AND state <> 'complete'""",
            (settings.symbol,settings.archive_start,end)).fetchone()["n"]
        seen = conn.execute("""SELECT count(*) AS n FROM ingestion_job
            WHERE source='trades_archive' AND symbol=%s
              AND day >= %s AND day < %s""",
            (settings.symbol,settings.archive_start,end)).fetchone()["n"]
        unvalidated = conn.execute("""SELECT count(*) AS n FROM ingestion_job j
            WHERE j.source='trades_archive' AND j.symbol=%s
              AND j.state='complete' AND j.day >= %s AND j.day < %s
              AND NOT EXISTS (
                SELECT 1 FROM ingestion_cursor c
                WHERE c.source='trades_archive_day_recheck'
                  AND c.symbol=j.symbol AND c.instrument=%s
                  AND c.interval=to_char(j.day,'YYYY-MM-DD')
                  AND c.status='volume_only_reconciled')""",
            (settings.symbol,settings.archive_start,end,settings.category)
            ).fetchone()["n"]
        rehydrate_backlog = conn.execute("""SELECT count(*) AS n
            FROM ingestion_cursor WHERE source='trades_archive_rehydrate'
              AND symbol=%s AND instrument=%s
              AND status IN ('pending','blocked_original_sha')""",
            (settings.symbol,settings.category)).fetchone()["n"]
        missing_stage = conn.execute("""SELECT count(*) AS n
            FROM ingestion_job j WHERE j.source='trades_archive'
              AND j.symbol=%s AND j.state='complete'
              AND j.day >= %s AND j.day < %s
              AND (j.sha256 IS NULL
                OR NOT EXISTS (SELECT 1 FROM archive_stage_receipts s
                   WHERE s.symbol=j.symbol AND s.instrument=%s
                     AND s.archive_day=j.day AND s.archive_sha256=j.sha256)
                OR NOT EXISTS (SELECT 1 FROM archive_source_price_bins b
                   WHERE b.symbol=j.symbol AND b.instrument=%s
                     AND b.archive_day=j.day AND b.archive_sha256=j.sha256))""",
            (settings.symbol,settings.archive_start,end,settings.category,
             settings.category)).fetchone()["n"]
        current = conn.execute("""SELECT cursor_ts FROM ingestion_cursor
            WHERE source='trades_archive_scan' AND symbol=%s
              AND instrument=%s AND interval=''""",
            (settings.symbol,settings.category)).fetchone()
    caught_up = bool(current and current["cursor_ts"]
                     and current["cursor_ts"].date() >= end)
    missing = max(0,(end-settings.archive_start).days-seen)
    result.update({"unresolved_days":unresolved,"unvalidated_days":unvalidated,
                   "unattempted_days":missing,
                   "sha_membership_missing_days":missing_stage,
                   "rehydration_backlog":rehydrate_backlog})
    result["status"] = ("cycle_completed" if caught_up and not unresolved
                        and not missing and not unvalidated and not missing_stage
                        and not result["recheck_pending"] else "partial")
    print("Archive bounded cycle:",result,flush=True)
    return result

async def run_archive_catchup(settings, *, max_cycles=8, budget_seconds=1800):
    """Bounded sequential catch-up without relaxing per-cycle/source checks.

    Each existing archive cycle loads at most 24 new days and 4 same-SHA
    rehydrations. A daily supervisor launch can catch up faster while disk,
    source SHA, network rate and fail-closed receipts remain authoritative.
    """
    if type(max_cycles) is not int or not 1 <= max_cycles <= 8:
        raise ValueError("Unsafe archive catch-up cycle count")
    if type(budget_seconds) not in (int,float) or not 30 <= budget_seconds <= 1800:
        raise ValueError("Unsafe archive catch-up time budget")
    loop = asyncio.get_running_loop()
    started = loop.time()
    result = None
    for i in range(max_cycles):
        remaining = budget_seconds-(loop.time()-started)
        if remaining < 30:
            break
        try:
            async with asyncio.timeout(remaining):
                result = await backfill_archives(settings)
        except TimeoutError:
            print("Archive catch-up time budget exhausted; partial",flush=True)
            break
        if ((result["unattempted_days"] == 0
             and result["sha_membership_missing_days"] == 0)
                or result["attempted"] == 0):
            break
        if i+1 < max_cycles and budget_seconds-(loop.time()-started) > 30:
            await asyncio.sleep(3)
    return result or {"status":"partial","exchange_complete":False,
                      "ml_eligible":False,"catchup_incomplete":True}
