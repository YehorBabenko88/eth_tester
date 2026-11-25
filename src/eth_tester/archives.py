import asyncio
import csv
from datetime import datetime,timedelta,timezone
from decimal import Decimal, ROUND_FLOOR
import gzip
import hashlib
import json
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
            # A pre-migration "complete" archive may lack SHA-bound membership.
            # If membership exists, do not download again. The separate
            # versioned candle recheck reads durable B price bins.
            membership = conn.execute(
                """SELECT 1 FROM archive_source_price_bins
                   WHERE symbol=%s AND instrument=%s
                     AND archive_day=%s AND archive_sha256=%s
                   LIMIT 1""",
                (symbol, settings.category, day, original_sha)).fetchone()
            if membership:
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
    """Bounded UTC cycle, excluding today's unfinished archive."""
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


def archive_cycle_plan(settings):
    """Load bounded retry eligibility and durable forward cursor."""
    start = settings.archive_start
    end = datetime.now(timezone.utc).date()
    with connection(settings) as conn:
        cursor = conn.execute("""SELECT cursor_ts FROM ingestion_cursor
            WHERE source='trades_archive_scan' AND symbol=%s
              AND instrument=%s AND interval=''""",
              (settings.symbol, settings.category)).fetchone()
        due = conn.execute("""SELECT day FROM ingestion_job
            WHERE source='trades_archive' AND symbol=%s
              AND day >= %s AND day < %s
              AND ((state='failed' AND updated_at < now()-interval '6 hours')
                 OR (state='unavailable' AND updated_at < now()-interval '24 hours'))
            ORDER BY updated_at,day LIMIT 8""",
            (settings.symbol,start,end)).fetchall()
    next_date = cursor["cursor_ts"].date() if cursor and cursor["cursor_ts"] else start
    return plan_archive_days(start,end,next_date,[r["day"] for r in due])


def advance_archive_cursor(settings, day):
    """After an actual day attempt, atomically remember crash-resumable progress."""
    stamp = datetime.combine(day+timedelta(days=1),datetime.min.time(),timezone.utc)
    with connection(settings) as conn:
        conn.execute("""INSERT INTO ingestion_cursor
             (source,symbol,instrument,interval,cursor_ts,last_success,status,meta)
             VALUES('trades_archive_scan',%s,%s,'',%s,clock_timestamp(),
                    'partial',%s::jsonb)
             ON CONFLICT(source,symbol,instrument,interval) DO UPDATE SET
             cursor_ts=greatest(ingestion_cursor.cursor_ts,excluded.cursor_ts),
             last_success=clock_timestamp(),status='partial',meta=excluded.meta""",
             (settings.symbol,settings.category,stamp,json.dumps({
                 "contract":"bounded_archive_cycle_v1","ml_eligible":False,
                 "exchange_complete":False})))


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
    """Process a finite restartable batch; leave source quality fail-closed."""
    days = archive_cycle_plan(settings)
    result = {"attempted":0,"failed":0,"unavailable":0,"completed":0,
              "recheck_pending":0,"exchange_complete":False,"ml_eligible":False}
    async with httpx.AsyncClient(follow_redirects=False) as client:
        for day in days:
            result["attempted"] += 1
            state = "failed"
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
                        # Database failure writing this ledger aborts the entire
                        # cycle instead of advancing a fake successful cursor.
                        quarantine_archive_day(settings,day,exc,permanent)
                        print(f"Archive quarantined {day}: {exc}",flush=True)
                        break
                    await asyncio.sleep(min(90,2**attempt*3))
            if state in ("cached","complete"):
                result["completed"] += 1
                try:
                    check = revalidate_completed_archive(settings,day)
                    if check.get("status") not in ("verified","volume_match"):
                        result["recheck_pending"] += 1
                except Exception as exc:
                    result["recheck_pending"] += 1
                    print(f"Archive recheck pending {day}: {exc}",flush=True)
            else:
                result["unavailable" if state=="unavailable" else "failed"] += 1
            # Advancing a cursor is allowed after the failure is DURABLY recorded.
            advance_archive_cursor(settings,day)
            if state != "cached":
                await asyncio.sleep(1/max(0.05,settings.archive_downloads_per_second))
    with connection(settings) as conn:
        unresolved = conn.execute("""SELECT count(*) AS n FROM ingestion_job
            WHERE source='trades_archive' AND symbol=%s
              AND state IN ('failed','unavailable')""",
              (settings.symbol,)).fetchone()["n"]
        current = conn.execute("""SELECT cursor_ts FROM ingestion_cursor
            WHERE source='trades_archive_scan' AND symbol=%s
              AND instrument=%s AND interval=''""",
              (settings.symbol,settings.category)).fetchone()
    today = datetime.now(timezone.utc).date()
    caught_up = bool(current and current["cursor_ts"]
                     and current["cursor_ts"].date() >= today)
    result["unresolved_days"] = unresolved
    result["status"] = ("cycle_completed" if caught_up and not unresolved
                        and not result["recheck_pending"] else "partial")
    print("Archive bounded cycle:",result,flush=True)
    return result
