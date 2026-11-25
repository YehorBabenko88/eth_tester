import asyncio
import csv
from datetime import datetime,timedelta,timezone
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
from shutil import disk_usage
import httpx
from .database import connection

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
    seconds=numeric_time/1000 if numeric_time>100000000000 else numeric_time
    stamp=datetime.fromtimestamp(float(seconds),timezone.utc)
    p,q=Decimal(price),Decimal(qty)
    if p<=0 or q<=0:
        raise ValueError("Nonpositive trade price/size")
    return stamp,str(identifier),p,q,side.title()

async def download_archive(settings,day,client):
    symbol=settings.symbol
    url=f"https://public.bybit.com/trading/{symbol}/{symbol}{day.isoformat()}.csv.gz"
    with connection(settings) as conn:
        state=conn.execute("""SELECT state FROM ingestion_job
         WHERE source='trades_archive' AND symbol=%s AND day=%s""",
          (symbol,day)).fetchone()
        if state and state["state"]=="complete":
            return "complete"
        conn.execute("""INSERT INTO ingestion_job(source,symbol,day,state,attempts)
          VALUES('trades_archive',%s,%s,'running',1)
          ON CONFLICT(source,symbol,day) DO UPDATE SET
          state='running',attempts=ingestion_job.attempts+1,updated_at=now()""",(symbol,day))
    free=disk_usage(tempfile.gettempdir()).free
    if free < settings.minimum_free_gb*1024**3:
        raise RuntimeError(f"Disk space too low: {free/1024**3:.1f} GiB")
    with tempfile.TemporaryDirectory() as directory:
        path=Path(directory)/"trades.csv.gz"
        checksum=hashlib.sha256()
        size=0
        async with client.stream("GET",url,timeout=httpx.Timeout(120,read=120)) as resp:
            if resp.status_code==404:
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
        count=0
        # Atomic COPY staging: never mark partial or damaged gzip archive complete.
        with connection(settings) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE eth_stage (LIKE trades) ON COMMIT DROP")
                with cur.copy("COPY eth_stage (symbol,ts,trade_id,price,qty,side,notional,source,raw) FROM STDIN") as writer:
                    with gzip.open(path,"rt",newline="",encoding="utf-8-sig") as file:
                        for row in csv.DictReader(file):
                            stamp,identifier,price,qty,side=parse_trade(row)
                            if stamp.date()!=day:
                                raise ValueError(f"Archive UTC day mismatch {stamp} {day}")
                            writer.write_row((symbol,stamp,identifier,price,qty,side,price*qty,
                                              "archive",json.dumps(row)))
                            count+=1
                cur.execute("""INSERT INTO trades(symbol,ts,trade_id,price,qty,side,notional,source,raw)
                    SELECT symbol,ts,trade_id,price,qty,side,notional,source,raw FROM eth_stage
                    ON CONFLICT(symbol,trade_id) DO NOTHING""")
            conn.execute("""UPDATE ingestion_job SET state='complete',bytes_downloaded=%s,
              row_count=%s,sha256=%s,error=NULL,updated_at=now()
              WHERE source='trades_archive' AND symbol=%s AND day=%s""",
              (size,count,checksum.hexdigest(),symbol,day))
    return "complete"

def resolve_archived_gaps(settings):
    resolved=0
    with connection(settings) as conn:
        gaps=conn.execute("SELECT id,start_ts,end_ts FROM data_gaps WHERE source='publicTrade' AND symbol=%s AND end_ts IS NOT NULL AND resolved_at IS NULL",(settings.symbol,)).fetchall()
        for gap in gaps:
            start=gap['start_ts'].date()
            end=gap['end_ts'].date()
            expected=(end-start).days+1
            row=conn.execute("SELECT count(*) FILTER (WHERE state='complete') AS complete FROM ingestion_job WHERE source='trades_archive' AND symbol=%s AND day BETWEEN %s AND %s",(settings.symbol,start,end)).fetchone()
            if row['complete']==expected:
                conn.execute('UPDATE data_gaps SET resolved_at=now() WHERE id=%s',(gap['id'],))
                resolved+=1
    return resolved

async def backfill_archives(settings):
    start=settings.archive_start
    end=datetime.now(timezone.utc).date()
    total=(end-start).days
    if total<0:
        raise ValueError("ARCHIVE_START in future")
    async with httpx.AsyncClient(follow_redirects=False) as client:
        for i in range(total):
            day=start+timedelta(days=i)
            for attempt in range(5):
                try:
                    state=await download_archive(settings,day,client)
                    break
                except Exception as exc:
                    if attempt==4:
                        with connection(settings) as conn:
                            conn.execute("""UPDATE ingestion_job SET state='failed',error=%s,
                              updated_at=now() WHERE source='trades_archive' AND symbol=%s AND day=%s""",
                              (str(exc)[:2000],settings.symbol,day))
                        raise
                    await asyncio.sleep(min(90,2**attempt*3))
            print(f"{day} {state} {i+1}/{total} ({100*(i+1)/max(total,1):.1f}%)",flush=True)
            await asyncio.sleep(1/max(0.05,settings.archive_downloads_per_second))
    print('Resolved full-day archived tick gaps:',resolve_archived_gaps(settings),flush=True)
