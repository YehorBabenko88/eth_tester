"""Idempotent historical multi-timeframe clusters from complete daily tick archives.

Each day's contribution is stored separately so calendar-week/month price bins
can be rebuilt without double-counting previously imported archive days.
"""
from datetime import datetime, timedelta, timezone
import json

from .analytics_core import value_profile

from .database import connection

MINUTES = {"1":1,"3":3,"5":5,"15":15,"30":30,"60":60,
           "120":120,"240":240,"360":360,"720":720,"D":1440}
TIMEFRAMES = (*MINUTES, "W", "M")


def bucket_expression(tf):
    if tf in MINUTES:
        return f"date_bin(interval '{MINUTES[tf]} minutes',ts,'1970-01-01 00:00+00'::timestamptz)"
    if tf == "W":
        return "date_trunc('week',ts)"
    if tf == "M":
        return "date_trunc('month',ts)"
    raise ValueError("Unknown time frame")


def rollup_day(settings, day, tf, digest):
    """Rebuild only buckets affected by a changed archive day.

    footprint_daily is the authoritative per-day contribution ledger.
    Replacing one day must include *previously* touched buckets, including
    now-empty buckets, and must aggregate every contributing day for W/M.
    """
    expression = bucket_expression(tf)
    beginning = datetime.combine(day, datetime.min.time(), timezone.utc)
    ending = beginning + timedelta(days=1)
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                     (f"{settings.symbol}:{tf}:cluster",))
        done = conn.execute("""SELECT 1 FROM aggregation_job WHERE symbol=%s
            AND tf=%s AND day=%s AND archive_sha256=%s""",
                            (settings.symbol, tf, day, digest)).fetchone()
        if done:
            return 0
        old_buckets = conn.execute("""SELECT DISTINCT bucket FROM footprint_daily
            WHERE symbol=%s AND tf=%s AND day=%s""",
                                   (settings.symbol, tf, day)).fetchall()
        conn.execute("""DELETE FROM footprint_daily
            WHERE symbol=%s AND tf=%s AND day=%s""",
                     (settings.symbol, tf, day))
        inserted = conn.execute(f"""INSERT INTO footprint_daily(
            symbol,tf,day,bucket,price,buy_qty,sell_qty,trades)
            SELECT %s,%s,%s,{expression} AS bucket,price,
              coalesce(sum(qty) FILTER(WHERE side='Buy'),0),
              coalesce(sum(qty) FILTER(WHERE side='Sell'),0),
              count(*)
            FROM trades
            WHERE symbol=%s AND ts>=%s AND ts<%s
            GROUP BY bucket,price""",
            (settings.symbol, tf, day, settings.symbol, beginning, ending))
        count = inserted.rowcount
        new_buckets = conn.execute("""SELECT DISTINCT bucket FROM footprint_daily
            WHERE symbol=%s AND tf=%s AND day=%s""",
                                   (settings.symbol, tf, day)).fetchall()
        affected = sorted({row["bucket"] for row in (*old_buckets, *new_buckets)})
        if affected:
            # Delete-and-rebuild is mandatory: ON CONFLICT cannot remove
            # a price bin that vanished after an archive correction.
            conn.execute("""DELETE FROM footprint
                WHERE symbol=%s AND tf=%s AND bucket=ANY(%s::timestamptz[])""",
                         (settings.symbol, tf, affected))
            conn.execute("""INSERT INTO footprint(symbol,tf,bucket,price,buy_qty,
                sell_qty,buy_notional,sell_notional,trades)
                SELECT symbol,tf,bucket,price,sum(buy_qty),sum(sell_qty),
                   price*sum(buy_qty),price*sum(sell_qty),sum(trades)
                FROM footprint_daily
                WHERE symbol=%s AND tf=%s AND bucket=ANY(%s::timestamptz[])
                GROUP BY symbol,tf,bucket,price""",
                         (settings.symbol, tf, affected))
            # A fully removed bucket has no valid POC or volume feature.
            conn.execute("""DELETE FROM features f
                WHERE f.symbol=%s AND f.tf=%s AND f.ts=ANY(%s::timestamptz[])
                  AND NOT EXISTS (
                    SELECT 1 FROM footprint p
                    WHERE p.symbol=f.symbol AND p.tf=f.tf
                      AND p.bucket=f.ts)""",
                         (settings.symbol, tf, affected))
            duration = ("EXTRACT(epoch FROM (a.bucket + interval '1 month' "
                        "- a.bucket))" if tf == "M" else
                        str(604800 if tf == "W" else MINUTES[tf] * 60))
            conn.execute(f"""WITH aggregate AS (
                SELECT f.bucket,sum(f.buy_qty) AS buy,sum(f.sell_qty) AS sell,
                       sum(f.trades) AS prints
                FROM footprint f
                WHERE f.symbol=%s AND f.tf=%s
                  AND f.bucket=ANY(%s::timestamptz[])
                GROUP BY f.bucket),
              poc AS (
                SELECT DISTINCT ON (f.bucket) f.bucket,f.price
                FROM footprint f
                WHERE f.symbol=%s AND f.tf=%s
                  AND f.bucket=ANY(%s::timestamptz[])
                ORDER BY f.bucket,(f.buy_qty+f.sell_qty) DESC,f.price ASC)
              INSERT INTO features(symbol,tf,ts,poc,delta,volume,tape_tps,quality)
              SELECT %s,%s,a.bucket,p.price,a.buy-a.sell,a.buy+a.sell,
                     a.prints / NULLIF({duration},0),'archive_rollup_partial'
              FROM aggregate a JOIN poc p ON p.bucket=a.bucket
              ON CONFLICT(symbol,tf,ts) DO UPDATE SET
                poc=EXCLUDED.poc,delta=EXCLUDED.delta,
                volume=EXCLUDED.volume,tape_tps=EXCLUDED.tape_tps,
                quality=EXCLUDED.quality""",
                (settings.symbol, tf, affected, settings.symbol, tf, affected,
                 settings.symbol, tf))
        if affected:
            # One ordered query per changed day/frame, not one query per price.
            price_rows = conn.execute("""SELECT bucket,price,buy_qty,sell_qty
                FROM footprint WHERE symbol=%s AND tf=%s
                  AND bucket=ANY(%s::timestamptz[])
                ORDER BY bucket,price""",
                (settings.symbol, tf, affected)).fetchall()
            profiles = {}
            for row in price_rows:
                histogram = profiles.setdefault(row["bucket"], {})
                histogram[row["price"]] = row["buy_qty"] + row["sell_qty"]
            for stamp, histogram in profiles.items():
                profile = value_profile(histogram)
                payload = json.dumps({
                    "vpoc": str(profile["vpoc"]),
                    "vah": str(profile["vah"]), "val": str(profile["val"]),
                    "hvn": [str(p) for p in profile["hvn"]],
                    "lvn": [str(p) for p in profile["lvn"]],
                    "value_area_fraction": "0.70",
                    "profile_coverage": "archive_days_observed_not_verified",
                    "profile_method": "contiguous_volume_expansion"})
                conn.execute("""UPDATE features
                    SET metadata=features.metadata || %s::jsonb
                    WHERE symbol=%s AND tf=%s AND ts=%s""",
                    (payload, settings.symbol, tf, stamp))
            # Historical corrections invalidate previously accumulated CVD.
            # Roll the checkpoint back to the last bucket before the change.
            predecessor = conn.execute("""SELECT ts FROM features
                WHERE symbol=%s AND tf=%s AND ts<%s
                ORDER BY ts DESC LIMIT 1""",
                (settings.symbol, tf, affected[0])).fetchone()
            conn.execute("""UPDATE analytics_cursor SET
                cursor_ts=%s,state='dirty',quality='recompute_required',
                updated_at=now()
                WHERE processor='cvd' AND symbol=%s AND tf=%s
                  AND cursor_ts>=%s""",
                (predecessor["ts"] if predecessor else None,
                 settings.symbol, tf, affected[0]))
        conn.execute("""INSERT INTO aggregation_job(symbol,tf,day,archive_sha256,rows)
            VALUES(%s,%s,%s,%s,%s)
            ON CONFLICT(symbol,tf,day) DO UPDATE SET
              archive_sha256=EXCLUDED.archive_sha256,rows=EXCLUDED.rows,
              completed_at=now()""",
                     (settings.symbol, tf, day, digest, count))
    return count


def aggregate_history(settings,max_days=0):
    with connection(settings) as conn:
        days=conn.execute("""SELECT day,sha256 FROM ingestion_job WHERE
          source='trades_archive' AND symbol=%s AND state='complete'
          ORDER BY day""",(settings.symbol,)).fetchall()
    if max_days>0:
        days=days[:max_days]
    total=len(days)*len(TIMEFRAMES)
    completed=0
    for row in days:
        if not row["sha256"]:
            raise RuntimeError("Complete archive missing SHA256")
        for tf in TIMEFRAMES:
            count=rollup_day(settings,row["day"],tf,row["sha256"])
            completed+=1
            print(f"footprint {row['day']} {tf}: {count} bins, "
                  f"{completed}/{total} ({100*completed/max(1,total):.1f}%)",flush=True)
    return completed
