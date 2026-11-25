"""Idempotent historical multi-timeframe clusters from complete daily tick archives.

Each day's contribution is stored separately so calendar-week/month price bins
can be rebuilt without double-counting previously imported archive days.
"""
import json

from .analytics_core import value_profile

from .database import connection

MINUTES = {"1":1,"3":3,"5":5,"15":15,"30":30,"60":60,
           "120":120,"240":240,"360":360,"720":720,"D":1440}
TIMEFRAMES = (*MINUTES, "W", "M")


def bucket_expression(tf, source_column="ts"):
    if source_column not in ("ts", "minute_ts"):
        raise ValueError("Unapproved archive timestamp column")
    if tf in MINUTES:
        return f"date_bin(interval '{MINUTES[tf]} minutes',{source_column},'1970-01-01 00:00+00'::timestamptz)"
    if tf == "W":
        return f"date_trunc('week',{source_column})"
    if tf == "M":
        return f"date_trunc('month',{source_column})"
    raise ValueError("Unknown time frame")



def _verify_archive_stage_source(conn, settings, day, digest):
    """Require B immutable gzip SHA membership before editing rollup rows."""
    job = conn.execute("""SELECT state,sha256 FROM ingestion_job
        WHERE source='trades_archive' AND symbol=%s AND day=%s FOR SHARE""",
        (settings.symbol, day)).fetchone()
    if not job or job["state"] != "complete" or job["sha256"] != digest:
        raise ValueError("archive_job_sha_missing_or_changed")
    source = conn.execute("""SELECT original_rows,distinct_trade_ids,price_bins,
            base_qty,quote_notional,exchange_complete
        FROM archive_stage_receipts WHERE symbol=%s AND instrument=%s
          AND archive_day=%s AND archive_sha256=%s FOR SHARE""",
        (settings.symbol, settings.category, day, digest)).fetchone()
    if source is None or source["exchange_complete"] is not False:
        raise ValueError("sha_pinned_archive_stage_receipt_missing")
    bins = conn.execute("""SELECT count(*) AS price_bins,
            coalesce(sum(trade_count),0) AS prints,
            coalesce(sum(base_qty),0) AS base_qty,
            coalesce(sum(quote_notional),0) AS quote_notional
        FROM archive_source_price_bins
        WHERE symbol=%s AND instrument=%s AND archive_day=%s
          AND archive_sha256=%s""",
        (settings.symbol, settings.category, day, digest)).fetchone()
    if (source["distinct_trade_ids"] != bins["prints"] or
            source["price_bins"] != bins["price_bins"] or
            source["base_qty"] != bins["base_qty"] or
            source["quote_notional"] != bins["quote_notional"] or
            bins["prints"] < 1):
        raise ValueError("archive_source_price_bins_manifest_mismatch")
    return source


def _daily_bins_match(conn, settings, day, tf, digest, expression):
    """Compare stored daily contribution with identical immutable SHA bins."""
    result = conn.execute(f"""WITH expected AS (
        SELECT {expression} AS bucket,price,
          coalesce(sum(base_qty) FILTER(WHERE side='Buy'),0) AS buy_qty,
          coalesce(sum(base_qty) FILTER(WHERE side='Sell'),0) AS sell_qty,
          sum(trade_count)::bigint AS trades
        FROM archive_source_price_bins
        WHERE symbol=%s AND instrument=%s AND archive_day=%s
          AND archive_sha256=%s GROUP BY 1,2),
      present AS (
        SELECT bucket,price,buy_qty,sell_qty,trades::bigint AS trades
        FROM footprint_daily WHERE symbol=%s AND tf=%s AND day=%s),
      difference AS (
        (SELECT * FROM expected EXCEPT ALL SELECT * FROM present)
        UNION ALL
        (SELECT * FROM present EXCEPT ALL SELECT * FROM expected))
      SELECT EXISTS(SELECT 1 FROM difference) AS differs""",
      (settings.symbol, settings.category, day, digest,
       settings.symbol, tf, day)).fetchone()
    return not result["differs"]

def rollup_day(settings, day, tf, digest):
    """Rebuild only buckets affected by a changed archive day.

    footprint_daily is the authoritative per-day contribution ledger.
    Replacing one day must include *previously* touched buckets, including
    now-empty buckets, and must aggregate every contributing day for W/M.
    """
    expression = bucket_expression(tf, "minute_ts")
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                     (f"{settings.symbol}:{tf}:cluster",))
        _verify_archive_stage_source(conn, settings, day, digest)
        done = conn.execute("""SELECT 1 FROM aggregation_job WHERE symbol=%s
            AND tf=%s AND day=%s AND archive_sha256=%s""",
                            (settings.symbol, tf, day, digest)).fetchone()
        if done and _daily_bins_match(
                conn, settings, day, tf, digest, expression):
            # Audited exact SHA membership: rotate historical days without
            # rewriting their correct footprints or invalidating CVD.
            conn.execute("""UPDATE aggregation_job SET completed_at=clock_timestamp()
                WHERE symbol=%s AND tf=%s AND day=%s AND archive_sha256=%s""",
                (settings.symbol, tf, day, digest))
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
              coalesce(sum(base_qty) FILTER(WHERE side='Buy'),0),
              coalesce(sum(base_qty) FILTER(WHERE side='Sell'),0),
              sum(trade_count)
            FROM archive_source_price_bins
            WHERE symbol=%s AND instrument=%s AND archive_day=%s
              AND archive_sha256=%s
            GROUP BY bucket,price""",
            (settings.symbol, tf, day, settings.symbol,
             settings.category, day, digest))
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
            predecessor = conn.execute("""SELECT ts,cvd FROM features
                WHERE symbol=%s AND tf=%s AND ts<%s
                ORDER BY ts DESC LIMIT 1""",
                (settings.symbol, tf, affected[0])).fetchone()
            conn.execute("""UPDATE analytics_cursor SET
                cursor_ts=%s,state='dirty',quality='recompute_required',
                updated_at=now()
                WHERE processor='cvd' AND symbol=%s AND tf=%s
                  AND cursor_ts>=%s""",
                (predecessor["ts"] if predecessor and predecessor["cvd"] is not None else None,
                 settings.symbol, tf, affected[0]))
            # Keep no stale CVD visible while an archive correction is
            # being recomputed; a dirty checkpoint can take several cycles.
            conn.execute("""UPDATE features SET cvd=NULL
                WHERE symbol=%s AND tf=%s AND ts>=%s
                  AND cvd IS NOT NULL""",
                (settings.symbol, tf, affected[0]))
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


def aggregate_pending(settings, max_days=4):
    """Repair and re-audit exact SHA archive price bins, one bounded slice.

    Earlier aggregation jobs could have read all global trades. Rotate source
    days by their last completed/audited clock, including formerly completed
    ones, without pretending an unavailable SHA membership is verified.
    """
    if max_days < 1:
        raise ValueError("max_days must be positive")
    with connection(settings) as conn:
        missing = conn.execute("""SELECT count(*) AS n
            FROM ingestion_job i WHERE i.source='trades_archive'
              AND i.symbol=%s AND i.state='complete'
              AND NOT EXISTS (SELECT 1 FROM archive_stage_receipts r
                WHERE r.symbol=i.symbol AND r.instrument=%s
                  AND r.archive_day=i.day AND r.archive_sha256=i.sha256)""",
            (settings.symbol, settings.category)).fetchone()["n"]
        days = conn.execute("""SELECT i.day,i.sha256 FROM ingestion_job i
            WHERE i.source='trades_archive' AND i.symbol=%s
              AND i.state='complete'
              AND EXISTS (SELECT 1 FROM archive_stage_receipts r
                WHERE r.symbol=i.symbol AND r.instrument=%s
                  AND r.archive_day=i.day AND r.archive_sha256=i.sha256)
            ORDER BY (
                SELECT count(*) FROM aggregation_job a
                WHERE a.symbol=i.symbol AND a.day=i.day
                  AND a.archive_sha256=i.sha256),
              (SELECT coalesce(min(a.completed_at),
                       'epoch'::timestamptz)
                FROM aggregation_job a
                WHERE a.symbol=i.symbol AND a.day=i.day
                  AND a.archive_sha256=i.sha256),
              i.day LIMIT %s""",
            (settings.symbol, settings.category, max_days)).fetchall()
    if missing:
        print(f"archive_membership_missing_days={missing}; "
              "replay original SHA through A/B stage before rollup",
              flush=True)
    count = 0
    for row in days:
        if not row["sha256"]:
            raise RuntimeError(f"Archive complete without checksum: {row['day']}")
        for tf in TIMEFRAMES:
            rollup_day(settings, row["day"], tf, row["sha256"])
            count += 1
    return count
