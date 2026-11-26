from datetime import datetime,timedelta,timezone
import json
from decimal import Decimal
from .database import connection

TF_SECONDS={"1":60,"3":180,"5":300,"15":900,"30":1800,
 "60":3600,"120":7200,"240":14400,"360":21600,"720":43200,"D":86400}
# A single live recomputation must never materialize an unbounded number of
# distinct bucket/price/side groups in the Python worker.
MAX_LIVE_FOOTPRINT_GROUPS = 8192
# Fail before mutating any feature if the bounded live window is over budget.
MAX_ENRICH_FEATURE_ROWS = 1536

def floor_time(ts,seconds):
    return datetime.fromtimestamp(int(ts.timestamp())//seconds*seconds,timezone.utc)

def poc(price_volume):
    if not price_volume:
        return None
    return min(price_volume,key=lambda price:(-price_volume[price],price))

def cluster(trades):
    bins={}
    buys=sells=Decimal(0)
    count=0
    for row in trades:
        price,qty,side=Decimal(row["price"]),Decimal(row["qty"]),row["side"]
        bins.setdefault(price,{"buy":Decimal(0),"sell":Decimal(0),"n":0})[side.lower()]+=qty
        bins[price]["n"]+=1
        count+=1
        if side=="Buy":
            buys+=qty
        else:
            sells+=qty
    return {"poc":poc({p:v["buy"]+v["sell"] for p,v in bins.items()}),
      "delta":buys-sells,"volume":buys+sells,"tps":count,"levels":bins}

def confirmed_pivots(candles,flank=2):
    levels=[]
    for i in range(flank,len(candles)-flank):
        bar=candles[i]
        others=candles[i-flank:i]+candles[i+1:i+flank+1]
        if all(bar["high"]>x["high"] for x in others):
            levels.append(("resistance",bar["ts"],bar["high"],candles[i+flank]["ts"]))
        if all(bar["low"]<x["low"] for x in others):
            levels.append(("support",bar["ts"],bar["low"],candles[i+flank]["ts"]))
    return levels

def classify_levels(candles,flank=2):
    result=[]
    for side,origin,price,confirmed in confirmed_pivots(candles,flank):
        broken=next((row["ts"] for row in candles if row["ts"]>confirmed and
         (row["close"]>price if side=="resistance" else row["close"]<price)),None)
        result.append((side,origin,price,confirmed,broken))
    return result

def build_levels(settings, tf="60", flank=2):
    """Checkpointed causal level lifecycle, persisted through Storage B migration."""
    from .levels_incremental import process_levels

    return process_levels(settings, tf, flank)



def build_footprint(settings, tf="1", hours=24):
    """Bounded live recompute; archive-owned days remain immutable to live jobs.

    Raw prints are deduplicated in trades. This routine explicitly labels
    partially observed buckets and never claims exchange completeness.
    Weekly/monthly profiles are accumulated by authoritative archive rollups.
    """
    if tf not in TF_SECONDS:
        raise ValueError("Live footprint supports the 11 fixed intervals; W/M use archive rollups")
    if type(hours) is not int or not 1 <= hours <= 24:
        raise ValueError("Live footprint hours must be an integer in 1..24")
    span = TF_SECONDS[tf]
    with connection(settings) as conn:
        # Bound even the grouped SQL read. A timeout/overflow rolls back the
        # whole transaction, never commits a truncated footprint or feature.
        conn.execute("SET LOCAL lock_timeout = '2000ms'")
        conn.execute("SET LOCAL statement_timeout = '10000ms'")
        # Database clock, not a host clock (Windows time jumps must not
        # manufacture future closed buckets).
        now = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                     (f"{settings.symbol}:{tf}:cluster",))
        start = floor_time(now - timedelta(hours=hours), span)
        # Aggregate NUMERIC/COUNT on the server, never fetch millions of raw
        # prints into Python; exact archive-owned dates are excluded by job
        # membership rather than by the mutable source='archive' tag.
        rows = conn.execute("""SELECT
            date_bin((%s || ' seconds')::interval,t.ts,
                     '1970-01-01 00:00+00'::timestamptz) AS bucket,
            t.price,t.side,sum(t.qty) AS qty,count(*) AS prints
            FROM trades t
            WHERE t.symbol=%s AND t.ts >= %s AND t.ts < %s
              AND NOT EXISTS (SELECT 1 FROM aggregation_job a
                WHERE a.symbol=t.symbol AND a.tf=%s
                  AND a.day=(t.ts AT TIME ZONE 'UTC')::date)
            GROUP BY 1,2,3 ORDER BY 1,2,3 LIMIT %s""",
            (span, settings.symbol, start, now, tf,
             MAX_LIVE_FOOTPRINT_GROUPS + 1)).fetchall()
        if len(rows) > MAX_LIVE_FOOTPRINT_GROUPS:
            raise RuntimeError("live_footprint_group_budget_exceeded")
        buckets = {}
        for row in rows:
            values = buckets.setdefault(row["bucket"], {}).setdefault(
                row["price"], {"buy": Decimal(0),
                               "sell": Decimal(0), "trades": 0})
            values[row["side"].lower()] += row["qty"]
            values["trades"] += row["prints"]
        completed = 0
        for stamp, bins in sorted(buckets.items()):
            # Archive has priority, including a corrected archive with zero rows.
            archive = conn.execute("""SELECT 1 FROM aggregation_job
                WHERE symbol=%s AND tf=%s AND day=%s""",
                (settings.symbol, tf, stamp.date())).fetchone()
            if archive:
                continue
            # Full bucket replacement removes stale price bins on corrections.
            conn.execute("""DELETE FROM footprint
                WHERE symbol=%s AND tf=%s AND bucket=%s""",
                (settings.symbol, tf, stamp))
            for price, values in bins.items():
                conn.execute("""INSERT INTO footprint(symbol,tf,bucket,price,buy_qty,sell_qty,
                    buy_notional,sell_notional,trades)
                  VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                  (settings.symbol, tf, stamp, price, values["buy"], values["sell"],
                   values["buy"] * price, values["sell"] * price, values["trades"]))
            buy = sum((v["buy"] for v in bins.values()), Decimal(0))
            sell = sum((v["sell"] for v in bins.values()), Decimal(0))
            peak = poc({p: v["buy"] + v["sell"] for p, v in bins.items()})
            meta = json.dumps({"source": "live_trade_window",
                               "observed_at": now.isoformat(),
                               "bucket_complete": now >= stamp + timedelta(seconds=span),
                               "coverage": "partial_unverified"})
            conn.execute("""INSERT INTO features(symbol,tf,ts,poc,delta,volume,tape_tps,
                quality,metadata)
              VALUES(%s,%s,%s,%s,%s,%s,%s,'partial_unverified',%s::jsonb)
              ON CONFLICT(symbol,tf,ts) DO UPDATE SET poc=EXCLUDED.poc,
                delta=EXCLUDED.delta,volume=EXCLUDED.volume,
                tape_tps=EXCLUDED.tape_tps,quality=EXCLUDED.quality,
                metadata=features.metadata || EXCLUDED.metadata""",
              (settings.symbol, tf, stamp, peak, buy - sell, buy + sell,
               Decimal(sum(v["trades"] for v in bins.values())) /
               Decimal(span), meta))
            completed += 1
    return completed


def enrich_features(settings, tf="1", hours=24):
    """Update bounded features without resetting cumulative delta at window start.

    Missing historical CVD provenance stays NULL rather than being presented
    as a full-history value. Exchange-time as-of joins are not receipt-time
    provenance and never generate ML-ready causal snapshots by themselves.
    """
    if tf not in TF_SECONDS:
        raise ValueError("Unsupported live interval")
    if type(hours) is not int or not 1 <= hours <= 24:
        raise ValueError("Live enrichment hours must be an integer in 1..24")
    from .book_analysis import snapshot_metrics

    with connection(settings) as conn:
        # PostgreSQL 17+ transaction_timeout bounds the entire multi-statement
        # transaction. Statement and lock timeouts alone do not bound it.
        # Timeout, over-budget rows or a failed UPDATE abort ALL feature writes.
        conn.execute("SET LOCAL transaction_timeout = '30000ms'")
        conn.execute("SET LOCAL statement_timeout = '10000ms'")
        conn.execute("SET LOCAL lock_timeout = '2000ms'")
        # Fix the interval at a database clock instant and EXCLUDE future rows.
        db_now = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        rows = conn.execute("""SELECT ts,delta,volume,quality FROM features
            WHERE symbol=%s AND tf=%s AND ts >= %s AND ts < %s
            ORDER BY ts LIMIT %s""",
            (settings.symbol, tf, db_now - timedelta(hours=hours), db_now,
             MAX_ENRICH_FEATURE_ROWS + 1)).fetchall()
        if len(rows) > MAX_ENRICH_FEATURE_ROWS:
            raise RuntimeError("live_enrichment_row_budget_exceeded")
        if not rows:
            return 0
        seed = conn.execute("""SELECT ts,cvd FROM features WHERE symbol=%s AND tf=%s
            AND ts<%s AND cvd IS NOT NULL ORDER BY ts DESC LIMIT 1""",
            (settings.symbol, tf, rows[0]["ts"])).fetchone()
        if seed:
            between = conn.execute("""SELECT coalesce(sum(delta),0) AS total
                FROM features WHERE symbol=%s AND tf=%s AND ts>%s AND ts<%s""",
                (settings.symbol, tf, seed["ts"], rows[0]["ts"])).fetchone()
            cvd = Decimal(seed["cvd"]) + Decimal(between["total"])
            baseline = "previous_persisted_cvd"
        else:
            earlier = conn.execute("""SELECT 1 FROM features
                WHERE symbol=%s AND tf=%s AND ts<%s LIMIT 1""",
                (settings.symbol, tf, rows[0]["ts"])).fetchone()
            cvd = None if earlier else Decimal(0)
            baseline = "missing_pre_window_history" if earlier else "first_observed_bar"
        for item in rows:
            ts = item["ts"]
            if cvd is not None:
                cvd += item["delta"] or Decimal(0)
            as_of = ts + timedelta(seconds=TF_SECONDS[tf])
            candle = conn.execute("""SELECT volume FROM candles WHERE symbol=%s
                AND kind='trade' AND tf=%s AND ts=%s AND closed""",
                (settings.symbol, tf, ts)).fetchone()
            oi = conn.execute("""SELECT ts,value FROM open_interest WHERE symbol=%s
                AND interval='5min' AND ts<=%s ORDER BY ts DESC LIMIT 1""",
                (settings.symbol, as_of)).fetchone()
            oi_value = (oi["value"] if oi and timedelta(0) <= as_of - oi["ts"]
                        <= timedelta(minutes=10) else None)
            funding = conn.execute("""SELECT rate FROM funding WHERE symbol=%s
                AND ts<=%s ORDER BY ts DESC LIMIT 1""",
                (settings.symbol, as_of)).fetchone()
            book = conn.execute("""SELECT bids,asks,updated_at,valid FROM market_book_state
                WHERE symbol=%s""", (settings.symbol,)).fetchone()
            imbalance = None
            if book and book["valid"]:
                metrics = snapshot_metrics(
                    book["bids"], book["asks"], observed_at=book["updated_at"],
                    as_of=as_of)
                imbalance = metrics.get("imbalance")
            # An archive job alone is NOT SHA membership, and equal volumes
            # cannot prove all exchange trade IDs were captured. Require the
            # exact B stage and C rollup SHA before describing *parity* only.
            archive = conn.execute("""SELECT i.state,i.sha256,
                EXISTS (SELECT 1 FROM archive_stage_receipts r
                  WHERE r.symbol=i.symbol AND r.instrument=%s
                    AND r.archive_day=i.day AND r.archive_sha256=i.sha256
                    AND r.exchange_complete=false) AS stage_sha_present,
                EXISTS (SELECT 1 FROM aggregation_job a
                  WHERE a.symbol=i.symbol AND a.tf=%s
                    AND a.day=i.day AND a.archive_sha256=i.sha256)
                    AS rollup_sha_present
                FROM ingestion_job i WHERE i.source='trades_archive'
                  AND i.symbol=%s AND i.day=%s""",
                (settings.category, tf, settings.symbol, ts.date())).fetchone()
            sha_witness = bool(archive and archive["state"] == "complete"
                               and archive["sha256"]
                               and archive["stage_sha_present"]
                               and archive["rollup_sha_present"])
            quality = "partial_unverified"
            volume_check = "not_comparable"
            if archive and archive["state"] == "complete" and not sha_witness:
                volume_check = "archive_sha_membership_missing"
            if (sha_witness and candle and candle["volume"] is not None
                    and item["volume"] is not None):
                observed, reference = (Decimal(item["volume"]),
                                       Decimal(candle["volume"]))
                if (observed.is_finite() and reference.is_finite()
                        and observed >= 0 and reference >= 0):
                    error = abs(observed - reference)
                    tolerance = max(Decimal("0.001"),
                                    reference * Decimal("0.0005"))
                    if error <= tolerance:
                        volume_check = "parity_only_not_trade_id_completeness"
                        quality = ("archive_rollup_partial"
                                   if item["quality"] == "archive_rollup_partial"
                                   else "volume_parity_only_unverified")
                    else:
                        volume_check = "mismatch"
                        quality = "volume_mismatch"
                else:
                    volume_check = "invalid_quantity"
                    quality = "volume_mismatch"
            meta = json.dumps({"cvd_baseline": baseline,
                               "cvd_scope": "observed_trades_only",
                               "orderbook": "fresh_replayed_L2_only",
                               "oi_availability": "exchange_timestamp_only",
                               "volume_check": volume_check,
                               "volume_sha_membership_present": sha_witness,
                               "volume_exchange_complete": False})
            conn.execute("""UPDATE features SET cvd=%s,oi=%s,funding_rate=%s,
                imbalance=%s,quality=%s,
                metadata=features.metadata || %s::jsonb
                WHERE symbol=%s AND tf=%s AND ts=%s""",
                (cvd, oi_value,
                 funding["rate"] if funding else None, imbalance, quality,
                 meta, settings.symbol, tf, ts))
    return len(rows)
