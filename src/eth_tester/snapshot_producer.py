"""Fail-closed immutable, *publication-time* feature snapshots.

Storage B owns the append-only feature_snapshots schema. Data A owns the
coverage and minute-volume evidence. This module does not assert that public
archive volume equality proves the exchange published every individual trade.
Nothing is backdated: every feature becomes available no earlier than the
actual PostgreSQL materialization clock, even when its bar is historical.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re

from .analytics_core import FRAMES, bucket, close_time, value_profile
from .book_analysis import snapshot_metrics
from .database import connection
from .ml import FEATURE_NAMES


UTC = timezone.utc
HEX64 = re.compile(r"^[0-9a-f]{64}$")
COVERAGE_PROOF_KEYS = ("gzip_crc_checked", "trade_identity_checked",
                       "volume_verified")
SEMANTICS = {
    "oi": "change_between_latest_two_consecutive_5min_OI_observations",
    "cvd": "verified_window_cumulative_signed_base_quantity",
    "delta": "bar_aggressor_buy_minus_sell_base_quantity",
    "poc": "bar_traded_volume_point_of_control_price",
    "footprint": "bar_total_base_quantity",
    "tape_speed": "bar_trade_count_per_second",
    "book_imbalance": "fresh_L2_bid_minus_ask_quantity_divided_by_total",
    "level_distance": "nearest_confirmed_active_level_price_minus_bar_close",
    "volatility": "rolling_unannualized_realized_log_return_volatility",
}


class CertificationRejected(ValueError):
    """A missing or contradictory proof must not create a causal snapshot."""


def _reject(reason):
    raise CertificationRejected(reason)


def _time(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        _reject("timestamp_not_timezone_aware")
    return value.astimezone(UTC)


def _decimal(value, name):
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            _reject("nonfinite_" + name)
        return number
    except (InvalidOperation, TypeError, ValueError):
        _reject("missing_or_invalid_" + name)


def _hash(data):
    return sha256(json.dumps(data, sort_keys=True, separators=(",", ":"),
                             default=str).encode("utf-8")).hexdigest()


def _minutes(first, end):
    delta = (_time(end) - _time(first)).total_seconds()
    if delta <= 0 or delta % 60:
        _reject("non_minute_aligned_interval")
    return int(delta / 60)


def coverage_certificate(start, end, sampled_at, coverage, jobs, comparisons):
    """Check whole UTC-day archive certificates and every overlapping minute.

    Mapping keys: jobs[UTC date], comparisons[(UTC date, archive_sha256)].
    Each comparison is a database aggregate with count, matched, unique_minute
    and latest_checked. An archive checksum alone is never a volume certificate.
    """
    start, end, sampled_at = _time(start), _time(end), _time(sampled_at)
    if start >= end or end > sampled_at:
        _reject("unclosed_or_future_source_window")
    ordered = sorted(coverage, key=lambda c: c["start_ts"])
    day = start.replace(hour=0, minute=0, second=0, microsecond=0)
    manifest = []
    while day < end:
        day_end = day + timedelta(days=1)
        current = [row for row in ordered if
                   _time(row["start_ts"]) <= day and _time(row["end_ts"]) >= day_end]
        if len(current) != 1 or day_end > sampled_at:
            _reject("unverified_or_overlapping_utc_day")
        item = current[0]
        if _time(item["start_ts"]) != day or _time(item["end_ts"]) != day_end:
            _reject("non_daily_archive_certificate")
        digest = item.get("checksum")
        evidence = item.get("evidence") or {}
        if isinstance(evidence, str):
            evidence = json.loads(evidence)
        if not isinstance(evidence, dict):
            _reject("invalid_archive_provenance")
        if (item.get("status") != "verified" or
                not isinstance(digest, str) or not HEX64.fullmatch(digest) or
                not all(evidence.get(key) is True for key in COVERAGE_PROOF_KEYS) or
                evidence.get("download_sha256") != digest):
            _reject("archive_coverage_not_independently_verified")
        observed_rows = item.get("observed_rows")
        expected_rows = item.get("expected_rows")
        if (not isinstance(observed_rows, int) or observed_rows < 1 or
                (expected_rows is not None and expected_rows != observed_rows)):
            _reject("invalid_archive_trade_count")
        if _time(item["checked_at"]) > sampled_at:
            _reject("future_archive_evidence")
        job = jobs.get(day.date())
        if (not job or job.get("state") != "complete" or
                job.get("sha256") != digest):
            _reject("archive_job_checksum_drift")
        section_start, section_end = max(start, day), min(end, day_end)
        required = _minutes(section_start, section_end)
        summary = comparisons.get((day.date(), digest))
        if (not summary or summary["total"] != required or
                summary["matched"] != required or
                summary["unique_minutes"] != required or
                summary.get("latest_checked") is None or
                _time(summary["latest_checked"]) > sampled_at):
            _reject("minute_volume_grid_not_verified")
        manifest.append({
            "day": day.date().isoformat(),
            "archive_sha256": digest,
            "coverage_checked_at": _time(item["checked_at"]).isoformat(),
            "minute_checks": required,
            "minute_checked_at": _time(summary["latest_checked"]).isoformat(),
            "trade_rows": observed_rows,
        })
        day = day_end
    return _hash(manifest), manifest


def _profile_values(feature, price_rows, start, end):
    if not price_rows:
        _reject("missing_price_footprint")
    histogram = {}
    buy = sell = Decimal(0)
    prints = 0
    for row in price_rows:
        price = _decimal(row["price"], "footprint_price")
        b = _decimal(row["buy_qty"], "footprint_buy")
        s = _decimal(row["sell_qty"], "footprint_sell")
        if price <= 0 or b < 0 or s < 0:
            _reject("invalid_price_footprint")
        histogram[price] = histogram.get(price, Decimal(0)) + b + s
        buy += b
        sell += s
        prints += int(row["trades"])
    profile = value_profile(histogram)
    if (profile["poc"] != _decimal(feature.get("poc"), "poc") or
            buy + sell != _decimal(feature.get("volume"), "volume") or
            buy - sell != _decimal(feature.get("delta"), "delta") or
            prints < 1):
        _reject("features_disagree_with_footprint")
    expected_speed = Decimal(prints) / Decimal(str((end - start).total_seconds()))
    measured_speed = _decimal(feature.get("tape_tps"), "tape_speed")
    if abs(expected_speed - measured_speed) > Decimal("0.000000000001"):
        _reject("trade_speed_disagrees_with_footprint")
    return buy + sell, buy - sell, measured_speed


def certified_values(feature, price_rows, level, book, oi_rows, source_sha256,
                     start, end, sampled_at):
    """Build all nine ML fields only from independently checked sources."""
    if feature.get("quality") in ("volume_mismatch", "gap", "unavailable"):
        _reject("feature_quality_conflict")
    meta = feature.get("metadata") or {}
    if isinstance(meta, str):
        meta = json.loads(meta)
    if not isinstance(meta, dict) or meta.get("asof_close") != end.isoformat():
        _reject("indicator_close_provenance_missing")
    # A mutable, recomputed historical CVD has no proven cumulative origin.
    if (meta.get("cvd_scope") != "verified_window" or
            meta.get("cvd_completeness") != "verified" or
            meta.get("cvd_source_sha256") != source_sha256):
        _reject("cvd_lineage_not_certified")
    volume, delta, speed = _profile_values(feature, price_rows, start, end)
    if not level or level.get("source") != "causal_closed_pivot" or (
            _time(level["confirmed_at"]) > sampled_at):
        _reject("confirmed_level_not_available")
    if not book or not book.get("valid"):
        _reject("book_unverified")
    if _time(book["updated_at"]) < end:
        _reject("book_precedes_closed_bar")
    metrics = snapshot_metrics(book["bids"], book["asks"],
                               observed_at=book["updated_at"], as_of=sampled_at)
    if not metrics["valid"]:
        _reject("book_" + metrics["reason"])
    if len(oi_rows) != 2:
        _reject("insufficient_oi_history")
    latest, earlier = oi_rows
    latest_ts, earlier_ts = _time(latest["ts"]), _time(earlier["ts"])
    if (latest_ts <= earlier_ts or latest_ts - earlier_ts != timedelta(minutes=5) or
            latest_ts > sampled_at or
            sampled_at - latest_ts > timedelta(minutes=10)):
        _reject("oi_gap_or_stale_source")
    market_close = _decimal(feature.get("market_close"), "market_close")
    values = {
        "oi": _decimal(latest["value"], "oi") - _decimal(earlier["value"], "oi_prior"),
        "cvd": _decimal(feature.get("cvd"), "cvd"),
        "delta": delta,
        "poc": _decimal(feature["poc"], "poc"),
        "footprint": volume,
        "tape_speed": speed,
        "book_imbalance": metrics["imbalance"],
        "level_distance": _decimal(level["level_price"], "level_price") - market_close,
        "volatility": _decimal(meta.get("realized_volatility"), "volatility"),
    }
    if values["volatility"] < 0 or values["footprint"] <= 0:
        _reject("invalid_volatility_or_footprint")
    if set(values) != set(FEATURE_NAMES):
        _reject("ml_feature_contract_mismatch")
    return {name: str(values[name]) for name in FEATURE_NAMES}, {
        "level_id": level["id"], "level_confirmed_at": _time(
            level["confirmed_at"]).isoformat(),
        "oi_exchange_asof": latest_ts.isoformat(),
        "orderbook_received_at": _time(book["updated_at"]).isoformat(),
        "feature_semantics": SEMANTICS,
        "archive_volume_interpretation": (
            "verified_against_pinned_public_archive_not_all_exchange_trade_ids"),
    }


def certify_snapshot(settings, tf="1"):
    """Publish one snapshot at its ACTUAL observation/computation time, or reject.

    Never retrodict an old paper decision. Only B's additive tables are written.
    Source certificates are pinned to archive checksums and all minute volumes.
    """
    if tf not in FRAMES:
        raise ValueError("unsupported_frame")
    try:
        with connection(settings) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                         (f"snapshot:{settings.symbol}:{tf}",))
            clock = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
            candidate = conn.execute("""SELECT f.*,c.close AS market_close
                FROM features f JOIN candles c ON c.symbol=f.symbol
                   AND c.tf=f.tf AND c.ts=f.ts AND c.kind='trade'
                WHERE f.symbol=%s AND f.tf=%s AND c.closed
                ORDER BY f.ts DESC LIMIT 5""",
                (settings.symbol, tf)).fetchall()
            feature = next((row for row in candidate if
                            close_time(row["ts"], tf) <= clock), None)
            if not feature:
                _reject("no_fully_closed_candle_and_feature")
            start = feature["ts"]
            if bucket(start, tf) != start:
                _reject("unaligned_source_candle")
            end = close_time(start, tf)
            # The source archive may be historical, but publication timestamps
            # are captured from the DB now and can only inform FUTURE decisions.
            rows = conn.execute("""SELECT * FROM ingestion_coverage
                WHERE source='trades_archive' AND symbol=%s AND instrument=%s
                  AND interval='' AND start_ts<%s AND end_ts>%s
                ORDER BY start_ts""",
                (settings.symbol, settings.category, end, start)).fetchall()
            first_day = start.date()
            # For intraday buckets the end is still within the same UTC
            # calendar day. Include that day; for D/W/M end is exclusive.
            last_day_exclusive = (end - timedelta(microseconds=1)).date() + timedelta(days=1)
            jobs = conn.execute("""SELECT day,state,sha256 FROM ingestion_job
                WHERE source='trades_archive' AND symbol=%s
                  AND day >= %s AND day < %s""",
                (settings.symbol, first_day, last_day_exclusive)).fetchall()
            jobs_by_day = {item["day"]: item for item in jobs}
            checks = {}
            for row in rows:
                digest = row.get("checksum")
                if not isinstance(digest, str) or not HEX64.fullmatch(digest):
                    continue
                a = max(start, row["start_ts"])
                b = min(end, row["end_ts"])
                if a >= b:
                    continue
                result = conn.execute("""SELECT count(*) AS total,
                    count(*) FILTER (WHERE status='match') AS matched,
                    count(DISTINCT minute_ts) AS unique_minutes,
                    max(checked_at) AS latest_checked
                    FROM trade_candle_volume_checks WHERE symbol=%s
                      AND instrument=%s AND minute_ts >= %s
                      AND minute_ts < %s AND archive_source='trades_archive'
                      AND archive_sha256=%s""",
                    (settings.symbol, settings.category, a, b, digest)).fetchone()
                checks[(row["start_ts"].date(), digest)] = result
            # Actual publication time is captured only AFTER reading source
            # data. Never use a bar close as an artificial observed_at.
            sampled = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
            source_sha, manifest = coverage_certificate(
                start, end, sampled, rows, jobs_by_day, checks)
            ledger = conn.execute("""SELECT day,archive_sha256 FROM aggregation_job
                WHERE symbol=%s AND tf=%s AND day >= %s AND day < %s""",
                (settings.symbol, tf, first_day, last_day_exclusive)).fetchall()
            archived = {row["day"]: row["archive_sha256"] for row in ledger}
            if len(archived) != len(manifest) or any(
                    archived.get(datetime.fromisoformat(item["day"]).date()) !=
                    item["archive_sha256"] for item in manifest):
                _reject("rollup_archive_checksum_drift")
            price_rows = conn.execute("""SELECT price,buy_qty,sell_qty,trades
                FROM footprint WHERE symbol=%s AND tf=%s AND bucket=%s
                ORDER BY price LIMIT 20001""",
                (settings.symbol, tf, start)).fetchall()
            if len(price_rows) > 20000:
                _reject("unbounded_footprint")
            book = conn.execute("""SELECT bids,asks,updated_at,valid
                FROM market_book_state WHERE symbol=%s""",
                (settings.symbol,)).fetchone()
            oi = conn.execute("""SELECT ts,value FROM open_interest
                WHERE symbol=%s AND interval='5min' AND ts<=%s
                ORDER BY ts DESC LIMIT 2""",
                (settings.symbol, sampled)).fetchall()
            level = conn.execute("""SELECT id,level_price,confirmed_at,source
                FROM levels WHERE symbol=%s AND tf=%s AND status='unbroken'
                  AND confirmed_at <= %s AND source='causal_closed_pivot'
                ORDER BY abs(level_price-%s),confirmed_at,id LIMIT 1""",
                (settings.symbol, tf, sampled, feature["market_close"])).fetchone()
            values, extra = certified_values(
                feature, price_rows, level, book, oi, source_sha,
                start, end, sampled)
            exists = conn.execute("""SELECT snapshot_key FROM feature_snapshots
                WHERE symbol=%s AND tf=%s AND bar_ts=%s AND
                  source_checksum=%s AND quality='complete' AND is_causal
                ORDER BY computed_at DESC LIMIT 1""",
                (settings.symbol, tf, start, source_sha)).fetchone()
            if exists:
                return {"status": "already_published", "tf": tf,
                        "snapshot_key": exists["snapshot_key"]}
            epoch = int(sampled.timestamp() * 1000)
            provenance = {
                "feature_available_ms": {name: epoch for name in FEATURE_NAMES},
                "bar_close_exchange_time": end.isoformat(),
                "source_manifest": manifest,
                "source_checksum": source_sha,
                "published_at": sampled.isoformat(),
                "availability_convention": "DB_materialization_time_not_bar_close",
                **extra,
            }
            identity = _hash({"symbol": settings.symbol, "tf": tf,
                              "bar_ts": start.isoformat(), "source": source_sha,
                              "values": values, "level_id": extra["level_id"]})
            inserted = conn.execute("""INSERT INTO feature_snapshots(
                snapshot_key,symbol,tf,bar_ts,available_at,observed_at,
                computed_at,source,source_checksum,quality,is_causal,
                values_json,provenance)
                VALUES(%s,%s,%s,%s,%s,%s,%s,'certified_archive_materialization',
                       %s,'complete',true,%s::jsonb,%s::jsonb)
                ON CONFLICT(snapshot_key) DO NOTHING RETURNING snapshot_key""",
                (identity, settings.symbol, tf, start, sampled, sampled, sampled,
                 source_sha, json.dumps(values), json.dumps(provenance))).fetchone()
            if not inserted:
                return {"status": "already_published", "tf": tf,
                        "snapshot_key": identity}
            return {"status": "published", "tf": tf,
                    "snapshot_key": identity, "source_checksum": source_sha}
    except CertificationRejected as exc:
        return {"status": "rejected", "tf": tf, "reason": str(exc)}
