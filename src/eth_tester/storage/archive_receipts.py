"""Transactional SHA-pinned archive stage price bins and versioned comparison.

The global trades table is a UNION of sources; it is never an archive-SHA
membership proof. Bin capture MUST run while the original validated eth_stage
temporary COPY table is present, after Storage's global trade identity guard.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
import re

from psycopg import sql

_SHA = re.compile(r"[0-9a-f]{64}\Z")
_UTC = timezone.utc
_ONE = timedelta(days=1)


def _validate(symbol, day, digest, instrument):
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("Symbol required")
    if not isinstance(day, date) or isinstance(day, datetime):
        raise ValueError("UTC archive day required")
    if not isinstance(digest, str) or not _SHA.fullmatch(digest):
        raise ValueError("Archive checksum must be SHA256")
    if instrument not in ("linear", "inverse", "spot"):
        raise ValueError("Invalid archive instrument")


def _clockday(day):
    return datetime.combine(day, datetime.min.time(), _UTC)


def _stage_sql(stage_name):
    if stage_name != "eth_stage":
        raise ValueError("Archive bins require original eth_stage COPY")
    return sql.SQL("pg_temp.{}").format(sql.Identifier(stage_name))


def persist_archive_stage_bins(
    conn, symbol, day, archive_sha256, expected_rows, *,
    stage_name="eth_stage", instrument="linear",
):
    """Capture immutable minute/price/side bins from exact downloaded gzip COPY.

    Replays with the SAME SHA must compare every bin and scalar quantity,
    including a WebSocket-first overlapping trade. Caller owns transaction
    (ingestion_job cursor, global identity merge and archive minute receipt).
    No status='verified' or exchange-complete claim is ever emitted.
    """
    _validate(symbol, day, archive_sha256, instrument)
    if type(expected_rows) is not int or expected_rows <= 0:
        raise ValueError("Positive original archive row count required")
    stage = _stage_sql(stage_name)
    start = _clockday(day)
    end = start + _ONE
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                 (f"eth_archive_stage:{symbol}:{day}:{archive_sha256}",))
    stage_rows = conn.execute(
        sql.SQL(
            """SELECT count(*) AS original_rows,
                      count(DISTINCT trade_id) AS distinct_ids
               FROM {} WHERE symbol=%s AND ts >= %s AND ts < %s"""
        ).format(stage),
        (symbol, start, end),
    ).fetchone()
    if stage_rows["original_rows"] != expected_rows:
        raise ValueError("Stage count differs from gzip row count or UTC day")
    if stage_rows["distinct_ids"] <= 0:
        raise ValueError("Missing canonical archive trade identity")
    # Guard against a mixed-symbol/day stage even if the expected matching
    # count happens to agree by accident.
    total = conn.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(stage)).fetchone()
    if total["n"] != expected_rows:
        raise ValueError("Archive stage contains unexpected symbol/day")
    distinct_stage = sql.SQL(
        """SELECT DISTINCT ON (symbol,trade_id)
                  symbol,trade_id,ts,price,qty,side,notional
           FROM {} WHERE symbol=%s AND ts >= %s AND ts < %s
           ORDER BY symbol,trade_id,ts"""
    ).format(stage)
    agg = sql.SQL(
        """SELECT date_trunc('minute',ts) AS minute_ts,price,side,
                  count(*)::bigint AS trade_count,
                  sum(qty) AS base_qty,sum(notional) AS quote_notional
           FROM ({}) AS distinct_trades
           GROUP BY 1,2,3"""
    ).format(distinct_stage)
    args = (symbol, start, end)
    summary = conn.execute(
        sql.SQL("""SELECT count(*) AS bin_count,
                          coalesce(sum(trade_count),0) AS prints,
                          coalesce(sum(base_qty),0) AS base_qty,
                          coalesce(sum(quote_notional),0) AS quote_notional
                   FROM ({}) AS bins""").format(agg),
        args,
    ).fetchone()
    if summary["prints"] != stage_rows["distinct_ids"] or summary["bin_count"] <= 0:
        raise ValueError("Archive stage price bins lose trade identities")
    scope = (symbol, instrument, day, archive_sha256)
    conn.execute(
        """INSERT INTO archive_stage_receipts(
             symbol,instrument,archive_day,archive_sha256,
             original_rows,distinct_trade_ids,price_bins,base_qty,quote_notional)
           VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT DO NOTHING""",
        (*scope, expected_rows, summary["prints"], summary["bin_count"],
         summary["base_qty"], summary["quote_notional"]),
    )
    original = conn.execute(
        """SELECT original_rows,distinct_trade_ids,price_bins,
                  base_qty,quote_notional,exchange_complete
           FROM archive_stage_receipts
           WHERE symbol=%s AND instrument=%s AND archive_day=%s
             AND archive_sha256=%s FOR SHARE""",
        scope,
    ).fetchone()
    if original is None or any(original[k] != expected for k, expected in (
        ("original_rows", expected_rows),
        ("distinct_trade_ids", summary["prints"]),
        ("price_bins", summary["bin_count"]),
        ("base_qty", summary["base_qty"]),
        ("quote_notional", summary["quote_notional"]),
        ("exchange_complete", False),
    )):
        raise ValueError("Conflicting immutable archive stage receipt")

    conn.execute(
        sql.SQL(
            """INSERT INTO archive_source_price_bins(
                 symbol,instrument,archive_day,archive_sha256,
                 minute_ts,price,side,trade_count,base_qty,quote_notional)
               SELECT %s,%s,%s,%s,minute_ts,price,side,trade_count,
                      base_qty,quote_notional FROM ({}) AS bins
               ON CONFLICT DO NOTHING"""
        ).format(agg),
        (*scope, *args),
    )
    # Exact symmetric EXCEPT ALL proves every SHA bin was captured and
    # never silently overwritten under identical gzip SHA.
    query = sql.SQL(
        """WITH stage_bins AS ({}),
           stored AS (
             SELECT minute_ts,price,side,trade_count,base_qty,quote_notional
             FROM archive_source_price_bins
             WHERE symbol=%s AND instrument=%s AND archive_day=%s
               AND archive_sha256=%s),
           mismatch AS (
             (SELECT * FROM stage_bins EXCEPT ALL SELECT * FROM stored)
             UNION ALL
             (SELECT * FROM stored EXCEPT ALL SELECT * FROM stage_bins))
           SELECT 1 AS bad FROM mismatch LIMIT 1"""
    ).format(agg)
    # CTE stage uses original (symbol,start,end), stored uses scope.
    if conn.execute(query, (*args, *scope)).fetchone() is not None:
        raise ValueError("Immutable archive SHA bins conflict with stage replay")
    return {
        "archive_sha256": archive_sha256,
        "original_rows": expected_rows,
        "distinct_trade_ids": summary["prints"],
        "price_bins": summary["bin_count"],
        "base_qty": summary["base_qty"],
        "quote_notional": summary["quote_notional"],
        "quality": "downloaded_archive_membership_only",
        "exchange_complete": False,
    }


def revalidate_archive_bins(conn, symbol, day, archive_sha256, *,
                            instrument="linear"):
    """Version a **volume-only** day comparison using original SHA price bins.

    Works after late arrival of official closed one-minute candles without
    deleting/downloading original stage bins or rewriting their first receipt.
    Never resolves all-trade-ID uncertainty or promotes archived ML eligibility.
    """
    _validate(symbol, day, archive_sha256, instrument)
    scope = (symbol, instrument, day, archive_sha256)
    manifest = conn.execute(
        """SELECT original_rows,distinct_trade_ids,price_bins,captured_at
           FROM archive_stage_receipts
           WHERE symbol=%s AND instrument=%s AND archive_day=%s
             AND archive_sha256=%s FOR SHARE""",
        scope,
    ).fetchone()
    if manifest is None:
        raise ValueError("Missing immutable source SHA stage membership")
    start = _clockday(day)
    end = start + _ONE
    rows = conn.execute(
        """SELECT minute_ts,sum(base_qty) AS base_qty,
                  sum(quote_notional) AS quote_notional
           FROM archive_source_price_bins
           WHERE symbol=%s AND instrument=%s AND archive_day=%s
             AND archive_sha256=%s
           GROUP BY minute_ts ORDER BY minute_ts""", scope,
    ).fetchall()
    bars = conn.execute(
        """SELECT ts,volume,turnover,source
           FROM candles WHERE symbol=%s AND kind='trade' AND tf='1'
             AND closed=true AND ts >= %s AND ts < %s ORDER BY ts""",
        (symbol, start, end),
    ).fetchall()
    archive_map = {row["minute_ts"]: row for row in rows}
    bar_map = {row["ts"]: row for row in bars}
    if len(bar_map) != len(bars):
        raise ValueError("Duplicate official candle minute")
    matched = 0
    mismatch = 0
    comparable = 0
    base_eps = Decimal("0.00000001")
    quote_eps = Decimal("0.000001")
    for i in range(1440):
        minute = start + timedelta(minutes=i)
        bar = bar_map.get(minute)
        if not bar or bar["volume"] is None or bar["turnover"] is None:
            continue
        comparable += 1
        item = archive_map.get(minute)
        actual_base = item["base_qty"] if item else Decimal("0")
        actual_quote = item["quote_notional"] if item else Decimal("0")
        expected_base = Decimal(bar["volume"])
        expected_quote = Decimal(bar["turnover"])
        if (abs(actual_base - expected_base) <= max(base_eps, abs(expected_base)*Decimal("1e-10"))
                and abs(actual_quote - expected_quote) <=
                max(quote_eps, abs(expected_quote)*Decimal("1e-10"))):
            matched += 1
        else:
            mismatch += 1
    status = ("mismatch" if mismatch else
              "volume_only_reconciled" if comparable == 1440 and matched == 1440
              else "incomplete")
    candle_sha = sha256(json.dumps(
        [(r["ts"].isoformat(), str(r["volume"]), str(r["turnover"]),
          r["source"]) for r in bars],
        separators=(",", ":"), ensure_ascii=True,
    ).encode()).hexdigest()
    evidence = {
        "scope": "price_bins_of_downloaded_archive_vs_closed_1m_only",
        "minutes_total": 1440, "minutes_available": comparable,
        "matched": matched, "mismatched": mismatch,
        "source_stage_captured_at": manifest["captured_at"].isoformat(),
        "exchange_trade_id_completeness": False,
        "allows_backdating": False,
    }
    canonical = (symbol, instrument, day.isoformat(), archive_sha256,
                 candle_sha, status, comparable, matched)
    key = sha256(json.dumps(canonical, separators=(",", ":")).encode()).hexdigest()
    conn.execute(
        """INSERT INTO archive_validation_receipts(
             validation_key,symbol,instrument,archive_day,archive_sha256,
             candle_sha256,status,minutes_checked,minutes_matched,evidence)
           VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
           ON CONFLICT DO NOTHING""",
        (key, *scope, candle_sha, status, comparable, matched,
         json.dumps(evidence, sort_keys=True)),
    )
    stored = conn.execute(
        """SELECT status,minutes_checked,minutes_matched,candle_sha256
           FROM archive_validation_receipts WHERE validation_key=%s""",
        (key,),
    ).fetchone()
    if (stored is None or stored["status"] != status
            or stored["minutes_checked"] != comparable
            or stored["minutes_matched"] != matched
            or stored["candle_sha256"] != candle_sha):
        raise ValueError("Conflicting archive comparison identity")
    return {"validation_key": key, "status": status, "minutes_checked": comparable,
            "minutes_matched": matched, "exchange_complete": False}
