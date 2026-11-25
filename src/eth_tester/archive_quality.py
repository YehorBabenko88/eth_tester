"""Auditable archive-vs-exchange 1-minute volume evidence on Storage B migration 0002.

Matching aggregate volume is NOT a proof of every exchange trade identity.
The observed_at receipt is the actual PostgreSQL clock, never a backdated bar time.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import re

from .data_coverage import digest_rows
from .data_quality import DataIntegrityError

UTC = timezone.utc
ZERO = Decimal("0")
BASE_EPS = Decimal("0.00000001")
QUOTE_EPS = Decimal("0.000001")


def _number(value):
    number = Decimal(str(value))
    if not number.is_finite() or number < 0:
        raise DataIntegrityError("Non-finite or negative reconciliation quantity")
    return number


def build_minute_evidence(day, candle_rows, trade_rows):
    """Exactly 1440 UTC minute comparisons, linear ETHUSDT base ETH / quote USDT."""
    midnight = datetime.combine(day, datetime.min.time(), UTC)
    official = {row["ts"]: row for row in candle_rows}
    tape = {row["ts"]: row for row in trade_rows}
    expected = {midnight + timedelta(minutes=i) for i in range(1440)}
    if (len(official) != len(candle_rows) or len(tape) != len(trade_rows)
            or set(official) - expected or set(tape) - expected):
        raise DataIntegrityError("Repeated or off-grid UTC minute in volume evidence")
    results = []
    for i in range(1440):
        minute = midnight + timedelta(minutes=i)
        bar, ticks = official.get(minute), tape.get(minute)
        count = int(ticks["trade_count"]) if ticks else 0
        if count < 0:
            raise DataIntegrityError("Negative minute trade count")
        base = _number(ticks["base_qty"]) if ticks and ticks["base_qty"] is not None else ZERO
        quote = _number(ticks["quote_qty"]) if ticks and ticks["quote_qty"] is not None else ZERO
        bar_base = _number(bar["volume"]) if bar and bar["volume"] is not None else None
        bar_quote = _number(bar["turnover"]) if bar and bar["turnover"] is not None else None
        base_diff = base - bar_base if bar_base is not None else None
        quote_diff = quote - bar_quote if bar_quote is not None else None
        if bar is None:
            status = "incomplete"
        elif bar_base is None or bar_quote is None:
            status = "not_comparable"
        else:
            base_ok = abs(base_diff) <= max(BASE_EPS, abs(bar_base) * Decimal("1e-10"))
            quote_ok = abs(quote_diff) <= max(QUOTE_EPS, abs(bar_quote) * Decimal("1e-10"))
            status = "match" if base_ok and quote_ok else "mismatch"
        results.append({
            "minute_ts": minute, "trade_count": count, "trade_base_qty": base,
            "trade_quote_notional": quote, "candle_base_volume": bar_base,
            "candle_quote_turnover": bar_quote, "base_difference": base_diff,
            "quote_difference": quote_diff, "status": status,
        })
    return results


def summarize_minutes(rows):
    counts = {state: sum(row["status"] == state for row in rows)
              for state in ("match", "mismatch", "incomplete", "not_comparable")}
    if counts["incomplete"]:
        status = "incomplete_candles"
    elif counts["mismatch"]:
        status = "volume_mismatch"
    elif counts["not_comparable"]:
        status = "not_comparable"
    elif counts["match"] == 1440:
        status = "verified"  # aggregate volume, NEVER individual print completeness
    else:
        status = "unknown"
    return {"status": status, "minutes": 1440, **counts}


def persist_minute_evidence(conn, symbol, day, archive_sha256, instrument="linear"):
    if instrument != "linear" or symbol != "ETHUSDT":
        raise ValueError("Only linear ETHUSDT base/quote reconciliation is certified")
    if not isinstance(archive_sha256, str) or not re.fullmatch("[0-9a-f]{64}", archive_sha256):
        raise ValueError("Canonical 64-character source archive SHA256 required")
    start = datetime.combine(day, datetime.min.time(), UTC)
    end = start + timedelta(days=1)
    candles = conn.execute(
        """SELECT ts,open,high,low,close,volume,turnover,source FROM candles
           WHERE symbol=%s AND kind='trade' AND tf='1' AND closed=true
             AND ts >= %s AND ts < %s ORDER BY ts""",
        (symbol, start, end)).fetchall()
    trades = conn.execute(
        """SELECT date_trunc('minute',ts) AS ts, count(*) AS trade_count,
                  sum(qty) AS base_qty, sum(notional) AS quote_qty
           FROM trades WHERE symbol=%s AND source='archive'
             AND ts >= %s AND ts < %s GROUP BY 1 ORDER BY 1""",
        (symbol, start, end)).fetchall()
    comparisons = build_minute_evidence(day, candles, trades)
    receipt = conn.execute("SELECT clock_timestamp() AS observed_at").fetchone()["observed_at"]
    if receipt.tzinfo is None:
        raise DataIntegrityError("Non-UTC PostgreSQL observation clock")
    observed_at = receipt.astimezone(UTC).isoformat()
    report = summarize_minutes(comparisons)
    report["certificate"] = {
        "source": "bybit_daily_archive", "archive_sha256": archive_sha256,
        "candle_source_sha256": digest_rows(candles),
        "archive_minute_aggregate_sha256": digest_rows(trades),
        "observed_at": observed_at, "not_available_before": observed_at,
        "expected_closed_minutes": 1440,
        "observed_closed_minutes": len(candles),
        "matched_minutes": report["match"], "status": report["status"],
        "source_units": {"base": "ETH", "quote": "USDT", "instrument": instrument},
        "expected_base_volume": str(sum((row["candle_base_volume"] or ZERO
                                          for row in comparisons), ZERO)),
        "actual_base_volume": str(sum((row["trade_base_qty"]
                                        for row in comparisons), ZERO)),
        "expected_quote_turnover": str(sum((row["candle_quote_turnover"] or ZERO
                                             for row in comparisons), ZERO)),
        "actual_quote_notional": str(sum((row["trade_quote_notional"]
                                           for row in comparisons), ZERO)),
        "proves_all_trade_ids": False, "allows_historical_backdating": False,
        "method": "archive_only_unique_id_vs_closed_exchange_1m",
        "base_epsilon": str(BASE_EPS), "quote_epsilon": str(QUOTE_EPS),
    }
    evidence = json.dumps(report["certificate"], sort_keys=True)
    statements = [
        (symbol, instrument, row["minute_ts"], "trades_archive", archive_sha256,
         row["trade_count"], row["trade_base_qty"], row["trade_quote_notional"],
         row["candle_base_volume"], row["candle_quote_turnover"],
         row["base_difference"], row["quote_difference"], row["status"],
         evidence, receipt)
        for row in comparisons
    ]
    with conn.cursor() as cursor:
        cursor.executemany(
            """INSERT INTO trade_candle_volume_checks
               (symbol,instrument,minute_ts,archive_source,archive_sha256,
                trade_count,trade_base_qty,trade_quote_notional,
                candle_base_volume,candle_quote_turnover,
                base_difference,quote_difference,status,evidence,checked_at)
               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
               ON CONFLICT(symbol,instrument,minute_ts,archive_source,archive_sha256)
               DO UPDATE SET
                 trade_count=EXCLUDED.trade_count,
                 trade_base_qty=EXCLUDED.trade_base_qty,
                 trade_quote_notional=EXCLUDED.trade_quote_notional,
                 candle_base_volume=EXCLUDED.candle_base_volume,
                 candle_quote_turnover=EXCLUDED.candle_quote_turnover,
                 base_difference=EXCLUDED.base_difference,
                 quote_difference=EXCLUDED.quote_difference,
                 status=EXCLUDED.status,evidence=EXCLUDED.evidence,
                 checked_at=EXCLUDED.checked_at""",
            statements)
    return report
