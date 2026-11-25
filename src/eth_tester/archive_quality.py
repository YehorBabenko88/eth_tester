"""Evidence for archive-vs-official minute volumes, using Storage B migration 0002."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json

UTC = timezone.utc
ZERO = Decimal("0")
BASE_EPS = Decimal("0.00000001")
QUOTE_EPS = Decimal("0.000001")


def build_minute_evidence(day, candle_rows, trade_rows):
    """Emit 1440 UTC minute comparisons for linear ETHUSDT (base ETH, quote USDT).

    Matching volumes is necessary, not sufficient, evidence for full print history.
    """
    midnight = datetime.combine(day, datetime.min.time(), UTC)
    official = {row["ts"]: row for row in candle_rows}
    tape = {row["ts"]: row for row in trade_rows}
    expected_minutes = {midnight + timedelta(minutes=i) for i in range(1440)}
    if set(official) - expected_minutes or set(tape) - expected_minutes:
        raise ValueError("UTC-day or minute-grid violation in archive reconciliation")
    comparisons = []
    for i in range(1440):
        minute = midnight + timedelta(minutes=i)
        bar = official.get(minute)
        ticks = tape.get(minute)
        count = int(ticks["trade_count"]) if ticks else 0
        base = Decimal(str(ticks["base_qty"])) if ticks and ticks["base_qty"] is not None else ZERO
        quote = Decimal(str(ticks["quote_qty"])) if ticks and ticks["quote_qty"] is not None else ZERO
        official_base = (Decimal(str(bar["volume"])) if bar and
                         bar["volume"] is not None else None)
        official_quote = (Decimal(str(bar["turnover"])) if bar and
                          bar["turnover"] is not None else None)
        delta_base = base - official_base if official_base is not None else None
        delta_quote = quote - official_quote if official_quote is not None else None
        if bar is None:
            status = "incomplete"
        elif official_base is None or official_quote is None:
            status = "not_comparable"
        else:
            base_ok = abs(delta_base) <= max(BASE_EPS, abs(official_base) * Decimal("1e-10"))
            quote_ok = abs(delta_quote) <= max(QUOTE_EPS, abs(official_quote) * Decimal("1e-10"))
            status = "match" if base_ok and quote_ok else "mismatch"
        comparisons.append({
            "minute_ts": minute, "trade_count": count, "trade_base_qty": base,
            "trade_quote_notional": quote, "candle_base_volume": official_base,
            "candle_quote_turnover": official_quote, "base_difference": delta_base,
            "quote_difference": delta_quote, "status": status,
        })
    return comparisons


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
        status = "verified"
    else:
        status = "unknown"
    return {"status": status, "minutes": 1440, **counts}


def persist_minute_evidence(conn, symbol, day, archive_sha256, instrument="linear"):
    if instrument != "linear":
        raise ValueError("Base/quote normalization must be separately approved for non-linear")
    if not archive_sha256 or len(archive_sha256) != 64:
        raise ValueError("Verified 64-character archive SHA256 required")
    start = datetime.combine(day, datetime.min.time(), UTC)
    end = start + timedelta(days=1)
    candles = conn.execute(
        """SELECT ts,volume,turnover FROM candles
           WHERE symbol=%s AND kind='trade' AND tf='1' AND closed=true
             AND ts >= %s AND ts < %s ORDER BY ts""",
        (symbol, start, end)).fetchall()
    trades = conn.execute(
        """SELECT date_trunc('minute',ts) AS ts, count(*) AS trade_count,
                  sum(qty) AS base_qty, sum(notional) AS quote_qty
           FROM trades WHERE symbol=%s AND ts >= %s AND ts < %s
           GROUP BY 1 ORDER BY 1""",
        (symbol, start, end)).fetchall()
    comparisons = build_minute_evidence(day, candles, trades)
    evidence = json.dumps({
        "base_unit": "ETH", "quote_unit": "USDT", "instrument": "linear",
        "method": "unique_trade_id_vs_exchange_closed_1m",
        "proves_all_trade_ids": False,
        "base_epsilon": str(BASE_EPS), "quote_epsilon": str(QUOTE_EPS),
    })
    statements = [
        (symbol, instrument, row["minute_ts"], "trades_archive", archive_sha256,
         row["trade_count"], row["trade_base_qty"], row["trade_quote_notional"],
         row["candle_base_volume"], row["candle_quote_turnover"],
         row["base_difference"], row["quote_difference"], row["status"], evidence)
        for row in comparisons
    ]
    with conn.cursor() as cursor:
        cursor.executemany(
            """INSERT INTO trade_candle_volume_checks
               (symbol,instrument,minute_ts,archive_source,archive_sha256,
                trade_count,trade_base_qty,trade_quote_notional,
                candle_base_volume,candle_quote_turnover,
                base_difference,quote_difference,status,evidence)
               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
               ON CONFLICT(symbol,instrument,minute_ts,archive_source,archive_sha256)
               DO UPDATE SET
                 trade_count=EXCLUDED.trade_count,
                 trade_base_qty=EXCLUDED.trade_base_qty,
                 trade_quote_notional=EXCLUDED.trade_quote_notional,
                 candle_base_volume=EXCLUDED.candle_base_volume,
                 candle_quote_turnover=EXCLUDED.candle_quote_turnover,
                 base_difference=EXCLUDED.base_difference,
                 quote_difference=EXCLUDED.quote_difference,
                 status=EXCLUDED.status,evidence=EXCLUDED.evidence,checked_at=now()""",
            statements)
    return summarize_minutes(comparisons)
