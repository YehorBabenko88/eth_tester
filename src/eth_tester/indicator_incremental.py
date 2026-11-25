"""Checkpointed candle indicators for all 13 exchange-closed timeframes.

No mutable future bar is consulted. OHLC ATR and realized variance are computed
incrementally; VWAP is based on actual trade footprint, never HLC3 pretending
to be execution-level VWAP. Missing coverage remains explicitly unverified.
"""
from collections import deque
from decimal import Decimal
from math import log, sqrt
import json

from .analytics_core import FRAMES, close_time, decimal
from .database import connection


def next_indicators(state, candle, *, period=14):
    """Pure bounded-state Wilder ATR and trailing unannualized realized vol."""
    if period < 1:
        raise ValueError("period must be >= 1")
    if not candle.get("closed", True):
        raise ValueError("Indicators require fully closed candles")
    h, low, c = (decimal(candle[key]) for key in ("high", "low", "close"))
    if not low > 0 or not low <= c <= h:
        raise ValueError("Invalid OHLC")
    prev = decimal(state["last_close"]) if state.get("last_close") is not None else None
    tr = max(h - low, abs(h - prev), abs(low - prev)) if prev is not None else h - low
    n = int(state.get("n", 0)) + 1
    sum_tr = decimal(state.get("initial_tr", "0"))
    atr = decimal(state["atr"]) if state.get("atr") is not None else None
    if n <= period:
        sum_tr += tr
        if n == period:
            atr = sum_tr / period
    else:
        if atr is None:
            raise ValueError("Missing Wilder ATR checkpoint")
        atr = (atr * (period - 1) + tr) / period
    previous_returns = deque((decimal(x) for x in state.get("returns", ())),
                             maxlen=period)
    if prev is not None:
        previous_returns.append(Decimal(str(log(float(c / prev)))))
    rv = Decimal(str(sqrt(sum((float(x * x) for x in previous_returns), 0.0)))) \
        if previous_returns else None
    new_state = {"last_close": str(c), "n": n,
                 "initial_tr": str(sum_tr) if n < period else None,
                 "atr": str(atr) if atr is not None else None,
                 "returns": [str(x) for x in previous_returns]}
    features = {"atr": atr, "realized_volatility": rv,
                "high_low_range": h - low,
                "high_low_range_pct": (h - low) / c * 100,
                "close": c}
    return new_state, features


def indicator_worker_step(settings, tf="1", *, batch=500, period=14):
    """Persist a bounded, atomic deterministic suffix with durable cursor.

    The cursor stores the last candle OPEN; computed features are available
    no earlier than its CLOSE. Quality never claims fully verified trades.
    """
    if tf not in FRAMES or batch < 1:
        raise ValueError("Invalid indicator timeframe/batch")
    with connection(settings) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                     (f"candle_indicators:{settings.symbol}:{tf}",))
        row = conn.execute("""SELECT cursor_ts,details FROM analytics_cursor
            WHERE processor='candle_indicators' AND symbol=%s AND tf=%s FOR UPDATE""",
            (settings.symbol, tf)).fetchone()
        cursor = row["cursor_ts"] if row else None
        details = row["details"] if row else {}
        if details and details.get("period") != period:
            raise RuntimeError("ATR period mismatch: reset checkpoint explicitly")
        state = details.get("state", {}) if details else {}
        candles = conn.execute("""SELECT ts,high,low,close,closed FROM candles
            WHERE symbol=%s AND kind='trade' AND tf=%s AND closed
              AND (%s::timestamptz IS NULL OR ts>%s)
            ORDER BY ts LIMIT %s""",
            (settings.symbol, tf, cursor, cursor, batch)).fetchall()
        previous = cursor
        for bar in candles:
            if previous is not None and bar["ts"] != close_time(previous, tf):
                raise RuntimeError(f"Candle coverage gap {settings.symbol} {tf} after {previous}")
            state, values = next_indicators(state, bar, period=period)
            available = close_time(bar["ts"], tf)
            observed = {"asof_close": available.isoformat(),
                        "source": "closed_exchange_candle",
                        "causally_certified": False,
                        "atr_period": period,
                        "rv_returns_window": period,
                        "realized_volatility_annualized": False}
            for key in ("atr", "realized_volatility", "high_low_range",
                        "high_low_range_pct"):
                observed[key] = str(values[key]) if values[key] is not None else None
            # Execution-level VWAP uses observed aggressor trade notional.
            # Do not substitute OHLC 'typical price' for actual trade VWAP.
            flow = conn.execute("""SELECT
                   sum(buy_notional+sell_notional) AS notional,
                   sum(buy_qty+sell_qty) AS qty
                FROM footprint WHERE symbol=%s AND tf=%s AND bucket=%s""",
                (settings.symbol, tf, bar["ts"])).fetchone()
            if flow and flow["qty"] and decimal(flow["qty"]) > 0:
                observed["trade_vwap"] = str(
                    decimal(flow["notional"]) / decimal(flow["qty"]))
                observed["trade_vwap_quality"] = "observed_volume_unverified"
            else:
                observed["trade_vwap"] = None
                observed["trade_vwap_quality"] = "no_verified_trade_volume"
            # The open-interest series is exchange-timestamped; without
            # observed/received provenance this is NOT a certified as-of
            # ML feature. Reject stale endpoint data rather than forward-fill.
            oi = conn.execute("""SELECT ts,value FROM open_interest
                WHERE symbol=%s AND interval='5min' AND ts<=%s
                ORDER BY ts DESC LIMIT 2""",
                (settings.symbol, available)).fetchall()
            observed["oi_delta"] = None
            observed["oi_source_quality"] = "unavailable"
            if len(oi) == 2 and 0 <= (available - oi[0]["ts"]).total_seconds() <= 600:
                observed["oi_delta"] = str(decimal(oi[0]["value"]) -
                                           decimal(oi[1]["value"]))
                observed["oi_source_quality"] = "exchange_time_only_unverified"
                observed["oi_exchange_asof"] = oi[0]["ts"].isoformat()
            conn.execute("""INSERT INTO features(symbol,tf,ts,metadata,quality)
                VALUES(%s,%s,%s,%s::jsonb,'indicator_partial')
                ON CONFLICT(symbol,tf,ts) DO UPDATE SET
                metadata=features.metadata || EXCLUDED.metadata""",
                (settings.symbol, tf, bar["ts"], json.dumps(observed)))
            previous = bar["ts"]
        if candles:
            conn.execute("""INSERT INTO analytics_cursor(
                    processor,symbol,tf,cursor_ts,quality,state,details)
                VALUES('candle_indicators',%s,%s,%s,
                       'closed_candle_not_trade_verified','processed',%s::jsonb)
                ON CONFLICT(processor,symbol,tf) DO UPDATE SET
                   cursor_ts=EXCLUDED.cursor_ts,quality=EXCLUDED.quality,
                   state=EXCLUDED.state,details=EXCLUDED.details,updated_at=now()""",
                (settings.symbol, tf, previous,
                 json.dumps({"period": period, "state": state})))
        return len(candles)
