"""Fail-closed live paper confirmation with explicit time and volume coverage.

Never interpret a score as win probability, nor orderbook loss as a fill.
All evidence must have existed at the observed decision instant.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from .strategies import Parameters
from .strategy import economics

VOLUME_REL_TOLERANCE = Decimal("0.0005")
VOLUME_ABS_TOLERANCE = Decimal("0.001")


def _decimal(value):
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("non-finite market data")
    return number


def _matches_volume(actual, stated):
    try:
        actual, stated = _decimal(actual), _decimal(stated)
        return (actual > 0 and stated > 0 and
                abs(actual - stated) <= max(VOLUME_ABS_TOLERANCE,
                                            stated * VOLUME_REL_TOLERANCE))
    except (InvalidOperation, ValueError, TypeError):
        return False


def _valid_oi(rows, cutoff):
    if len(rows) != 2:
        return False
    latest, previous = rows
    if any(item["ts"].tzinfo is None for item in rows):
        return False
    if (latest["ts"] <= previous["ts"] or
            latest["ts"] - previous["ts"] != timedelta(minutes=5) or
            not timedelta(0) <= cutoff - latest["ts"] <= timedelta(minutes=10)):
        return False
    if any(int(item["ts"].timestamp()) % 300 != 0 for item in rows):
        return False
    try:
        return all(_decimal(item["value"]) > 0 for item in rows)
    except (InvalidOperation, TypeError, ValueError):
        return False


def _baseline(conn, symbol, start):
    """Validate five consecutive one-minute trades against five closed candles."""
    previous = start - timedelta(minutes=5)
    ticks = conn.execute("""SELECT date_trunc('minute',ts) AS ts,count(*) AS n,
      coalesce(sum(qty) FILTER(WHERE side='Buy'),0) AS buys,
      coalesce(sum(qty) FILTER(WHERE side='Sell'),0) AS sells
      FROM trades WHERE symbol=%s AND ts>=%s AND ts<%s GROUP BY 1 ORDER BY 1""",
      (symbol, previous, start)).fetchall()
    candles = conn.execute("""SELECT ts,volume FROM candles WHERE symbol=%s
      AND kind='trade' AND tf='1' AND closed AND ts>=%s AND ts<%s ORDER BY ts""",
      (symbol, previous, start)).fetchall()
    if len(ticks) != 5 or len(candles) != 5:
        return None
    total_count, total_delta = 0, Decimal(0)
    for index, (tick, candle) in enumerate(zip(ticks, candles)):
        expected = previous + timedelta(minutes=index)
        if (tick["ts"] != expected or candle["ts"] != expected
                or tick["n"] < 2):
            return None
        try:
            buys, sells = _decimal(tick["buys"]), _decimal(tick["sells"])
            if min(buys, sells) < 0 or not _matches_volume(
                    buys + sells, candle["volume"]):
                return None
        except (InvalidOperation, ValueError, TypeError):
            return None
        total_count += tick["n"]
        total_delta += buys - sells
    return total_count, total_delta


def _mtf(conn, symbol, end, sign):
    trends = {}
    for tf, minutes in (("5", 5), ("15", 15), ("60", 60)):
        span = timedelta(minutes=minutes)
        bars = conn.execute("""SELECT ts,close FROM candles WHERE symbol=%s
          AND kind='trade' AND tf=%s AND closed AND ts<=%s
          ORDER BY ts DESC LIMIT 3""", (symbol, tf, end - span)).fetchall()
        if (len(bars) != 3 or end - bars[0]["ts"] > 2 * span
                or any(a["ts"] - b["ts"] != span for a, b in zip(bars, bars[1:]))):
            return None
        if any(item["ts"].tzinfo is None for item in bars):
            return None
        try:
            trends[tf] = sign * (_decimal(bars[0]["close"]) -
                                _decimal(bars[-1]["close"])) > 0
        except (InvalidOperation, TypeError, ValueError):
            return None
    return trends



def _certified_snapshot(conn, symbol, start, end, observed_at):
    """Require B's immutable C/F as-of feature snapshot before paper admission.

    A complete snapshot must be computed and genuinely observed by decision
    time, with per-feature availability and certified source coverage.
    This intentionally denies trading while producers remain unverified.
    """
    from .ml import FEATURE_NAMES

    row = conn.execute("""SELECT snapshot_key,bar_ts,available_at,observed_at,
          computed_at,source,source_checksum,quality,is_causal,values_json,provenance
          FROM feature_snapshots WHERE symbol=%s AND tf='1' AND bar_ts=%s
          AND is_causal AND quality='complete' AND available_at<=%s
          AND observed_at<=%s AND computed_at<=%s
          ORDER BY available_at DESC,computed_at DESC,snapshot_key DESC LIMIT 1""",
          (symbol, start, observed_at, observed_at, observed_at)).fetchone()
    if not row:
        return None
    try:
        timestamps = [row[key] for key in ("bar_ts", "available_at",
                                          "observed_at", "computed_at")]
        if any(value.tzinfo is None or value.utcoffset() != timedelta(0)
               for value in timestamps):
            return None
        bar, available, received, computed = timestamps
        checksum = row["source_checksum"]
        provenance = row["provenance"]
        values = row["values_json"]
        if (bar != start or not end <= available <= received <= computed <= observed_at
                or row["quality"] != "complete" or row["is_causal"] is not True
                or not isinstance(row["source"], str) or not row["source"]
                or not isinstance(row["snapshot_key"], str) or not row["snapshot_key"]
                or not isinstance(checksum, str) or len(checksum) != 64
                or any(c not in "0123456789abcdef" for c in checksum)
                or not isinstance(provenance, dict)
                or provenance.get("coverage_status") != "verified"
                or not isinstance(values, dict)):
            return None
        times = provenance["feature_available_ms"]
        checked_ms = provenance["source_checked_at_ms"]
        if not isinstance(times, dict) or type(checked_ms) is not int:
            return None
        end_ms = int(end.timestamp() * 1000)
        available_ms = int(available.timestamp() * 1000)
        if not end_ms <= checked_ms <= available_ms:
            return None
        feature_availability = {}
        for name in FEATURE_NAMES:
            t = times[name]
            if type(t) is not int or not 0 <= t <= available_ms:
                return None
            _decimal(values[name])
            feature_availability[name] = t
        return {"snapshot_key": row["snapshot_key"],
                "source_checksum": checksum,
                "available_at": available.isoformat(),
                "computed_at": computed.isoformat(),
                "coverage_checked_at_ms": checked_ms,
                "feature_available_ms": feature_availability}
    except (AttributeError, InvalidOperation, KeyError, TypeError,
            ValueError, OverflowError):
        return None

def confirm_rejection(conn, settings, signal, bar, *, observed_at=None):
    """Return (admissible, JSON-compatible evidence) at the observed instant."""
    now = observed_at if observed_at is not None else datetime.now(timezone.utc)

    def reject(reason):
        return False, {"reason": reason, "score": 0, "admissible": False}

    if signal is None or signal.direction not in ("long", "short"):
        return reject("invalid_signal")
    start = bar["ts"]
    if start.tzinfo is None or now.tzinfo is None:
        return reject("naive_timestamp")
    end = start + timedelta(minutes=1)
    if not timedelta(0) <= now - end <= timedelta(seconds=90):
        return reject("stale_or_open_bar")
    symbol = settings.symbol
    rows = conn.execute("""SELECT price,sum(qty) FILTER(WHERE side='Buy') AS buys,
      sum(qty) FILTER(WHERE side='Sell') AS sells,count(*) AS n
      FROM trades WHERE symbol=%s AND ts>=%s AND ts<%s GROUP BY price""",
      (symbol, start, end)).fetchall()
    try:
        if any(_decimal(row["price"]) <= 0 or row["n"] <= 0 for row in rows):
            return reject("invalid_tick_price")
        buys = sum((_decimal(row["buys"] or 0) for row in rows), Decimal(0))
        sells = sum((_decimal(row["sells"] or 0) for row in rows), Decimal(0))
        count = sum(row["n"] for row in rows)
        volume = buys + sells
        if min(buys, sells) < 0 or count < 20 or not _matches_volume(
                volume, bar.get("volume")):
            return reject("tick_volume_unverified")
        baseline = _baseline(conn, symbol, start)
        if baseline is None:
            return reject("incomplete_five_minute_tick_baseline")
        previous_count, previous_delta = baseline
        oi = conn.execute("""SELECT ts,value FROM open_interest WHERE symbol=%s
          AND interval='5min' AND ts<=%s ORDER BY ts DESC LIMIT 2""",
          (symbol, end)).fetchall()
        if not _valid_oi(oi, end):
            return reject("stale_or_gapped_oi")
        book = conn.execute("""SELECT bids,asks,updated_at,valid
          FROM market_book_state WHERE symbol=%s""", (symbol,)).fetchone()
        if (not book or not book["valid"] or book["updated_at"].tzinfo is None
                or not timedelta(0) <= now - book["updated_at"] <= timedelta(seconds=10)):
            return reject("stale_book")
        bid_levels = [_decimal(qty) for qty in book["bids"].values()]
        ask_levels = [_decimal(qty) for qty in book["asks"].values()]
        if (not bid_levels or not ask_levels or
                any(qty <= 0 for qty in bid_levels + ask_levels)):
            return reject("invalid_book_liquidity")
        bid = sum(bid_levels, Decimal(0))
        ask = sum(ask_levels, Decimal(0))
        sign = 1 if signal.direction == "long" else -1
        mtf = _mtf(conn, symbol, end, sign)
        if mtf is None:
            return reject("missing_multitimeframe")
        recent = conn.execute("""SELECT price,ts FROM trades WHERE symbol=%s
          AND source='websocket' AND ts<=%s ORDER BY ts DESC,trade_id DESC LIMIT 1""",
          (symbol, now)).fetchone()
        if (not recent or recent["ts"].tzinfo is None or
                not timedelta(0) <= now - recent["ts"] <= timedelta(seconds=10)):
            return reject("stale_entry")
        delta = (buys - sells) / volume
        speed = Decimal(count * 5) / Decimal(previous_count)
        imbalance = (bid - ask) / (bid + ask)
        oi_change = _decimal(oi[0]["value"]) / _decimal(oi[1]["value"]) - 1
        rolling_cvd = previous_delta + buys - sells
        poc = min(rows, key=lambda item: (-(_decimal(item["buys"] or 0) +
                                               _decimal(item["sells"] or 0)),
                                          _decimal(item["price"])))["price"]
        near = [item for item in rows if abs(_decimal(item["price"]) -
                 signal.level) <= _decimal(bar["close"]) * Decimal("0.001")]
        local_buy = sum((_decimal(item["buys"] or 0) for item in near), Decimal(0))
        local_sell = sum((_decimal(item["sells"] or 0) for item in near), Decimal(0))
        local_volume = local_buy + local_sell
        local_delta = local_buy - local_sell
        footprint = local_volume / volume >= Decimal("0.05")
        reversal = signal.reason.get("strategy") in (
            "rejection", "false_breakout", "naked_poc_rejection",
            "hvn_rejection")
        absorption = (reversal and footprint and
                      sign * delta < Decimal("-0.15") and
                      sign * local_delta < -local_volume * Decimal("0.15"))
        checks = {
            "directional_flow_or_absorption": (absorption if reversal
                                               else sign * delta > Decimal("0.10")),
            "tape_acceleration": speed >= Decimal("1.15"),
            "oi_building": oi_change > Decimal("0.001"),
            "book_agrees": sign * imbalance > Decimal("0.1"),
            "multitimeframe": sum(mtf.values()) >= 2,
            "cvd_agrees": sign * rolling_cvd > 0,
            "footprint_near_level": footprint,
        }
        weights = {"directional_flow_or_absorption": 25, "tape_acceleration": 10,
                   "oi_building": 10, "book_agrees": 15, "multitimeframe": 20,
                   "cvd_agrees": 10, "footprint_near_level": 10}
        score = sum(weight for key, weight in weights.items() if checks[key])
        costs = economics(signal.direction, _decimal(recent["price"]),
                          signal.stop, signal.take, settings.taker_fee_rate,
                          settings.slippage_bps)
        accepted = (score >= Parameters().min_score and
                    checks["directional_flow_or_absorption"] and
                    checks["multitimeframe"] and costs is not None)
        certified = None
        if accepted:
            certified = _certified_snapshot(conn, symbol, start, end, now)
            if certified is None:
                return reject("missing_verified_feature_snapshot")
        liquidation = conn.execute("""SELECT side,coalesce(sum(qty),0) AS qty
          FROM liquidation_events WHERE symbol=%s AND ts>=%s AND ts<%s
          GROUP BY side""", (symbol, start, end)).fetchall()
        return accepted, {
            "reason": "accepted" if accepted else "confirmation_or_cost_gate",
            "admissible": accepted, "score": score, "minimum": Parameters().min_score,
            "checks": checks, "weights": weights, "mtf": mtf,
            "delta_fraction": str(delta), "rolling_cvd_6m_qty": str(rolling_cvd),
            "tape_speed_ratio": str(speed), "oi_change": str(oi_change),
            "book_imbalance": str(imbalance), "minute_poc": str(poc),
            "level_local_delta": str(local_delta),
            "level_volume_fraction": str(local_volume / volume),
            "absorption_proxy": absorption,
            "liquidations_observed_unverified": {
                item["side"]: str(item["qty"]) for item in liquidation},
            "economics_at_observed_entry": costs,
            "immutable_feature_snapshot": certified,
            "baseline": "five_closed_1m_candles_volume_matched",
            "warning": ("Score is not a probability; book changes and liquidation "
                        "counts cannot prove absorption or fill attribution."),
        }
    except (InvalidOperation, TypeError, ValueError, KeyError, ZeroDivisionError):
        return reject("invalid_market_observation")
