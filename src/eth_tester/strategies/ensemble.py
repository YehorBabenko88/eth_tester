"""Causal, deterministic ETHUSDT strategy hypotheses -- SHADOW research only.

No model is trained or promoted here. OHLCV does not prove trade-ID coverage,
orderflow absorption, profitable expectancy, or permission to open any order.
Only the existing B/D/E evidence and position gates authorize paper admission.
"""

from datetime import datetime, timedelta, timezone
from decimal import (
    Decimal, InvalidOperation, DivisionByZero, Context, localcontext, ROUND_HALF_EVEN,
)
from hashlib import sha256
import json

CONTRACT = "eth_strategy_ensemble_v1"
_MAX_BARS = 180
_MIN_BARS = 60
_TICK_SECONDS = 60
_MAX_ATR_FRACTION = Decimal("0.03")
_MIN_ATR_FRACTION = Decimal("0.0001")
_MAX_STOP_FRACTION = Decimal("0.02")
_MAX_RESEARCH_CANDIDATES = 8


def _decimal(value):
    if isinstance(value, bool):
        raise ValueError("boolean_market_number")
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("nonfinite_market_number")
    return number


def _empty(reason, *, bar_ts=None, regime="unavailable", context=None):
    return {
        "contract": CONTRACT,
        "version": "1",
        "mode": "shadow_only",
        "approved": False,
        "status": "no_trade",
        "reason": reason,
        "bar_ts": bar_ts,
        "regime": regime,
        "context": context if context is not None else {},
        "candidates": [],
        "selected": None,
        "paper_entry_authorized": False,
        "ml_eligible": False,
        "promoted": False,
        "calibration": "not_independently_calibrated",
        "expected_net_lower_bound": None,
        "source_quality": "closed_candle_research_only",
    }


def _canonical(result):
    """A checksum for reproducible diagnosis, NEVER a B source certificate."""
    clone = dict(result)
    clone.pop("diagnostic_sha256", None)
    result["diagnostic_sha256"] = sha256(json.dumps(
        clone, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")).hexdigest()
    return result


def _validated_bars(closed_candles, *, decision_at=None):
    if not isinstance(closed_candles, (list, tuple)):
        raise ValueError("closed_bar_sequence_required")
    if not _MIN_BARS <= len(closed_candles) <= _MAX_BARS:
        raise ValueError("insufficient_or_unbounded_closed_history")
    if decision_at is not None and (
        not isinstance(decision_at, datetime) or decision_at.tzinfo is None
        or decision_at.utcoffset() != timedelta(0)
    ):
        raise ValueError("actual_utc_decision_time_required")
    result = []
    previous = None
    for raw in closed_candles:
        if not isinstance(raw, dict):
            raise ValueError("closed_bar_mapping_required")
        stamp = raw.get("ts")
        if (not isinstance(stamp, datetime) or stamp.tzinfo is None
                or stamp.utcoffset() != timedelta(0)
                or stamp.second != 0 or stamp.microsecond != 0):
            raise ValueError("utc_minute_aligned_candle_required")
        if previous is not None and stamp - previous != timedelta(minutes=1):
            raise ValueError("missing_or_out_of_order_bar")
        if raw.get("closed") is not True:
            raise ValueError("open_candle_unavailable")
        if decision_at is not None and stamp + timedelta(minutes=1) > decision_at:
            raise ValueError("future_candle_relative_to_observation")
        o, h, l, c, volume = (_decimal(raw[k]) for k in
                            ("open", "high", "low", "close", "volume"))
        if not (0 < l <= min(o, c) <= max(o, c) <= h and volume >= 0):
            raise ValueError("invalid_ohlcv_candle")
        result.append({"ts": stamp, "open": o, "high": h,
                       "low": l, "close": c, "volume": volume})
        previous = stamp
    return result


def _atr(bars, window=14):
    values = []
    for previous, current in zip(bars[-window-1:-1], bars[-window:]):
        values.append(max(current["high"] - current["low"],
                          abs(current["high"] - previous["close"]),
                          abs(current["low"] - previous["close"])))
    if len(values) != window:
        raise ValueError("atr_missing_complete_window")
    return sum(values, Decimal(0)) / Decimal(window)


def _complete_mtf(bars, minutes, *, as_of):
    """Only closed aligned groups of original UTC minute bars count."""
    size = timedelta(minutes=minutes)
    groups = {}
    for bar in bars:
        stamp = bar["ts"]
        start = stamp - timedelta(minutes=(stamp.minute % minutes))
        groups.setdefault(start, []).append(bar)
    complete = []
    for start in sorted(groups):
        group = groups[start]
        if (start + size <= as_of and len(group) == minutes
                and group[0]["ts"] == start
                and group[-1]["ts"] == start + size - timedelta(minutes=1)):
            complete.append((start, group[-1]["close"]))
    if len(complete) < 3:
        raise ValueError(f"insufficient_closed_{minutes}m_context")
    recent = complete[-3:]
    if any(recent[i][0] - recent[i-1][0] != size for i in (1, 2)):
        raise ValueError(f"gapped_closed_{minutes}m_context")
    change = recent[-1][1] - recent[0][1]
    direction = "up" if change > 0 else "down" if change < 0 else "flat"
    return {"direction": direction, "last_close": str(recent[-1][1]),
            "last_closed_at": (recent[-1][0] + size).isoformat(),
            "change": str(change)}


def _confirmed_levels(bars):
    """A pivot is usable only after TWO subsequent closed confirmation bars."""
    history = bars[:-1]
    levels = []
    for i in range(2, len(history) - 2):
        origin = history[i]
        neighbors = history[i-2:i] + history[i+1:i+3]
        for side, price_key, comparison in (
            ("resistance", "high", lambda a, b: a > b),
            ("support", "low", lambda a, b: a < b),
        ):
            level = origin[price_key]
            if all(comparison(level, item[price_key]) for item in neighbors):
                confirmed_at = history[i+2]["ts"] + timedelta(minutes=1)
                if confirmed_at <= bars[-1]["ts"]:
                    levels.append((side, level, origin["ts"], confirmed_at))
    return levels


def _patterns(bars, *, atr):
    previous, last = bars[-2], bars[-1]
    price = last["close"]
    names = []
    for side, level, origin, confirmation in _confirmed_levels(bars):
        if level <= 0 or abs(price - level) / level > Decimal("0.005"):
            continue
        earlier = [bar for bar in bars[:-1]
                   if bar["ts"] >= confirmation]
        broken = any(bar["close"] > level if side == "resistance"
                     else bar["close"] < level for bar in earlier)
        p, c = previous["close"], last["close"]
        candidate = None
        if side == "resistance":
            if not broken and p <= level < c:
                candidate = ("breakout", "long")
            elif broken and p > level and last["low"] <= level < c:
                candidate = ("retest", "long")
            elif broken and p > level > c and last["high"] >= level:
                candidate = ("false_breakout", "short")
            elif not broken and last["high"] > level > c and c < last["open"]:
                candidate = ("bounce", "short")
        else:
            if not broken and p >= level > c:
                candidate = ("breakout", "short")
            elif broken and p < level and last["high"] >= level > c:
                candidate = ("retest", "short")
            elif broken and p < level < c and last["low"] <= level:
                candidate = ("false_breakout", "long")
            elif not broken and last["low"] < level < c and c > last["open"]:
                candidate = ("bounce", "long")
        if candidate is not None:
            names.append((candidate[0], candidate[1], side, level,
                          origin, confirmation))
        # High-volume long wick is NOT proof of orderflow absorption: only
        # a clearly labeled research hypothesis may be emitted.
        body = abs(c - last["open"])
        wick = ((last["high"] - max(last["open"], c))
                if side == "resistance" else
                (min(last["open"], c) - last["low"]))
        past_volumes = sorted(item["volume"] for item in bars[-11:-1])
        median_volume = ((past_volumes[4] + past_volumes[5]) / Decimal(2))
        if (median_volume > 0 and last["volume"] >=
                median_volume * Decimal("1.5")
                and wick >= max(body * Decimal(2), atr * Decimal("0.25"))
                and last["low"] <= level <= last["high"]):
            direction = "short" if side == "resistance" else "long"
            names.append(("absorption_proxy", direction, side, level,
                          origin, confirmation))
    return names


def _evaluate_shadow_ensemble_impl(closed_candles, *, taker_fee_rate=Decimal("0.00055"),
                             slippage_bps=Decimal("2"), decision_at=None):
    """Return a reproducible, never executable research snapshot.

    Historical closed 1m candles produce ONLY hypotheses, not actual source
    completeness, calibrated win probabilities, entries, or ML certificates.
    This API never reads a DB, trains, sends orders, or changes positions.
    """
    try:
        bars = _validated_bars(closed_candles, decision_at=decision_at)
        # Reproducible input identity, never independent source certification.
        input_sha256 = sha256(json.dumps([
            [bar["ts"].isoformat(), *(str(bar[k]) for k in
                                     ("open", "high", "low", "close", "volume"))]
            for bar in bars
        ], separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()

        def seal(result):
            result["input_sha256"] = input_sha256
            result["input_bar_count"] = len(bars)
            result["input_window_closed_at"] = (
                bars[-1]["ts"] + timedelta(minutes=1)).isoformat()
            result["decision_cutoff_at"] = (
                decision_at.isoformat() if decision_at is not None else None)
            result["source_identity_authoritative"] = False
            return _canonical(result)
        fee, slip = _decimal(taker_fee_rate), _decimal(slippage_bps)
        if fee < 0 or fee >= Decimal("0.05") or slip < 0 or slip >= Decimal(100):
            raise ValueError("fee_or_slippage_outside_research_limits")
        last, previous = bars[-1], bars[-2]
        as_of = last["ts"] + timedelta(minutes=1)
        atr = _atr(bars)
        if atr <= 0:
            raise ValueError("zero_range_or_unusable_atr")
        atr_fraction = atr / last["close"]
        ranges = sorted(bar["high"] - bar["low"] for bar in bars[-11:-1])
        typical_range = (ranges[4] + ranges[5]) / Decimal(2)
        context = {"atr": str(atr), "atr_fraction": str(atr_fraction),
                   "last_volume": str(last["volume"]),
                   "mtf_5m": _complete_mtf(bars, 5, as_of=as_of),
                   "mtf_15m": _complete_mtf(bars, 15, as_of=as_of)}
        if atr_fraction < _MIN_ATR_FRACTION or atr_fraction > _MAX_ATR_FRACTION:
            return seal(_empty("unsafe_volatility_regime",
                bar_ts=last["ts"].isoformat(), context=context))
        if (typical_range <= 0 or last["high"] - last["low"] >
                typical_range * Decimal(4)):
            return seal(_empty("range_shock_or_unreliable_baseline",
                bar_ts=last["ts"].isoformat(), regime="shock", context=context))
        if any(bar["volume"] <= 0 for bar in bars[-5:]):
            return seal(_empty("no_recent_volume_observed",
                bar_ts=last["ts"].isoformat(), context=context))
        net_move = last["close"] - bars[-16]["close"]
        regime = ("trending_up" if net_move >= 3*atr else
                  "trending_down" if net_move <= -3*atr else "range")
        names = _patterns(bars, atr=atr)
        candidates = []
        for pattern, direction, side, level, origin, confirmation in names:
            sign = Decimal(1) if direction == "long" else Decimal(-1)
            anchor = (min(last["low"], level) if sign > 0
                      else max(last["high"], level))
            buffer = max(atr*Decimal("0.7"), last["close"]*Decimal("0.001"))
            stop = anchor - sign * buffer
            risk_distance = abs(last["close"] - stop)
            risk_fraction = risk_distance / last["close"]
            if (stop <= 0 or risk_distance <= 0 or
                    risk_fraction > _MAX_STOP_FRACTION):
                continue
            ratio = Decimal("3.5") if regime != "range" else Decimal(3)
            take = last["close"] + sign * risk_distance * ratio
            # Reuse the canonical cost accounting; do not mistake feasibility
            # or modelled break-even for a calibrated win-probability bound.
            from ..strategy import economics
            model_cost = economics(direction, last["close"], stop, take,
                                   fee, slip)
            if model_cost is None:
                continue
            against = ("down" if direction == "long" else "up")
            mtf_conflict = any(context[f"mtf_{tf}m"]["direction"] == against
                               for tf in (5, 15))
            eligibility = ("requires_real_orderflow_confirmation" if
                           pattern == "absorption_proxy" else
                           "mtf_conflict" if mtf_conflict else
                           "unvalidated_shadow_candidate")
            candidates.append({
                "pattern": pattern, "direction": direction, "side": side,
                "level": str(level), "origin": origin.isoformat(),
                "confirmed_available_at": confirmation.isoformat(),
                "entry_reference": str(last["close"]),
                "adaptive_stop": str(stop), "adaptive_take": str(take),
                "stop_fraction": str(risk_fraction),
                "atr_fraction": str(atr_fraction),
                "costed_break_even_win_rate":
                    model_cost["break_even_win_rate"],
                "modelled_net_target_fraction":
                    model_cost["net_target_fraction"],
                "modelled_net_risk_fraction":
                    model_cost["net_risk_fraction"],
                "calibrated_probability_lower": None,
                "expected_net_lower_bound": None,
                "eligibility": eligibility,
                "paper_entry_authorized": False,
                "ml_eligible": False,
            })
        priority = {"false_breakout": 0, "retest": 1, "bounce": 2,
                    "breakout": 3, "absorption_proxy": 4}
        candidates.sort(key=lambda item: (
            priority[item["pattern"]],
            abs(Decimal(item["level"]) - last["close"]),
            item["direction"], item["origin"], item["level"],
        ))
        full_count = len(candidates)
        eligible_all = [x for x in candidates if x["eligibility"] ==
                        "unvalidated_shadow_candidate"]
        opposed = len({x["direction"] for x in eligible_all}) > 1
        overflow = full_count > _MAX_RESEARCH_CANDIDATES
        candidates = candidates[:_MAX_RESEARCH_CANDIDATES]
        result = _empty("no_causal_pattern", bar_ts=last["ts"].isoformat(),
                        regime=regime, context=context)
        result["candidates"] = candidates
        result["evaluated_candidate_count"] = full_count
        result["candidate_overflow"] = overflow
        if opposed:
            result["reason"] = "conflicting_shadow_directions"
        elif overflow:
            result["reason"] = "candidate_cap_exceeded_no_trade"
        elif eligible_all:
            result["status"] = "shadow_candidates"
            result["reason"] = "await_independent_calibration_and_live_proof"
            result["selected"] = eligible_all[0]["pattern"]
        elif candidates:
            result["reason"] = "all_candidates_unconfirmed_or_mtf_conflicted"
        # Selected is a name for *shadow research only*, never permission for
        # E to open or F to label/train a paper trade.
        return seal(result)
    except (ValueError, KeyError, TypeError, AttributeError, InvalidOperation,
            DivisionByZero, OverflowError, ArithmeticError) as exc:
        code = str(exc)
        # Do not leak arbitrary exceptions or strategy/candle raw data.
        known = {
            "closed_bar_sequence_required", "insufficient_or_unbounded_closed_history",
            "actual_utc_decision_time_required", "utc_minute_aligned_candle_required",
            "missing_or_out_of_order_bar", "open_candle_unavailable",
            "future_candle_relative_to_observation", "invalid_ohlcv_candle",
            "atr_missing_complete_window", "zero_range_or_unusable_atr",
            "fee_or_slippage_outside_research_limits",
            "closed_bar_mapping_required", "boolean_market_number",
            "nonfinite_market_number", "insufficient_closed_5m_context",
            "insufficient_closed_15m_context", "gapped_closed_5m_context",
            "gapped_closed_15m_context",
        }
        return _canonical(_empty(code if code in known else
                                 "unavailable_invalid_shadow_input"))


def evaluate_shadow_ensemble(closed_candles, *, taker_fee_rate=Decimal("0.00055"),
                             slippage_bps=Decimal("2"), decision_at=None):
    """Stable high-precision envelope even if other workers alter Decimal context."""
    with localcontext(Context(prec=42, rounding=ROUND_HALF_EVEN)):
        return _evaluate_shadow_ensemble_impl(
            closed_candles, taker_fee_rate=taker_fee_rate,
            slippage_bps=slippage_bps, decision_at=decision_at,
        )


def evaluate_closed_bar_research(closed_candles, *, observed_at,
                                 taker_fee_rate=Decimal("0.00055"),
                                 slippage_bps=Decimal("2")):
    """Independently evaluate every eligible observed 1m bar, including no-trade.

    E must call this outside the incumbent v3 signal-selection gate.
    B's actual append-only source/receipt identity is a separate requirement:
    this diagnostic NEVER certifies observation time or historical ML validity.
    """
    with localcontext(Context(prec=42, rounding=ROUND_HALF_EVEN)):
        if (not isinstance(observed_at, datetime)
                or observed_at.tzinfo is None
                or observed_at.utcoffset() != timedelta(0)):
            return _canonical(_empty("actual_utc_decision_time_required"))
        observed = observed_at.astimezone(timezone.utc)
        result = _evaluate_shadow_ensemble_impl(
            closed_candles, taker_fee_rate=taker_fee_rate,
            slippage_bps=slippage_bps, decision_at=observed,
        )
        result["research_observed_at"] = observed.isoformat()
        result["independent_of_incumbent"] = True
        result["observation_receipt_authoritative"] = False
        result["paper_entry_authorized"] = False
        result["ml_eligible"] = False
        result["approved"] = False
        result["promoted"] = False
        result["calibration"] = "not_independently_calibrated"
        result["expected_net_lower_bound"] = None
        return _canonical(result)
