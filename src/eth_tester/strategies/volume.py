"""Verified tick-volume profile and causal untouched-POC / node rules.

These are research candidates. The live engine must supply independently verified
trade-coverage provenance; a candle's total volume is not a price profile.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR


def number(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("nonfinite_volume_profile_input")
    return result


@dataclass(frozen=True)
class VolumeProfile:
    start: datetime
    end: datetime
    tick_size: Decimal
    poc: Decimal
    value_area_low: Decimal
    value_area_high: Decimal
    by_price: dict
    total_quantity: Decimal
    trade_count: int
    coverage: str

    @property
    def high_volume_nodes(self):
        peak = max(self.by_price.values())
        return tuple(sorted(price for price, qty in self.by_price.items()
                            if price != self.poc and qty >= peak * Decimal("0.65")
                            and qty >= self.by_price.get(price - self.tick_size, Decimal(0))
                            and qty >= self.by_price.get(price + self.tick_size, Decimal(0))))

    @property
    def low_volume_nodes(self):
        peak = max(self.by_price.values())
        return tuple(sorted(price for price, qty in self.by_price.items()
                            if Decimal(0) < qty <= peak * Decimal("0.35")
                            and qty < self.by_price.get(price - self.tick_size, Decimal(0))
                            and qty < self.by_price.get(price + self.tick_size, Decimal(0))))


def build_profile(trades, start, end, tick_size, *, coverage_verified=False,
                  expected_trade_count=None):
    """Reject non-verified/partial windows; deterministic POC and contiguous 70% VA.

    The caller must prove coverage externally. A matching trade count alone
    does not establish archive or websocket completeness.
    """
    if not coverage_verified or expected_trade_count is None:
        return None
    if (start.tzinfo is None or end.tzinfo is None or
            start.utcoffset() != end.utcoffset() or start >= end):
        return None
    try:
        tick = number(tick_size)
        if tick <= 0 or expected_trade_count <= 0:
            return None
        bins = {}
        seen_ids = set()
        count = 0
        for trade in trades:
            ts = trade["ts"]
            if ts.tzinfo is None or not start <= ts < end:
                return None
            price, qty = number(trade["price"]), number(trade["qty"])
            if price <= 0 or qty <= 0:
                return None
            trade_id = trade.get("trade_id")
            if trade_id is not None:
                if trade_id in seen_ids:
                    return None
                seen_ids.add(trade_id)
            bucket = (price / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
            bins[bucket] = bins.get(bucket, Decimal(0)) + qty
            count += 1
        if count != expected_trade_count or not bins:
            return None
        if (max(bins) - min(bins)) / tick > 20000:
            return None
        total = sum(bins.values(), Decimal(0))
        poc = min(bins, key=lambda price: (-bins[price], price))
        left = right = poc
        allocated = bins[poc]
        target = total * Decimal("0.70")
        while allocated < target:
            next_low = left - tick
            next_high = right + tick
            low_volume = bins.get(next_low, Decimal(0))
            high_volume = bins.get(next_high, Decimal(0))
            if low_volume >= high_volume:
                left = next_low
                allocated += low_volume
            else:
                right = next_high
                allocated += high_volume
        return VolumeProfile(start, end, tick, poc, left, right, bins, total,
                             count, "externally_verified")
    except (InvalidOperation, TypeError, ValueError, KeyError, OverflowError):
        return None


def untouched_poc(profile, closed_bars, *, through):
    """At least one complete bar after the profile, no POC touch through cutoff."""
    if through <= profile.end:
        return False
    evaluated = [b for b in closed_bars if profile.end <= b["ts"] < through]
    if not evaluated:
        return False
    for bar in evaluated:
        if bar.get("closed", True) is not True:
            return False
        if number(bar["low"]) <= profile.poc <= number(bar["high"]):
            return False
    return True


def volume_signal(closed_candles, profile, fee_rate=Decimal("0.00055"),
                  slip_bps=Decimal("2")):
    """Candidate from a prior, verified profile; no future trade or bar access.

    The caller must separately confirm flow, freshness, fees and risk at entry.
    """
    from ..strategy import Signal, economics
    from . import Parameters

    if profile is None or profile.coverage != "externally_verified":
        return None
    candles = list(closed_candles)
    if len(candles) < 4:
        return None
    prior, last = candles[-2], candles[-1]
    if prior["ts"] < profile.end or last["ts"] <= prior["ts"]:
        return None
    if any(b.get("closed", True) is not True for b in candles):
        return None
    try:
        price = number(last["close"])
        prev = number(prior["close"])
        high, low = number(last["high"]), number(last["low"])
        opening = number(last["open"])
        if not 0 < low <= min(opening, price) <= max(opening, price) <= high:
            return None
        candidates = []
        # The POC was naked until the signal bar (which may test it).
        pre_signal = candles[:-1]
        nodes = [("naked_poc", profile.poc)]
        nodes += [("hvn", p) for p in profile.high_volume_nodes]
        nodes += [("lvn", p) for p in profile.low_volume_nodes]
        for kind, level in nodes:
            if abs(price - level) / price > Decimal("0.004"):
                continue
            if kind == "naked_poc" and not untouched_poc(profile, pre_signal,
                                                         through=last["ts"]):
                continue
            if kind == "lvn":
                if prev < level < price and price > opening:
                    side, name = "long", "lvn_breakout"
                elif prev > level > price and price < opening:
                    side, name = "short", "lvn_breakout"
                else:
                    continue
            elif prev >= level and low <= level < price and price > opening:
                side, name = "long", kind + "_rejection"
            elif prev <= level and high >= level > price and price < opening:
                side, name = "short", kind + "_rejection"
            else:
                continue
            direction = Decimal(1) if side == "long" else Decimal(-1")
            # Current bar range and historical zone width determine stop.
            buffer = max(high - low, price * Decimal("0.002")) * Decimal("0.5")
            anchor = min(low, level) if side == "long" else max(high, level)
            stop = anchor - direction * buffer
            take = price + direction * abs(price - stop) * Decimal("3")
            cost = economics(side, price, stop, take, fee_rate, slip_bps)
            if cost is None:
                continue
            params = Parameters()
            reason = {"strategy": name, "strategy_version": params.version,
                      **params.identity(), "level": str(level),
                      "profile_start": profile.start.isoformat(),
                      "profile_end": profile.end.isoformat(),
                      "profile_trade_count": profile.trade_count,
                      "profile_quality": profile.coverage,
                      "value_area": [str(profile.value_area_low), str(profile.value_area_high)],
                      "economics": cost, "requires_live_confirmation": True,
                      "source": "verified_tick_volume"}
            candidates.append((abs(price - level), name,
                               Signal(side, level, stop, take, reason)))
        return min(candidates, key=lambda item: (item[0], item[1]))[2] if candidates else None
    except (InvalidOperation, TypeError, ValueError, KeyError):
        return None
