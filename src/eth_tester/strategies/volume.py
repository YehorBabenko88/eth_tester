"""Verified tick-volume profile and causal untouched-POC / node rules.

These are research candidates. The live engine must supply independently verified
trade-coverage provenance; a candle's total volume is not a price profile.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_FLOOR


def number(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("nonfinite_volume_profile_input")
    return result


@dataclass(frozen=True)
class VolumeZone:
    low: Decimal
    high: Decimal
    quantity: Decimal
    kind: str


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
    available_at: datetime | None = None
    source_checksum: str | None = None

    def zones(self, kind):
        """Contiguous tick zones inside the actually observed price range.

        Empty internal bins count as low volume, not as traded prices.
        """
        if kind not in ("high", "low"):
            raise ValueError("unknown_zone_kind")
        peak = max(self.by_price.values())
        threshold = peak * (Decimal("0.65") if kind == "high"
                            else Decimal("0.35"))
        floor = min(self.by_price)
        count = int((max(self.by_price) - floor) / self.tick_size) + 1
        zones = []
        opening = None
        quantity = Decimal(0)
        for index in range(count):
            price = floor + self.tick_size * index
            current = self.by_price.get(price, Decimal(0))
            matching = current >= threshold if kind == "high" else current <= threshold
            if matching:
                if opening is None:
                    opening = price
                quantity += current
            elif opening is not None:
                zones.append(VolumeZone(opening, price - self.tick_size,
                                        quantity, kind))
                opening = None
                quantity = Decimal(0)
        if opening is not None:
            zones.append(VolumeZone(opening, floor + self.tick_size * (count - 1),
                                    quantity, kind))
        return tuple(zones)

    @property
    def high_volume_zones(self):
        return self.zones("high")

    @property
    def low_volume_zones(self):
        return self.zones("low")

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


def _coverage_attestation(record, start, end, observed_count):
    """Validate B-owned ingestion_coverage evidence, not a caller's boolean.

    The record must describe this exact interval and be recorded after it
    closed; older backfills become available only at checked_at, never
    retroactively at exchange event time.
    """
    if not isinstance(record, dict):
        return None
    try:
        stamp = record["checked_at"]
        checksum = record["checksum"]
        if (record.get("source") != "trades_archive"
                or record.get("symbol") != "ETHUSDT"
                or record.get("instrument") != "linear"
                or record.get("status") != "verified"
                or record.get("quality") not in ("verified", "complete")
                or record.get("start_ts") != start or record.get("end_ts") != end
                or type(record.get("observed_rows")) is not int
                or type(record.get("expected_rows")) is not int
                or record["observed_rows"] != observed_count
                or record["expected_rows"] != observed_count
                or not isinstance(checksum, str) or len(checksum) != 64
                or any(ch not in "0123456789abcdef" for ch in checksum)
                or not isinstance(stamp, datetime) or stamp.tzinfo is None
                or stamp.utcoffset() != timedelta(0) or stamp < end):
            return None
        return stamp, checksum
    except (KeyError, TypeError, ValueError):
        return None


def build_profile(trades, start, end, tick_size, *, coverage_verified=False,
                  expected_trade_count=None, provenance=None):
    """Reject non-verified/partial windows; deterministic POC and contiguous 70% VA.

    The caller must prove coverage externally. A matching trade count alone
    does not establish archive or websocket completeness.
    """
    if not coverage_verified or expected_trade_count is None:
        return None
    if (start.tzinfo is None or end.tzinfo is None or
            start.utcoffset() != timedelta(0) or
            end.utcoffset() != timedelta(0) or start >= end):
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
            if ts.tzinfo is None or ts.utcoffset() != timedelta(0) or not start <= ts < end:
                return None
            price, qty = number(trade["price"]), number(trade["qty"])
            if price <= 0 or qty <= 0:
                return None
            trade_id = trade.get("trade_id")
            if provenance is not None and (not isinstance(trade_id, str) or not trade_id):
                return None
            if trade_id is not None:
                if trade_id in seen_ids:
                    return None
                seen_ids.add(trade_id)
            bucket = (price / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
            if bucket != price:
                return None
            bins[bucket] = bins.get(bucket, Decimal(0)) + qty
            count += 1
        if count != expected_trade_count or not bins:
            return None
        if (max(bins) - min(bins)) / tick > 20000:
            return None
        attestation = (_coverage_attestation(provenance, start, end, count)
                       if provenance is not None else None)
        if provenance is not None and attestation is None:
            return None
        total = sum(bins.values(), Decimal(0))
        poc = min(bins, key=lambda price: (-bins[price], price))
        left = right = poc
        low_bound, high_bound = min(bins), max(bins)
        allocated = bins[poc]
        target = total * Decimal("0.70")
        # The value area must be contiguous and may cross empty internal bins.
        # Never walk outside the observed price span on a zero-volume tie.
        max_expansions = int((high_bound - low_bound) / tick)
        for _ in range(max_expansions):
            if allocated >= target:
                break
            can_expand_low = left > low_bound
            can_expand_high = right < high_bound
            next_low = left - tick
            next_high = right + tick
            low_volume = (bins.get(next_low, Decimal(0)) if can_expand_low
                          else Decimal(-1))
            high_volume = (bins.get(next_high, Decimal(0)) if can_expand_high
                           else Decimal(-1))
            if low_volume >= high_volume:
                left = next_low
                allocated += max(low_volume, Decimal(0))
            else:
                right = next_high
                allocated += max(high_volume, Decimal(0))
        if allocated < target:
            return None
        return VolumeProfile(start, end, tick, poc, left, right, bins, total,
                             count, "externally_verified" if attestation
                             else "self_reported_unverified",
                             attestation[0] if attestation else None,
                             attestation[1] if attestation else None)
    except (InvalidOperation, TypeError, ValueError, KeyError, OverflowError):
        return None


def untouched_poc(profile, closed_bars, *, through, bar_span=timedelta(minutes=1)):
    """Naked only with gap-free, complete bars from profile end to cutoff."""
    if through <= profile.end or bar_span.total_seconds() <= 0:
        return False
    bars = list(closed_bars)
    if not bars or (through - profile.end) % bar_span != timedelta(0):
        return False
    expected = int((through - profile.end) / bar_span)
    if len(bars) != expected:
        return False
    try:
        for index, bar in enumerate(bars):
            if (bar["ts"] != profile.end + index * bar_span
                    or bar.get("closed", True) is not True):
                return False
            if number(bar["low"]) <= profile.poc <= number(bar["high"]):
                return False
        return True
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return False


def volume_signal(closed_candles, profile, fee_rate=Decimal("0.00055"),
                  slip_bps=Decimal("2"), *, parameters=None):
    """Candidate from a prior, verified profile; no future trade or bar access.

    The caller must separately confirm flow, freshness, fees and risk at entry.
    """
    from ..strategy import Signal, economics
    from . import Parameters

    if (profile is None or profile.coverage != "externally_verified"
            or profile.available_at is None or not profile.source_checksum):
        return None
    candles = list(closed_candles)
    if len(candles) < 4:
        return None
    prior, last = candles[-2], candles[-1]
    if (prior["ts"] < profile.end or last["ts"] <= prior["ts"]
            or last["ts"].tzinfo is None
            or last["ts"].utcoffset() != timedelta(0)
            or profile.available_at > last["ts"] + timedelta(minutes=1)):
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
        pre_signal = [bar for bar in candles[:-1] if bar["ts"] >= profile.end]
        # All volume signals require a fully observed post-profile 1m window.
        if (not pre_signal or pre_signal[0]["ts"] != profile.end or
                pre_signal[-1]["ts"] + timedelta(minutes=1) != last["ts"] or
                any(b["ts"] - a["ts"] != timedelta(minutes=1)
                    for a, b in zip(pre_signal, pre_signal[1:]))):
            return None
        nodes = [("naked_poc", profile.poc)]
        nodes += [("hvn", p) for p in profile.high_volume_nodes]
        nodes += [("lvn", p) for p in profile.low_volume_nodes]
        for zone in profile.high_volume_zones:
            nodes.extend((("hvn_zone", zone.low), ("hvn_zone", zone.high)))
        for zone in profile.low_volume_zones:
            nodes.extend((("lvn_zone", zone.low), ("lvn_zone", zone.high)))
        for kind, level in nodes:
            if abs(price - level) / price > Decimal("0.004"):
                continue
            if kind == "naked_poc" and not untouched_poc(profile, pre_signal,
                                                         through=last["ts"]):
                continue
            if kind in ("lvn", "lvn_zone"):
                if prev < level < price and price > opening:
                    side, name = "long", kind + "_breakout"
                elif prev > level > price and price < opening:
                    side, name = "short", kind + "_breakout"
                else:
                    continue
            elif prev >= level and low <= level < price and price > opening:
                side, name = "long", kind + "_rejection"
            elif prev <= level and high >= level > price and price < opening:
                side, name = "short", kind + "_rejection"
            else:
                continue
            direction = Decimal(1) if side == "long" else Decimal(-1)
            # Current bar range and historical zone width determine stop.
            buffer = max(high - low, price * Decimal("0.002")) * Decimal("0.5")
            anchor = min(low, level) if side == "long" else max(high, level)
            stop = anchor - direction * buffer
            take = price + direction * abs(price - stop) * Decimal("4")
            cost = economics(side, price, stop, take, fee_rate, slip_bps)
            if cost is None:
                continue
            params = Parameters() if parameters is None else parameters
            if not isinstance(params, Parameters):
                return None
            reason = {"strategy": name, "strategy_version": params.engine_identity(),
                      "rules_version": params.version,
                      **params.identity(), "level": str(level),
                      "profile_start": profile.start.isoformat(),
                      "profile_end": profile.end.isoformat(),
                      "profile_trade_count": profile.trade_count,
                      "profile_quality": profile.coverage,
                      "profile_available_at": profile.available_at.isoformat(),
                      "source_checksum": profile.source_checksum,
                      "signal_bar_ts": last["ts"].isoformat(),
                      "value_area": [str(profile.value_area_low), str(profile.value_area_high)],
                      "economics": cost, "requires_live_confirmation": True,
                      "source": "verified_tick_volume"}
            priority = {"naked_poc_rejection": 0, "hvn_rejection": 1,
                        "hvn_zone_rejection": 2, "lvn_breakout": 3,
                        "lvn_zone_breakout": 4}
            candidates.append((abs(price - level), priority.get(name, 9), name,
                               Signal(side, level, stop, take, reason)))
        return min(candidates, key=lambda item: (item[0], item[1], item[2]))[3] if candidates else None
    except (InvalidOperation, TypeError, ValueError, KeyError):
        return None
