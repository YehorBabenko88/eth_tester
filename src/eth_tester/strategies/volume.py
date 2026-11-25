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

    def __post_init__(self):
        # Direct construction cannot bypass bounded resource limits.
        self._bounded_bins()

    def _bounded_bins(self):
        try:
            values = self.by_price
            if not isinstance(values, dict) or not 1 <= len(values) <= 20001:
                raise ValueError("unsafe_profile_bins")
            tick = number(self.tick_size)
            if tick <= 0:
                raise ValueError("unsafe_tick_size")
            prices = tuple(values)
            if (any(type(p) is not Decimal or not p.is_finite() or p <= 0
                    for p in prices)
                    or any(type(q) is not Decimal or not q.is_finite() or q < 0
                           for q in values.values())):
                raise ValueError("invalid_profile_bins")
            span = (max(prices) - min(prices)) / tick
            if (span > 20000 or span != span.to_integral_value()
                    or any(p / tick != (p / tick).to_integral_value()
                           for p in prices)):
                raise ValueError("unsafe_profile_span")
            if (self.poc not in values
                    or not min(prices) <= self.value_area_low <= self.poc
                    <= self.value_area_high <= max(prices)
                    or number(self.total_quantity) !=
                    sum(values.values(), Decimal(0))
                    or self.total_quantity <= 0
                    or type(self.trade_count) is not int
                    or self.trade_count <= 0
                    or self.coverage not in
                    ("externally_verified", "self_reported_unverified")):
                raise ValueError("inconsistent_profile")
            if (self.start.tzinfo is None or self.end.tzinfo is None
                    or self.start.utcoffset() != timedelta(0)
                    or self.end.utcoffset() != timedelta(0)
                    or self.start >= self.end):
                raise ValueError("invalid_profile_window")
            if self.coverage == "externally_verified" and (
                    self.available_at is None
                    or self.available_at.tzinfo is None
                    or self.available_at.utcoffset() != timedelta(0)
                    or self.available_at < self.end
                    or not isinstance(self.source_checksum, str)
                    or len(self.source_checksum) != 64
                    or any(c not in "0123456789abcdef"
                           for c in self.source_checksum)):
                raise ValueError("unverified_profile_identity")
            return int(span) + 1
        except (TypeError, KeyError, InvalidOperation, ArithmeticError,
                AttributeError, OverflowError) as exc:
            raise ValueError("invalid_profile_geometry") from exc

    def zones(self, kind):
        """Contiguous tick zones inside the actually observed price range.

        Empty internal bins count as low volume, not as traded prices.
        """
        if kind not in ("high", "low"):
            raise ValueError("unknown_zone_kind")
        count = self._bounded_bins()
        peak = max(self.by_price.values())
        threshold = peak * (Decimal("0.65") if kind == "high"
                            else Decimal("0.35"))
        floor = min(self.by_price)
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
        self._bounded_bins()
        peak = max(self.by_price.values())
        return tuple(sorted(price for price, qty in self.by_price.items()
                            if price != self.poc and qty >= peak * Decimal("0.65")
                            and qty >= self.by_price.get(price - self.tick_size, Decimal(0))
                            and qty >= self.by_price.get(price + self.tick_size, Decimal(0))))

    @property
    def low_volume_nodes(self):
        self._bounded_bins()
        peak = max(self.by_price.values())
        return tuple(sorted(price for price, qty in self.by_price.items()
                            if Decimal(0) < qty <= peak * Decimal("0.35")
                            and qty < self.by_price.get(price - self.tick_size, Decimal(0))
                            and qty < self.by_price.get(price + self.tick_size, Decimal(0))))


def _coverage_attestation(record, start, end, observed_count):
    """Verify a full UTC-day archive plus every matching minute check.

    The source coverage window describes exchange event time; the archive and
    minute-check timestamps describe real observation. Matching volume alone
    never proves the exchange published every public trade ID.
    """
    if not isinstance(record, dict):
        return None
    try:
        archive = record["archive"]
        minutes = record["minute_checks"]
        if (not isinstance(archive, dict) or not isinstance(minutes, (list, tuple))
                or not isinstance(start, datetime) or not isinstance(end, datetime)
                or start.utcoffset() != timedelta(0)
                or end.utcoffset() != timedelta(0) or start >= end
                or start.second or start.microsecond or end.second or end.microsecond
                or (end - start) % timedelta(minutes=1) != timedelta(0)):
            return None
        day = start.replace(hour=0, minute=0, second=0, microsecond=0)
        if not (day <= start < end <= day + timedelta(days=1)):
            return None
        source_checksum = archive["checksum"]
        checked = archive["checked_at"]
        if (archive.get("source") != "trades_archive"
                or archive.get("symbol") != "ETHUSDT"
                or archive.get("instrument") != "linear"
                or archive.get("interval") != ""
                or archive.get("status") != "verified"
                or archive.get("start_ts") != day
                or archive.get("end_ts") != day + timedelta(days=1)
                or not isinstance(source_checksum, str)
                or len(source_checksum) != 64
                or any(char not in "0123456789abcdef" for char in source_checksum)
                or not isinstance(checked, datetime) or checked.tzinfo is None
                or checked.utcoffset() != timedelta(0) or checked < end
                or archive.get("quality") == "partial_unverified"):
            return None
        expected_minutes = int((end - start) / timedelta(minutes=1))
        if len(minutes) != expected_minutes:
            return None
        proof_at = checked
        total_trades = 0
        for index, minute in enumerate(minutes):
            expected = start + index * timedelta(minutes=1)
            stamp = minute["minute_ts"]
            proof_time = minute["checked_at"]
            if (stamp != expected or minute.get("symbol") != "ETHUSDT"
                    or minute.get("instrument") != "linear"
                    or minute.get("archive_source") != "trades_archive"
                    or minute.get("archive_sha256") != source_checksum
                    or minute.get("status") != "match"
                    or not isinstance(proof_time, datetime)
                    or proof_time.tzinfo is None
                    or proof_time.utcoffset() != timedelta(0)
                    or proof_time < expected + timedelta(minutes=1)):
                return None
            qty = number(minute["trade_base_qty"])
            official = number(minute["candle_base_volume"])
            count = minute["trade_count"]
            if (type(count) is not int or count <= 0
                    or qty <= 0 or official <= 0
                    or abs(qty - official) > max(
                        Decimal("0.001"), official * Decimal("0.0005"))):
                return None
            total_trades += count
            proof_at = max(proof_at, proof_time)
        if total_trades != observed_count:
            return None
        return proof_at, source_checksum
    except (KeyError, TypeError, InvalidOperation, ValueError, OverflowError):
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
                    or bar.get("closed") is not True):
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
    if any(b.get("closed") is not True for b in candles):
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
