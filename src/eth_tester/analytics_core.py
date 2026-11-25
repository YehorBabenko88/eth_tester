"""Causal, deterministic ETHUSDT analytics. Pure functions: no database or live feed.

All timestamps denote *closed* observations. Values are unavailable until the
close of their source interval. Missing inputs return None rather than zero.
Volume is base-asset quantity; prices and quantities use Decimal.
"""
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from math import log, sqrt

FRAMES = ("1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "W", "M")
MINUTES = {"1": 1, "3": 3, "5": 5, "15": 15, "30": 30, "60": 60,
           "120": 120, "240": 240, "360": 360, "720": 720, "D": 1440}
ZERO = Decimal(0)


def decimal(value):
    return value if isinstance(value, Decimal) else Decimal(str(value))


def close_time(start, tf):
    """UTC close, including ISO Monday weeks and variable calendar months."""
    if start.tzinfo is None:
        raise ValueError("Timezone-aware UTC candle timestamps required")
    if tf in MINUTES:
        return start + timedelta(minutes=MINUTES[tf])
    if tf == "W":
        return start + timedelta(days=7)
    if tf == "M":
        year = start.year + (start.month == 12)
        month = start.month % 12 + 1
        return start.replace(year=year, month=month, day=1)
    raise ValueError("Unsupported timeframe")


def bucket(ts, tf):
    if ts.tzinfo is None:
        raise ValueError("Timezone-aware UTC timestamps required")
    t = ts.astimezone(timezone.utc)
    if tf in MINUTES:
        span = MINUTES[tf] * 60
        return datetime.fromtimestamp(int(t.timestamp()) // span * span, timezone.utc)
    if tf == "W":
        return (t - timedelta(days=t.weekday())).replace(hour=0, minute=0, second=0,
                                                         microsecond=0)
    if tf == "M":
        return t.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    raise ValueError("Unsupported timeframe")


def value_profile(volume_at_price, fraction=Decimal("0.70")):
    """POC and contiguous 70%-value area. Ties prefer lower price."""
    items = sorted((decimal(p), decimal(q)) for p, q in volume_at_price.items())
    items = [(p, q) for p, q in items if q > 0]
    if not items:
        return {"poc": None, "vpoc": None, "vah": None, "val": None,
                "hvn": [], "lvn": [], "total": ZERO}
    if not ZERO < decimal(fraction) <= 1:
        raise ValueError("Value-area fraction must be in (0,1]")
    volumes = [q for _, q in items]
    total = sum(volumes, ZERO)
    peak = min(range(len(items)), key=lambda i: (-volumes[i], items[i][0]))
    left = right = peak
    covered = volumes[peak]
    while covered < total * decimal(fraction):
        lv = volumes[left - 1] if left > 0 else Decimal("-1")
        rv = volumes[right + 1] if right + 1 < len(items) else Decimal("-1")
        if lv >= rv and left > 0:
            left -= 1
            covered += volumes[left]
        elif right + 1 < len(items):
            right += 1
            covered += volumes[right]
        else:
            break
    hvn = [items[i][0] for i in range(1, len(items) - 1)
           if volumes[i] > volumes[i - 1] and volumes[i] > volumes[i + 1]]
    lvn = [items[i][0] for i in range(1, len(items) - 1)
           if volumes[i] < volumes[i - 1] and volumes[i] < volumes[i + 1]]
    return {"poc": items[peak][0], "vpoc": items[peak][0],
            "vah": items[right][0], "val": items[left][0],
            "hvn": hvn, "lvn": lvn, "total": total}


def footprint(trades, *, imbalance_ratio=Decimal("3"), min_quantity=ZERO,
              min_stack=3):
    """Aggressor-side volume at exact prices and diagonal stacked imbalance.

    Source side must be aggressor Buy/Sell; no maker-side inference.
    """
    levels = {}
    previous_time = None
    count = 0
    for trade in trades:
        p = decimal(trade["price"])
        qty = decimal(trade["qty"])
        side = trade["side"]
        ts = trade.get("ts")
        if qty <= 0 or side not in ("Buy", "Sell"):
            raise ValueError("Trade quantity and aggressor side invalid")
        if ts is not None and previous_time is not None and ts < previous_time:
            raise ValueError("Trades must be chronologically ordered")
        previous_time = ts if ts is not None else previous_time
        row = levels.setdefault(p, {"buy": ZERO, "sell": ZERO, "trades": 0})
        row[side.lower()] += qty
        row["trades"] += 1
        count += 1
    prices = sorted(levels)
    buy = sum((v["buy"] for v in levels.values()), ZERO)
    sell = sum((v["sell"] for v in levels.values()), ZERO)
    ratio = decimal(imbalance_ratio)
    threshold = decimal(min_quantity)
    if ratio <= 1 or threshold < 0 or min_stack < 1:
        raise ValueError("Invalid imbalance parameters")
    flags = {"buy": [], "sell": []}
    for idx, p in enumerate(prices):
        row = levels[p]
        if idx > 0:
            other = levels[prices[idx - 1]]["sell"]
            if row["buy"] >= threshold and row["buy"] > 0 and (
                    other == 0 or row["buy"] >= other * ratio):
                flags["buy"].append(p)
        if idx + 1 < len(prices):
            other = levels[prices[idx + 1]]["buy"]
            if row["sell"] >= threshold and row["sell"] > 0 and (
                    other == 0 or row["sell"] >= other * ratio):
                flags["sell"].append(p)
    stack = {}
    for side, marked in flags.items():
        marked_set = set(marked)
        runs = []
        run = []
        for price in prices:
            if price in marked_set:
                run.append(price)
            else:
                if len(run) >= min_stack:
                    runs.append(run)
                run = []
        if len(run) >= min_stack:
            runs.append(run)
        stack[side] = runs
    profile = value_profile({p: v["buy"] + v["sell"] for p, v in levels.items()})
    return {"levels": levels, "buy": buy, "sell": sell, "delta": buy - sell,
            "volume": buy + sell, "trades": count, "profile": profile,
            "imbalance": flags, "stacked_imbalance": stack}


def market_indicators(bars, *, previous_close=None, atr_period=14):
    """Only closed bars. ATR uses Wilder smoothing; realized vol is unannualized."""
    if atr_period < 1:
        raise ValueError("ATR period must be positive")
    trs = []
    returns = []
    pv = ZERO
    vol = ZERO
    last = decimal(previous_close) if previous_close is not None else None
    low_price = high_price = None
    for item in bars:
        if not item.get("closed", True):
            raise ValueError("Unclosed bar would introduce look-ahead")
        hi, lo, close = (decimal(item[k]) for k in ("high", "low", "close"))
        if hi < lo or close <= 0 or hi <= 0 or lo <= 0:
            raise ValueError("Invalid OHLC")
        trs.append(max(hi - lo, abs(hi - last), abs(lo - last)) if last else hi - lo)
        if last is not None:
            returns.append(log(float(close / last)))
        weight = decimal(item["volume"]) if item.get("volume") is not None else ZERO
        typical = (hi + lo + close) / 3
        pv += typical * weight
        vol += weight
        low_price = min(lo, low_price) if low_price is not None else lo
        high_price = max(hi, high_price) if high_price is not None else hi
        last = close
    atr = None
    if len(trs) >= atr_period:
        atr = sum(trs[:atr_period], ZERO) / atr_period
        for value in trs[atr_period:]:
            atr = (atr * (atr_period - 1) + value) / atr_period
    return {"atr": atr, "realized_volatility": sqrt(sum(x * x for x in returns))
            if returns else None, "range": high_price - low_price
            if high_price is not None else None,
            "range_pct": (high_price - low_price) / last * 100
            if last is not None else None,
            "vwap": pv / vol if vol > 0 else None}


def tape_metrics(trades, *, duration_seconds, large_trade_qty,
                 reference_tps=None, reference_price=None):
    """Measurable proxies, never assert that absorption/exhaustion was proven."""
    if duration_seconds <= 0:
        raise ValueError("Duration must be positive")
    rows = list(trades)
    fp = footprint(rows)
    large = [r for r in rows if decimal(r["qty"]) >= decimal(large_trade_qty)]
    tps = Decimal(len(rows)) / decimal(duration_seconds)
    price_change = (decimal(rows[-1]["price"]) - decimal(rows[0]["price"])
                    if rows else None)
    imbalance = abs(fp["delta"]) / fp["volume"] if fp["volume"] else None
    absorbed_proxy = bool(price_change is not None and fp["volume"]
                          and imbalance >= Decimal("0.6")
                          and price_change == 0)
    exhausted_proxy = bool(reference_tps is not None and tps <
                           decimal(reference_tps) * Decimal("0.5"))
    return {"tps": tps, "large_count": len(large),
            "large_volume": sum((decimal(r["qty"]) for r in large), ZERO),
            "buy": fp["buy"], "sell": fp["sell"], "delta": fp["delta"],
            "absorption_proxy": absorbed_proxy,
            "exhaustion_proxy": exhausted_proxy,
            "price_change": price_change,
            "reference_price": decimal(reference_price)
            if reference_price is not None else None}


def delta_open_interest(current, previous):
    if current is None or previous is None:
        return None
    return decimal(current) - decimal(previous)


def quality_flags(*, archive_complete, expected_candles, actual_candles,
                  trade_volume=None, candle_volume=None, tolerance=Decimal("0.0005")):
    """No 'verified' result without both time-grid and comparable base quantities."""
    gaps = sorted(set(expected_candles) - set(actual_candles))
    unexpected = sorted(set(actual_candles) - set(expected_candles))
    result = {"complete": False, "gaps": gaps, "unexpected": unexpected,
              "volume_match": None, "reason": "unverified"}
    if not archive_complete:
        result["reason"] = "archive_incomplete"
    elif gaps or unexpected:
        result["reason"] = "candle_grid_mismatch"
    elif trade_volume is None or candle_volume is None:
        result["reason"] = "volume_unavailable"
    else:
        diff = abs(decimal(trade_volume) - decimal(candle_volume))
        permitted = max(Decimal("0.001"), abs(decimal(candle_volume)) * decimal(tolerance))
        result["volume_match"] = diff <= permitted
        result["complete"] = diff <= permitted
        result["reason"] = "verified" if result["complete"] else "volume_mismatch"
    return result


@dataclass
class TrackedLevel:
    side: str
    origin_ts: datetime
    confirmation_ts: datetime
    price: Decimal
    tf: str
    status: str = "unbroken"
    touches: int = 0
    broken_at: datetime | None = None
    retested_at: datetime | None = None
    invalidated_at: datetime | None = None
    events: list = field(default_factory=list)

    def event(self, name, timestamp):
        self.events.append((name, timestamp))


class LevelEngine:
    """O(flank) pivot memory plus O(active levels) updates per closed candle.

    Confirm a pivot only when its RIGHT flank has fully closed, then track it
    starting with the NEXT candle. Events carry bar-close timestamps.
    """

    def __init__(self, tf, flank=2, tolerance=ZERO):
        if tf not in FRAMES or flank < 1 or decimal(tolerance) < 0:
            raise ValueError("Invalid level engine parameters")
        self.tf = tf
        self.flank = flank
        self.tolerance = decimal(tolerance)
        self.history = deque(maxlen=2 * flank + 1)
        self.levels = {}
        self.last_closed = None

    def update(self, bar):
        if not bar.get("closed", True):
            raise ValueError("Only closed candles may be processed")
        ts = bar["ts"]
        availability = close_time(ts, self.tf)
        if self.last_closed is not None and availability <= self.last_closed:
            raise ValueError("Bars must be ordered; replay corrections from checkpoint")
        self.last_closed = availability
        high, low, close = (decimal(bar[k]) for k in ("high", "low", "close"))
        if not low <= close <= high:
            raise ValueError("Invalid candle range")
        emitted = []
        for level in self.levels.values():
            if level.status == "invalidated":
                continue
            price = level.price
            touched = low - self.tolerance <= price <= high + self.tolerance
            cross = (close > price + self.tolerance if level.side == "resistance"
                     else close < price - self.tolerance)
            reversed_cross = (close < price - self.tolerance if level.side == "resistance"
                              else close > price + self.tolerance)
            if level.status == "unbroken":
                if cross:
                    level.status = "broken"
                    level.broken_at = availability
                    level.event("break", availability)
                    emitted.append((level, "break"))
                elif touched:
                    level.touches += 1
                    level.event("touch", availability)
                    emitted.append((level, "touch"))
            elif level.status == "broken":
                if reversed_cross:
                    level.status = "invalidated"
                    level.invalidated_at = availability
                    level.event("invalidation", availability)
                    emitted.append((level, "invalidation"))
                elif touched:
                    level.status = "retested"
                    level.retested_at = availability
                    level.event("retest", availability)
                    emitted.append((level, "retest"))
            elif level.status == "retested" and reversed_cross:
                level.status = "invalidated"
                level.invalidated_at = availability
                level.event("invalidation", availability)
                emitted.append((level, "invalidation"))
        self.history.append({"ts": ts, "high": high, "low": low, "close": close})
        if len(self.history) == self.history.maxlen:
            bars = list(self.history)
            pivot = bars[self.flank]
            neighbors = bars[:self.flank] + bars[self.flank + 1:]
            candidates = []
            if all(pivot["high"] > x["high"] for x in neighbors):
                candidates.append(("resistance", pivot["high"]))
            if all(pivot["low"] < x["low"] for x in neighbors):
                candidates.append(("support", pivot["low"]))
            for side, price in candidates:
                key = (side, pivot["ts"], price)
                if key in self.levels:
                    continue
                level = TrackedLevel(side, pivot["ts"], availability, price, self.tf)
                level.event("confirmed", availability)
                self.levels[key] = level
                emitted.append((level, "confirmed"))
        return emitted

    def nearest(self, price, limit=5):
        last = decimal(price)
        active = [x for x in self.levels.values() if x.status == "unbroken"]
        active.sort(key=lambda x: (abs(x.price - last), -x.touches,
                                   x.confirmation_ts, x.price))
        return [{"level": x, "distance": x.price - last,
                 "distance_pct": (x.price - last) / last * 100 if last else None,
                 "score": x.touches + 1} for x in active[:limit]]
