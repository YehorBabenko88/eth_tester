"""Causal Naked POC state: untouched VPOCs require verified profile provenance."""
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .analytics_core import FRAMES, close_time, decimal


@dataclass
class NakedPOC:
    tf: str
    origin_ts: datetime
    price: Decimal
    available_at: datetime
    status: str = "naked"
    touched_at: datetime | None = None


class NakedPOCBook:
    """Incremental observed-only naked-POC registry; never backdates availability.

    Inputs must arrive in processing order with a known verification status.
    An archive loaded retrospectively cannot become a historically available
    profile before its true observed_at. No future candle tests are used.
    """

    def __init__(self):
        self.active = {}
        self.records = {}
        self.last_bar_close = {}

    def register(self, tf, origin_ts, price, *, available_at, quality,
                 observed_at=None):
        if tf not in FRAMES or available_at.tzinfo is None:
            raise ValueError("Invalid interval or unavailable timestamp")
        profile_close = close_time(origin_ts, tf)
        if available_at < profile_close:
            raise ValueError("POC may not exist before the source bucket closes")
        if quality != "verified" or observed_at is None:
            return None
        if observed_at.tzinfo is None:
            raise ValueError("Observed timestamp must be timezone-aware")
        if available_at < observed_at:
            raise ValueError("Feature cannot be available before it is observed")
        key = (tf, origin_ts, decimal(price))
        if key in self.records:
            return self.records[key]
        state = NakedPOC(tf, origin_ts, decimal(price), available_at)
        self.records[key] = state
        self.active[key] = state
        return state

    def observe(self, tf, bar):
        if tf not in FRAMES or not bar.get("closed", True):
            raise ValueError("Naked POC requires closed candles")
        current_close = close_time(bar["ts"], tf)
        previous = self.last_bar_close.get(tf)
        if previous is not None and current_close <= previous:
            raise ValueError("Out-of-order candle: replay from checkpoint")
        lo, hi = decimal(bar["low"]), decimal(bar["high"])
        if lo > hi:
            raise ValueError("Invalid candle range")
        touched = []
        for key, state in list(self.active.items()):
            if state.tf != tf or state.available_at >= current_close:
                continue
            if lo <= state.price <= hi:
                state.status = "touched"
                state.touched_at = current_close
                del self.active[key]
                touched.append(state)
        self.last_bar_close[tf] = current_close
        return touched

    def nearest(self, tf, price, limit=5):
        market = decimal(price)
        levels = sorted(
            (s for s in self.active.values() if s.tf == tf),
            key=lambda s: (abs(s.price - market), s.available_at, s.price))
        return [{"state": s, "distance": s.price - market,
                 "distance_pct": (s.price - market) / market * 100
                 if market else None} for s in levels[:limit]]
