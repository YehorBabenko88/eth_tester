"""Deterministic, causal paper strategy rules."""
from dataclasses import dataclass, asdict
from decimal import Decimal as D
import hashlib
import json

@dataclass(frozen=True)
class Parameters:
    version: str = "eth_rules_v2"
    min_score: int = 70
    min_net: str = "0.002"
    funding_reserve: str = "0.001"
    reward_risk: str = "2"
    max_distance: str = "0.004"

    def identity(self):
        payload = asdict(self)
        return {"parameters": payload, "parameter_sha256": hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode()).hexdigest()}

def pattern(side, level, prior, bar, broken):
    """Return direction and pattern using only already closed bars."""
    p, c, o, h, low = map(D, (prior["close"], bar["close"], bar["open"], bar["high"], bar["low"]))
    if side == "resistance":
        if not broken and p <= level < c:
            return "long", "breakout"
        if broken and p > level and low <= level < c:
            return "long", "retest"
        if broken and p > level > c:
            return "short", "false_breakout"
        if not broken and h > level > c and c < o:
            return "short", "rejection"
    else:
        if not broken and p >= level > c:
            return "short", "breakout"
        if broken and p < level and h >= level > c:
            return "short", "retest"
        if broken and p < level < c:
            return "long", "false_breakout"
        if not broken and low < level < c and c > o:
            return "long", "rejection"
    return None
