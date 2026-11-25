"""Point-in-time dataset contract. All timestamps are UTC epoch milliseconds."""
from dataclasses import dataclass
from math import isfinite

FEATURES = ("oi", "cvd", "delta", "poc", "footprint", "tape_speed",
            "book_imbalance", "level_distance", "volatility")
LABELS = ("breakout", "bounce", "uncertain")

@dataclass(frozen=True)
class Observation:
    event_id: str
    decision_ms: int
    available_ms: tuple[int, ...]
    values: tuple[float, ...]
    exit_ms: int
    outcome: str
    gross_return: float
    costs: float

    @property
    def net_return(self):
        return self.gross_return - self.costs

def dataset(rows, as_of_ms):
    result = []
    seen = set()
    for row in rows:
        if row.event_id in seen:
            raise ValueError("duplicate event")
        seen.add(row.event_id)
        if len(row.values) != len(FEATURES) or len(row.available_ms) != len(FEATURES):
            raise ValueError("feature schema mismatch")
        if any(t > row.decision_ms for t in row.available_ms):
            raise ValueError("future feature")
        if row.exit_ms <= row.decision_ms or row.outcome not in LABELS:
            raise ValueError("invalid outcome")
        if not all(isfinite(v) for v in (*row.values, row.gross_return, row.costs)) or row.costs < 0:
            raise ValueError("invalid numeric input")
        if row.exit_ms <= as_of_ms:
            result.append(row)
    return sorted(result, key=lambda r: (r.decision_ms, r.event_id))

def split(rows, validation_start, test_start, embargo_ms=0):
    if validation_start >= test_start or embargo_ms < 0:
        raise ValueError("invalid boundaries")
    train = [r for r in rows if r.decision_ms < validation_start and r.exit_ms < validation_start - embargo_ms]
    validation = [r for r in rows if validation_start <= r.decision_ms < test_start and r.exit_ms < test_start - embargo_ms]
    test = [r for r in rows if r.decision_ms >= test_start]
    if not all((train, validation, test)):
        raise ValueError("empty chronological partition")
    return train, validation, test

def walk_forward(rows, boundaries, embargo_ms=0):
    if embargo_ms < 0 or len(boundaries) < 2 or any(a >= b for a,b in zip(boundaries,boundaries[1:])):
        raise ValueError("invalid fold boundaries")
    for start, end in zip(boundaries, boundaries[1:]):
        train = [r for r in rows if r.decision_ms < start and r.exit_ms < start - embargo_ms]
        validation = [r for r in rows if start <= r.decision_ms < end and r.exit_ms < end]
        if not train or not validation:
            raise ValueError("empty fold")
        yield train, validation
