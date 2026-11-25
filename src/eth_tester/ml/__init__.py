"""Causal three-class learning engine. No exchange orders or execution side effects."""
from dataclasses import dataclass, asdict
from hashlib import sha256
import json
import math

FEATURE_NAMES = ("oi", "cvd", "delta", "poc", "footprint", "tape_speed",
                 "book_imbalance", "level_distance", "volatility")
CLASSES = ("breakout", "bounce", "uncertain")
EPS = 1e-12


@dataclass(frozen=True)
class Example:
    event_id: str
    decision_ms: int
    exit_ms: int
    values: tuple[float, ...]
    available_ms: tuple[int, ...]
    outcome: str
    gross_return: float
    costs: float

    @property
    def net_return(self):
        return self.gross_return - self.costs


def validate(rows, cutoff_ms):
    """Never train with an unfinished label or information not available at decision."""
    seen = set()
    checked = []
    for row in rows:
        if row.event_id in seen:
            raise ValueError("duplicate event_id")
        seen.add(row.event_id)
        if len(row.values) != len(FEATURE_NAMES) or len(row.available_ms) != len(FEATURE_NAMES):
            raise ValueError("feature schema mismatch")
        if row.decision_ms >= row.exit_ms or any(
            not isinstance(t, int) or t > row.decision_ms for t in row.available_ms
        ):
            raise ValueError("future information or invalid horizon")
        if row.outcome not in CLASSES or not all(
            math.isfinite(float(x)) for x in (*row.values, row.gross_return, row.costs)
        ):
            raise ValueError("invalid label or numerical value")
        if row.exit_ms <= cutoff_ms:
            checked.append(row)
    return sorted(checked, key=lambda r: (r.decision_ms, r.event_id))


def chronological(rows, validation_ms, test_ms, embargo_ms=0):
    if validation_ms >= test_ms or embargo_ms < 0:
        raise ValueError("invalid boundary")
    train = [r for r in rows if r.decision_ms < validation_ms
             and r.exit_ms < validation_ms - embargo_ms]
    val = [r for r in rows if validation_ms <= r.decision_ms < test_ms
           and r.exit_ms < test_ms - embargo_ms]
    test = [r for r in rows if r.decision_ms >= test_ms]
    if not all((train, val, test)):
        raise ValueError("empty partition")
    return train, val, test


def purged_walk_forward(rows, boundaries, embargo_ms=0):
    if embargo_ms < 0 or len(boundaries) < 2 or any(
        a >= b for a, b in zip(boundaries, boundaries[1:])
    ):
        raise ValueError("invalid boundaries")
    for start, end in zip(boundaries, boundaries[1:]):
        train = [r for r in rows if r.decision_ms < start
                 and r.exit_ms < start - embargo_ms]
        val = [r for r in rows if start <= r.decision_ms < end and r.exit_ms < end]
        if not train or not val:
            raise ValueError("empty fold")
        yield train, val


def _softmax(scores):
    shift = max(scores)
    e = [math.exp(s - shift) for s in scores]
    return tuple(x / sum(e) for x in e)


@dataclass(frozen=True)
class Model:
    version: str
    mean: tuple[float, ...]
    std: tuple[float, ...]
    weights: tuple[tuple[float, ...], ...]
    class_net: tuple[float, ...]
    train_to_ms: int

    def probabilities(self, values):
        if len(values) != len(FEATURE_NAMES) or not all(math.isfinite(float(x)) for x in values):
            raise ValueError("invalid inference features")
        x = [1.0] + [(float(v) - m) / s for v, m, s in zip(values, self.mean, self.std)]
        return dict(zip(CLASSES, _softmax([sum(a*b for a, b in zip(w, x))
                                          for w in self.weights])))

    def predict(self, values):
        p = self.probabilities(values)
        return max(CLASSES, key=lambda label: p[label])

    def expected_net(self, values):
        p = self.probabilities(values)
        return sum(p[label] * reward for label, reward in zip(CLASSES, self.class_net))

    def as_json(self):
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, data):
        obj = json.loads(data)
        return cls(obj["version"], tuple(obj["mean"]), tuple(obj["std"]),
                   tuple(tuple(w) for w in obj["weights"]), tuple(obj["class_net"]),
                   obj["train_to_ms"])


def fit(train, *, steps=240, learning_rate=0.035):
    """Deterministic regularized softmax; scaling and payoff estimates only from train."""
    if len(train) < 12 or len({r.outcome for r in train}) != len(CLASSES):
        raise ValueError("need >=12 labeled observations spanning all classes")
    if steps < 1 or learning_rate <= 0:
        raise ValueError("invalid optimization")
    ordered = sorted(train, key=lambda r: (r.decision_ms, r.event_id))
    n = len(ordered)
    means = [sum(r.values[i] for r in ordered) / n for i in range(len(FEATURE_NAMES))]
    stds = [max(1e-9, math.sqrt(sum((r.values[i] - means[i])**2 for r in ordered)/n))
            for i in range(len(FEATURE_NAMES))]
    matrix = [[1.0] + [max(-8.0, min(8.0, (r.values[i]-means[i])/stds[i]))
                       for i in range(len(FEATURE_NAMES))] for r in ordered]
    weights = [[0.0] * (len(FEATURE_NAMES)+1) for _ in CLASSES]
    for _ in range(steps):
        grad = [[0.0] * len(weights[0]) for _ in CLASSES]
        for row, item in zip(matrix, ordered):
            probs = _softmax([sum(a*b for a, b in zip(w, row)) for w in weights])
            for k, label in enumerate(CLASSES):
                err = probs[k] - (item.outcome == label)
                for j, value in enumerate(row):
                    grad[k][j] += err*value
        for k in range(len(CLASSES)):
            for j in range(len(weights[0])):
                reg = 0.0 if j == 0 else .01*weights[k][j]
                weights[k][j] -= learning_rate*(grad[k][j]/n + reg)
    payoffs = tuple(sum(r.net_return for r in ordered if r.outcome == label) /
                    sum(r.outcome == label for r in ordered) for label in CLASSES)
    fingerprint = sha256(json.dumps(
        [(r.event_id, r.decision_ms, r.exit_ms, r.values, r.available_ms,
          r.outcome, r.gross_return, r.costs) for r in ordered],
        separators=(",", ":")).encode()).hexdigest()[:16]
    return Model("softmax3-" + fingerprint, tuple(means), tuple(stds),
                 tuple(tuple(w) for w in weights), payoffs, max(r.exit_ms for r in ordered))


def brier(model, rows):
    if not rows:
        raise ValueError("empty evaluation")
    return sum(sum((model.probabilities(r.values)[label] - (r.outcome == label))**2
                   for label in CLASSES) for r in rows)/len(rows)


def evaluate(model, rows, minimum_ev=0.0):
    """Both candidate and always-trade baseline use realized NET returns after costs."""
    if not rows:
        raise ValueError("empty evaluation")
    if any(r.decision_ms <= model.train_to_ms for r in rows):
        raise ValueError("evaluation overlaps training labels")
    picked = [r for r in rows if model.predict(r.values) != "uncertain"
              and model.expected_net(r.values) > minimum_ev]
    return {"n": len(rows), "traded": len(picked),
            "brier": brier(model, rows),
            "candidate_net": sum(r.net_return for r in picked),
            "candidate_mean_net_per_signal": sum(r.net_return for r in picked)/len(rows),
            "baseline_always_net": sum(r.net_return for r in rows),
            "baseline_mean_net_per_signal": sum(r.net_return for r in rows)/len(rows)}


def population_shift(reference, current, bins=10):
    """Population stability index using TRAIN-only cutpoints, with smoothing."""
    if len(reference) < 20 or len(current) < 10 or bins < 2:
        raise ValueError("insufficient drift samples")
    result = []
    for i in range(len(FEATURE_NAMES)):
        ordered = sorted(r.values[i] for r in reference)
        edges = [ordered[min(len(ordered)-1, j*len(ordered)//bins)]
                 for j in range(1, bins)]
        def frequencies(rows):
            counts = [1.0] * bins
            for row in rows:
                index = sum(row.values[i] > edge for edge in edges)
                counts[index] += 1
            total = sum(counts)
            return [n/total for n in counts]
        a, b = frequencies(reference), frequencies(current)
        result.append(sum((q-p)*math.log(q/p) for p, q in zip(a, b)))
    return dict(zip(FEATURE_NAMES, result))


@dataclass(frozen=True)
class Gates:
    min_train: int = 100
    min_validation: int = 30
    min_shadow: int = 30
    min_trades: int = 20
    max_brier: float = 0.55
    max_drift: float = 0.2
    min_improvement: float = 0.0


def approve_candidate(candidate, champion, train, validation, shadow, gates=Gates()):
    """Fail closed. Paper *registry* approval only, never an exchange order."""
    if len(train) < gates.min_train or len(validation) < gates.min_validation or len(shadow) < gates.min_shadow:
        return False, "insufficient independent samples"
    if not max(r.exit_ms for r in train) < min(r.decision_ms for r in validation):
        return False, "purge violation"
    if not max(r.exit_ms for r in validation) < min(r.decision_ms for r in shadow):
        return False, "shadow overlap"
    if candidate.train_to_ms >= min(r.decision_ms for r in validation):
        return False, "training leakage"
    try:
        val = evaluate(candidate, validation)
        sh = evaluate(candidate, shadow)
        drift = population_shift(train, shadow)
        incumbent = evaluate(champion, shadow) if champion is not None else None
    except (ValueError, OverflowError):
        return False, "unverifiable evaluation"
    if max(drift.values()) > gates.max_drift:
        return False, "feature drift"
    if val["traded"] < gates.min_trades or sh["traded"] < gates.min_trades:
        return False, "not enough independent trades"
    if val["brier"] > gates.max_brier or sh["brier"] > gates.max_brier:
        return False, "calibration gate failed"
    if val["candidate_mean_net_per_signal"] <= max(
        0.0, val["baseline_mean_net_per_signal"] + gates.min_improvement
    ):
        return False, "validation net benchmark failed"
    if sh["candidate_mean_net_per_signal"] <= max(
        0.0, sh["baseline_mean_net_per_signal"] + gates.min_improvement,
        incumbent["candidate_mean_net_per_signal"] + gates.min_improvement if incumbent else 0.0
    ):
        return False, "shadow net benchmark failed"
    if incumbent and sh["brier"] >= incumbent["brier"]:
        return False, "champion calibration not beaten"
    return True, "passed independent paper-only gates"


def should_retrain(last_trained_count, available_count, minimum_new=100):
    if min(last_trained_count, available_count) < 0 or minimum_new < 1:
        raise ValueError("invalid sample counts")
    return available_count - last_trained_count >= minimum_new


def rollback_required(champion_metrics, recent_metrics, *, max_brier_increase=.05,
                      max_net_decrease=.01, min_samples=30):
    if recent_metrics.get("n", 0) < min_samples:
        return False
    return (recent_metrics["brier"] > champion_metrics["brier"] + max_brier_increase
            or recent_metrics["candidate_mean_net_per_signal"] <
            champion_metrics["candidate_mean_net_per_signal"] - max_net_decrease)
