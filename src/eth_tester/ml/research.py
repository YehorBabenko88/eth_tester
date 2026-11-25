"""Reproducible *research-only* comparison of strictly historical ML-v2 examples.

This module has no DB writes, no external network and no order/approval path.
Prospective bounded-live paper outcomes require a separate B-owned append-only
ledger and are deliberately not accepted as historical training observations.

All training, calibration and policy selection use TRAIN or VALIDATION ONLY;
shadow is reported once as an evaluation cohort, never as a tuning target.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
import math
import random
import re

from . import CLASSES, FEATURE_NAMES, Model, brier, fit, validate

CONTRACT = "eth_feature_snapshot_v2"
RESEARCH_VERSION = "historical_shadow_research_v1"
MAX_EVIDENCE_ROWS = 100_000
_BLOCK_REPS = 256


def _fingerprint(payload):
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"),
                             allow_nan=False).encode("utf-8")).hexdigest()


def _finite_number(x):
    if isinstance(x, bool) or not isinstance(x, (int, float)):
        raise ValueError("nonfinite research value")
    y = float(x)
    if not math.isfinite(y) or abs(y) > 1e100:
        raise ValueError("nonfinite or unbounded research value")
    return y


def _signed_row(row):
    return (row.event_id, row.decision_ms, row.exit_ms, row.values,
            row.available_ms, row.outcome, row.gross_return, row.costs)


def verify_history_partition(train, validation, shadow, *, embargo_ms=60_000):
    """Reject duplicate, out-of-order, future-available or overlapping cohorts.

    This is a SECOND line of defence after the strict storage/extract certificate;
    a caller cannot use it to certify the origin of arbitrary rows.
    """
    if type(embargo_ms) is not int or embargo_ms < 60_000:
        raise ValueError("insufficient chronological embargo")
    parts = (tuple(train), tuple(validation), tuple(shadow))
    if any(not group for group in parts):
        raise ValueError("empty train validation or shadow cohort")
    if sum(map(len, parts)) > MAX_EVIDENCE_ROWS:
        raise ValueError("research evidence exceeds bounded population")
    ids = set()
    for group in parts:
        cutoff = max(r.exit_ms for r in group)
        ordered = validate(group, cutoff)
        if ordered != list(group):
            raise ValueError("unsorted or invalid immutable research cohort")
        for row in group:
            if not isinstance(row.event_id, str) or re.fullmatch(r"paper:[1-9][0-9]*", row.event_id) is None:
                raise ValueError("historical cohort requires exact paper ledger IDs")
            if row.event_id in ids:
                raise ValueError("duplicate event ID across research partitions")
            ids.add(row.event_id)
            if (type(row.decision_ms) is not int or type(row.exit_ms) is not int
                    or row.decision_ms <= 0 or row.exit_ms <= row.decision_ms
                    or len(row.values) != len(FEATURE_NAMES)
                    or len(row.available_ms) != len(FEATURE_NAMES)
                    or any(type(t) is not int or not 0 < t <= row.decision_ms
                           for t in row.available_ms)):
                raise ValueError("future evidence or invalid research timestamp")
            for value in (*row.values, row.gross_return, row.costs):
                _finite_number(value)
    if (max(r.decision_ms for r in parts[0]) >= min(r.decision_ms for r in parts[1])
            or max(r.decision_ms for r in parts[1]) >= min(r.decision_ms for r in parts[2])
            or max(r.exit_ms for r in parts[0]) >= min(r.decision_ms for r in parts[1]) - embargo_ms
            or max(r.exit_ms for r in parts[1]) >= min(r.decision_ms for r in parts[2]) - embargo_ms):
        raise ValueError("overlapping or insufficiently purged research cohorts")
    return parts


def _class_prior(rows):
    count = {label: 1 for label in CLASSES}  # Laplace smoothing, TRAIN ONLY
    for row in rows:
        count[row.outcome] += 1
    total = sum(count.values())
    return tuple(count[label] / total for label in CLASSES)


def _class_payoff(rows):
    result = []
    for label in CLASSES:
        selected = [r.net_return for r in rows if r.outcome == label]
        result.append(sum(selected) / len(selected) if selected else 0.0)
    return tuple(result)


@dataclass(frozen=True)
class ResearchPolicy:
    name: str
    version: str
    probabilities_by_side: tuple[tuple[float, ...], ...]
    feature_index: int
    threshold: float
    class_payoff: tuple[float, ...]
    train_to_ms: int

    def probabilities(self, values):
        if len(values) != len(FEATURE_NAMES):
            raise ValueError("incorrect prediction feature width")
        x = _finite_number(values[self.feature_index]) if self.feature_index >= 0 else 0
        scores = self.probabilities_by_side[int(x > self.threshold)] if self.feature_index >= 0 else self.probabilities_by_side[0]
        return dict(zip(CLASSES, scores))

    def expected_net(self, values):
        p = self.probabilities(values)
        return sum(p[c] * z for c, z in zip(CLASSES, self.class_payoff))

    def predict(self, values):
        p = self.probabilities(values)
        return max(CLASSES, key=lambda label: p[label])


def prior_policy(train):
    """Honest constant-probability empirical baseline fitted on TRAIN only."""
    if not train:
        raise ValueError("empty training baseline")
    prior = _class_prior(train)
    return ResearchPolicy("prior", "prior_v1", (prior, prior), -1, 0.0,
                          _class_payoff(train), max(r.exit_ms for r in train))


def _rule_side(values):
    # Fixed sign-based orderflow hypothesis, not optimized on holdouts.
    delta = _finite_number(values[FEATURE_NAMES.index("delta")])
    book = _finite_number(values[FEATURE_NAMES.index("book_imbalance")])
    if delta > 0 and book > 0:
        return "breakout"
    if delta < 0 and book < 0:
        return "bounce"
    return "uncertain"


@dataclass(frozen=True)
class FlowRulePolicy:
    class_payoff: tuple[float, ...]
    train_to_ms: int
    name: str = "flow_rule"
    version: str = "fixed_delta_book_v1"

    def probabilities(self, values):
        target = _rule_side(values)
        return {label: (0.8 if label == target else 0.1) for label in CLASSES}

    def expected_net(self, values):
        p = self.probabilities(values)
        return sum(p[c] * z for c, z in zip(CLASSES, self.class_payoff))

    def predict(self, values):
        return _rule_side(values)


def _smoothed_probs(rows):
    return _class_prior(rows)


def fit_stump(train, *, max_thresholds=9):
    """One-node interpretable tree. All thresholds and class posteriors use TRAIN.

    Optimize TRAIN multiclass Brier only; never touch validation/shadow.
    """
    if len(train) < 12 or type(max_thresholds) is not int or not 1 <= max_thresholds <= 30:
        raise ValueError("insufficient stump training evidence")
    base = _class_prior(train)
    result = None
    for feature_index in range(len(FEATURE_NAMES)):
        values = sorted(set(_finite_number(r.values[feature_index]) for r in train))
        if len(values) < 2:
            continue
        choices = sorted(set(values[min(len(values)-2, int((len(values)-1)*k/(max_thresholds+1)))]
                             for k in range(1, max_thresholds+1)))
        for threshold in choices:
            left = [r for r in train if r.values[feature_index] <= threshold]
            right = [r for r in train if r.values[feature_index] > threshold]
            if min(len(left), len(right)) < 6:
                continue
            probs = (_smoothed_probs(left), _smoothed_probs(right))
            loss = sum(sum((probs[int(r.values[feature_index] > threshold)][k]
                                  - (r.outcome == label)) ** 2
                                 for k, label in enumerate(CLASSES)) for r in train) / len(train)
            candidate = (loss, feature_index, threshold, probs)
            if result is None or candidate[:3] < result[:3]:
                result = candidate
    if result is None:
        return ResearchPolicy("stump", "one_node_v1", (base, base), -1, 0.0,
                              _class_payoff(train), max(r.exit_ms for r in train))
    _, idx, threshold, probs = result
    return ResearchPolicy("stump", "one_node_v1", probs, idx, threshold,
                          _class_payoff(train), max(r.exit_ms for r in train))


def _probability_vector(policy, values):
    p = policy.probabilities(values)
    vector = tuple(_finite_number(p[label]) for label in CLASSES)
    if any(x < 0 or x > 1 for x in vector) or abs(sum(vector) - 1) > 1e-9:
        raise ValueError("invalid research probabilities")
    return vector


def _net_vector(policy, rows):
    payoff = []
    picked = 0
    for r in rows:
        if r.decision_ms <= policy.train_to_ms:
            raise ValueError("future leakage into research evaluation")
        p = _probability_vector(policy, r.values)
        predicted = CLASSES[max(range(len(CLASSES)), key=lambda k: p[k])]
        expected = _finite_number(policy.expected_net(r.values))
        selected = predicted != "uncertain" and expected > 0
        picked += selected
        payoff.append(r.net_return if selected else 0.0)
    return payoff, picked


def calibration(policy, rows, *, bins=5):
    """Multi-class Brier and maximum-class reliability; no calibrator fit on OOS."""
    if not rows or bins < 2:
        raise ValueError("missing calibration cohort")
    groups = [[0, 0.0, 0.0] for _ in range(bins)]
    total_brier = 0.0
    for row in rows:
        if row.decision_ms <= policy.train_to_ms:
            raise ValueError("calibration overlaps training")
        probabilities = _probability_vector(policy, row.values)
        predicted_index = max(range(len(CLASSES)), key=lambda i: probabilities[i])
        confidence = probabilities[predicted_index]
        bucket = min(bins - 1, int(confidence * bins))
        groups[bucket][0] += 1
        groups[bucket][1] += confidence
        groups[bucket][2] += int(CLASSES[predicted_index] == row.outcome)
        total_brier += sum((p - (label == row.outcome)) ** 2 for label, p in zip(CLASSES, probabilities))
    ece = sum(abs(hit / n - conf / n) * n / len(rows) for n, conf, hit in groups if n)
    return {"multiclass_brier": total_brier / len(rows),
            "expected_calibration_error": ece,
            "reliability_bins": [{"count": n, "mean_confidence": conf / n,
                                  "accuracy": hit / n} for n, conf, hit in groups if n]}


def _quantile(values, fraction):
    values = sorted(values)
    return values[int((len(values) - 1) * fraction)]


def moving_block_bootstrap_lower(deltas, *, repetitions=_BLOCK_REPS, seed=0):
    """Deterministic circular moving block bootstrap of *paired* net returns.

    Prevents IID resampling of consecutive dependent paper positions. Still
    only a research diagnostic, NOT proof of independent market observations.
    """
    n = len(deltas)
    if n < 30 or type(repetitions) is not int or not 100 <= repetitions <= 2000:
        raise ValueError("insufficient correlated samples")
    sample = tuple(_finite_number(v) for v in deltas)
    block = max(2, round(math.sqrt(n)))
    rng = random.Random(seed)
    means = []
    for _ in range(repetitions):
        selected = []
        for __ in range((n + block - 1) // block):
            start = rng.randrange(n)
            selected.extend(sample[(start + k) % n] for k in range(block))
        means.append(sum(selected[:n]) / n)
    return {"block_size": block, "repetitions": repetitions,
            "lower_p05": _quantile(means, 0.05),
            "upper_p95": _quantile(means, 0.95),
            "observed_mean": sum(sample) / n}


def _drawdown(values):
    high, equity, worst = 0.0, 0.0, 0.0
    for value in values:
        equity += value
        high = max(high, equity)
        worst = max(worst, high - equity)
    return worst


def score_oos(policy, rows, *, evidence_fingerprint, seed_label):
    if len(rows) < 30:
        raise ValueError("insufficient independent OOS samples")
    payoffs, picked = _net_vector(policy, rows)
    returns = [r.net_return for r in rows]  # all observed paper trades, not all signals
    diffs = [a - b for a, b in zip(payoffs, returns)]
    seed = int(_fingerprint([evidence_fingerprint, seed_label])[:16], 16)
    bounds = moving_block_bootstrap_lower(diffs, seed=seed)
    return {"n": len(rows), "selected": picked,
            "mean_net_per_observed_paper": sum(payoffs) / len(rows),
            "always_paper_mean_net": sum(returns) / len(rows),
            "none_mean_net": 0.0,
            "max_normalized_drawdown": _drawdown(payoffs),
            "paired_delta_vs_observed_always": bounds,
            "calibration": calibration(policy, rows),
            "selection_bias": "observed_paper_trades_only_not_all_signals"}


def _folds(train, *, embargo_ms=60_000):
    ordered = sorted(train, key=lambda r: (r.decision_ms, r.event_id))
    n = len(ordered)
    for fraction in (0.55, 0.7, 0.85):
        cut = int(n * fraction)
        if cut < 12 or n - cut < 10:
            continue
        begin = ordered[cut].decision_ms
        history = [r for r in ordered[:cut] if r.exit_ms < begin - embargo_ms]
        upcoming = [r for r in ordered[cut:min(n,cut + max(10,n//10))] if r.decision_ms >= begin]
        if len(history) >= 12 and len(upcoming) >= 10:
            yield history, upcoming


def walk_forward_train_only(train, *, embargo_ms=60_000):
    """Uses earlier training subset as folds; never validation or shadow rows."""
    folds = []
    for history, upcoming in _folds(train, embargo_ms=embargo_ms):
        try:
            m = fit(history)
            st = fit_stump(history)
            folds.append({"train": len(history), "test": len(upcoming),
                          "first_test_ms": upcoming[0].decision_ms,
                          "softmax_brier": calibration(m, upcoming)["multiclass_brier"],
                          "stump_brier": calibration(st, upcoming)["multiclass_brier"]})
        except (ValueError, OverflowError, ZeroDivisionError):
            # No quiet synthetic replacement: return explicit missing folds.
            continue
    return folds


def historical_shadow_report(train, validation, shadow, model, *,
                             embargo_ms=60_000, cohort=CONTRACT):
    """One reproducible model-comparison study, NO activation/optimization on shadow.

    Historical feature+outcome source certification must already have passed
    B/F DB-backed extraction; no caller flags can certify market truth here.
    """
    if cohort != CONTRACT:
        raise ValueError("prospective paper is not historical ML-v2 evidence")
    train, validation, shadow = verify_history_partition(train, validation, shadow,
                                                         embargo_ms=embargo_ms)
    if not isinstance(model, Model) or model != fit(train):
        raise ValueError("noncanonical softmax model or altered training weights")
    if model.train_to_ms >= validation[0].decision_ms:
        raise ValueError("model sees validation period")
    evidence = _fingerprint({"cohort": cohort, "train": [_signed_row(r) for r in train],
                            "validation": [_signed_row(r) for r in validation],
                            "shadow": [_signed_row(r) for r in shadow],
                            "embargo_ms": embargo_ms})
    candidates = {"prior": prior_policy(train),
                  "flow_rule": FlowRulePolicy(_class_payoff(train), max(r.exit_ms for r in train)),
                  "stump": fit_stump(train), "softmax3": model}
    scored_validation = {name: score_oos(policy, validation,
                             evidence_fingerprint=evidence, seed_label="val:" + name)
                         for name, policy in candidates.items()}
    # Selection uses ONLY validation proper Brier. Never peek at shadow to pick.
    selected = min(candidates, key=lambda name:
                   (scored_validation[name]["calibration"]["multiclass_brier"], name))
    scored_shadow = {name: score_oos(policy, shadow,
                        evidence_fingerprint=evidence, seed_label="shadow:" + name)
                     for name, policy in candidates.items()}
    from . import population_shift
    drift = population_shift(train, shadow)
    chosen = scored_shadow[selected]
    # These are diagnostics only; not an auto-promotion decision. Even an
    # apparently strong result is observational and may reuse the same OOS.
    limitations = ["observed_trades_are_not_all_market_signals",
                   "repeated_shadow_look_is_not_independent_forward_OOS",
                   "missing_B_append_only_prospective_outcome_ledger"]
    if max(drift.values()) > 0.2:
        limitations.append("excessive_feature_population_shift")
    if chosen["selected"] < 20:
        limitations.append("too_few_selected_shadow_observations")
    if chosen["calibration"]["multiclass_brier"] > 0.55:
        limitations.append("weak_multiclass_calibration")
    if chosen["paired_delta_vs_observed_always"]["lower_p05"] <= 0:
        limitations.append("uncertain_net_improvement_after_costs")
    return {"version": RESEARCH_VERSION, "cohort": cohort,
            "fingerprint_sha256": evidence,
            "model_version": model.version,
            "observational_reference": "always_trade_among_already_executed_paper",
            "neural_net_status": "not_justified_without_independent_forward_cohort",
            "limitations": limitations,
            "candidates": ["flow_rule", "prior", "softmax3", "stump"],
            "selected_on": "validation_multiclass_brier_only",
            "validation_selected": selected,
            "validation": scored_validation,
            "shadow": scored_shadow,
            "train_only_walk_forward": walk_forward_train_only(train, embargo_ms=embargo_ms),
            "max_population_stability_index": max(drift.values()),
            "feature_drift": drift,
            "observed_only": True,
            "uncertainty": "paired_circular_moving_block_bootstrap_p05_p95_not_coverage_guarantee",
            "shadow_reuse_not_independently_excluded": True,
            "promotion_eligible": False,
            "go": False,
            "actions": "research_only_no_orders_no_model_activation"}


def prospective_paper_cohort_status(*, ledger_contract=None):
    """An explicit, separate no-data status until B owns immutable forward labels."""
    return {"cohort": "forward_observed_bounded_paper",
            "status": "blocked_missing_independent_prospective_outcome_ledger",
            "expected_contract": "eth_forward_paper_outcomes_v1",
            "received_contract": ledger_contract,
            "historical_ml_eligible": False, "promotion_eligible": False}
