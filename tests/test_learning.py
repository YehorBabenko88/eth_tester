"""Independent deterministic checks for causal learning (no database required)."""
from datetime import datetime, timedelta, timezone

import pytest

from eth_tester.ml import (
    CLASSES, Example, FEATURE_NAMES, Gates, Model, approve_candidate, brier,
    chronological, evaluate, fit, population_shift, purged_walk_forward,
    rollback_required, should_retrain, validate,
)
from eth_tester.ml.extract import from_trade_records


def sample(i, *, outcome=None, feature=None, exit_offset=2):
    values = tuple(float((i % 7) + j/10) for j in range(len(FEATURE_NAMES)))
    if feature is not None:
        values = tuple(float(feature) for _ in FEATURE_NAMES)
    return Example(str(i), i*1000, i*1000+exit_offset, values,
                   (i*1000,) * len(values), outcome or CLASSES[i % 3],
                   .03 if i % 3 == 0 else -.015, .002)


def training():
    return [sample(i) for i in range(1, 121)]


def test_point_in_time_and_incomplete_labels():
    a = sample(1)
    b = sample(2)
    assert validate([b, a], 999999) == [a, b]
    assert validate([a], a.exit_ms-1) == []
    with pytest.raises(ValueError, match="future"):
        validate([Example("bad", 500, 600, a.values, (501,)*9,
                          "bounce", .1, .01)], 99999)
    with pytest.raises(ValueError, match="duplicate"):
        validate([a, a], 999999)
    with pytest.raises(ValueError, match="feature schema"):
        validate([Example("bad", 5, 6, (), (), "bounce", 0, 0)], 9)


def test_purged_splits_and_walkforward():
    rows = [sample(i, exit_offset=200) for i in range(1, 100)]
    tr, va, te = chronological(rows, 40000, 70000, embargo_ms=100)
    assert tr and va and te
    assert max(r.exit_ms for r in tr) < 39900
    assert max(r.exit_ms for r in va) < 69900
    folds = list(purged_walk_forward(rows, [30000, 50000, 70000], 100))
    assert len(folds) == 2
    assert all(max(r.exit_ms for r in train) < min(r.decision_ms for r in val)
               for train, val in folds)
    with pytest.raises(ValueError):
        list(purged_walk_forward(rows, [50000, 30000]))


def test_training_is_deterministic_and_serializable():
    rows = training()
    a = fit(rows, steps=25)
    b = fit(rows, steps=25)
    assert a == b
    assert a.version == b.version
    p = a.probabilities(sample(200).values)
    assert set(p) == set(CLASSES)
    assert sum(p.values()) == pytest.approx(1.0)
    assert 0 <= brier(a, [sample(i) for i in range(200, 220)]) <= 2
    assert Model.from_json(a.as_json()) == a
    assert fit(rows[:-1]).version != a.version
    with pytest.raises(ValueError):
        fit(rows[:2])
    with pytest.raises(ValueError):
        a.probabilities([1.0])


def test_net_comparison_uses_trade_costs():
    rows = training()
    model = fit(rows, steps=20)
    test = [sample(i) for i in range(300, 330)]
    stat = evaluate(model, test)
    assert stat["n"] == 30
    assert stat["baseline_always_net"] == pytest.approx(sum(r.net_return for r in test))
    assert stat["candidate_net"] <= sum(max(r.net_return, 0) for r in test)
    with pytest.raises(ValueError, match="overlaps"):
        evaluate(model, rows)


def test_gates_fail_closed_without_shadow_and_drift():
    tr = training()
    val = [sample(i) for i in range(200, 240)]
    model = fit(tr, steps=20)
    ok, why = approve_candidate(model, None, tr, val, [], Gates())
    assert not ok and "samples" in why
    ok, why = approve_candidate(model, None, tr, val,
                                [sample(i, feature=1e9) for i in range(300, 340)])
    assert not ok and why in ("feature drift", "not enough independent trades")
    assert max(population_shift(tr, [sample(i, feature=1e9)
                                    for i in range(300, 340)]).values()) > .2


def test_rollback_and_retrain_thresholds():
    assert should_retrain(100, 201, 100)
    assert not should_retrain(100, 150, 100)
    metrics = {"n": 35, "brier": .2, "candidate_mean_net_per_signal": .02}
    assert rollback_required(metrics, {"n": 35, "brier": .3,
                                     "candidate_mean_net_per_signal": .02})
    assert not rollback_required(metrics, {"n": 3, "brier": .9,
                                         "candidate_mean_net_per_signal": -.5})


def test_extraction_requires_captured_verified_snapshot_and_realized_costs():
    t = datetime(2026, 1, 1, tzinfo=timezone.utc)
    start_ms = int(t.timestamp() * 1000)
    snapshot = {"values": dict(zip(FEATURE_NAMES, [1.0]*9)),
                "available_ms": dict(zip(FEATURE_NAMES, [start_ms-2000]*9)),
                "recorded_ms": start_ms-1000, "quality": "verified", "version": "v1"}
    record = {"event_id": "paper:1", "opened_at": t,
              "closed_at": t+timedelta(minutes=1), "notional": 1000,
              "gross_pnl": 12, "fees": 1, "funding": .5, "net_pnl": 10.5,
              "reason": {"ml_outcome": "breakout", "ml_snapshot": snapshot}}
    rows, rejected = from_trade_records([record], start_ms+120000)
    assert len(rows) == 1 and not rejected
    assert rows[0].net_return == pytest.approx(.0105)
    invalid = {**record, "event_id": "paper:2", "reason": {
        "ml_outcome": "breakout",
        "ml_snapshot": {**snapshot, "available_ms": dict.fromkeys(FEATURE_NAMES, start_ms)}}}
    rows, rejected = from_trade_records([invalid], start_ms+120000)
    assert rows == [] and rejected
    invalid = {**record, "event_id": "paper:3", "reason": {
        "ml_outcome": "breakout", "ml_snapshot": {**snapshot, "quality": "partial"}}}
    assert from_trade_records([invalid], start_ms+120000)[0] == []
