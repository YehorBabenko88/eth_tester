"""Conservative calibration evidence, no conversion of market-class odds to win odds."""
import pytest

from eth_tester.ml import CLASSES, Example, FEATURE_NAMES, Model
from eth_tester.ml.calibration import wilson_lower, winrate_certificate


def always_breakout():
    zeros = (0.0,) * len(FEATURE_NAMES)
    ones = (1.0,) * len(FEATURE_NAMES)
    weights = ((8.0,) + zeros, (-8.0,) + zeros, (-8.0,) + zeros)
    return Model("frozen-test", zeros, ones, weights, (.01, .01, .01), 0)


def sample(i):
    values = (1.0,)*len(FEATURE_NAMES)
    return Example(str(i), i*1000, i*1000+100, values,
                   (i*1000,)*9, CLASSES[i%3], .012 if i%3 else -.008, .002)


def test_wilson_lower_is_conservative():
    assert wilson_lower(0, 1) == 0.0
    assert wilson_lower(0, 30) == 0.0
    assert wilson_lower(0, 300) == 0.0
    assert 0.9 < wilson_lower(100, 100) < 1.0
    assert 0 < wilson_lower(50, 100) < .5
    assert wilson_lower(0, 0) is None
    with pytest.raises(ValueError):
        wilson_lower(31, 30)


def test_winrate_certificate_preserves_provenance_and_costs():
    rows = [sample(i) for i in range(1, 121)]
    certificate = winrate_certificate(
        always_breakout(), rows, strategy_version="paper-v1"
    )
    assert certificate is not None
    assert certificate["strategy_version"] == "paper-v1"
    assert certificate["model_version"] == "frozen-test"
    assert certificate["schema_version"] == 2
    assert certificate["issuer"] == "eth_tester.ml.calibration.v2"
    assert certificate["issued_at_ms"] >= certificate["holdout_end_ms"]
    assert certificate["authenticity_verified"] is False
    assert certificate["net_wins"] == 80
    assert certificate["observations"] == 120
    assert certificate["probability_lower"] < 80/120
    assert len(certificate["selected_evidence_sha256"]) == 64


def test_calibration_refuses_small_or_unprofitable_holdout():
    model = always_breakout()
    assert winrate_certificate(model, [sample(i) for i in range(1, 5)],
                               strategy_version="paper-v1") is None
    losing = [Example(str(i), i*1000, i*1000+100, (1.0,)*9, (i*1000,)*9,
                      "bounce", -.03, .01) for i in range(1, 100)]
    assert winrate_certificate(model, losing, strategy_version="paper-v1") is None
    with pytest.raises(ValueError):
        winrate_certificate(model, [sample(1)], strategy_version="")


def test_certificate_rejects_unpublished_holdout(monkeypatch):
    from eth_tester.ml import calibration
    monkeypatch.setattr(calibration.time, "time_ns", lambda: 50_000_000)
    assert winrate_certificate(always_breakout(),
                               [sample(i) for i in range(1, 121)],
                               strategy_version="paper-v1") is None
