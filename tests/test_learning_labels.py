"""Fixed-horizon labeling must never infer intrabar order or future incomplete bars."""
import pytest

from eth_tester.ml.labels import label_level_outcome


def label(side, bars, **kw):
    return label_level_outcome(
        side, 100.0, bars, decision_ms=0, horizon_ms=120000,
        as_of_ms=120000, bar_interval_ms=60000, **kw)


def test_resistance_breakout_first():
    bars = [{"closed_ms": 60000, "low": 99.95, "high": 100.4},
            {"closed_ms": 120000, "low": 99.5, "high": 100.5}]
    assert label("resistance", bars) == "breakout"
    assert label("support", bars) == "bounce"


def test_rejection_is_bounce():
    bars = [{"closed_ms": 60000, "low": 99.7, "high": 100.1},
            {"closed_ms": 120000, "low": 99.6, "high": 100.1}]
    assert label("resistance", bars) == "bounce"
    assert label("support", bars) == "breakout"


def test_same_bar_both_barriers_is_uncertain():
    bars = [{"closed_ms": 60000, "low": 99.6, "high": 100.5},
            {"closed_ms": 120000, "low": 100, "high": 100.1}]
    assert label("resistance", bars) == "uncertain"


def test_incomplete_horizon_is_rejected_not_uncertain():
    with pytest.raises(ValueError, match="incomplete"):
        label("resistance", [{"closed_ms": 60000, "low": 99.9, "high": 100.1}])


def test_future_not_observed_cannot_label():
    with pytest.raises(ValueError, match="unresolved"):
        label_level_outcome("resistance", 100, [], decision_ms=0,
                            horizon_ms=120000, as_of_ms=119999,
                            bar_interval_ms=60000)


def test_future_or_unordered_bar_rejected():
    bars = [{"closed_ms": 60000, "low": 99.9, "high": 100.1},
            {"closed_ms": 180000, "low": 99.9, "high": 100.5}]
    with pytest.raises(ValueError, match="unordered"):
        label("resistance", bars)
