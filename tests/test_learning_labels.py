"""Canonical UTC post-decision barrier labels (no intrabar path assumptions)."""
import pytest
from eth_tester.ml.labels import full_future_grid, label_level_outcome

DECISION = 1795610060123  # fixed, deliberately not aligned to UTC minute
STEP = 60000

def label(side, bars, **kwargs):
    first, _, horizon = full_future_grid(
        DECISION, bar_interval_ms=STEP, horizon_bars=len(bars))
    closed = [{"start_ms": first+i*STEP, "closed_ms": first+(i+1)*STEP,
               "low": lo, "high": hi} for i,(lo,hi) in enumerate(bars)]
    return label_level_outcome(
        side, 100.0, closed, decision_ms=DECISION,
        horizon_ms=horizon, as_of_ms=horizon,
        bar_interval_ms=STEP, **kwargs)

def test_resistance_breakout_first():
    assert label("resistance", [(99.95,100.4),(99.5,100.5)]) == "breakout"
    assert label("support", [(99.95,100.4),(99.5,100.5)]) == "bounce"

def test_rejection_is_bounce():
    assert label("resistance", [(99.7,100.1),(99.6,100.1)]) == "bounce"
    assert label("support", [(99.7,100.1),(99.6,100.1)]) == "breakout"

def test_same_bar_both_barriers_is_uncertain():
    assert label("resistance", [(99.6,100.5),(100,100.1)]) == "uncertain"

def test_incomplete_horizon_is_rejected_not_uncertain():
    start,_,end=full_future_grid(DECISION,bar_interval_ms=STEP,horizon_bars=2)
    with pytest.raises(ValueError,match="unresolved"):
        label_level_outcome("resistance",100,[{"closed_ms":start+STEP,
            "low":99.9,"high":100.1}], decision_ms=DECISION,
            horizon_ms=end,as_of_ms=end,bar_interval_ms=STEP)

def test_future_not_observed_cannot_label():
    start,_,end=full_future_grid(DECISION,bar_interval_ms=STEP,horizon_bars=2)
    bars=[{"closed_ms":start+STEP*(i+1),"low":99,"high":101} for i in range(2)]
    with pytest.raises(ValueError,match="unresolved"):
        label_level_outcome("resistance",100,bars,decision_ms=DECISION,
                            horizon_ms=end,as_of_ms=end-1,bar_interval_ms=STEP)

def test_future_or_unordered_bar_rejected():
    start,_,end=full_future_grid(DECISION,bar_interval_ms=STEP,horizon_bars=2)
    bars=[{"closed_ms":start+STEP,"low":99.9,"high":100.1},
          {"closed_ms":start+3*STEP,"low":99.9,"high":100.5}]
    with pytest.raises(ValueError,match="unordered"):
        label_level_outcome("resistance",100,bars,decision_ms=DECISION,
                            horizon_ms=end,as_of_ms=end,bar_interval_ms=STEP)
