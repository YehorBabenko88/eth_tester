"""Strict future-grid regressions; no real exchange certificate implied."""
from datetime import datetime, timezone
import pytest
from eth_tester.ml.labels import full_future_grid, label_level_outcome

UTC = timezone.utc

def ms(h, m, s=0, milli=0):
    return int(datetime(2026, 11, 25, h, m, s, milli * 1000, tzinfo=UTC).timestamp()*1000)

def bars(first_start, lows_highs, step=60000):
    return [{"start_ms": first_start + i*step,
             "closed_ms": first_start+(i+1)*step,
             "low": low, "high": high}
            for i, (low, high) in enumerate(lows_highs)]

def label(decision, lohi, side="resistance", **kw):
    start, _, horizon=full_future_grid(decision,bar_interval_ms=60000,horizon_bars=len(lohi))
    return label_level_outcome(side,100,bars(start,lohi),decision_ms=decision,
                                horizon_ms=horizon,as_of_ms=horizon,bar_interval_ms=60000,
                                **kw)

def test_non_aligned_decision_excludes_current_minute():
    decision=ms(12,34,20,123)
    start, first_close, horizon=full_future_grid(decision,bar_interval_ms=60000,horizon_bars=2)
    assert start==ms(12,35) and first_close==ms(12,36) and horizon==ms(12,37)
    assert label(decision,[(99.9,100.4),(99.8,100.6)])=="breakout"

def test_at_exact_boundary_excludes_just_started_candle():
    start, first_close, _=full_future_grid(ms(12,34),bar_interval_ms=60000,horizon_bars=1)
    assert start==ms(12,35) and first_close==ms(12,36)

def test_midnight_utc_rollover():
    start, first_close, horizon=full_future_grid(ms(23,59,50),bar_interval_ms=60000,horizon_bars=3)
    assert start==ms(23,59,50)//60000*60000+60000
    assert first_close==start+60000 and horizon==start+180000

def test_predecision_bar_must_not_enter_horizon():
    decision=ms(12,34,20)
    start,_,end=full_future_grid(decision,bar_interval_ms=60000,horizon_bars=2)
    old=bars(start,[(99,101),(99,101)])
    old[0]["start_ms"]=ms(12,34)
    with pytest.raises(ValueError,match="unordered"):
        label_level_outcome("resistance",100,old,decision_ms=decision,
            horizon_ms=end,as_of_ms=end,bar_interval_ms=60000)

def test_gap_in_future_grid_rejected():
    decision=ms(12,34,20)
    start,_,end=full_future_grid(decision,bar_interval_ms=60000,horizon_bars=2)
    series=bars(start,[(99,101),(99,101)])
    series[1]["closed_ms"]+=60000
    with pytest.raises(ValueError,match="unordered"):
        label_level_outcome("resistance",100,series,decision_ms=decision,
            horizon_ms=end,as_of_ms=end,bar_interval_ms=60000)

def test_no_early_label():
    decision=ms(12,34,20)
    start,_,end=full_future_grid(decision,bar_interval_ms=60000,horizon_bars=2)
    with pytest.raises(ValueError,match="unresolved"):
        label_level_outcome("resistance",100,bars(start,[(99,101),(99,101)]),
            decision_ms=decision,horizon_ms=end,as_of_ms=end-1,bar_interval_ms=60000)

def test_same_bar_two_barriers_is_uncertain():
    assert label(ms(12,34,20),[(99,101),(99,101)])=="uncertain"

def test_bounce_and_support_direction():
    decision=ms(12,34,20)
    assert label(decision,[(99.5,100.1)],"resistance")=="bounce"
    assert label(decision,[(99.5,100.1)],"support")=="breakout"

def test_no_fabricated_offset_grid():
    decision=ms(12,34,20)
    _,_,end=full_future_grid(decision,bar_interval_ms=60000,horizon_bars=2)
    fake=[{"closed_ms":decision+i*60000,"low":99,"high":101} for i in (1,2)]
    with pytest.raises(ValueError,match="unordered"):
        label_level_outcome("resistance",100,fake,decision_ms=decision,
            horizon_ms=end,as_of_ms=end,bar_interval_ms=60000)

def test_lookahead_horizon_mismatch():
    decision=ms(12,34,20)
    start,_,end=full_future_grid(decision,bar_interval_ms=60000,horizon_bars=2)
    with pytest.raises(ValueError,match="unresolved"):
        label_level_outcome("resistance",100,bars(start,[(99,101),(99,101)]),
            decision_ms=decision,horizon_ms=end+60000,as_of_ms=end+60000,
            bar_interval_ms=60000)
