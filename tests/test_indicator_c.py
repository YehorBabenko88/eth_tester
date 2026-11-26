from decimal import Decimal
import pytest

from eth_tester.indicator_incremental import next_indicators


def bar(high, low, close):
    return {"high": Decimal(high), "low": Decimal(low),
            "close": Decimal(close), "closed": True}


def test_wilder_atr_checkpoint_and_incremental_rv():
    state = {}
    state, first = next_indicators(state, bar(11, 9, 10), period=2)
    assert first["atr"] is None
    state, second = next_indicators(state, bar(12, 10, 11), period=2)
    assert second["atr"] == Decimal(2)
    assert second["realized_volatility"] > 0
    state, third = next_indicators(state, bar(15, 9, 14), period=2)
    assert third["atr"] == Decimal(4)
    assert third["high_low_range_pct"] == Decimal(6) / 14 * 100
    assert len(state["returns"]) == 2


def test_no_preclose_or_invalid_candle():
    with pytest.raises(ValueError):
        next_indicators({}, dict(bar(11, 9, 10), closed=False))
    with pytest.raises(ValueError):
        next_indicators({}, bar(9, 10, 11))


def test_state_json_replay_gives_equal_result():
    import json

    state = {}
    state, _ = next_indicators(state, bar(11, 9, 10), period=2)
    replay = json.loads(json.dumps(state))
    a, _ = next_indicators(state, bar(12, 10, 11), period=2)
    b, _ = next_indicators(replay, bar(12, 10, 11), period=2)
    assert a == b


def test_post_warmup_json_restart_and_fourth_closed_candle():
    import json

    state = {}
    for item in (bar(11, 9, 10), bar(12, 10, 11)):
        state, _ = next_indicators(state, item, period=2)
    assert state["initial_tr"] is None
    restored = json.loads(json.dumps(state))
    original_next, third = next_indicators(state, bar(15, 9, 14), period=2)
    resumed_next, resumed_third = next_indicators(restored, bar(15, 9, 14),
                                                 period=2)
    assert third == resumed_third
    assert original_next == resumed_next
    assert third["atr"] == Decimal(4)
    last_state, fourth = next_indicators(resumed_next, bar(16, 14, 15),
                                         period=2)
    assert fourth["atr"] == Decimal(3)
    assert last_state["initial_tr"] is None
    assert len(last_state["returns"]) == 2


def test_missing_warmup_or_post_warmup_checkpoint_fails_closed():
    state, _ = next_indicators({}, bar(11, 9, 10), period=2)
    with pytest.raises(ValueError, match="Missing Wilder initial TR"):
        next_indicators({**state, "initial_tr": None},
                        bar(12, 10, 11), period=2)
    state, _ = next_indicators(state, bar(12, 10, 11), period=2)
    with pytest.raises(ValueError, match="Missing Wilder ATR checkpoint"):
        next_indicators({**state, "atr": None}, bar(15, 9, 14), period=2)
