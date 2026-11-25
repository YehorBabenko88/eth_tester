from datetime import datetime, timedelta, timezone
from decimal import Decimal as D

from eth_tester.strategies.volume import build_profile, untouched_poc, volume_signal


START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = START + timedelta(minutes=1)


def samples():
    return [
        {"ts": START + timedelta(seconds=1), "price": "100", "qty": "5", "trade_id": "1"},
        {"ts": START + timedelta(seconds=2), "price": "101", "qty": "2", "trade_id": "2"},
        {"ts": START + timedelta(seconds=3), "price": "99", "qty": "1", "trade_id": "3"},
    ]


def test_profile_rejects_unverified_windows():
    assert build_profile(samples(), START, END, D(1)) is None
    assert build_profile(samples(), START, END, D(1),
                         coverage_verified=True, expected_trade_count=4) is None
    corrupted = samples()
    corrupted[1] = dict(corrupted[1], trade_id="1")
    assert build_profile(corrupted, START, END, D(1),
                         coverage_verified=True, expected_trade_count=3) is None
    future = samples()
    future[1] = dict(future[1], ts=END)
    assert build_profile(future, START, END, D(1),
                         coverage_verified=True, expected_trade_count=3) is None


def test_profile_contiguous_value_area_and_ties():
    profile = build_profile(samples(), START, END, D(1),
                            coverage_verified=True, expected_trade_count=3)
    assert profile.poc == D(100)
    assert profile.value_area_low == D(100)
    assert profile.value_area_high == D(101)
    assert profile.total_quantity == D(8)
    ties = [
        {"ts": START, "price": "101", "qty": 1},
        {"ts": START, "price": "100", "qty": 1},
    ]
    tied = build_profile(ties, START, END, D(1),
                         coverage_verified=True, expected_trade_count=2)
    assert tied.poc == D(100)


def candle(minute, low, high, close, opening=None):
    return {"ts": START + timedelta(minutes=minute),
            "low": D(low), "high": D(high), "close": D(close),
            "open": D(close if opening is None else opening), "closed": True}


def test_naked_poc_uses_only_previous_closed_bars():
    profile = build_profile(samples(), START, END, D(1),
                            coverage_verified=True, expected_trade_count=3)
    bars = [candle(1, "100.1", "100.8", "100.5"),
            candle(2, "100.1", "100.9", "100.4"),
            candle(3, "100.1", "100.8", "100.5")]
    assert untouched_poc(profile, bars, through=START + timedelta(minutes=4))
    assert not untouched_poc(profile, bars + [candle(3, "99.9", "101", "100.5")],
                             through=START + timedelta(minutes=4))
    assert not untouched_poc(profile, [], through=START + timedelta(minutes=4))
    assert not untouched_poc(profile, bars, through=END)


def test_naked_poc_rejection_signal_and_no_signal():
    profile = build_profile(samples(), START, END, D(1),
                            coverage_verified=True, expected_trade_count=3)
    bars = [candle(1, "100.1", "100.8", "100.5"),
            candle(2, "100.1", "100.9", "100.4"),
            candle(3, "100.1", "100.8", "100.5"),
            candle(4, "99.9", "100.6", "100.2", "100.1")]
    signal = volume_signal(bars, profile)
    assert signal is not None
    assert signal.direction == "long"
    assert signal.reason["strategy"] == "naked_poc_rejection"
    assert signal.reason["requires_live_confirmation"]
    assert signal.stop < bars[-1]["close"] < signal.take
    assert volume_signal(bars, None) is None
    assert volume_signal(bars[:-1], profile) is None


def test_profile_nodes_and_bad_prices():
    trades = [{"ts": START, "price": p, "qty": qty}
              for p, qty in [(98, 8), (99, 2), (100, 10),
                             (101, 2), (102, 8)]]
    profile = build_profile(trades, START, END, D(1),
                            coverage_verified=True, expected_trade_count=5)
    assert profile.poc == D(100)
    assert profile.high_volume_nodes == (D(98), D(102))
    assert profile.low_volume_nodes == (D(99), D(101))
    invalid = samples()
    invalid[0] = dict(invalid[0], price="NaN")
    assert build_profile(invalid, START, END, D(1),
                         coverage_verified=True, expected_trade_count=3) is None

def test_price_signal_can_consume_verified_profile_without_lookahead():
    from eth_tester.strategy import price_signal

    profile = build_profile(samples(), START, END, D(1),
                            coverage_verified=True, expected_trade_count=3)
    before = [candle(index, "100.1", "100.8", "100.5")
              for index in range(-7, 1)]
    afterwards = [candle(1, "100.1", "100.8", "100.5"),
                  candle(2, "100.1", "100.9", "100.4"),
                  candle(3, "100.1", "100.8", "100.5"),
                  candle(4, "99.9", "100.6", "100.2", "100.1")]
    bars = before + afterwards
    without_profile = price_signal(bars)
    assert without_profile is None
    with_profile = price_signal(bars, volume_profiles=[profile])
    assert with_profile is not None
    assert with_profile.reason["strategy"] == "naked_poc_rejection"
    assert price_signal(bars[:-1], volume_profiles=[profile]) is None


def test_volume_signal_rejects_unobserved_time_holes():
    profile = build_profile(samples(), START, END, D(1),
                            coverage_verified=True, expected_trade_count=3)
    bars = [candle(1, "100.1", "100.8", "100.5"),
            candle(3, "100.1", "100.8", "100.5"),
            candle(4, "99.9", "100.6", "100.2", "100.1"),
            candle(5, "99.9", "100.6", "100.2", "100.1")]
    assert volume_signal(bars, profile) is None

def test_zero_volume_internal_zone_is_identified_without_made_up_trades():
    trades = [{"ts": START, "price": p, "qty": qty}
              for p, qty in ((100, 10), (101, 1), (103, 1), (104, 9))]
    profile = build_profile(trades, START, END, D(1),
                            coverage_verified=True, expected_trade_count=4)
    assert [(z.low, z.high, z.quantity) for z in profile.low_volume_zones] == [
        (D(101), D(103), D(2))]
    assert [(z.low, z.high) for z in profile.high_volume_zones] == [
        (D(100), D(100)), (D(104), D(104))]
