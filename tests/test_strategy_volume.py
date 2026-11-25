from datetime import datetime, timedelta, timezone
from decimal import Decimal as D

from eth_tester.strategies.volume import build_profile, untouched_poc, volume_signal


START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = START + timedelta(minutes=1)


def verified_provenance(count=3, checked_at=None):
    """Fixture shaped like B daily ingestion_coverage + minute comparison."""
    archive = {
        "source": "trades_archive", "symbol": "ETHUSDT",
        "instrument": "linear", "interval": "", "status": "verified",
        "quality": "complete", "start_ts": START,
        "end_ts": START + timedelta(days=1),
        "observed_rows": count, "expected_rows": count, "checksum": "a" * 64,
        "checked_at": checked_at if checked_at is not None
        else END + timedelta(seconds=5),
    }
    minute = {
        "symbol": "ETHUSDT", "instrument": "linear",
        "minute_ts": START, "archive_source": "trades_archive",
        "archive_sha256": "a" * 64, "status": "match", "trade_count": count,
        "trade_base_qty": "8", "candle_base_volume": "8",
        "checked_at": END + timedelta(seconds=6),
    }
    return {"archive": archive, "minute_checks": [minute]}


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
                            coverage_verified=True, expected_trade_count=3,
                            provenance=verified_provenance())
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
                            coverage_verified=True, expected_trade_count=3,
                            provenance=verified_provenance())
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
                            coverage_verified=True, expected_trade_count=3,
                            provenance=verified_provenance())
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

def test_sparse_profile_value_area_finishes_inside_observed_bounds():
    trades = [{"ts": START, "price": "100", "qty": "5"},
              {"ts": START, "price": "105", "qty": "5"}]
    profile = build_profile(trades, START, END, D(1),
                            coverage_verified=True, expected_trade_count=2)
    assert profile is not None
    assert profile.poc == D(100)
    assert profile.value_area_low == D(100)
    assert profile.value_area_high == D(105)
    very_wide = [dict(trades[0]), dict(trades[1], price="30000")]
    assert build_profile(very_wide, START, END, D(1),
                         coverage_verified=True, expected_trade_count=2) is None

def test_only_database_attestation_can_authorize_paper_volume():
    raw = build_profile(samples(), START, END, D(1), coverage_verified=True,
                        expected_trade_count=3)
    assert raw.coverage == "self_reported_unverified"
    bars = [candle(1, "100.1", "100.8", "100.5"),
            candle(2, "100.1", "100.9", "100.4"),
            candle(3, "100.1", "100.8", "100.5"),
            candle(4, "99.9", "100.6", "100.2", "100.1")]
    assert volume_signal(bars, raw) is None
    good = build_profile(samples(), START, END, D(1), coverage_verified=True,
                         expected_trade_count=3, provenance=verified_provenance())
    assert good.coverage == "externally_verified"
    assert good.source_checksum == "a" * 64
    assert volume_signal(bars, good) is not None
    late = build_profile(samples(), START, END, D(1), coverage_verified=True,
                         expected_trade_count=3,
                         provenance=verified_provenance(
                             checked_at=END + timedelta(hours=5)))
    assert late is not None
    assert volume_signal(bars, late) is None


def test_tampered_archive_provenance_and_off_tick_prices_rejected():
    for change in (
        {"status": "partial"}, {"quality": "partial_unverified"},
        {"checksum": "not-a-sha"}, {"instrument": "inverse"},
        {"end_ts": END}, {"checked_at": START},
    ):
        source = verified_provenance()
        source["archive"].update(change)
        assert build_profile(samples(), START, END, D(1),
                             coverage_verified=True, expected_trade_count=3,
                             provenance=source) is None
    for change in (
        {"status": "mismatch"}, {"archive_sha256": "b" * 64},
        {"trade_count": 2}, {"trade_base_qty": "6"},
        {"minute_ts": END}, {"checked_at": START},
    ):
        source = verified_provenance()
        source["minute_checks"][0].update(change)
        assert build_profile(samples(), START, END, D(1),
                             coverage_verified=True, expected_trade_count=3,
                             provenance=source) is None
    bad = samples()
    bad[1] = dict(bad[1], price="101.5")
    assert build_profile(bad, START, END, D(1),
                         coverage_verified=True, expected_trade_count=3,
                         provenance=verified_provenance()) is None
    without_id = samples()
    without_id[1] = {k: v for k, v in without_id[1].items()
                     if k != "trade_id"}
    assert build_profile(without_id, START, END, D(1),
                         coverage_verified=True, expected_trade_count=3,
                         provenance=verified_provenance()) is None


def test_daily_archive_requires_every_minute_and_matching_checksum():
    trades = samples() + [{
        "ts": END + timedelta(seconds=1), "price": "100",
        "qty": "1", "trade_id": "4",
    }]
    valid = verified_provenance()
    valid["minute_checks"].append({
        **valid["minute_checks"][0], "minute_ts": END,
        "trade_count": 1, "trade_base_qty": "1",
        "candle_base_volume": "1",
        "checked_at": END + timedelta(minutes=1, seconds=6),
    })
    candidate = build_profile(trades, START, END + timedelta(minutes=1),
                              D(1), coverage_verified=True,
                              expected_trade_count=4, provenance=valid)
    assert candidate is not None and candidate.coverage == "externally_verified"
    incomplete = verified_provenance()
    assert build_profile(trades, START, END + timedelta(minutes=1),
                         D(1), coverage_verified=True, expected_trade_count=4,
                         provenance=incomplete) is None
    replaced = verified_provenance()
    replaced["minute_checks"].append(dict(
        valid["minute_checks"][0], checked_at=END + timedelta(minutes=1, seconds=6)))
    assert build_profile(trades, START, END + timedelta(minutes=1),
                         D(1), coverage_verified=True, expected_trade_count=4,
                         provenance=replaced) is None

def test_huge_direct_profile_rejected_before_zone_iteration():
    import pytest
    from eth_tester.strategies.volume import VolumeProfile
    with pytest.raises(ValueError, match="unsafe_profile_span"):
        VolumeProfile(START, END, D("0.00000001"), D(100), D(100),
                      D(100), {D(100): D(5), D("100000000"): D(1)},
                      D(6), 2, "self_reported_unverified")


def test_mutated_direct_profile_still_bounded():
    import pytest
    profile = build_profile(samples(), START, END, D(1),
                            coverage_verified=True, expected_trade_count=3)
    profile.by_price[D("100000000")] = D(1)
    with pytest.raises(ValueError, match="unsafe_profile_span"):
        profile.zones("low")


def test_missing_closed_volume_bar_denied():
    profile = build_profile(samples(), START, END, D(1),
                            coverage_verified=True, expected_trade_count=3,
                            provenance=verified_provenance())
    bars = [candle(1, "100.1", "100.8", "100.5"),
            candle(2, "100.1", "100.9", "100.4"),
            candle(3, "100.1", "100.8", "100.5"),
            candle(4, "99.9", "100.6", "100.2", "100.1")]
    bars[0].pop("closed")
    assert volume_signal(bars, profile) is None
