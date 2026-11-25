"""D round-9 shadow ensemble: synthetic OHLCV, never real Bybit evidence."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D

import pytest

from eth_tester.strategies.ensemble import (
    CONTRACT, _complete_mtf, _confirmed_levels, _decimal,
    evaluate_shadow_ensemble,
)

START = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)


def candles(count=80):
    bars = [{
        "ts": START + timedelta(minutes=i),
        "open": D(100), "high": D("100.4"),
        "low": D("99.6"), "close": D(100), "volume": D(5),
        "closed": True,
    } for i in range(count)]
    bars[60]["high"] = D("101.1")  # Confirmed resistance after +2 CLOSED bars.
    bars[-1].update(open=D("100.1"), high=D("101.6"),
                    low=D(100), close=D("101.2"), volume=D(8))
    return bars


def check_no_promotion(result):
    assert result["contract"] == CONTRACT
    assert result["mode"] == "shadow_only"
    assert result["approved"] is False
    assert result["paper_entry_authorized"] is False
    assert result["ml_eligible"] is False
    assert result["promoted"] is False
    assert result["expected_net_lower_bound"] is None
    assert result["calibration"] == "not_independently_calibrated"
    assert result["source_quality"] == "closed_candle_research_only"
    assert len(result["diagnostic_sha256"]) == 64


def test_breakout_proposed_only_as_shadow_and_no_automatic_paper():
    bars = candles()
    snapshot = evaluate_shadow_ensemble(bars, decision_at=bars[-1]["ts"]+timedelta(minutes=1))
    check_no_promotion(snapshot)
    assert snapshot["status"] == "shadow_candidates", snapshot
    assert snapshot["selected"] == "breakout"
    candidate = next(c for c in snapshot["candidates"] if c["pattern"] == "breakout")
    assert candidate["direction"] == "long"
    assert candidate["eligibility"] == "unvalidated_shadow_candidate"
    assert D(candidate["adaptive_stop"]) < D(candidate["entry_reference"]) < D(candidate["adaptive_take"])
    assert D(candidate["stop_fraction"]) <= D("0.02")
    assert D(candidate["costed_break_even_win_rate"]) > 0
    assert candidate["calibrated_probability_lower"] is None
    assert candidate["paper_entry_authorized"] is False


def test_idempotent_checksum_same_original_closed_bars():
    bars = candles()
    first = evaluate_shadow_ensemble(bars)
    second = evaluate_shadow_ensemble(deepcopy(bars))
    assert first == second
    check_no_promotion(first)
    assert first["context"]["mtf_15m"]["last_closed_at"] <= (
        bars[-1]["ts"]+timedelta(minutes=1)).isoformat()


@pytest.mark.parametrize("mutation,reason", [
    ("missing", "missing_or_out_of_order_bar"),
    ("reversed", "missing_or_out_of_order_bar"),
    ("open", "open_candle_unavailable"),
    ("naive", "utc_minute_aligned_candle_required"),
    ("subminute", "utc_minute_aligned_candle_required"),
    ("future", "future_candle_relative_to_observation"),
    ("negative_volume", "invalid_ohlcv_candle"),
    ("crossed", "invalid_ohlcv_candle"),
    ("nan", "nonfinite_market_number"),
    ("bool", "boolean_market_number"),
])
def test_invalid_or_missing_market_data_fail_closed(mutation, reason):
    bars = candles()
    as_of = bars[-1]["ts"] + timedelta(minutes=1)
    if mutation == "missing":
        del bars[14]
    elif mutation == "reversed":
        bars[14], bars[15] = bars[15], bars[14]
    elif mutation == "open":
        bars[-1]["closed"] = False
    elif mutation == "naive":
        bars[-1]["ts"] = bars[-1]["ts"].replace(tzinfo=None)
    elif mutation == "subminute":
        bars[-1]["ts"] += timedelta(seconds=1)
    elif mutation == "future":
        as_of -= timedelta(seconds=1)
    elif mutation == "negative_volume":
        bars[-1]["volume"] = D(-1)
    elif mutation == "crossed":
        bars[-1]["low"] = D("102.0")
    elif mutation == "nan":
        bars[-1]["high"] = D("NaN")
    elif mutation == "bool":
        bars[-1]["volume"] = True
    result = evaluate_shadow_ensemble(bars, decision_at=as_of)
    check_no_promotion(result)
    assert result["status"] == "no_trade"
    assert result["reason"] == reason, result


def test_unbounded_or_short_history_cannot_authorize_entry():
    for history in ([], candles()[:59], candles()*3):
        result = evaluate_shadow_ensemble(history)
        check_no_promotion(result)
        assert result["status"] == "no_trade"
        assert result["reason"] == "insufficient_or_unbounded_closed_history"


def test_recovery_after_gapped_history_never_replays_previous_signal():
    bars = candles()
    complete = evaluate_shadow_ensemble(bars)
    broken = bars[:45] + bars[46:]
    rejected = evaluate_shadow_ensemble(broken)
    check_no_promotion(rejected)
    assert rejected["status"] == "no_trade"
    assert rejected["reason"] == "missing_or_out_of_order_bar"
    assert complete["diagnostic_sha256"] != rejected["diagnostic_sha256"]
    # Only the actual original aligned window can be evaluated: adding later
    # candles never changes an archived proposal into a real order.
    reconstructed = evaluate_shadow_ensemble(bars)
    assert reconstructed == complete
    assert reconstructed["paper_entry_authorized"] is False


def test_complete_multitimeframe_never_uses_future_unclosed_bucket():
    bars = candles()
    latest_close = bars[-1]["ts"] + timedelta(minutes=1)
    five = _complete_mtf(bars, 5, as_of=latest_close)
    fifteen = _complete_mtf(bars, 15, as_of=latest_close)
    assert five["last_closed_at"] == latest_close.isoformat()
    assert fifteen["last_closed_at"] < latest_close.isoformat()
    assert fifteen["direction"] == "flat"


def test_cost_slippage_fee_outliers_and_volatility_shock_fail_closed():
    for kwargs in ({"slippage_bps": "NaN"},
                   {"slippage_bps": -1},
                   {"taker_fee_rate": "0.1"}):
        result = evaluate_shadow_ensemble(candles(), **kwargs)
        check_no_promotion(result)
        assert result["status"] == "no_trade"
    blown = candles()
    blown[-1]["high"] = D(150)
    result = evaluate_shadow_ensemble(blown)
    check_no_promotion(result)
    assert result["status"] == "no_trade"
    assert result["reason"] in ("unsafe_volatility_regime", "range_shock_or_unreliable_baseline")


def test_missing_recent_volume_blocks_shadow_even_with_pattern():
    bars = candles()
    bars[-3]["volume"] = D(0)
    result = evaluate_shadow_ensemble(bars)
    check_no_promotion(result)
    assert result["reason"] == "no_recent_volume_observed"


def test_absorption_only_proxy_never_claims_execution_attribution():
    bars = candles()
    bars[60]["high"] = D("100.48")
    # High wick on a resistance-pivot retest; high observed volume is only an
    # OHLCV hint and MUST NOT become a certified footprint/absorption fill.
    bars[-1].update(open=D("100.1"), close=D(100),
                    high=D("101.25"), low=D("99.6"), volume=D(50))
    result = evaluate_shadow_ensemble(bars)
    check_no_promotion(result)
    absorptions = [c for c in result["candidates"] if c["pattern"] == "absorption_proxy"]
    assert absorptions
    assert all(c["eligibility"] == "requires_real_orderflow_confirmation"
               for c in absorptions)
    assert all(c["paper_entry_authorized"] is False for c in absorptions)


def test_causal_pivots_require_two_previous_confirmed_closes():
    bars = candles()
    # A pivot formed just one minute before signal is not confirmed.
    bars[-3]["high"] = D(102)
    levels = _confirmed_levels(bars)
    assert not any(side == "resistance" and origin == bars[-3]["ts"]
                   for side, _, origin, _ in levels)
    assert all(confirmation <= bars[-1]["ts"]
               for _, _, _, confirmation in levels)


def test_absent_external_calibration_never_claims_positive_expectancy():
    result = evaluate_shadow_ensemble(candles())
    check_no_promotion(result)
    assert all(c["expected_net_lower_bound"] is None for c in result["candidates"])
    assert all(c["paper_entry_authorized"] is False for c in result["candidates"])
    assert "win_probability" not in result


def test_shadow_record_is_bounded_and_no_credentials_or_external_actions():
    results = [evaluate_shadow_ensemble(candles()) for _ in range(3)]
    assert all(len(r["candidates"]) <= 8 for r in results)
    assert results[0] == results[1] == results[2]
    assert _decimal("0.0000000000001") == D("0.0000000000001")

@pytest.mark.parametrize("scenario,expected,direction", [
    ("breakout", "breakout", "long"),
    ("retest", "retest", "long"),
    ("false_breakout", "false_breakout", "short"),
    ("bounce", "bounce", "short"),
    ("support_bounce", "bounce", "long"),
])
def test_all_price_pattern_families_are_research_only(scenario, expected, direction):
    bars = candles()
    if scenario == "retest":
        bars[70].update(open=D(100), high=D("101.5"), close=D("101.3"))
        bars[-2].update(open=D(100), high=D("101.5"), close=D("101.3"))
        bars[-1].update(open=D("101.2"), low=D(100), close=D("101.2"))
    elif scenario == "false_breakout":
        bars[70].update(open=D(100), high=D("101.5"), close=D("101.3"))
        bars[-2].update(open=D(100), high=D("101.5"), close=D("101.3"))
        bars[-1].update(open=D("101.2"), high=D("101.3"),
                        low=D("100.5"), close=D("100.7"))
    elif scenario == "bounce":
        bars[-1].update(open=D("101.2"), high=D("101.4"),
                        low=D("100.5"), close=D("100.7"))
    elif scenario == "support_bounce":
        bars[60]["high"] = D("100.4")
        bars[60]["low"] = D("99.4")
        bars[-1].update(open=D("99.4"), high=D("100.1"),
                        low=D("99.3"), close=D("99.8"))
    result = evaluate_shadow_ensemble(bars)
    check_no_promotion(result)
    matching = [c for c in result["candidates"] if c["pattern"] == expected
                and c["direction"] == direction]
    assert matching, (scenario, result)
    assert all(x["expected_net_lower_bound"] is None for x in matching)


def test_no_pattern_and_extremely_low_volatility_report_no_trade():
    bars = candles()
    bars[60]["high"] = D("100.4")
    bars[-1].update(open=D(100), close=D(100), high=D("100.4"), low=D("99.6"))
    result = evaluate_shadow_ensemble(bars)
    check_no_promotion(result)
    assert result["reason"] == "no_causal_pattern"
    tiny = candles()
    for bar in tiny:
        bar.update(open=D(100), high=D("100.001"),
                   low=D("99.999"), close=D(100))
    result = evaluate_shadow_ensemble(tiny)
    check_no_promotion(result)
    assert result["status"] == "no_trade"
    assert result["reason"] == "unsafe_volatility_regime"


def test_decimal_context_changes_cannot_mutate_research_decision():
    from decimal import Context, localcontext
    bars = candles()
    expected = evaluate_shadow_ensemble(bars)
    with localcontext(Context(prec=7)):
        actual = evaluate_shadow_ensemble(bars)
    assert actual == expected


def test_mutually_opposed_shadow_candidates_cannot_select_direction(monkeypatch):
    import eth_tester.strategies.ensemble as engine
    bars = candles()
    original = engine._patterns
    bars[-1].update(open=D(100), high=D("100.5"), low=D("99.5"), close=D(100))
    def opposing(*args, **kwargs):
        return [
            ("breakout", "long", "resistance", D("101.1"),
             bars[60]["ts"], bars[63]["ts"]),
            ("breakout", "short", "support", D("100.9"),
             bars[61]["ts"], bars[64]["ts"]),
        ]
    monkeypatch.setattr(engine, "_patterns", opposing)
    result = evaluate_shadow_ensemble(bars)
    check_no_promotion(result)
    assert result["status"] == "no_trade"
    assert result["reason"] == "conflicting_shadow_directions"
    assert result["selected"] is None
    monkeypatch.setattr(engine, "_patterns", original)

def test_closed_flag_missing_is_not_confirmed():
    bars = candles()
    bars[0].pop("closed")
    outcome = evaluate_shadow_ensemble(
        bars, decision_at=bars[-1]["ts"] + timedelta(minutes=1))
    check_no_promotion(outcome)
    assert outcome["reason"] == "open_candle_unavailable"
    assert outcome.get("input_sha256") is None


def test_hidden_opposite_direction_fails_closed(monkeypatch):
    import eth_tester.strategies.ensemble as m
    bars = candles()
    bars[-1].update(open=D(100), high=D("100.5"), low=D("99.5"), close=D(100))
    pivot = (bars[60]["ts"], bars[63]["ts"])
    candidates = [("breakout", "long", "resistance", D("101.1"), *pivot)
                  for _ in range(9)]
    candidates.append(("breakout", "short", "support", D("100.9"), *pivot))
    monkeypatch.setattr(m, "_patterns", lambda *a, **kw: candidates)
    result = evaluate_shadow_ensemble(bars)
    check_no_promotion(result)
    assert result["reason"] == "conflicting_shadow_directions"
    assert result["selected"] is None
    assert result["candidate_overflow"] is True
    assert result["evaluated_candidate_count"] == 10


def test_same_direction_overflow_has_no_research_selection(monkeypatch):
    import eth_tester.strategies.ensemble as m
    bars = candles()
    monkeypatch.setattr(m, "_patterns", lambda *a, **kw: [
        ("breakout", "long", "resistance", D("101.1"),
         bars[60]["ts"], bars[63]["ts"]) for _ in range(9)])
    result = evaluate_shadow_ensemble(bars)
    assert result["reason"] == "candidate_cap_exceeded_no_trade"
    check_no_promotion(result)


def test_raw_volume_revision_changes_fingerprint():
    bars = candles()
    original = evaluate_shadow_ensemble(bars)
    altered = deepcopy(bars)
    altered[1]["volume"] += D("0.0001")
    revised = evaluate_shadow_ensemble(altered)
    assert original["input_sha256"] != revised["input_sha256"]
    assert original["source_identity_authoritative"] is False


def test_every_closed_bar_research_is_independent_and_non_executable():
    from eth_tester.strategies.ensemble import evaluate_closed_bar_research
    bars = candles()
    received_at = bars[-1]["ts"] + timedelta(minutes=1)
    observation = evaluate_closed_bar_research(bars, observed_at=received_at)
    check_no_promotion(observation)
    assert observation["independent_of_incumbent"] is True
    assert observation["research_observed_at"] == received_at.isoformat()
    assert observation["observation_receipt_authoritative"] is False
    assert observation["source_identity_authoritative"] is False
    assert observation["input_bar_count"] == len(bars)
    assert len(observation["input_sha256"]) == 64


def test_every_closed_bar_research_includes_no_trade_without_incumbent():
    from eth_tester.strategies.ensemble import evaluate_closed_bar_research
    bars = candles()
    for bar in bars:
        bar.update(open=D(100), high=D("100.25"), low=D("99.75"),
                   close=D(100), volume=D(10))
    observation = evaluate_closed_bar_research(
        bars, observed_at=bars[-1]["ts"] + timedelta(minutes=1))
    check_no_promotion(observation)
    assert observation["status"] == "no_trade"
    assert observation["selected"] is None
    assert observation["reason"] in (
        "no_causal_pattern", "all_candidates_unconfirmed_or_mtf_conflicted")
    assert observation["independent_of_incumbent"] is True
    assert len(observation["input_sha256"]) == 64


@pytest.mark.parametrize("bad_time", [None, START.replace(tzinfo=None)])
def test_every_closed_bar_research_rejects_missing_real_utc_clock(bad_time):
    from eth_tester.strategies.ensemble import evaluate_closed_bar_research
    result = evaluate_closed_bar_research(candles(), observed_at=bad_time)
    check_no_promotion(result)
    assert result["status"] == "no_trade"
    assert result["reason"] == "actual_utc_decision_time_required"


def test_every_closed_bar_research_rejects_future_and_unconfirmed_bars():
    from eth_tester.strategies.ensemble import evaluate_closed_bar_research
    bars = candles()
    end = bars[-1]["ts"] + timedelta(minutes=1)
    future = evaluate_closed_bar_research(
        bars, observed_at=end - timedelta(seconds=1))
    check_no_promotion(future)
    assert future["reason"] == "future_candle_relative_to_observation"
    unconfirmed = deepcopy(bars)
    unconfirmed[0].pop("closed")
    result = evaluate_closed_bar_research(unconfirmed, observed_at=end)
    check_no_promotion(result)
    assert result["reason"] == "open_candle_unavailable"


def test_every_closed_bar_research_rejects_gaps_and_short_windows():
    from eth_tester.strategies.ensemble import evaluate_closed_bar_research
    bars = candles()
    end = bars[-1]["ts"] + timedelta(minutes=1)
    short = evaluate_closed_bar_research(bars[:59], observed_at=end)
    check_no_promotion(short)
    assert short["reason"] == "insufficient_or_unbounded_closed_history"
    broken = deepcopy(bars)
    broken[13]["ts"] += timedelta(minutes=1)
    gap = evaluate_closed_bar_research(broken, observed_at=end)
    check_no_promotion(gap)
    assert gap["reason"] == "missing_or_out_of_order_bar"


def test_every_closed_bar_research_input_revision_is_detected():
    from eth_tester.strategies.ensemble import evaluate_closed_bar_research
    bars = candles()
    at = bars[-1]["ts"] + timedelta(minutes=1)
    first = evaluate_closed_bar_research(bars, observed_at=at)
    revised = deepcopy(bars)
    revised[4]["volume"] += D("0.0001")
    second = evaluate_closed_bar_research(revised, observed_at=at)
    assert first["input_sha256"] != second["input_sha256"]
    assert first["diagnostic_sha256"] != second["diagnostic_sha256"]
    assert first["observation_receipt_authoritative"] is False
    assert second["ml_eligible"] is False
