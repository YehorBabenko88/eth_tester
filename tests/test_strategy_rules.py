from decimal import Decimal as D
from eth_tester.strategies import Parameters, pattern, strategy_identity
from eth_tester.strategy import economics, price_signal

def bar(o,h,low,c):
    return dict(open=D(o),high=D(h),low=D(low),close=D(c))

def test_patterns():
    level=D(100)
    assert pattern("resistance",level,bar(99,100,98,99),bar(99,102,99,101),False)==("long","breakout")
    assert pattern("resistance",level,bar(100,102,100,101),bar(101,103,99,102),True)==("long","retest")
    assert pattern("resistance",level,bar(100,102,100,101),bar(101,102,98,99),True)==("short","false_breakout")
    assert pattern("support",level,bar(101,102,100,101),bar(100,102,99,101),False)==("long","rejection")

def test_cost_gate():
    assert economics("long",D(100),D(99),D("100.1"),D(".00055"),D(2)) is None
    assert economics("long",D(100),D(99),D(105),D(".00055"),D(2)) is not None

def test_no_signal_and_version():
    assert price_signal([]) is None
    assert Parameters().identity()==Parameters().identity()
    assert Parameters(min_score=80).identity()!=Parameters().identity()

def test_calibrated_expectancy_requires_conservative_probability():
    from eth_tester.strategy import calibrated_expectancy

    args = ("long", D(100), D(99), D(105), D("0.00055"), D(2))
    assert calibrated_expectancy(*args, None) is None
    assert calibrated_expectancy(*args, D("0.1")) is None
    assert calibrated_expectancy(*args, D("NaN")) is None
    assert calibrated_expectancy(*args, D("1.01")) is None
    qualified = calibrated_expectancy(*args, D("0.8"))
    assert qualified is not None
    assert D(qualified["expected_net_fraction_lower_bound"]) >= D("0.002")

def test_economics_invalid_inputs_fail_closed():
    args = (D(100), D(99), D(105), D("0.00055"), D(2))
    assert economics("invalid", *args) is None
    assert economics("long", *args[:3], D(-1), args[4]) is None
    assert economics("long", *args[:3], args[3], D(-2)) is None
    assert economics("long", D("NaN"), *args[1:]) is None
    assert economics("long", D(100), D(100), D(105), args[3], args[4]) is None

def test_open_or_naive_candle_exits_before_pivot(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from eth_tester import strategy

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = [dict(ts=start + timedelta(minutes=index), open=D(100),
                 high=D(101), low=D(99), close=D(100), closed=True)
            for index in range(12)]

    def unexpected(*args, **kwargs):
        raise AssertionError("unverified bars must not reach pivot evaluator")

    monkeypatch.setattr(strategy, "confirmed_pivots", unexpected)
    bars[3]["closed"] = False
    assert strategy.price_signal(bars) is None
    bars[3]["closed"] = True
    bars[3]["ts"] = bars[3]["ts"].replace(tzinfo=None)
    assert strategy.price_signal(bars) is None

def test_certified_expectancy_checks_issuer_time_and_version():
    from datetime import datetime, timezone
    from eth_tester.strategy import Signal, certified_expectancy

    decision = datetime(2026, 1, 2, tzinfo=timezone.utc)
    deadline = int(decision.timestamp() * 1000)
    signal = Signal("long", D(100), D(99), D(105),
                    {"strategy_version": "eth_rules_v2"})
    certificate = {
        "strategy_version": "eth_rules_v2", "model_version": "frozen-model-a",
        "method": "one_sided_wilson_95_iid_assumption",
        "selection": "frozen_model_nonuncertain_positive_predicted_net",
        "probability_lower": 0.9, "observations": 100, "net_wins": 95,
        "holdout_start_ms": deadline - 10_000_000,
        "holdout_end_ms": deadline - 100_000,
        "issued_at_ms": deadline - 10_000,
        "selected_evidence_sha256": "a" * 64,
        "paper_only": True, "independent_trades_assumed": True,
    }
    def evaluate_cert(data):
        return certified_expectancy(signal, data, decision_at=decision,
                                    entry_price=D(100),
                                    fee=D("0.00055"), slip=D(2))

    accepted = evaluate_cert(certificate)
    assert accepted is not None
    assert accepted["certificate_model_version"] == "frozen-model-a"
    assert accepted["iid_assumption_not_verified"]
    assert evaluate_cert({k: v for k, v in certificate.items()
                          if k != "issued_at_ms"}) is None
    assert evaluate_cert({**certificate, "issued_at_ms": deadline + 1}) is None
    assert evaluate_cert({**certificate, "strategy_version": "other"}) is None
    assert evaluate_cert({**certificate, "probability_lower": 0.1}) is None
    assert evaluate_cert({**certificate, "observations": 2}) is None

def test_all_eight_price_patterns_and_strict_no_match():
    level = D(100)
    cases = [
        ("resistance", False, 99, 99, 102, 98, 101, ("long", "breakout")),
        ("resistance", False, 99, 101, 102, 98, 99, ("short", "rejection")),
        ("resistance", True, 101, 101, 103, 99, 102, ("long", "retest")),
        ("resistance", True, 101, 101, 102, 98, 99, ("short", "false_breakout")),
        ("support", False, 101, 101, 102, 98, 99, ("short", "breakout")),
        ("support", False, 101, 99, 102, 98, 101, ("long", "rejection")),
        ("support", True, 99, 99, 101, 97, 98, ("short", "retest")),
        ("support", True, 99, 99, 102, 98, 101, ("long", "false_breakout")),
    ]
    for side, broken, previous, opening, high, low, close, expected in cases:
        assert pattern(side, level, {"close": D(previous)},
                       bar(opening, high, low, close), broken) == expected
    assert pattern("invalid", level, {"close": D(99)},
                   bar(99, 102, 98, 101), False) is None


def test_strategy_identity_is_stable_and_parameter_sensitive():
    params = Parameters()
    engine = strategy_identity(params)
    assert engine == params.engine_identity()
    assert engine.startswith("eth_rules_v3:")
    assert len(engine.split(":")[1]) == 12
    assert engine != strategy_identity(Parameters(min_score=80))
    assert params.identity()["parameter_sha256"].startswith(engine.split(":")[1])


def test_price_signal_includes_engine_hash_and_causal_confirmation():
    from datetime import datetime, timedelta, timezone
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = []
    for index in range(15):
        opening, high, low, close = D(100), D("100.5"), D("99.8"), D(100)
        if index == 4:
            low = D("99.6")
        if index == 14:
            opening, high, low, close = (
                D("99.7"), D("100.6"), D("99.0"), D("99.8"))
        bars.append({"ts": start + timedelta(minutes=index), "open": opening,
                     "high": high, "low": low, "close": close, "closed": True})
    signal = price_signal(bars)
    assert signal is not None
    assert signal.reason["strategy"] == "rejection"
    assert signal.reason["strategy_version"] == strategy_identity()
    assert signal.reason["parameter_sha256"] == Parameters().identity()["parameter_sha256"]
    assert signal.reason["confirmed_available_at"] == (
        start + timedelta(minutes=7)).isoformat()
    assert signal.reason["requires_live_confirmation"] is True
    shadow = signal.reason["ensemble_shadow"]
    assert shadow["contract"] == "eth_strategy_ensemble_v1"
    assert shadow["mode"] == "shadow_only"
    assert shadow["paper_entry_authorized"] is False
    assert shadow["ml_eligible"] is False
    assert shadow["approved"] is False
    assert shadow["expected_net_lower_bound"] is None
    assert signal.reason["strategy_version"] == strategy_identity()

