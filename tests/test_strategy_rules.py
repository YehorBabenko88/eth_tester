from decimal import Decimal as D
from eth_tester.strategies import Parameters, pattern
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
