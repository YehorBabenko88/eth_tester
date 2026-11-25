"""E round9 explicit simulation-only pricing and excursion negative tests."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import pytest

from eth_tester.simulation.operations import (
    conservative_exit_proxy, observed_excursions, closed_equity_drawdown,
)

T=datetime(2026,11,25,20,1,15,tzinfo=timezone.utc)

@pytest.mark.parametrize("direction,print_price,bid,ask,expected", [
    ("long","100","99.5","100.1","99.5"),
    ("short","100","99.9","100.6","100.6"),
    ("long","100","101","101.2","100"),
    ("short","100","98.8","99","100"),
])
def test_quote_never_improves_trade_print(direction,print_price,bid,ask,expected):
    quote={"exchange_ts":T-timedelta(seconds=1),
           "received_ts":T-timedelta(milliseconds=100),
           "bid_price":bid,"ask_price":ask}
    result=conservative_exit_proxy(direction,print_price,T,quote)
    assert result["reference"]==D(expected)
    assert result["quality"].startswith("observed_bbo_proxy")
    assert result["spread_reference_cost_per_unit"]>=0

@pytest.mark.parametrize("mutation",[
    {"exchange_ts":T+timedelta(milliseconds=1)},
    {"received_ts":T+timedelta(milliseconds=1)},
    {"exchange_ts":T-timedelta(seconds=4)},
    {"bid_price":"102","ask_price":"100"},
    {"bid_price":"NaN"},
])
def test_future_stale_crossed_or_invalid_quote_falls_back(mutation):
    quote={"exchange_ts":T-timedelta(seconds=1),
           "received_ts":T-timedelta(milliseconds=100),
           "bid_price":"99","ask_price":"101"}
    quote.update(mutation)
    result=conservative_exit_proxy("long",100,T,quote)
    assert result["quality"]=="trade_print_proxy_no_liquidity"
    assert result["reference"]==100

def test_excursions_decimal_long_short_and_missing():
    assert observed_excursions("long","100","96","108",22)["mae_fraction"]==D("-.04")
    assert observed_excursions("short","100","96","108",22)["mae_fraction"]==D("-.08")
    assert observed_excursions("short","100","96","108",22)["mfe_fraction"]==D(".04")
    assert observed_excursions("long","100",None,None,0)["mae_fraction"] is None

@pytest.mark.parametrize("value",["NaN","Infinity",0,-1])
def test_invalid_price_never_enters_exit_model(value):
    with pytest.raises(ValueError):
        conservative_exit_proxy("long",value,T)

def test_realized_only_drawdown_excludes_unrealized_profit():
    result=closed_equity_drawdown(D("10000"),[D("500"),D("-1000"),D("100")])
    assert result["ending_equity"]==D("9600")
    assert result["max_drawdown_fraction"]==D("1000")/D("10500")
    assert result["quality"]=="realized_closed_paper_only"

def test_overlong_drawdown_history_is_unavailable_not_truncated():
    result=closed_equity_drawdown("100",[D("2"),D("3")],max_rows=1)
    assert result["max_drawdown_fraction"] is None
    assert result["quality"]=="unavailable_unbounded_history"

def test_nan_realized_pnl_fails_closed():
    with pytest.raises(ValueError):
        closed_equity_drawdown("100",[D("NaN")])
