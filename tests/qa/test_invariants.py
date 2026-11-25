"""Independent arithmetic, causal timing, and security properties."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from random import Random
from types import SimpleNamespace

import pytest

from eth_tester import backtest
from eth_tester.analytics import cluster, confirmed_pivots, poc
from eth_tester.archives import parse_trade
from eth_tester.paper import estimate_trade
from eth_tester.strategy import Signal
from eth_tester.telegram_control import authorise


def test_poc_delta_footprint_randomized_invariants():
    rng = Random(481516)
    for _ in range(250):
        trades = [
            {"price": str(rng.randint(95, 105)),
             "qty": str(rng.randint(1, 100) / 10),
             "side": rng.choice(["Buy", "Sell"])}
            for _ in range(rng.randint(1, 30))
        ]
        result = cluster(trades)
        buy = sum((D(t["qty"]) for t in trades if t["side"] == "Buy"), D(0))
        sell = sum((D(t["qty"]) for t in trades if t["side"] == "Sell"), D(0))
        assert result["delta"] == buy - sell
        assert result["volume"] == buy + sell
        assert result["tps"] == len(trades)
        assert sum((v["buy"] + v["sell"] for v in result["levels"].values()), D(0)) == buy + sell
        bins = {k: v["buy"] + v["sell"] for k, v in result["levels"].items()}
        assert result["poc"] == min(bins, key=lambda k: (-bins[k], k))


def test_poc_ties_and_empty_input():
    assert poc({}) is None
    assert poc({D("101"): D(4), D("100"): D(4)}) == D(100)


@pytest.mark.parametrize("side,exit_price", [("long", D(110)), ("short", D(90))])
def test_long_short_fees_funding_slippage(side, exit_price):
    zero = estimate_trade(side, D(100), exit_price, D(1000), D(0), D(0))
    costly = estimate_trade(side, D(100), exit_price, D(1000),
                            D("0.00055"), D(2), D("1.25"))
    assert zero["gross"] == D(100)
    assert costly["net"] < zero["net"]
    assert costly["fees"] > 0
    assert costly["funding"] == D("1.25")
    assert costly["net"] == costly["gross"] - costly["fees"] - costly["funding"]


def test_pivot_confirmation_cannot_use_future_bar():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    highs = [100, 102, 110, 106, 105]
    lows = [90, 91, 92, 93, 94]
    bars = [{"ts": start + timedelta(minutes=i), "high": D(high),
             "low": D(low)} for i, (high, low) in enumerate(zip(highs, lows))]
    assert confirmed_pivots(bars[:4], flank=2) == []
    results = confirmed_pivots(bars, flank=2)
    assert ("resistance", bars[2]["ts"], D(110), bars[4]["ts"]) in results


def test_backtest_signal_sees_only_prior_closed_bars(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = [{"ts": start + timedelta(minutes=i), "open": D(100), "high": D(101),
             "low": D(99), "close": D(100)} for i in range(35)]
    bars[20]["high"], bars[20]["low"] = D(105), D(95)
    observed = []

    def strategy(prefix, *_args):
        observed.append(prefix[-1]["ts"])
        assert prefix[-1]["ts"] < bars[len(prefix)]["ts"]
        if len(prefix) == 20:
            return Signal("long", D(100), D(98), D(104), {"causal": True})
        return None

    monkeypatch.setattr(backtest, "price_signal", strategy)
    deals = backtest.simulate(bars, fees=D(0), slip=D(0))
    assert observed and observed[0] == bars[19]["ts"]
    assert all(ts < bars[20 + i]["ts"] for i, ts in enumerate(observed))
    assert len(deals) == 1
    assert deals[0]["opened_at"] == bars[20]["ts"]
    assert deals[0]["outcome"] == "stop"


def test_telegram_requires_private_chat_and_human_owner(monkeypatch):
    cfg = SimpleNamespace(tg_chat_id="123")
    user = SimpleNamespace(id=456, is_bot=False)
    chat = SimpleNamespace(id=123, type="private")
    update = SimpleNamespace(effective_chat=chat, effective_user=user)
    assert not authorise(cfg, update)
    monkeypatch.setenv("ETH_TESTER_TG_USER_ID", "456")
    assert authorise(cfg, update)
    assert not authorise(cfg, SimpleNamespace(
        effective_chat=SimpleNamespace(id=124, type="private"), effective_user=user))
    assert not authorise(cfg, SimpleNamespace(
        effective_chat=SimpleNamespace(id=123, type="group"), effective_user=user))
    assert not authorise(cfg, SimpleNamespace(
        effective_chat=chat, effective_user=SimpleNamespace(id=457, is_bot=False)))
    assert not authorise(cfg, SimpleNamespace(
        effective_chat=chat, effective_user=SimpleNamespace(id=456, is_bot=True)))
    cfg.tg_chat_id = ""
    assert not authorise(cfg, update)


def test_archive_timestamps_and_reject_invalid_side():
    value = {"timestamp": "1764000000.123", "trdMatchID": "test-1",
             "price": "3500.25", "size": "1.2", "side": "Buy"}
    ts, trade_id, price, qty, side = parse_trade(value)
    assert ts.tzinfo == timezone.utc
    assert (trade_id, price * qty, side) == ("test-1", D("4200.300"), "Buy")
    with pytest.raises(ValueError, match="aggressor"):
        parse_trade({**value, "side": "Unknown"})


def test_telegram_partial_update_fails_closed(monkeypatch):
    """Missing top-level Telegram objects must never grant control."""
    monkeypatch.setenv("ETH_TESTER_TG_USER_ID", "456")
    config = SimpleNamespace(tg_chat_id="123")
    person = SimpleNamespace(id=456, is_bot=False)
    chat = SimpleNamespace(id=123, type="private")
    assert not authorise(config, SimpleNamespace())
    assert not authorise(config, SimpleNamespace(effective_user=person))
    assert not authorise(config, SimpleNamespace(effective_chat=chat))
    assert not authorise(config, SimpleNamespace(
        effective_user=None, effective_chat=chat))
