from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from types import SimpleNamespace

from eth_tester.orderflow import _matches_volume, _valid_oi, confirm_rejection
from eth_tester.strategy import Signal


START = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
NOW = START + timedelta(minutes=1, seconds=10)


class Result:
    def __init__(self, data):
        self.data = data

    def fetchone(self):
        return self.data

    def fetchall(self):
        return self.data


class FakeConn:
    def __init__(self):
        self.rows = [{"price": D(100), "buys": D(20), "sells": D(5), "n": 30},
                     {"price": D(102), "buys": D(20), "sells": D(5), "n": 30}]
        self.baseline_ticks = [
            {"ts": START - timedelta(minutes=5 - i), "n": 20,
             "buys": D(15), "sells": D(5)} for i in range(5)]
        self.baseline_candles = [
            {"ts": START - timedelta(minutes=5 - i), "volume": D(20)}
            for i in range(5)]
        self.oi = [
            {"ts": START, "value": D(1010)},
            {"ts": START - timedelta(minutes=5), "value": D(1000)}]
        self.book = {"bids": {"100": "200"}, "asks": {"102": "100"},
                     "updated_at": NOW - timedelta(seconds=1), "valid": True}
        self.recent = {"ts": NOW - timedelta(seconds=1), "price": D(102)}
        self.liquidations = [{"side": "Sell", "qty": D(2)}]

    def execute(self, statement, args):
        if "GROUP BY price" in statement:
            return Result(self.rows)
        if "date_trunc('minute',ts)" in statement:
            return Result(self.baseline_ticks)
        if "FROM candles" in statement and "tf='1'" in statement:
            return Result(self.baseline_candles)
        if "FROM open_interest" in statement:
            return Result(self.oi)
        if "FROM market_book_state" in statement:
            return Result(self.book)
        if "FROM candles" in statement and "tf=%s" in statement:
            minutes = int(args[1])
            latest = START - timedelta(minutes=minutes)
            rows = [{"ts": latest - timedelta(minutes=minutes * i),
                     "close": D(103 - i)} for i in range(3)]
            return Result(rows)
        if "source='websocket'" in statement:
            return Result(self.recent)
        if "FROM liquidation_events" in statement:
            return Result(self.liquidations)
        raise AssertionError("Unexpected SQL: " + statement)


def evaluate(conn, bar_volume=D(50), now=NOW):
    settings = SimpleNamespace(symbol="ETHUSDT", taker_fee_rate=D("0.00055"),
                               slippage_bps=D(2))
    bar = {"ts": START, "open": D(101), "high": D(103),
           "low": D(100), "close": D(102), "volume": bar_volume}
    signal = Signal("long", D(100), D(100), D(110),
                    {"strategy": "breakout", "strategy_version": "eth_rules_v2"})
    return confirm_rejection(conn, settings, signal, bar, observed_at=now)


def test_positive_path_checks_all_required_gates():
    accepted, evidence = evaluate(FakeConn())
    assert accepted
    assert evidence["score"] == 100
    assert evidence["mtf"]["5"] and evidence["mtf"]["15"] and evidence["mtf"]["60"]
    assert evidence["baseline"] == "five_closed_1m_candles_volume_matched"
    assert evidence["liquidations_observed_unverified"]["Sell"] == "2"
    assert D(evidence["economics_at_observed_entry"]["net_reward_risk"]) >= 2


def test_incomplete_or_stale_data_rejected():
    conn = FakeConn()
    conn.baseline_ticks.pop()
    assert evaluate(conn)[1]["reason"] == "incomplete_five_minute_tick_baseline"
    conn = FakeConn()
    conn.book["updated_at"] = NOW - timedelta(seconds=11)
    assert evaluate(conn)[1]["reason"] == "stale_book"
    conn = FakeConn()
    conn.oi[1]["ts"] += timedelta(seconds=1)
    assert evaluate(conn)[1]["reason"] == "stale_or_gapped_oi"
    assert evaluate(FakeConn(), bar_volume=D(90))[1]["reason"] == "tick_volume_unverified"
    assert evaluate(FakeConn(), now=START + timedelta(seconds=30))[1]["reason"] == "stale_or_open_bar"


def test_invalid_baseline_volume_and_oi_grid():
    conn = FakeConn()
    conn.baseline_candles[2]["volume"] = D(200)
    assert not evaluate(conn)[0]
    assert not _matches_volume(D(10), D(9))
    assert not _matches_volume(D("NaN"), D(9))
    assert _valid_oi(FakeConn().oi, START + timedelta(minutes=1))
    assert not _valid_oi(FakeConn().oi, START + timedelta(hours=1))
