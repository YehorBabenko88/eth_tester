from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from types import SimpleNamespace

from eth_tester.orderflow import _matches_volume, _valid_oi, confirm_rejection, _certified_snapshot
from eth_tester.ml import FEATURE_NAMES
from eth_tester.strategies import strategy_identity
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
        end_ms = int((START + timedelta(minutes=1)).timestamp() * 1000)
        event_ms = end_ms - 1000
        receipt_ms = end_ms + 1000
        checked_ms = end_ms + 2000
        available_ms = end_ms + 3000
        proof = {
            "source": "validated_market_data", "instrument": "linear",
            "interval": "", "checksum": "c" * 64,
            "coverage_status": "verified",
            "source_event_ms": event_ms,
            "source_receipt_ms": receipt_ms,
            "source_checked_at_ms": checked_ms,
            "evidence_ref": "immutable-verification",
        }
        self.snapshot = {
            "snapshot_key": "a" * 64, "bar_ts": START,
            "available_at": START + timedelta(minutes=1, seconds=3),
            "observed_at": START + timedelta(minutes=1, seconds=7),
            "computed_at": START + timedelta(minutes=1, seconds=9),
            "source": "verified_causal_analytics", "source_checksum": "b" * 64,
            "quality": "complete", "is_causal": True,
            "values_json": {name: "1" for name in FEATURE_NAMES},
            "provenance": {
                "contract": "eth_feature_snapshot_v2",
                "certified_by": "independent_source_proof",
                "source_coverage_verified": True,
                "coverage_status": "verified",
                "complete_trade_ids_proven": False,
                "source_receipt_ms": receipt_ms,
                "source_checked_at_ms": checked_ms,
                "feature_sources": {name: dict(proof) for name in FEATURE_NAMES},
                "feature_available_ms": {
                    name: available_ms for name in FEATURE_NAMES
                },
            },
        }

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
        if "FROM feature_snapshots" in statement:
            return Result(self.snapshot)
        raise AssertionError("Unexpected SQL: " + statement)


def evaluate(conn, bar_volume=D(50), now=NOW):
    settings = SimpleNamespace(symbol="ETHUSDT", taker_fee_rate=D("0.00055"),
                               slippage_bps=D(2))
    bar = {"ts": START, "open": D(101), "high": D(103),
           "low": D(100), "close": D(102), "volume": bar_volume}
    signal = Signal("long", D(100), D(100), D(110),
                    {"strategy": "breakout", "strategy_version": strategy_identity()})
    return confirm_rejection(conn, settings, signal, bar, observed_at=now)


def test_positive_path_checks_all_required_gates():
    accepted, evidence = evaluate(FakeConn())
    assert accepted
    assert evidence["score"] == 100
    assert evidence["mtf"]["5"] and evidence["mtf"]["15"] and evidence["mtf"]["60"]
    assert evidence["baseline"] == "five_closed_1m_candles_volume_matched"
    assert evidence["immutable_feature_snapshot"]["snapshot_key"] == "a" * 64
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

def test_book_rejects_negative_level_even_if_total_positive():
    conn = FakeConn()
    conn.book["bids"] = {"100": "200", "99": "-1"}
    assert evaluate(conn)[1]["reason"] == "invalid_book_liquidity"
    conn = FakeConn()
    conn.book["asks"] = {"102": "0"}
    assert evaluate(conn)[1]["reason"] == "invalid_book_liquidity"

def test_missing_or_late_snapshot_denies_strong_signal():
    conn = FakeConn()
    conn.snapshot = None
    assert evaluate(conn)[1]["reason"] == "missing_verified_feature_snapshot"
    conn = FakeConn()
    conn.snapshot["computed_at"] = NOW + timedelta(seconds=1)
    assert evaluate(conn)[1]["reason"] == "missing_verified_feature_snapshot"
    conn = FakeConn()
    conn.snapshot["provenance"]["coverage_status"] = "observed_partial"
    assert evaluate(conn)[1]["reason"] == "missing_verified_feature_snapshot"
    conn = FakeConn()
    conn.snapshot["provenance"]["feature_available_ms"]["oi"] += 60_000
    assert evaluate(conn)[1]["reason"] == "missing_verified_feature_snapshot"
    conn = FakeConn()
    conn.snapshot["source_checksum"] = ""
    assert evaluate(conn)[1]["reason"] == "missing_verified_feature_snapshot"


def test_causal_snapshot_requires_complete_feature_availability():
    conn = FakeConn()
    assert _certified_snapshot(conn, "ETHUSDT", START, START + timedelta(minutes=1),
                               NOW)["snapshot_key"] == "a" * 64
    conn.snapshot["values_json"].pop("volatility")
    assert _certified_snapshot(conn, "ETHUSDT", START, START + timedelta(minutes=1),
                               NOW) is None


def test_v1_or_unverified_source_cannot_unlock_paper():
    for field, value in (
        ("contract", "eth_feature_snapshot_v1"),
        ("certified_by", "none"),
        ("source_coverage_verified", False),
        ("complete_trade_ids_proven", True),
        ("source_checked_at_ms", 0),
        ("source_receipt_ms", int((START + timedelta(minutes=1, seconds=5))
                                  .timestamp() * 1000)),
    ):
        conn = FakeConn()
        conn.snapshot["provenance"][field] = value
        assert evaluate(conn)[1]["reason"] == "missing_verified_feature_snapshot"


def test_per_feature_receipt_and_checksum_enforced():
    for field, value in (
        ("coverage_status", "partial"),
        ("source_receipt_ms", 0),
        ("source_checked_at_ms", 0),
        ("source_event_ms", int((START + timedelta(minutes=1, seconds=5))
                                 .timestamp() * 1000)),
        ("checksum", "fake"),
    ):
        conn = FakeConn()
        conn.snapshot["provenance"]["feature_sources"]["oi"][field] = value
        assert evaluate(conn)[1]["reason"] == "missing_verified_feature_snapshot"
    conn = FakeConn()
    del conn.snapshot["provenance"]["feature_sources"]["volatility"]
    assert evaluate(conn)[1]["reason"] == "missing_verified_feature_snapshot"


def test_snapshot_is_asof_actual_decision_and_never_selected_by_hindsight():
    conn = FakeConn()
    conn.snapshot["available_at"] = NOW + timedelta(seconds=1)
    assert evaluate(conn)[1]["reason"] == "missing_verified_feature_snapshot"
    conn = FakeConn()
    conn.snapshot["snapshot_key"] = "not-a-hash"
    assert evaluate(conn)[1]["reason"] == "missing_verified_feature_snapshot"
