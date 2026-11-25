"""QA H R14: cross-owner shadow research, cost and volume provenance contracts."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest


def _candles():
    start = datetime(2026, 11, 24, 12, tzinfo=timezone.utc)
    return [{"ts": start + timedelta(minutes=i),
             "open": Decimal("3000"), "high": Decimal("3002"),
             "low": Decimal("2998"), "close": Decimal("3000"),
             "volume": Decimal("10"), "closed": True}
            for i in range(75)]


def _evaluate(bars, observed_at):
    from eth_tester.strategies.ensemble import evaluate_closed_bar_research
    return evaluate_closed_bar_research(
        bars, observed_at=observed_at,
        taker_fee_rate=Decimal("0.00055"), slippage_bps=Decimal("2"),
    )


def test_all_closed_minutes_include_no_trade_observation_without_admission():
    bars = _candles()
    observed = bars[-1]["ts"] + timedelta(minutes=1, seconds=2)
    first, repeated = _evaluate(bars, observed), _evaluate(bars, observed)
    assert first == repeated
    assert first["status"] == "no_trade"
    assert first["bar_ts"] == bars[-1]["ts"].isoformat()
    assert first["research_observed_at"] == observed.isoformat()
    assert first["independent_of_incumbent"] is True
    assert first["observation_receipt_authoritative"] is False
    assert first["paper_entry_authorized"] is False
    assert first["ml_eligible"] is False
    assert first["approved"] is False
    assert first["expected_net_lower_bound"] is None
    assert len(first["diagnostic_sha256"]) == 64


def test_no_trade_original_input_identity_changes_without_certifying_tape():
    left, right = _candles(), _candles()
    right[0]["high"] = Decimal("3003")
    observed = left[-1]["ts"] + timedelta(minutes=1, seconds=2)
    a, b = _evaluate(left, observed), _evaluate(right, observed)
    assert a["status"] == b["status"] == "no_trade"
    assert a["input_sha256"] != b["input_sha256"]
    assert a["diagnostic_sha256"] != b["diagnostic_sha256"]
    assert a["source_identity_authoritative"] is False
    assert b["source_identity_authoritative"] is False


def test_open_original_candle_never_cast_to_closed():
    bars = _candles()
    bars[-1]["closed"] = False
    result = _evaluate(bars, bars[-1]["ts"] + timedelta(minutes=1, seconds=2))
    assert result["status"] == "no_trade"
    assert result["reason"] == "open_candle_unavailable"
    assert result["paper_entry_authorized"] is False
    assert result["ml_eligible"] is False


def test_future_or_gapped_original_utc_bar_never_eligible():
    bars = _candles()
    last = bars[-1]["ts"]
    for sequence, observed in (
        (bars, last + timedelta(seconds=59)),
        (bars[:-2] + bars[-1:], last + timedelta(minutes=1)),
    ):
        result = _evaluate(sequence, observed)
        assert result["status"] == "no_trade"
        assert result["paper_entry_authorized"] is False
        assert result["ml_eligible"] is False
        assert result["approved"] is False
        assert result["reason"] in (
            "future_candle_relative_to_observation",
            "missing_or_out_of_order_bar",
        )


def test_missing_real_utc_observation_clock_not_replaced_by_backfill():
    bars = _candles()
    for observed in (None, datetime(2026, 11, 24, 12)):
        result = _evaluate(bars, observed)
        assert result["status"] == "no_trade"
        assert result["reason"] == "actual_utc_decision_time_required"
        assert result["paper_entry_authorized"] is False
        assert result["ml_eligible"] is False


def test_equal_volume_not_promoted_to_exchange_complete(monkeypatch):
    """Complete archive job plus identical quantities is NOT trade-ID proof."""
    from eth_tester import analytics
    stamp = datetime(2026, 11, 24, 12, tzinfo=timezone.utc)
    updates = []

    class Rows:
        def __init__(self, one=None, many=None):
            self.one = one
            self.many = many if many is not None else []
        def fetchone(self):
            return self.one
        def fetchall(self):
            return self.many

    class FakeConn:
        def execute(self, sql, args=None):
            query = " ".join(str(sql).split())
            if query.startswith("SELECT ts,delta,volume FROM features"):
                return Rows(many=[{"ts": stamp, "delta": Decimal(0),
                                   "volume": Decimal("0.001")}])
            if "SELECT ts,cvd FROM features" in query:
                return Rows()
            if "SELECT 1 FROM features" in query:
                return Rows()
            if "FROM candles" in query:
                return Rows(one={"volume": Decimal("0.001")})
            if "FROM open_interest" in query or "FROM funding" in query:
                return Rows()
            if "FROM market_book_state" in query:
                return Rows()
            if "FROM ingestion_job" in query:
                return Rows(one={"state": "complete"})
            if "UPDATE features SET" in query:
                updates.append(args)
                return Rows()
            raise AssertionError("Unexpected C QA query: " + query[:150])

    @contextmanager
    def fake_db(_settings):
        yield FakeConn()

    monkeypatch.setattr(analytics, "connection", fake_db)
    assert analytics.enrich_features(
        SimpleNamespace(symbol="ETHUSDT"), tf="1", hours=1
    ) == 1
    assert len(updates) == 1
    assert updates[0][4] in (
        "volume_parity_only_unverified", "archive_rollup_partial"
    )


def test_forged_verified_paper_net_cannot_override_missing_funding():
    from eth_tester.ml.extract import _require_complete_paper_economics
    reason = {
        "net_pnl_certified": True,
        "paper_cost_policy": {"version": "paper_taker_cost_v1",
                              "notional_usd": "1000",
                              "taker_fee_rate": "0.00055",
                              "slippage_bps": "2"},
    }
    row = {"funding_verified": False, "execution_quality": "paper_proxy",
           "execution_events": []}
    with pytest.raises(ValueError, match="provisional_paper_economics"):
        _require_complete_paper_economics(
            row, reason, Decimal("1.10"), Decimal("1000")
        )
