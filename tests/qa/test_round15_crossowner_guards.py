"""H R15: safety invariants across D, F, E and B; no DB/orders needed.

These import the actual repository modules in CI. Local isolated mirrors do not
count as real PG or native Windows acceptance.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest


def test_empty_prospective_population_is_missing_not_no_signal():
    from eth_tester.ml.prospective import audit_closed_minute_research

    start = 1_795_521_600_000  # UTC minute-aligned diagnostic window
    start -= start % 60_000
    report = audit_closed_minute_research(
        [], [], symbol="ETHUSDT", start_ms=start,
        end_ms=start + 3 * 60_000, as_of_ms=start + 10 * 60_000,
    )
    assert report["expected_closed_minutes"] == 3
    assert report["missing_observations"] == 3
    assert report["no_signal_minutes"] == 0
    assert report["original_observations"] == 0
    assert report["ml_eligible"] is False
    assert report["returns_computed"] is False
    assert report["go"] is False


def test_shadow_research_no_signal_is_not_real_order_permission():
    from eth_tester.strategies.ensemble import evaluate_closed_bar_research

    start = datetime(2026, 11, 24, 12, tzinfo=timezone.utc)
    bars = [
        {"ts": start + timedelta(minutes=i), "open": Decimal("3000"),
         "high": Decimal("3002"), "low": Decimal("2998"),
         "close": Decimal("3000"), "volume": Decimal("10"), "closed": True}
        for i in range(75)
    ]
    observed = bars[-1]["ts"] + timedelta(minutes=1, seconds=3)
    row = evaluate_closed_bar_research(bars, observed_at=observed)
    assert row["status"] == "no_trade"
    assert row["paper_entry_authorized"] is False
    assert row["ml_eligible"] is False
    assert row["observation_receipt_authoritative"] is False
    assert row["approved"] is False
    assert row["expected_net_lower_bound"] is None


def test_forged_cost_verification_flag_does_not_certify_paper_economics():
    from eth_tester.ml.extract import _require_complete_paper_economics

    record = {"funding_verified": False,
              "execution_quality": "paper_proxy",
              "execution_events": []}
    forged = {"net_pnl_certified": True,
              "funding_schedule_complete": True,
              "ml_eligible": True}
    with pytest.raises(ValueError, match="provisional_paper_economics"):
        _require_complete_paper_economics(
            record, forged, Decimal("1.10"), Decimal("1000")
        )


def test_trusted_backup_binary_cannot_be_selected_from_path(monkeypatch):
    from eth_tester.storage.secure_backup import trusted_tool

    monkeypatch.delenv("ETH_TESTER_PG_BIN_DIR", raising=False)
    with pytest.raises(RuntimeError):
        trusted_tool("pg_dump")
    with pytest.raises((RuntimeError, ValueError)):
        trusted_tool("pg_restore")
    with pytest.raises((RuntimeError, ValueError)):
        trusted_tool("curl")


def test_fixed_1000_usd_paper_trade_calculates_only_simulated_net():
    from eth_tester.paper import estimate_trade

    outcome = estimate_trade(
        "long", Decimal("100"), Decimal("102"), Decimal("1000"),
        Decimal("0.00055"), Decimal("2"), funding=Decimal("3"),
    )
    assert outcome["quantity"] > 0
    assert outcome["fees"] > 0
    assert outcome["net"] == (
        outcome["gross"] - outcome["fees"] - outcome["funding"]
    )
    assert outcome["funding"] == Decimal("3")
    assert "funding_verified" not in outcome
