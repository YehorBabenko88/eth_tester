"""E Round15 pure policy tests; never represent PostgreSQL or exchange acceptance."""
from decimal import Decimal

import pytest

from eth_tester.simulation.provisional_budget import provisional_budget


def _closed(pnl="0", notional="1000", funding_verified=False):
    return {"notional": Decimal(notional), "net_pnl": Decimal(pnl),
            "funding_verified": funding_verified}


def test_one_provisional_close_allows_second_virtual_trade():
    cap = provisional_budget(10000, 10040, [_closed("40")], 1000)
    assert cap["risk_capital_usd"] == Decimal(9000)
    assert cap["quarantined_usd"] == Decimal(1000)
    assert cap["closed_quarantined_count"] == 1
    assert cap["costs_certified"] is False
    assert cap["funding_unknown_is_zero"] is False
    assert cap["reserve_is_funding_loss_upper_bound"] is False


def test_provisional_losses_are_not_reused_or_hidden():
    cap = provisional_budget(10000, 9700, [_closed("-300")], 1000)
    assert cap["quarantined_usd"] == Decimal(1300)
    assert cap["risk_capital_usd"] == Decimal(8400)


def test_full_budget_exhausts_after_ten_closed_positions():
    with pytest.raises(RuntimeError, match="budget exhausted"):
        provisional_budget(10000, 10000, [_closed()] * 10, 1000)


def test_does_not_auto_release_on_funding_flag():
    cap = provisional_budget(10000, 10000,
                             [_closed(funding_verified=True)], 1000)
    assert cap["quarantined_usd"] == Decimal(1000)


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity", None, "bad"])
def test_malformed_history_fails_closed(bad):
    with pytest.raises(RuntimeError):
        provisional_budget(10000, 10000, [{"notional": 1000,
                                          "net_pnl": bad}], 1000)


@pytest.mark.parametrize("bad", [0, -1, "NaN", "Infinity", None])
def test_no_invalid_capital(bad):
    with pytest.raises(RuntimeError):
        provisional_budget(10000, bad, [], 1000)


def test_legacy_nonconforming_nominal_refuses():
    with pytest.raises(RuntimeError, match="Unrecognized"):
        provisional_budget(10000, 10000, [_closed(notional="800")], 1000)


def test_restart_idempotence_without_secondary_balance():
    rows = [_closed("8"), _closed("-12")]
    assert provisional_budget(10000, 9996, rows, 1000) == provisional_budget(
        10000, 9996, rows, 1000)
