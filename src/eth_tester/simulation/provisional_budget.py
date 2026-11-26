"""Conservative finite *virtual* paper-risk quarantine; not cost verification.

Every historically closed paper position consumes its full nominal from the
experimental capital allocation, whether or not its funding flag is true.
This intentionally never releases capital based on provisional reported PnL.
It is NOT a verified upper bound on missing exchange funding, slippage or fees.
"""
from decimal import Decimal, InvalidOperation


POLICY_VERSION = "paper_closed_nominal_quarantine_v1"


def _amount(value, label):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise RuntimeError(f"Invalid paper {label}") from exc
    if not result.is_finite():
        raise RuntimeError(f"Nonfinite paper {label}")
    return result


def provisional_budget(initial_equity, equity, closed_trades, nominal):
    """Derive non-releasable virtual reserve from committed paper history.

    Caller holds paper global advisory mutex and paper-account FOR UPDATE.
    No writes, release guesses, funding defaults, external APIs or ML labels.
    A 100%-of-nominal *virtual allocation*, not economic loss certification.
    """
    start = _amount(initial_equity, "initial equity")
    current = _amount(equity, "equity")
    size = _amount(nominal, "notional")
    if start <= 0 or current <= 0 or size != Decimal("1000"):
        raise RuntimeError("Invalid or exhausted virtual paper capital")
    quarantine = Decimal(0)
    count = 0
    for trade in closed_trades:
        trade_notional = _amount(trade["notional"], "historical notional")
        pnl = _amount(trade["net_pnl"], "provisional net")
        if trade_notional != size:
            raise RuntimeError("Unrecognized closed paper nominal; risk blocked")
        # Reserve full spent nominal independently of uncertain paper PnL;
        # retain a second conservative debit for any reported loss. Never
        # credit provisional profits and never infer zero missing funding.
        quarantine += size + max(Decimal(0), -pnl)
        count += 1
    available = min(start, current) - quarantine
    if available < size:
        raise RuntimeError("Provisional paper budget exhausted or uncertified")
    return {
        "version": POLICY_VERSION,
        "risk_capital_usd": available,
        "quarantined_usd": quarantine,
        "closed_quarantined_count": count,
        "costs_certified": False,
        "funding_unknown_is_zero": False,
        "reserve_is_funding_loss_upper_bound": False,
        "paper_only": True,
    }
