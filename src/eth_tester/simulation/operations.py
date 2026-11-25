"""Fail-closed ETHUSDT paper execution evidence and observed-only risk review.

Never claim a trade print or top-of-book quote proves executable liquidity.
Neither exit pricing nor these diagnostics may qualify a row for ML training.
"""
from datetime import timedelta
from decimal import Decimal, InvalidOperation


def _positive(value):
    try:
        amount = Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError("Invalid paper price") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValueError("Invalid paper price")
    return amount


def conservative_exit_proxy(direction, print_price, event_at, quote=None):
    """Use only already-received BBO no older than 3s at the crossing event.

    A stale, future, crossed or absent book is NOT evidence of executable
    liquidity; fall back to clearly marked print-only paper simulation. The
    BBO may worsen a fill but can never improve the trade-print reference.
    """
    if direction not in ("long", "short") or event_at.tzinfo is None:
        raise ValueError("Invalid paper exit side/clock")
    observed = _positive(print_price)
    fallback = {"reference": observed, "quality": "trade_print_proxy_no_liquidity",
                "spread_reference_cost_per_unit": Decimal(0)}
    if not isinstance(quote, dict):
        return fallback
    source_at = quote.get("exchange_ts")
    received_at = quote.get("received_ts")
    if (source_at is None or received_at is None or source_at.tzinfo is None
            or received_at.tzinfo is None or not source_at <= received_at <= event_at
            or not timedelta(0) <= event_at - source_at <= timedelta(seconds=3)):
        return fallback
    try:
        bid = _positive(quote.get("bid_price"))
        ask = _positive(quote.get("ask_price"))
    except ValueError:
        return fallback
    if bid > ask:
        return fallback
    # Raw exchange BBO is still only indicative: do not simulate a better exit
    # than the crossing trade print, and apply configured slippage separately.
    reference = min(observed, bid) if direction == "long" else max(observed, ask)
    return {"reference": reference,
            "quality": "observed_bbo_proxy_no_depth_or_executable_fill",
            "spread_reference_cost_per_unit": abs(reference - observed)}


def observed_excursions(direction, entry, minimum, maximum, print_count):
    """Decimal MAE/MFE against entry fill, explicitly observed-only/partial.

    Historical tick gaps and incomplete publicTrade IDs must never become a
    statistical completeness or training guarantee. Missing ticks -> None.
    """
    if direction not in ("long", "short"):
        raise ValueError("Invalid trade direction")
    if (minimum is None or maximum is None or print_count is None
            or int(print_count) <= 0):
        return {"quality": "unavailable_no_observed_prints",
                "mae_fraction": None, "mfe_fraction": None,
                "observed_prints": 0}
    price, low, high = _positive(entry), _positive(minimum), _positive(maximum)
    if low > high:
        raise ValueError("Corrupt observed price range")
    if direction == "long":
        adverse = min(Decimal(0), (low - price) / price)
        favorable = max(Decimal(0), (high - price) / price)
    else:
        adverse = min(Decimal(0), (price - high) / price)
        favorable = max(Decimal(0), (price - low) / price)
    return {"quality": "observed_websocket_partial_not_exchange_complete",
            "mae_fraction": adverse, "mfe_fraction": favorable,
            "observed_prints": int(print_count)}


def closed_equity_drawdown(initial_equity, ordered_net_pnl, *, max_rows=10000):
    """Exact Decimal realized-only drawdown, never infer unrealized equity."""
    initial = _positive(initial_equity)
    if len(ordered_net_pnl) > max_rows:
        return {"max_drawdown_fraction": None, "ending_equity": None,
                "quality": "unavailable_unbounded_history"}
    equity = peak = initial
    worst = Decimal(0)
    for net in ordered_net_pnl:
        change = Decimal(str(net))
        if not change.is_finite():
            raise ValueError("Invalid paper net PnL")
        equity += change
        peak = max(peak, equity)
        worst = max(worst, (peak - equity) / peak)
    return {"max_drawdown_fraction": worst, "ending_equity": equity,
            "quality": "realized_closed_paper_only"}
