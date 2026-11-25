"""QA H round eight — exact causality and fail-closed policy regression.

These tests operate on in-memory fabricated bars, not exchange completeness.
No external network, order endpoints or database modifications are performed.
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from eth_tester.ml.labels import full_future_grid, label_level_outcome
from eth_tester.ml.label_worker import _reason_policy
from eth_tester.live_paper_window import bounded_live_window

UTC = timezone.utc
DECISION = datetime(2026, 11, 25, 12, 34, 17, 123000, tzinfo=UTC)
LEVEL = Decimal("392.282749")
BPS = 20
UPPER = LEVEL * (Decimal(1) + Decimal(BPS) / Decimal(10000))
LOWER = LEVEL * (Decimal(1) - Decimal(BPS) / Decimal(10000))


def _full_future_bars(high, low):
    decision_ms = int(DECISION.timestamp() * 1000)
    start, _, horizon = full_future_grid(
        decision_ms, bar_interval_ms=60000, horizon_bars=3
    )
    bars = [
        {"start_ms": start + index * 60000,
         "closed_ms": start + (index + 1) * 60000,
         "high": str(high), "low": str(low)}
        for index in range(3)
    ]
    return decision_ms, horizon, bars


@pytest.mark.parametrize(
    "side,high,low,expected",
    [
        ("resistance", UPPER, LEVEL - Decimal(".01"), "breakout"),
        ("support", UPPER, LEVEL - Decimal(".01"), "bounce"),
        ("resistance", LEVEL + Decimal(".01"), LOWER, "bounce"),
        ("support", LEVEL + Decimal(".01"), LOWER, "breakout"),
        ("resistance", UPPER, LOWER, "uncertain"),
        ("support", UPPER, LOWER, "uncertain"),
        ("resistance", UPPER - Decimal(".000000001"), LOWER + Decimal(".000000001"),
         "uncertain"),
    ],
)
def test_decimal_barrier_exact_postgres_numeric_boundary(side, high, low, expected):
    """F must use the same exact decimal inequalities as SQL NUMERIC in B 0007."""
    decision_ms, horizon, bars = _full_future_bars(high, low)
    answer = label_level_outcome(
        side, LEVEL, bars, decision_ms=decision_ms, horizon_ms=horizon,
        as_of_ms=horizon + 1000, bar_interval_ms=60000, barrier_bps=BPS,
    )
    assert answer == expected


def test_next_full_utc_bar_grid_keeps_original_millisecond_decision():
    ms = int(DECISION.timestamp() * 1000)
    first, close, horizon = full_future_grid(
        ms, bar_interval_ms=60000, horizon_bars=3
    )
    assert datetime.fromtimestamp(first / 1000, UTC) == datetime(
        2026, 11, 25, 12, 35, tzinfo=UTC
    )
    assert close == first + 60000
    assert horizon == first + 180000
    assert first > ms


def test_bounded_live_bar_never_reopens_from_late_rest_or_archives():
    start = DECISION.replace(second=0, microsecond=0)
    for seconds in (91, 540, 86400):
        value = bounded_live_window(
            start, start + timedelta(minutes=1, seconds=seconds)
        )
        assert value["status"] == "expired"
        assert value["reason"] == "decision_window_elapsed"


def test_live_paper_can_never_be_training_label_even_with_forged_policy():
    reason = {
        "paper_only": True, "ml_eligible": False,
        "side": "resistance", "level": str(LEVEL),
        "ml_label_policy": {
            "version": "utc_full_bars_v1",
            "method": "fixed_horizon_barrier_v1",
            "bar_interval_ms": 60000, "horizon_bars": 3,
            "barrier_bps": 20,
            "anchor": "first_full_bar_strictly_after_decision",
        },
        "orderflow": {
            "immutable_live_evidence": {
                "contract": "eth_paper_live_evidence_v1",
                "quality": "live_observed_bounded",
                "paper_only": True, "ml_eligible": False,
            },
        },
    }
    with pytest.raises(ValueError, match="bounded-live"):
        _reason_policy(reason)
