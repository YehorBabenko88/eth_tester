from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from eth_tester.book_analysis import (
    apply_updates, parse_levels, significant, snapshot_metrics,
)


NOW = datetime(2026, 11, 25, 17, tzinfo=timezone.utc)


def test_orderbook_snapshot_fresh_and_uncrossed():
    metrics = snapshot_metrics(
        {"100": "5", "99": "2"}, {"101": "3", "102": "1"},
        observed_at=NOW - timedelta(seconds=3), as_of=NOW)
    assert metrics["valid"]
    assert metrics["spread"] == Decimal(1)
    assert metrics["imbalance"] == Decimal(3) / Decimal(11)
    assert metrics["age_seconds"] == 3


@pytest.mark.parametrize("age,reason", [
    (timedelta(seconds=11), "stale_or_future_book"),
    (timedelta(seconds=-1), "stale_or_future_book"),
])
def test_orderbook_freshness_rejects_stale_and_future(age, reason):
    assert snapshot_metrics(
        {"100": "5"}, {"101": "3"},
        observed_at=NOW - age, as_of=NOW)["reason"] == reason


def test_orderbook_crossed_book_fails_closed():
    assert snapshot_metrics(
        {"101": "5"}, {"100": "3"},
        observed_at=NOW, as_of=NOW)["valid"] is False


def test_decreased_order_quantity_is_not_proven_execution():
    book = parse_levels([["100", "60"]])
    changes = apply_updates(book, [["100", "0"]])
    assert book == {}
    assert significant(changes, "bid", Decimal(50)) == [
        ("bid", "100", Decimal(60), Decimal(0), "vanished_unknown")]
    with pytest.raises(ValueError):
        apply_updates({}, [["100", "-1"]])
