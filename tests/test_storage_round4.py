"""Round-4 pure validation regressions: fail closed BEFORE touching PostgreSQL."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from eth_tester.ml import FEATURE_NAMES
from eth_tester.storage.labels import record_paper_ml_label
from eth_tester.storage.snapshots import write_feature_snapshot


class NoSql:
    def execute(self, *_args, **_kwargs):
        raise AssertionError("Invalid evidence must not reach PostgreSQL")


def _snapshot():
    now = datetime.now(timezone.utc)
    instant = int(now.timestamp() * 1000)
    return dict(
        symbol="ETHUSDT", tf="1", bar_ts=now-timedelta(minutes=1),
        values={name: Decimal("1.5") for name in FEATURE_NAMES},
        feature_available_ms={name: instant for name in FEATURE_NAMES},
        source="unverified", source_checksum="a"*64,
        observed_at=now, certified=True,
    )


def test_snapshot_requires_every_independent_feature_proof():
    with pytest.raises(ValueError, match="nine per-feature"):
        write_feature_snapshot(NoSql(), **_snapshot())


def test_snapshot_rejects_nonfinite_values_before_sql():
    args = _snapshot()
    args["certified"] = False
    args["values"] = {**args["values"], "oi": float("nan")}
    with pytest.raises(ValueError, match="Non-finite"):
        write_feature_snapshot(NoSql(), **args)


def test_snapshot_rejects_missing_one_feature_before_sql():
    args = _snapshot()
    args["certified"] = False
    args["feature_available_ms"] = {
        key: value for key, value in args["feature_available_ms"].items()
        if key != "book_imbalance"
    }
    with pytest.raises(ValueError, match="nine-feature"):
        write_feature_snapshot(NoSql(), **args)


def test_snapshot_rejects_future_claimed_receipt_before_sql():
    args = _snapshot()
    args["certified"] = False
    args["feature_available_ms"] = {
        key: -1 for key in args["feature_available_ms"]
    }
    with pytest.raises(ValueError, match="availability"):
        write_feature_snapshot(NoSql(), **args)


def test_label_needs_real_barrier_evidence_before_sql():
    old = datetime.now(timezone.utc)-timedelta(hours=1)
    args = dict(
        paper_trade_id=1, snapshot_key="bounded", source_checksum="a"*64,
        outcome="bounce", horizon_end_at=old + timedelta(minutes=2),
        label_source="kline:trade", label_interval="1",
        covered_from_at=old, coverage_sha256="b"*64,
        source_receipt_at=old+timedelta(minutes=3),
        coverage_checked_at=old+timedelta(minutes=4),
        source_evidence={},
    )
    with pytest.raises(ValueError, match="barrier evidence"):
        record_paper_ml_label(NoSql(), **args)


def test_label_cannot_have_future_horizon_before_sql():
    old = datetime.now(timezone.utc)-timedelta(hours=1)
    args = dict(
        paper_trade_id=1, snapshot_key="bounded", source_checksum="a"*64,
        outcome="bounce", horizon_end_at=old,
        label_source="kline:trade", label_interval="1",
        covered_from_at=old, coverage_sha256="b"*64,
        source_receipt_at=old, coverage_checked_at=old,
        source_evidence={"barrier_evidence_sha256": "c"*64},
    )
    with pytest.raises(ValueError, match="horizon"):
        record_paper_ml_label(NoSql(), **args)
