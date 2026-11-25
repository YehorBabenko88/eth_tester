"""Round-6 QA: historical-vs-bounded-live separation and causal clock guards.

These regression tests MUST NOT turn partial public market capture into exchange
trade-ID completeness. They perform no network requests, market orders, or DDL.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import inspect
from types import SimpleNamespace

import pytest

from eth_tester import archives
from eth_tester.ml.contract import validate_snapshot
from eth_tester.ml.labels import label_level_outcome
from eth_tester.orderflow import confirm_rejection
from eth_tester.simulation.identity import bind_snapshot_reference, rule_identity
from eth_tester.snapshot_producer import CertificationRejected, coverage_certificate
from eth_tester.strategies import Parameters

UTC = timezone.utc
SOURCE_ROOT = Path(__file__).resolve().parents[2] / "src" / "eth_tester"


class NoSql:
    def execute(self, *_args, **_kwargs):
        raise AssertionError("Expired evidence must be rejected before SQL")


@pytest.mark.parametrize("offset", [-1, 91, 540, 86400])
def test_paper_rejection_window_never_reopens_an_old_bar(offset):
    start = datetime(2026, 11, 25, 12, 34, tzinfo=UTC)
    signal = SimpleNamespace(direction="long")
    admissible, info = confirm_rejection(
        NoSql(), SimpleNamespace(symbol="ETHUSDT"), signal,
        {"ts": start}, observed_at=start + timedelta(minutes=1, seconds=offset),
    )
    assert admissible is False
    assert info["reason"] == "stale_or_open_bar"


def test_official_archive_volume_match_remains_partial_not_trade_id_certified(monkeypatch):
    day = datetime(2026, 11, 24, tzinfo=UTC).date()
    report = {
        "status": "verified", "certificate": {
            "source_event_start_ms": 1, "source_event_end_exclusive_ms": 2,
            "source_receipt_ms": 3, "proves_all_trade_ids": False,
        },
    }
    captured = []
    monkeypatch.setattr(archives, "persist_minute_evidence",
                        lambda *_a, **_kw: report)
    monkeypatch.setattr(archives, "write_coverage",
                        lambda *_a, **kw: captured.append(kw))
    actual = archives.certify_archive_day(
        object(), SimpleNamespace(symbol="ETHUSDT", category="linear"),
        day, "a"*64, stage_name="eth_stage", row_count=1440,
    )
    assert actual is report
    assert len(captured) == 1
    assert captured[0]["status"] == "partial"
    assert captured[0]["quality"] == "volume_reconciled_not_trade_id_complete"
    assert captured[0]["evidence"]["certificate"]["proves_all_trade_ids"] is False


def test_current_utc_day_cannot_be_historical_certificate():
    start = datetime(2026, 11, 25, 12, 34, tzinfo=UTC)
    with pytest.raises(CertificationRejected):
        coverage_certificate(
            start, start + timedelta(minutes=1),
            start + timedelta(minutes=1, seconds=10), [], {}, {},
        )


def test_immutable_historical_snapshot_cannot_be_published_after_decision():
    start = datetime(2026, 11, 25, 12, 34, tzinfo=UTC)
    end = start + timedelta(minutes=1)
    decision = end + timedelta(seconds=15)
    version, meta = rule_identity()
    reason = {
        "strategy_version": version,
        "rules_version": Parameters().version,
        **meta,
        "orderflow": {
            "admissible": True,
            "immutable_feature_snapshot": {
                "snapshot_key": "a"*64, "source_checksum": "b"*64,
                "contract": "eth_feature_snapshot_v2",
                "source_receipt_ms": int(end.timestamp()*1000),
                "source_checked_at_ms": int(end.timestamp()*1000),
                "available_at": end.isoformat(),
                "computed_at": (decision + timedelta(milliseconds=1)).isoformat(),
            },
        },
    }
    with pytest.raises(ValueError, match="after decision"):
        bind_snapshot_reference(reason, decision_at=decision, bar_ts=start)


def test_live_observed_paper_reason_is_not_causal_ml_proof():
    key = "a"*64
    checksum = "b"*64
    live = {
        "snapshot_key": key, "source_checksum": checksum,
        "contract": "eth_paper_live_evidence_v1",
        "quality": "live_observed_bounded",
        "paper_only": True, "ml_eligible": False,
    }
    reason = {"orderflow": {"admissible": True,
                            "immutable_live_evidence": live}}
    with pytest.raises((KeyError, ValueError, TypeError)):
        validate_snapshot(
            {"snapshot_key": key, "source_checksum": checksum}, reason,
        )


def test_bounded_live_evidence_is_not_masquerading_as_feature_snapshot():
    """Cross-module architecture requires a distinct immutable paper ledger."""
    storage = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (SOURCE_ROOT / "storage").glob("*.py")
    )
    migrations = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (SOURCE_ROOT / "migrations").glob("*.sql")
    )
    assert "paper_live_evidence" in (storage + migrations)
    assert "live_observed_bounded" in (storage + migrations)
    assert "eth_paper_live_evidence_v1" in (storage + migrations)
    assert "paper_live_evidence" in (
        SOURCE_ROOT / "paper.py").read_text(encoding="utf-8")
    assert "immutable_live_evidence" in (
        SOURCE_ROOT / "orderflow.py").read_text(encoding="utf-8")


def test_first_full_post_decision_utc_candles_are_labelable():
    """Never alter the true 12:34:17.123 paper decision or use partial 12:35 close."""
    decision = datetime(2026, 11, 25, 12, 34, 17, 123000, tzinfo=UTC)
    first = datetime(2026, 11, 25, 12, 36, tzinfo=UTC)
    second = first + timedelta(minutes=1)
    bars = [
        {"closed_ms": int(first.timestamp()*1000),
         "high": 100.0, "low": 99.5},
        {"closed_ms": int(second.timestamp()*1000),
         "high": 100.0, "low": 99.5},
    ]
    label = label_level_outcome(
        "resistance", 100.0, bars,
        decision_ms=int(decision.timestamp()*1000),
        horizon_ms=int(second.timestamp()*1000),
        as_of_ms=int((second+timedelta(seconds=2)).timestamp()*1000),
        bar_interval_ms=60000,
    )
    assert label == "bounce"


def test_entry_cannot_use_partially_predecision_future_label_bar():
    decision = datetime(2026, 11, 25, 12, 34, 17, 123000, tzinfo=UTC)
    partial_close = datetime(2026, 11, 25, 12, 35, tzinfo=UTC)
    with pytest.raises(ValueError):
        label_level_outcome(
            "resistance", 100.0,
            [{"closed_ms": int(partial_close.timestamp()*1000),
              "high": 101.0, "low": 99.0}],
            decision_ms=int(decision.timestamp()*1000),
            horizon_ms=int(partial_close.timestamp()*1000),
            as_of_ms=int((partial_close+timedelta(seconds=2)).timestamp()*1000),
            bar_interval_ms=60000,
        )


def test_supervisor_schedules_only_causal_ml_not_legacy_pnl_training():
    from eth_tester import cli, supervisor

    entry = inspect.getsource(cli.main)
    schedule = inspect.getsource(supervisor)
    assert "train_shadow(settings)" not in entry
    assert "train_causal_shadow" in entry
    assert "label-horizons" in schedule
    assert "learn" in schedule
