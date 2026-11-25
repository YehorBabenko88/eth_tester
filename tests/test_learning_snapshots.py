"""Regression checks for Storage B append-only as-of snapshot contract."""
from datetime import datetime, timedelta, timezone

import pytest

from eth_tester.ml import CLASSES, Example, FEATURE_NAMES, Gates, approve_candidate, fit
from eth_tester.ml.extract import epoch_ms, from_snapshot_records
from eth_tester.ml import lifecycle


def record():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    trade = {
        "event_id": "paper:1", "decision_at": start+timedelta(minutes=10),
        "opened_at": start+timedelta(minutes=11),
        "closed_at": start+timedelta(minutes=16),
        "bar_ts": start, "available_at": start+timedelta(minutes=5),
        "observed_at": start+timedelta(minutes=6),
        "computed_at": start+timedelta(minutes=7),
        "snapshot_key": "snap-paper-1",
        "source_checksum": "a"*64, "snapshot_quality": "complete",
        "is_causal": True, "reason": {
            "ml_snapshot_key": "snap-paper-1", "ml_source_checksum": "a"*64,
            "ml_outcome": "bounce",
            "ml_label": {
                "outcome": "bounce", "method": "fixed_horizon_barrier_v1",
                "horizon_end_ms": epoch_ms(start+timedelta(minutes=16)),
                "issued_at_ms": epoch_ms(start+timedelta(minutes=16)),
                "coverage_verified": True, "source_checksum": "b"*64}},
        "values_json": dict.fromkeys(FEATURE_NAMES, 1.0),
        "provenance": {
            "source_coverage_verified": True,
            "source_receipt_ms": epoch_ms(start+timedelta(minutes=6)),
            "feature_available_ms": dict.fromkeys(
                FEATURE_NAMES, epoch_ms(start+timedelta(minutes=4)))},
        "notional": 1000, "gross_pnl": 12, "fees": 2,
        "funding": 1, "net_pnl": 9,
    }
    return trade


def test_only_point_in_time_b_snapshot_passes():
    row = record()
    cutoff = epoch_ms(row["closed_at"])+1000
    result, rejects = from_snapshot_records([row], cutoff)
    assert len(result) == 1 and not rejects
    assert result[0].net_return == pytest.approx(.009)
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])-1)[0] == []


@pytest.mark.parametrize("field", ["available_at", "observed_at", "computed_at"])
def test_hindsight_snapshot_rejected(field):
    row = record()
    row[field] = row["decision_at"] + timedelta(seconds=1)
    result, rejects = from_snapshot_records([row], epoch_ms(row["closed_at"])+1)
    assert result == [] and rejects


def test_missing_provenance_or_checksum_fails_closed():
    row = record()
    row["provenance"] = {}
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])+1)[0] == []
    row = record()
    row["source_checksum"] = None
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])+1)[0] == []


def test_unverified_label_and_future_subfeature_rejected():
    row = record()
    row["reason"]["ml_outcome"] = "take_profit"
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])+1)[0] == []
    row = record()
    row["provenance"]["feature_available_ms"]["oi"] = epoch_ms(
        row["decision_at"]+timedelta(minutes=1))
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])+1)[0] == []


def example(i):
    return Example(str(i), i*1000, i*1000+20, (float(i%5),)*9, (i*1000,)*9,
                   CLASSES[i%3], .02 if i%3 == 0 else -.01, .002)


def test_canonical_version_and_weakened_gates():
    train = [example(i) for i in range(1, 121)]
    a, b = fit(train, steps=20), fit(train, steps=21)
    assert a.version != b.version
    validation = [example(i) for i in range(200, 240)]
    shadow = [example(i) for i in range(300, 340)]
    ok, why = approve_candidate(a, None, train, validation, shadow, Gates(min_train=1))
    assert not ok and why == "weakened admission policy"


def test_retraining_hook_requires_new_labels(monkeypatch):
    rows = [example(i) for i in range(1, 201)]
    called = []
    def fake_storage(settings, model, train, val, shadow):
        called.append((model.version, len(train), len(val), len(shadow)))
        return 12, {"state": "shadow"}
    monkeypatch.setattr(lifecycle, "store_shadow", fake_storage)
    assert lifecycle.run_shadow_cycle(None, rows, [110000, 150000],
                                     previous_trained_count=175, min_new=100) is None
    result = lifecycle.run_shadow_cycle(None, rows, [110000, 150000], min_new=100)
    assert result == (12, {"state": "shadow"})
    assert called and all(n > 0 for n in called[0][1:])


def test_exact_snapshot_and_source_identity_binding():
    row = record()
    row["reason"]["ml_snapshot_key"] = "other"
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])+1)[0] == []
    row = record()
    row["reason"]["ml_source_checksum"] = "f"*64
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])+1)[0] == []


def test_unverified_coverage_and_backdated_computation_rejected():
    row = record()
    row["provenance"]["source_coverage_verified"] = False
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])+1)[0] == []
    row = record()
    row["computed_at"] = row["observed_at"] - timedelta(seconds=1)
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])+1)[0] == []


def test_label_must_be_complete_and_available_as_of():
    row = record()
    row["reason"]["ml_label"]["issued_at_ms"] = epoch_ms(row["closed_at"])+2000
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])+1)[0] == []
    row = record()
    row["reason"]["ml_label"]["coverage_verified"] = False
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])+1)[0] == []
