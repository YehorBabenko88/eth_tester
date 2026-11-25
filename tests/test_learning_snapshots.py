"""Regression checks for Storage B append-only as-of snapshot contract."""
from datetime import timedelta
from copy import deepcopy

import pytest

from eth_tester.ml import CLASSES, Example, Gates, approve_candidate, fit
from eth_tester.ml.extract import epoch_ms, from_snapshot_records
from eth_tester.ml import lifecycle
from test_learning_v2_contract import sample


def record():
    return deepcopy(sample()[0])


def test_only_point_in_time_b_snapshot_passes():
    row = record()
    cutoff = sample()[1]+1000
    result, rejects = from_snapshot_records([row], cutoff)
    assert len(result) == 1 and not rejects
    assert result[0].net_return == pytest.approx(.009)
    assert from_snapshot_records([row], epoch_ms(row["closed_at"])-1)[0] == []


@pytest.mark.parametrize("field", ["available_at", "observed_at", "computed_at"])
def test_hindsight_snapshot_rejected(field):
    row = record()
    row[field] = row["decision_at"] + timedelta(seconds=1)
    result, rejects = from_snapshot_records([row], sample()[1]+1)
    assert result == [] and rejects


def test_missing_provenance_or_checksum_fails_closed():
    row = record()
    row["provenance"] = {}
    assert from_snapshot_records([row], sample()[1]+1)[0] == []
    row = record()
    row["source_checksum"] = None
    assert from_snapshot_records([row], sample()[1]+1)[0] == []


def test_unverified_label_and_future_subfeature_rejected():
    row = record()
    row["label_ledger"]["outcome"] = "take_profit"
    assert from_snapshot_records([row], sample()[1]+1)[0] == []
    row = record()
    row["provenance"]["feature_available_ms"]["oi"] = epoch_ms(
        row["decision_at"]+timedelta(minutes=1))
    assert from_snapshot_records([row], sample()[1]+1)[0] == []


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
    assert from_snapshot_records([row], sample()[1]+1)[0] == []
    row = record()
    row["reason"]["ml_source_checksum"] = "f"*64
    assert from_snapshot_records([row], sample()[1]+1)[0] == []


def test_unverified_coverage_and_backdated_computation_rejected():
    row = record()
    row["provenance"]["source_coverage_verified"] = False
    assert from_snapshot_records([row], sample()[1]+1)[0] == []
    row = record()
    row["computed_at"] = row["observed_at"] - timedelta(seconds=1)
    assert from_snapshot_records([row], sample()[1]+1)[0] == []


def test_label_must_be_complete_and_available_as_of():
    row = record()
    row["label_ledger"]["issued_at"] = "2030-01-01T00:00:00+00:00"
    assert from_snapshot_records([row], sample()[1]+1)[0] == []
    row = record()
    row["label_ledger"]["source_evidence"]["complete_closed_bar_grid"] = False
    assert from_snapshot_records([row], sample()[1]+1)[0] == []
