"""Automatic shadow retraining must not use legacy PnL or truncated samples."""
from types import SimpleNamespace
import pytest

from eth_tester import learning
from eth_tester.ml import extract


def test_legacy_pnl_shadow_is_always_disabled():
    with pytest.raises(RuntimeError, match="Legacy paper-trade labels"):
        learning.train_shadow(SimpleNamespace(symbol="ETHUSDT"))


def test_truncated_population_blocks_training_not_shadow_approval(monkeypatch):
    monkeypatch.setattr(extract, "load_causal_feature_examples",
                        lambda *args, **kw: ([], {"scan_limit_reached": 1}))
    result = learning.train_causal_shadow(
        SimpleNamespace(symbol="ETHUSDT"), 1795610100000,
        tf="1", min_rows=180, min_new=100, previous_count=180)
    assert result == {"status": "incomplete_verified_population_scan",
                      "usable": 0, "required": 280,
                      "rejections": {"scan_limit_reached": 1},
                      "approved": False}


def test_no_independent_labels_no_shadow_candidate(monkeypatch):
    monkeypatch.setattr(extract, "load_causal_feature_examples",
                        lambda *args, **kw: ([], {"missing_post_horizon_label_ledger": 1}))
    result = learning.train_causal_shadow(
        SimpleNamespace(symbol="ETHUSDT"), 1795610100000,
        tf="1", min_rows=180, min_new=100, previous_count=0)
    assert result["status"] == "insufficient_new_verified_data"
    assert result["usable"] == 0 and result["approved"] is False
