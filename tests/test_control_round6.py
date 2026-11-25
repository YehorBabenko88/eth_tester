"""Only G routing/telemetry contract; no synthetic source may gain ML approval."""
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from eth_tester import control_jobs, supervisor


@pytest.fixture
def calls(monkeypatch):
    recorded = []
    @contextmanager
    def fake_db(_):
        yield SimpleNamespace(execute=lambda statement, args: recorded.append((statement, args)))
    monkeypatch.setattr(control_jobs, "connection", fake_db)
    return recorded


def test_missing_verified_label_worker_is_visible_and_non_crashing(monkeypatch, calls):
    monkeypatch.setattr(control_jobs, "_label_horizons", lambda _: {
        "status": "blocked_missing_verified_label_worker"})
    result = control_jobs.run_control_job(object(), "label-horizons")
    assert result["status"] == "blocked_missing_verified_label_worker"
    assert result["ml_certified_by_control"] is False
    assert calls[0][1][0] == "control_label_horizons"
    assert calls[0][1][1] == "blocked_missing_verified_label_worker"


def test_shadow_only_uses_f_owned_adapter(monkeypatch, calls):
    observed = []
    monkeypatch.setattr(control_jobs, "_causal_shadow", lambda s:
                        observed.append(s) or {
                            "status": "insufficient_new_verified_data", "usable": 0,
                            "required": 180, "approved": False})
    settings = object()
    result = control_jobs.run_control_job(settings, "train-causal-shadow")
    assert observed == [settings]
    assert result["usable"] == 0
    assert result["required"] == 180
    assert "legacy" not in json.dumps(result)
    assert calls[0][1][0] == "control_train_causal_shadow"


def test_auto_approval_is_rejected_and_audited(monkeypatch, calls):
    monkeypatch.setattr(control_jobs, "_causal_shadow", lambda _: {
        "status": "shadow_registered", "approved": True})
    with pytest.raises(RuntimeError, match="Automatic model approval"):
        control_jobs.run_control_job(object(), "train-causal-shadow")
    assert calls[0][1][1] == "error"
    assert json.loads(calls[0][1][2])["error_type"] == "RuntimeError"


def test_missing_explicit_status_cannot_be_success(monkeypatch, calls):
    monkeypatch.setattr(control_jobs, "_label_horizons", lambda _: {"inserted": 1})
    with pytest.raises(RuntimeError, match="explicit status|invalid status"):
        control_jobs.run_control_job(object(), "label-horizons")
    assert calls[0][1][1] == "error"


def test_unexpected_failure_surfaces_to_supervisor(monkeypatch, calls):
    def fail(_):
        raise ConnectionError("source unavailable")
    monkeypatch.setattr(control_jobs, "_label_horizons", fail)
    with pytest.raises(ConnectionError):
        control_jobs.run_control_job(object(), "label-horizons")
    assert json.loads(calls[0][1][2])["error_type"] == "ConnectionError"


def test_invalid_command_has_no_side_effects(calls):
    with pytest.raises(ValueError):
        control_jobs.run_control_job(object(), "live-trading")
    assert not calls


def test_supervisor_periodic_is_bounded_and_paper_only():
    assert supervisor.PERIODIC["label-horizons"] >= 300
    assert supervisor.PERIODIC["train-causal-shadow"] >= 3600
    assert "learn" not in supervisor.PERIODIC
    assert "trade" not in supervisor.PERIODIC


def test_status_sanitization_does_not_emit_secrets(calls, monkeypatch):
    monkeypatch.setattr(control_jobs, "_label_horizons", lambda _: {
        "status": "blocked_unverified_coverage", "token": "secret",
        "source": "raw account", "processed": 5, "details": {"password": "secret"}})
    result = control_jobs.run_control_job(object(), "label-horizons")
    payload = json.dumps(result) + calls[0][1][2]
    assert "secret" not in payload and "password" not in payload
    assert result["processed"] == 5
    assert result["status"] == "blocked_unverified_coverage"


def test_f_import_exception_unrelated_to_missing_module_fails_closed(monkeypatch):
    from builtins import __import__ as base_import
    def imp(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "eth_tester.ml.label_worker" or (
            level == 1 and fromlist and "run_label_horizons" in fromlist
        ):
            raise ModuleNotFoundError("missing dependency", name="market_dependency")
        return base_import(name, globals, locals, fromlist, level)
    monkeypatch.setattr("builtins.__import__", imp)
    with pytest.raises(ModuleNotFoundError):
        control_jobs._label_horizons(object())


def test_cli_has_causal_alias_and_never_calls_legacy_trainer():
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / "src" / "eth_tester" / "cli.py"
    source = path.read_text(encoding="utf-8")
    assert '"train-causal-shadow"' in source
    assert '"label-horizons"' in source
    assert 'train_shadow(settings)' not in source
    assert 'run_control_job' in source


def test_control_is_not_a_market_proof_or_label_writer():
    from pathlib import Path
    code = (Path(__file__).resolve().parents[1] / "src" / "eth_tester" /
            "control_jobs.py").read_text(encoding="utf-8").lower()
    assert 'insert into paper_ml_labels' not in code
    assert 'insert into feature_snapshots' not in code
    assert 'set approved=true' not in code
