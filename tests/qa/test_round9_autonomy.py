"""Round-9 autonomy/fault-injection contracts for a PAPER-ONLY 24/7 service.

These tests deliberately discriminate *living processes* from progressing
workers and preserve operator pause. No live exchange or private API calls.
The hung-worker acceptance test must FAIL until G implements progress-aware
watchdog escalation (a 30s process wait timeout is not health evidence).
"""
import asyncio
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from eth_tester import supervisor, retention


def test_cleanup_policy_requires_operator_opt_in():
    """Never run automatic diagnostic deletion merely to satisfy QA."""
    assert "cleanup" not in supervisor.PERIODIC
    assert "trades" not in retention._purge_bounded.__code__.co_consts
    assert "feature_snapshots" not in retention._purge_bounded.__code__.co_consts
    assert any("paper_trade_review_v1" in str(c)
               for c in retention._purge_bounded.__code__.co_consts)


def test_independent_application_watchdog_restarts_only_stalled_owned_pid():
    """The child wait timeout does not establish health; G watchdog does."""
    from eth_tester.service_health import HealthWatchdog

    observed, terminated = [], []
    async def terminate(name, proc):
        terminated.append(name)

    fake = SimpleNamespace(
        children={"engine": SimpleNamespace(pid=911, returncode=None),
                  "analyse": SimpleNamespace(pid=912, returncode=None),
                  "live": SimpleNamespace(pid=913, returncode=None)},
        process_starts={"engine": 0, "analyse": 0, "live": 0},
        _record=lambda name, state, **kw: observed.append((name, state, kw)),
        _terminate=terminate,
    )
    watchdog = HealthWatchdog(clock=lambda: 500, checker=lambda _: {
        "paper_engine": 999, "live_paper_window": 0,
    })
    assert asyncio.run(watchdog.check_once(fake, object())) == "checked"
    assert terminated == []
    assert asyncio.run(watchdog.check_once(fake, object())) == "checked"
    assert terminated == ["engine"]
    assert any(a == "watchdog" and b == "recovering_worker"
               for a, b, _ in observed)


def test_cleanup_is_bounded_and_never_mutates_raw_tape(monkeypatch):
    purged, ownership = [], []
    rows = {"feed_health": 2, "bot_events": 1, "eth_storage_events": 0}

    class FakeConn:
        def execute(self, query, params=None):
            if "pg_try_advisory_xact_lock" in str(query):
                return SimpleNamespace(fetchone=lambda: {"acquired": True})
            if "set_config" in str(query):
                return SimpleNamespace(fetchone=lambda: None)
            raise AssertionError("Unexpected diagnostic SQL")

    @contextmanager
    def fake_connection(_settings):
        yield FakeConn()

    monkeypatch.setattr(retention, "connection", fake_connection)
    monkeypatch.delenv("ETH_TESTER_OWNED_TEMP_DIR", raising=False)
    monkeypatch.setattr(
        retention, "check_ownership", lambda _conn: ownership.append(True)
    )

    def limited(_conn, table, column, days, limit):
        assert limit == 2
        purged.append((table, column, days))
        return rows[table]

    monkeypatch.setattr(retention, "_purge_bounded", limited)
    result = retention.cleanup_diagnostics(
        object(), health_days=30, event_days=60, max_rows_per_table=2
    )
    assert ownership == [True]
    assert [name for name, *_ in purged] == [
        "feed_health", "bot_events", "eth_storage_events"
    ]
    assert result["raw_market_data_deleted"] == 0
    assert result["status"] == "ok" and result["serialized"] is True
    assert result["batch_limited"] is True
    with pytest.raises(ValueError):
        retention.cleanup_diagnostics(object(), max_rows_per_table=0)


def test_supervisor_status_checkpoint_atomic_even_after_relaunch(tmp_path):
    sup = supervisor.Supervisor(path=tmp_path)
    sup._record("engine", "restarting", retry_seconds=3)
    sup._record("engine", "running", pid=123)
    data = (tmp_path / "supervisor.json").read_text(encoding="utf8")
    assert '"running"' in data and '"pid": 123' in data
    assert not list(tmp_path.glob(".supervisor.*.tmp"))


def test_paper_service_source_never_calls_private_order_endpoint():
    root = Path(__file__).resolve().parents[2] / "src" / "eth_tester"
    for name in ("paper.py", "runner.py", "live.py", "telegram_control.py"):
        code = (root / name).read_text(encoding="utf-8")
        assert "/v5/order/create" not in code
        assert "place_order(" not in code


def test_soak_sampler_rejects_non_disposable_database():
    from importlib.util import module_from_spec, spec_from_file_location

    path = Path(__file__).resolve().parents[2] / "scripts" / "qa_readonly_soak.py"
    spec = spec_from_file_location("qa_readonly_soak", path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    for name in ("postgres", "eth_tester", "tester", "main", "eth_tester_prod"):
        with pytest.raises(ValueError, match="disposable"):
            module.assert_disposable_target(
                f"postgresql://postgres:secret@localhost:5432/{name}"
            )
    assert module.assert_disposable_target(
        "postgresql://postgres:secret@localhost/eth_tester_qa_round9_temp"
    ) == "eth_tester_qa_round9_temp"
