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


def test_autonomous_cleanup_must_be_scheduled_not_manual_only():
    """Bounded diagnostics cleanup needs a recurring, observable service lane."""
    assert "cleanup" in supervisor.PERIODIC
    assert 3600 <= supervisor.PERIODIC["cleanup"] <= 7 * 86400
    # Historical/raw market and ML provenance must never be wholesale purged.
    assert "trades" not in retention._purge_bounded.__code__.co_consts
    assert "feature_snapshots" not in retention._purge_bounded.__code__.co_consts


def test_hung_process_escalates_instead_of_false_healthy(monkeypatch, tmp_path):
    """A child whose wait() times out forever must eventually be recognized."""
    launches, status = [], []

    class HungProcess:
        pid = 911
        returncode = None
        waits = 0

        async def wait(self):
            if self.returncode is not None:
                return self.returncode
            self.waits += 1
            if self.waits > 4:
                raise asyncio.CancelledError
            raise asyncio.TimeoutError

        def terminate(self):
            self.returncode = -15

        def kill(self):
            self.returncode = -9

    async def spawn(*_a, **_kw):
        launches.append(1)
        return HungProcess()

    monkeypatch.setattr(supervisor.asyncio, "create_subprocess_exec", spawn)
    sup = supervisor.Supervisor(path=tmp_path)
    original_record = sup._record

    def record(name, state, **extra):
        status.append((name, state, extra))
        original_record(name, state, **extra)

    sup._record = record

    async def probe():
        with pytest.raises(asyncio.CancelledError):
            await sup.child("engine")

    asyncio.run(probe())
    states = [s for name, s, _ in status if name == "engine"]
    assert "stopped" in states
    # Exit-code monitoring is not enough: watchdog must detect stalled
    # independent DB heartbeat/cursor progress and publish a degraded state.
    assert any(s in {"unresponsive", "hung", "unhealthy"} for s in states), states


def test_cleanup_is_bounded_and_never_mutates_raw_tape(monkeypatch):
    purged, ownership = [], []
    rows = {"feed_health": 2, "bot_events": 1, "eth_storage_events": 0}

    class FakeConn:
        pass

    @contextmanager
    def fake_connection(_settings):
        yield FakeConn()

    monkeypatch.setattr(retention, "connection", fake_connection)
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
