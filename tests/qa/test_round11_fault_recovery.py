"""QA H round 11: outage recovery and cross-owner fail-closed contracts.

All faults are synthetic and act only on isolated in-memory objects or
pytest-owned temporary folders. No production database, Bybit or order route.
"""
import asyncio
from contextlib import contextmanager
from importlib.util import module_from_spec, spec_from_file_location
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


def _sampler():
    path = Path(__file__).resolve().parents[2] / "scripts" / "qa_readonly_soak.py"
    spec = spec_from_file_location("qa_round11_sampler", path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Clock:
    def __init__(self):
        self.counter = 0

    def monotonic(self):
        value = self.counter
        self.counter += 1
        return value

    def sleep(self, seconds):
        assert 0 <= seconds <= 1


class _FakeConn:
    def __init__(self):
        self.closed = 0
        self.statements = []

    def close(self):
        self.closed += 1

    @contextmanager
    def transaction(self):
        yield

    def execute(self, statement):
        self.statements.append(statement)
        if statement == "SET TRANSACTION READ ONLY":
            return SimpleNamespace(fetchone=lambda: None)
        if "clock_timestamp() AS now" in statement:
            return SimpleNamespace(fetchone=lambda: (None,))
        if "FROM bot_heartbeat" in statement:
            return SimpleNamespace(fetchall=lambda: [])
        return SimpleNamespace(fetchone=lambda: (None, None, 0, 0))


def _run_sampler(module, capsys):
    module.main([
        "--dsn", "postgresql://localhost/eth_tester_qa_h11_isolated",
        "--seconds", "3", "--interval", "1",
    ])
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


def test_initial_postgres_outage_recovers_in_readonly_sampler(monkeypatch, capsys):
    module = _sampler()
    clock = _Clock()
    monkeypatch.setattr(module, "time", SimpleNamespace(
        monotonic=clock.monotonic, sleep=clock.sleep,
    ))
    made, attempts = [], []

    def connect(*_args, **_kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise module.psycopg.OperationalError("QA simulated DB outage")
        result = _FakeConn()
        made.append(result)
        return result

    monkeypatch.setattr(module.psycopg, "connect", connect)
    observed = _run_sampler(module, capsys)
    assert len(observed) == 3
    assert observed[0]["quality"] == "sampling_degraded"
    assert observed[0]["production_go"] is False
    assert observed[0]["real_orders_allowed"] is False
    assert observed[1]["quality"] == "diagnostic_only_not_source_certification"
    assert len(made) == 1 and made[0].closed == 1
    assert "SET TRANSACTION READ ONLY" in made[0].statements


def test_midrun_db_restart_closes_every_replaced_connection(monkeypatch, capsys):
    module = _sampler()
    clock = _Clock()
    monkeypatch.setattr(module, "time", SimpleNamespace(
        monotonic=clock.monotonic, sleep=clock.sleep,
    ))
    made = []

    def connect(*_args, **_kwargs):
        item = _FakeConn()
        made.append(item)
        return item

    monkeypatch.setattr(module.psycopg, "connect", connect)
    original = module.sample
    count = 0

    def inject(conn):
        nonlocal count
        count += 1
        if count == 1:
            raise module.psycopg.OperationalError("QA simulated restart")
        return original(conn)

    monkeypatch.setattr(module, "sample", inject)
    rows = _run_sampler(module, capsys)
    assert rows[0]["quality"] == "sampling_degraded"
    assert rows[1]["quality"] == "diagnostic_only_not_source_certification"
    assert len(made) == 2 and [c.closed for c in made] == [1, 1]
    assert "SET TRANSACTION READ ONLY" in made[1].statements


def test_sampler_refuses_non_disposable_databases():
    module = _sampler()
    for name in ("postgres", "eth_tester", "main", "eth_tester_production"):
        with pytest.raises(ValueError, match="disposable"):
            module.assert_disposable_target("postgresql://localhost/" + name)


def test_watchdog_terminate_permission_error_does_not_stop_all_workers():
    """G owner must isolate a failed owned-PID termination from healthy PIDs."""
    from eth_tester.service_health import HealthWatchdog
    calls, records = [], []

    async def deny(name, proc):
        calls.append(name)
        raise OSError("QA simulated permission denied")

    supervisor = SimpleNamespace(
        children={"engine": SimpleNamespace(returncode=None),
                  "analyse": SimpleNamespace(returncode=None)},
        process_starts={"engine": 0, "analyse": 0},
        _record=lambda *args, **kw: records.append((args, kw)),
        _terminate=deny,
    )
    watchdog = HealthWatchdog(
        clock=lambda: 1000,
        checker=lambda _: {"paper_engine": 999, "live_paper_window": 0},
    )
    assert asyncio.run(watchdog.check_once(supervisor, object())) == "checked"
    assert asyncio.run(watchdog.check_once(supervisor, object())) == "checked"
    assert calls == ["engine"]
    assert any(args[:2] == ("watchdog", "repair_failed") for args, _ in records)


@pytest.mark.skipif(os.name == "nt", reason="POSIX directory-swap injection")
def test_owned_temp_root_replacement_never_deletes_foreign_file(monkeypatch, tmp_path):
    """Rename+symlink root after marker validation must not cross ownership."""
    from eth_tester.storage.maintenance import (
        cleanup_owned_temp, _MARKER, _MARKER_TEXT,
    )
    own, foreign, saved = (tmp_path / part for part in
                           ("owned", "foreign", "owned-original"))
    own.mkdir()
    foreign.mkdir()
    (own / _MARKER).write_text(_MARKER_TEXT, encoding="utf-8")
    victim = foreign / ("eth-tester-stage-" + "a" * 32 + ".part")
    victim.write_bytes(b"FOREIGN: never delete")
    os.utime(victim, (1, 1))
    real_iterdir = Path.iterdir
    did_swap = False

    def swap_at_iteration(path):
        nonlocal did_swap
        if path == own and not did_swap:
            did_swap = True
            own.rename(saved)
            own.symlink_to(foreign, target_is_directory=True)
        return real_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", swap_at_iteration)
    try:
        try:
            cleanup_owned_temp(own, now=7200, older_than_seconds=3600)
        except RuntimeError:
            pass
        assert victim.is_file(), "TOCTOU deleted a foreign QA fixture"
        assert victim.read_bytes() == b"FOREIGN: never delete"
    finally:
        if own.is_symlink():
            own.unlink()
            saved.rename(own)
