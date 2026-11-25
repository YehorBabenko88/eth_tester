"""Round-two Control tests: isolated process lifecycle; no live DB/Telegram/tasks."""

import asyncio
from contextlib import contextmanager
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from eth_tester import supervisor


def test_partial_updates_do_not_authorise_or_crash(monkeypatch):
    from eth_tester import telegram_control as control
    monkeypatch.setenv("ETH_TESTER_TG_USER_ID", "700")
    cfg = SimpleNamespace(tg_chat_id="700")
    for update in (
        SimpleNamespace(),
        SimpleNamespace(effective_chat=SimpleNamespace(id=700, type="private")),
        SimpleNamespace(effective_chat=SimpleNamespace(id=700, type="private"),
                        effective_user=SimpleNamespace(id=700)),
        SimpleNamespace(effective_chat=SimpleNamespace(id=700),
                        effective_user=SimpleNamespace(id=700, is_bot=False)),
    ):
        assert control.authorise(cfg, update) is False

    recorded = []

    @contextmanager
    def fake_db(_):
        yield SimpleNamespace(execute=lambda query, params: recorded.append(params))

    monkeypatch.setattr(control, "connection", fake_db)
    control._audit(cfg, SimpleNamespace(), "help", False, "denied")
    assert recorded and '"user_id": null' in recorded[0][1]


def test_windows_graceful_ctrl_break(monkeypatch, tmp_path):
    monkeypatch.setattr(supervisor, "_is_windows", lambda: True)
    monkeypatch.setattr(supervisor.signal, "CTRL_BREAK_EVENT", 1, raising=False)
    called = []

    class FakeProc:
        pid = 111
        returncode = None

        def send_signal(self, sig):
            called.append(("signal", sig))
            self.returncode = 0

        def terminate(self):
            called.append(("terminate",))

        async def wait(self):
            called.append(("wait",))
            return 0

    asyncio.run(supervisor.Supervisor(path=tmp_path)._terminate("live", FakeProc()))
    assert called == [("signal", 1), ("wait",)]


def test_windows_ctrl_break_failure_falls_back_to_terminate(monkeypatch, tmp_path):
    monkeypatch.setattr(supervisor, "_is_windows", lambda: True)
    monkeypatch.setattr(supervisor.signal, "CTRL_BREAK_EVENT", 1, raising=False)
    called = []

    class FakeProc:
        pid = 112
        returncode = None

        def send_signal(self, sig):
            called.append("ctrlbreak")
            raise OSError("no console attached")

        def terminate(self):
            called.append("terminate")
            self.returncode = 0

        async def wait(self):
            called.append("wait")
            return 0

    asyncio.run(supervisor.Supervisor(path=tmp_path)._terminate("live", FakeProc()))
    assert called == ["ctrlbreak", "terminate", "wait"]


@pytest.mark.skipif(os.name == "nt", reason="POSIX child lifetime regression")
def test_real_child_terminated_without_touching_outside_pid(tmp_path):
    sentinel = tmp_path / "owned-child.pid"

    class Probe(supervisor.Supervisor):
        def _argv(self, name):
            script = (
                "import os,time;from pathlib import Path;"
                f"Path({str(sentinel)!r}).write_text(str(os.getpid()));"
                "time.sleep(60)"
            )
            return (sys.executable, "-u", "-c", script)

    async def probe():
        sup = Probe(path=tmp_path)
        task = asyncio.create_task(sup.child("engine"))
        for _ in range(200):
            if sentinel.exists():
                break
            await asyncio.sleep(0.01)
        assert sentinel.exists(), "owned subprocess did not start"
        pid = int(sentinel.read_text())
        assert pid != os.getpid()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=8)
        assert sup.status["engine"]["state"] == "stopped"
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)

    asyncio.run(probe())


def test_windows_installer_preflights_without_db_mutation():
    root = Path(__file__).resolve().parents[1]
    installer = (root / "install" / "install.ps1").read_text(encoding="utf-8")
    assert "eth_tester.cli --help" in installer
    assert "S-1-5-19" in installer and "S-1-1-0" in installer
    assert "ETH_TESTER_TG_USER_ID" in installer
    assert "DROP DATABASE" not in installer.upper()
