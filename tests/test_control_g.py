"""Unit checks for isolated Control G logic; no DB or network required."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from eth_tester import supervisor


def test_authorise_private_chat_and_user(monkeypatch):
    from eth_tester.telegram_control import authorise
    settings = SimpleNamespace(tg_chat_id="42")
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=42, type="private"),
        effective_user=SimpleNamespace(id=42, is_bot=False),
    )
    monkeypatch.delenv("ETH_TESTER_TG_USER_ID", raising=False)
    assert not authorise(settings, update)
    monkeypatch.setenv("ETH_TESTER_TG_USER_ID", "42")
    assert authorise(settings, update)
    update.effective_user.id = 43
    assert not authorise(settings, update)
    update.effective_user.id = 42
    update.effective_chat.type = "supergroup"
    assert not authorise(settings, update)
    update.effective_chat.type = "private"
    update.effective_user.is_bot = True
    assert not authorise(settings, update)


def test_menu_and_no_remote_delete():
    from eth_tester.telegram_control import ALIASES, COMMANDS, _control
    assert {"startsim", "pause", "resume", "stop", "history", "progress", "ws"} <= set(COMMANDS)
    assert {"startsim", "pause", "resume", "stop"} <= set(ALIASES.values())
    with pytest.raises(KeyError):
        _control(SimpleNamespace(), "delete")


def test_supervisor_atomic_status(tmp_path):
    sup = supervisor.Supervisor(path=tmp_path)
    sup._record("engine", "running", pid=10)
    state = json.loads((tmp_path / "supervisor.json").read_text())
    assert state["engine"]["pid"] == 10
    assert not list(tmp_path.glob(".supervisor.*.tmp"))


def test_permanent_worker_restarts_even_after_exit_zero(monkeypatch, tmp_path):
    launches = []

    class FakeProc:
        pid = 411
        returncode = 0

        async def wait(self):
            return 0

    async def spawn(*args, **kwargs):
        launches.append(args)
        return FakeProc()

    async def cancel_sleep(_):
        raise asyncio.CancelledError

    monkeypatch.setattr(supervisor.asyncio, "create_subprocess_exec", spawn)
    sup = supervisor.Supervisor(path=tmp_path, sleep=cancel_sleep)

    async def run():
        with pytest.raises(asyncio.CancelledError):
            await sup.child("engine")

    asyncio.run(run())
    assert len(launches) == 1
    assert sup.status["engine"]["state"] == "stopped"


def test_shutdown_only_own_child(tmp_path):
    calls = []

    class FakeProc:
        pid = 123
        returncode = None

        def terminate(self):
            calls.append("terminate")
            self.returncode = 0

        async def wait(self):
            calls.append("wait")
            return 0

        def kill(self):
            calls.append("kill")

    asyncio.run(supervisor.Supervisor(path=tmp_path)._terminate("engine", FakeProc()))
    assert calls == ["terminate", "wait"]


def test_windows_uninstall_requires_ownership_manifest():
    root = Path(__file__).resolve().parents[1]
    install = (root / "install" / "install.ps1").read_text(encoding="utf-8")
    uninstall = (root / "install" / "uninstall.ps1").read_text(encoding="utf-8")
    assert "DROP DATABASE" not in install.upper() + uninstall.upper()
    assert "Remove-Item -Recurse" not in install + uninstall
    assert "Ownership manifest missing" in uninstall
    assert "Task action does not match manifest" in uninstall
