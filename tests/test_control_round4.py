"""G round 4: read-only startup and Windows lifecycle regressions, no real trading."""
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from eth_tester import supervisor


def test_readonly_verify_precedes_worker_start(monkeypatch, tmp_path):
    calls = []
    ready = asyncio.Event()

    def verify(_settings):
        calls.append("verify")
        return {"database": "eth_tester_test", "verified": True}

    async def child(self, name, recurring_seconds=None):
        calls.append(("worker", name))
        ready.set()
        await asyncio.Event().wait()

    async def shutdown(self):
        await ready.wait()

    monkeypatch.setattr(supervisor, "verify_existing_schema", verify)
    monkeypatch.setattr(supervisor.Supervisor, "child", child)
    monkeypatch.setattr(supervisor.Supervisor, "_wait_shutdown", shutdown)
    asyncio.run(supervisor.Supervisor(path=tmp_path).run(
        SimpleNamespace(tg_bot_token="", tg_chat_id="")))
    assert calls[0] == "verify"
    assert any(isinstance(v, tuple) and v[0] == "worker" for v in calls)


def test_invalid_database_prevents_any_worker_spawn(monkeypatch, tmp_path):
    launched = []

    def reject(_settings):
        raise RuntimeError("schema checksum mismatch")

    async def child(self, name, recurring_seconds=None):
        launched.append(name)

    monkeypatch.setattr(supervisor, "verify_existing_schema", reject)
    monkeypatch.setattr(supervisor.Supervisor, "child", child)
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        asyncio.run(supervisor.Supervisor(path=tmp_path).run(SimpleNamespace()))
    assert launched == []
    assert not (tmp_path / "supervisor.json").exists()


def test_telegram_process_disabled_without_numeric_owner(monkeypatch, tmp_path):
    started = []
    monkeypatch.setenv("ETH_TESTER_TG_USER_ID", "not-an-id")
    monkeypatch.setattr(supervisor, "verify_existing_schema", lambda _: {"verified": True})

    async def child(self, name, recurring_seconds=None):
        started.append(name)
        await asyncio.Event().wait()

    async def shutdown(self):
        await asyncio.sleep(0)

    monkeypatch.setattr(supervisor.Supervisor, "child", child)
    monkeypatch.setattr(supervisor.Supervisor, "_wait_shutdown", shutdown)
    asyncio.run(supervisor.Supervisor(path=tmp_path).run(
        SimpleNamespace(tg_bot_token="configured", tg_chat_id="123")))
    assert "telegram" not in started


def test_status_write_failure_does_not_orphan_worker(monkeypatch, tmp_path):
    events = []

    class Child:
        pid = 123
        returncode = None

        def terminate(self):
            events.append("terminate")
            self.returncode = 0

        async def wait(self):
            events.append("wait")
            return 0

    async def spawn(*args, **kwargs):
        events.append("spawn")
        return Child()

    def fail_record(self, *args, **kwargs):
        events.append("record")
        raise OSError("disk full")

    monkeypatch.setattr(supervisor.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(supervisor.Supervisor, "_record", fail_record)
    with pytest.raises(OSError, match="disk full"):
        asyncio.run(supervisor.Supervisor(path=tmp_path).child("engine"))
    assert events[:3] == ["spawn", "record", "terminate"]
    assert "wait" in events


def test_supervisor_never_bootstraps_database():
    path = Path(__file__).resolve().parents[1] / "src" / "eth_tester" / "supervisor.py"
    source = path.read_text(encoding="utf-8")
    assert "verify_existing_schema(settings)" in source
    assert "init_database" not in source


def test_installer_one_task_and_guarded_runtime():
    root = Path(__file__).resolve().parents[1]
    installer = (root / "install" / "install.ps1").read_text(encoding="utf-8")
    assert installer.count("Register-ScheduledTask -TaskName") == 1
    assert installer.count("Set-Acl -LiteralPath $runtimeRoot") == 1
    assert "$ownerId -notmatch '^[0-9]+$'" in installer
    assert "Existing runtime root without ownership manifest" in installer
    assert "SetAccessRuleProtection($true, $false)" in installer
    assert "eth_tester.cli --help" in installer
    assert "verify_existing_schema" in installer
    assert "DROP DATABASE" not in installer.upper()


def test_uninstall_rejects_foreign_task_principal():
    root = Path(__file__).resolve().parents[1]
    uninstall = (root / "install" / "uninstall.ps1").read_text(encoding="utf-8")
    assert "Task action does not match manifest" in uninstall
    assert "Task principal does not match owner" in uninstall
    assert "S-1-5-19" in uninstall
    assert "Remove-Item -Recurse" not in uninstall


@pytest.mark.skipif(__import__("sys").platform != "win32", reason="Windows parser only")
def test_windows_scripts_parse_without_executing():
    import subprocess

    root = Path(__file__).resolve().parents[1]
    for filename in ("install.ps1", "uninstall.ps1"):
        path = (root / "install" / filename).as_posix().replace("'", "''")
        command = (
            "$tokens=$null;$errors=$null;"
            "[System.Management.Automation.Language.Parser]::ParseFile("
            f"'{path}',[ref]$tokens,[ref]$errors)|Out-Null;"
            "if($errors.Count -gt 0){$errors|Out-String|Write-Error;exit 1}"
        )
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
            capture_output=True, text=True, check=False, timeout=30,
        )
        assert result.returncode == 0, result.stderr
