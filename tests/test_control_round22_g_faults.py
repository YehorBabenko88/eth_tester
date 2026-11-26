"""G R22: isolated, paper-only control fault regressions.

Tests do not connect to exchange/Telegram, write PostgreSQL or start services.
No code or test is skipped; run in the repository's standard dev environment.
"""

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from eth_tester import service_health, supervisor, telegram_control, telegram_views


class OwnedWorkers:
    """A supervisor double with only explicitly tracked child ownership."""

    def __init__(self):
        self.children = {
            "engine": SimpleNamespace(returncode=None, pid=113),
            "analyse": SimpleNamespace(returncode=None, pid=114),
        }
        self.process_starts = {"engine": 1000, "analyse": 1000}
        self.terminated = []
        self.records = []

    def _record(self, *args, **kwargs):
        self.records.append((args, kwargs))

    async def _terminate(self, name, child):
        assert child is self.children[name]
        self.terminated.append((name, child.pid))


def test_db_outage_never_terminates_workers_or_enters_restart_cooldown():
    def disconnected(_settings):
        raise ConnectionError("synthetic PG unavailable")

    owned = OwnedWorkers()
    watchdog = service_health.HealthWatchdog(
        clock=lambda: 2000, checker=disconnected,
    )
    watchdog.misses["engine"] = 1  # prior single stale heartbeat
    for _ in range(3):
        assert asyncio.run(watchdog.check_once(owned, SimpleNamespace())) == (
            "database_unavailable"
        )
    assert owned.terminated == []
    assert watchdog.misses == {}
    assert watchdog.last_restart == {}
    assert all(args[:2] == ("watchdog", "database_unavailable")
               for args, _ in owned.records)


def test_engine_stale_two_misses_then_cooldown_prevents_restart_storm():
    owned = OwnedWorkers()
    watchdog = service_health.HealthWatchdog(
        clock=lambda: 2000,
        checker=lambda _: {"paper_engine": 121, "live_paper_window": 1,
                           "analytics": 1},
    )
    assert asyncio.run(watchdog.check_once(owned, SimpleNamespace())) == "checked"
    assert owned.terminated == []
    assert asyncio.run(watchdog.check_once(owned, SimpleNamespace())) == "checked"
    assert owned.terminated == [("engine", 113)]
    for _ in range(5):
        asyncio.run(watchdog.check_once(owned, SimpleNamespace()))
    assert owned.terminated == [("engine", 113)]
    assert watchdog.last_restart["engine"] == 2000


def test_analytics_slow_heartbeat_cannot_be_hidden_by_fast_heartbeat():
    owned = OwnedWorkers()
    watchdog = service_health.HealthWatchdog(
        clock=lambda: 2000,
        checker=lambda _: {"paper_engine": 1, "live_paper_window": 1,
                           "analytics": 421},
    )
    asyncio.run(watchdog.check_once(owned, SimpleNamespace()))
    asyncio.run(watchdog.check_once(owned, SimpleNamespace()))
    assert owned.terminated == [("analyse", 114)]


def test_status_file_failure_is_observability_failure_not_worker_failure(tmp_path):
    occupied = tmp_path / "not_a_directory"
    occupied.write_text("unrelated", encoding="utf-8")
    watched = supervisor.Supervisor(path=occupied)
    watched._record("engine", "running", pid=113)
    assert watched.status["engine"]["pid"] == 113
    assert watched.status["engine"]["state"] == "running"
    assert occupied.read_text(encoding="utf-8") == "unrelated"


def test_permanent_worker_failure_has_bounded_exponential_restart_delays(
    monkeypatch, tmp_path,
):
    launches = []
    delays = []

    class FailedChild:
        pid = 113
        returncode = 1

        async def wait(self):
            return 1

    async def spawn(*_args, **_kwargs):
        launches.append(True)
        return FailedChild()

    async def no_wall_clock_sleep(seconds):
        delays.append(seconds)
        if len(delays) == 4:
            raise asyncio.CancelledError

    monkeypatch.setattr(supervisor.asyncio, "create_subprocess_exec", spawn)
    watched = supervisor.Supervisor(path=tmp_path, sleep=no_wall_clock_sleep)

    async def exercise():
        with pytest.raises(asyncio.CancelledError):
            await watched.child("engine")

    asyncio.run(exercise())
    assert len(launches) == 4
    assert delays == [3.0, 6.0, 12.0, 24.0]
    assert watched.status["engine"]["state"] == "stopped"
    assert watched.children == {}


def test_telegram_db_audit_outage_never_dispatches_owner_stop(monkeypatch):
    commands = []
    responses = []

    async def reply_text(text, **_kwargs):
        responses.append(text)

    def refuse_audit(*_args, **_kwargs):
        raise RuntimeError("Refusing command without audit")

    def execute_control(*_args, **_kwargs):
        commands.append("unexpected DB mutation")

    monkeypatch.setattr(telegram_control, "authorise", lambda *_: True)
    monkeypatch.setattr(telegram_views, "allow_message", lambda *_: True)
    monkeypatch.setattr(telegram_control, "_audit", refuse_audit)
    monkeypatch.setattr(telegram_control, "_control", execute_control)
    message = SimpleNamespace(reply_text=reply_text)
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=42, type="private"),
        effective_user=SimpleNamespace(id=42, is_bot=False),
        effective_message=message,
    )
    asyncio.run(telegram_control.dispatch(
        SimpleNamespace(), update, SimpleNamespace(args=[]), "stop",
    ))
    assert commands == []
    assert len(responses) == 1
    assert "Не удалось" in responses[0]


def test_wrong_private_user_cannot_issue_stop_even_with_matching_chat(
    monkeypatch,
):
    updates = []
    mutations = []

    monkeypatch.setenv("ETH_TESTER_TG_USER_ID", "42")
    monkeypatch.setattr(telegram_views, "allow_message", lambda *_: True)
    monkeypatch.setattr(telegram_control, "_audit", lambda *args: updates.append(args))
    monkeypatch.setattr(telegram_control, "_control", lambda *_: mutations.append(True))
    msg = SimpleNamespace(reply_text=lambda *_a, **_k: None)
    intruder = SimpleNamespace(
        effective_chat=SimpleNamespace(id=42, type="private"),
        effective_user=SimpleNamespace(id=99, is_bot=False),
        effective_message=msg,
    )
    asyncio.run(telegram_control.dispatch(
        SimpleNamespace(tg_chat_id="42"), intruder,
        SimpleNamespace(args=[]), "stop",
    ))
    assert mutations == []
    assert len(updates) == 1
    assert updates[0][3] is False


def test_stop_command_updates_only_control_not_paper_trades(monkeypatch):
    statements = []

    class FakeConn:
        def execute(self, sql, params=None):
            statements.append((sql, params))

    @contextmanager
    def connect(_settings):
        yield FakeConn()

    monkeypatch.setattr(telegram_control, "connection", connect)
    out = telegram_control._control(SimpleNamespace(), "stop")
    assert "Реальные ордера" in out
    assert len(statements) == 1
    assert "UPDATE bot_control" in statements[0][0]
    assert statements[0][1] == (False, True)
    assert "paper_trades" not in statements[0][0]


def test_redaction_of_labelled_secrets_bearer_and_postgres_url():
    key = "R22_CANARY_NEVER_SEND_92713"
    value = {
        "bot_token": key,
        "diagnostic": f"Bearer {key}; password={key}; postgresql://user:{key}@host/db",
        "nested": [{"password": key}],
    }
    output = telegram_views._json(value, limit=4000)
    assert key not in output
    assert "[REDACTED]" in output
    text = telegram_views.sanitize_message(
        f"Bearer {key} password={key} postgresql://user:{key}@host/db"
    )
    assert key not in text
