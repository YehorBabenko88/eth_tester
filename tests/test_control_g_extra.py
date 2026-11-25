"""Security and state control tests with mocked database and Telegram operations."""

import asyncio
from contextlib import contextmanager
from decimal import Decimal
import json
from types import SimpleNamespace

import pytest


class FakeConn:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        return self.rows.pop(0) if self.rows else []


@contextmanager
def fake_connection(conn):
    yield conn


def test_control_never_closes_open_trade(monkeypatch):
    from eth_tester import telegram_control as control
    conn = FakeConn()
    monkeypatch.setattr(control, "connection", lambda _: fake_connection(conn))
    for cmd, expected in [
        ("startsim", (True, False)), ("pause", (True, True)),
        ("resume", (True, False)), ("stop", (False, True)),
    ]:
        assert "Реальные ордера" in control._control(SimpleNamespace(), cmd)
        assert conn.calls[-1][1] == expected
    assert all("paper_trades" not in sql.lower() for sql, _ in conn.calls)


def test_audit_redacts_message_text_and_token(monkeypatch):
    from eth_tester import telegram_control as control
    conn = FakeConn()
    monkeypatch.setattr(control, "connection", lambda _: fake_connection(conn))
    u = SimpleNamespace(
        effective_chat=SimpleNamespace(id=45),
        effective_user=SimpleNamespace(id=12),
        effective_message=SimpleNamespace(text="secret token 123"),
    )
    control._audit(SimpleNamespace(tg_bot_token="SECRET_TOKEN"), u, "stats", True, "done")
    _, params = conn.calls[0]
    assert "SECRET_TOKEN" not in str(params)
    assert "secret token" not in str(params)
    assert json.loads(params[1])["user_id"] == "12"


def test_audit_unavailable_fails_closed(monkeypatch):
    from eth_tester import telegram_control as control

    @contextmanager
    def broken(_):
        raise OSError("db down")
        yield

    monkeypatch.setattr(control, "connection", broken)
    u = SimpleNamespace(
        effective_chat=SimpleNamespace(id=45),
        effective_user=SimpleNamespace(id=12),
    )
    with pytest.raises(RuntimeError, match="command"):
        control._audit(SimpleNamespace(), u, "stop", True, "requested")
    control._audit(SimpleNamespace(), u, "stop", False, "denied")


def test_progress_per_source(monkeypatch):
    from eth_tester import telegram_control as control
    conn = FakeConn(rows=[
        [{"source": "archive", "state": "complete", "n": 3, "rows": 50, "bytes": 1024}],
        [{"source": "rest", "interval": "1", "status": "partial", "cursor_ts": "2026-01-01"}],
    ])
    monkeypatch.setattr(control, "connection", lambda _: fake_connection(conn))
    text = control._read(SimpleNamespace(), "progress")
    assert "archive" in text and "rest/1" in text
    assert "partial" in text and "1024" in text


def test_stats_zero_closed_trades(monkeypatch):
    from eth_tester import telegram_control as control
    conn = FakeConn(rows=[
        {"equity": Decimal("10000")},
        {"n": 0, "wins": 0, "gross": Decimal(0), "net": Decimal(0),
         "fees": Decimal(0), "funding": Decimal(0), "slippage": Decimal(0)},
    ])
    monkeypatch.setattr(control, "connection", lambda _: fake_connection(conn))
    assert "0.0%" in control._read(SimpleNamespace(), "stats")


def test_delete_command_refused_even_for_owner(monkeypatch):
    from eth_tester import telegram_control as control
    replies = []

    async def reply_text(s, **kwargs):
        replies.append(s)

    monkeypatch.setattr(control, "authorise", lambda *args: True)
    monkeypatch.setattr(control, "_audit", lambda *args: None)
    u = SimpleNamespace(effective_message=SimpleNamespace(reply_text=reply_text))
    asyncio.run(control.dispatch(SimpleNamespace(), u, SimpleNamespace(args=[]), "delete"))
    assert len(replies) == 1
    assert "заблокировано" in replies[0]

def test_cooperative_supervisor_shutdown_request(tmp_path):
    from eth_tester.supervisor import Supervisor
    marker = tmp_path / "shutdown.request"
    marker.write_text("owned uninstall", encoding="utf-8")
    asyncio.run(Supervisor(path=tmp_path)._wait_shutdown())
    assert not marker.exists()
