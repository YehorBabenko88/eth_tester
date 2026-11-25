"""G round9 failure-injection and liveness regressions. No live network or DB."""
import asyncio
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from eth_tester import service_health, service_lease, supervisor, telegram_views


class FakeSupervisor:
    def __init__(self):
        self.children = {
            'engine': SimpleNamespace(returncode=None, pid=11),
            'analyse': SimpleNamespace(returncode=None, pid=12),
            'live': SimpleNamespace(returncode=None, pid=13),
        }
        self.process_starts = {'engine': 0, 'analyse': 0, 'live': 0}
        self.stopped = []
        self.status = {}

    def _record(self, key, state, **kwargs):
        self.status[key] = {'state': state, **kwargs}

    async def _terminate(self, name, process):
        self.stopped.append(name)
        process.returncode = -15


def test_watchdog_restarts_hung_engine_after_two_misses_only():
    sup = FakeSupervisor()
    w = service_health.HealthWatchdog(clock=lambda: 500,
                                      checker=lambda _: {
                                          'paper_engine': 1000,
                                          'live_paper_window': 5})
    assert asyncio.run(w.check_once(sup, object())) == 'checked'
    assert sup.stopped == []
    assert asyncio.run(w.check_once(sup, object())) == 'checked'
    assert sup.stopped == ['engine']
    assert sup.status['watchdog']['worker'] == 'engine'
    assert sup.children['live'].returncode is None


def test_db_offline_does_not_kill_any_running_worker():
    sup = FakeSupervisor()
    def unavailable(_):
        raise ConnectionError('private database DSN must not appear')
    w = service_health.HealthWatchdog(clock=lambda: 500, checker=unavailable)
    for _ in range(8):
        assert asyncio.run(w.check_once(sup, object())) == 'database_unavailable'
    assert sup.stopped == []
    assert sup.status['watchdog']['error_type'] == 'ConnectionError'
    assert 'DSN' not in str(sup.status)


def test_db_recovery_resets_stale_streak():
    sup = FakeSupervisor()
    seq = iter([{'paper_engine': 900, 'live_paper_window': 5},
                ConnectionError('network down'),
                {'paper_engine': 900, 'live_paper_window': 5}])
    def sample(_):
        item = next(seq)
        if isinstance(item, Exception):
            raise item
        return item
    w = service_health.HealthWatchdog(clock=lambda: 500, checker=sample)
    for _ in range(3):
        asyncio.run(w.check_once(sup, object()))
    assert sup.stopped == []


def test_watchdog_no_restart_within_grace_period():
    sup = FakeSupervisor()
    sup.process_starts['engine'] = 450
    sup.process_starts['analyse'] = 450
    w = service_health.HealthWatchdog(clock=lambda: 500, checker=lambda _: {})
    for _ in range(4):
        asyncio.run(w.check_once(sup, object()))
    assert not sup.stopped


def test_cooldown_rejects_repeat_restarts():
    sup = FakeSupervisor()
    w = service_health.HealthWatchdog(clock=lambda: 500, checker=lambda _: {})
    w.last_restart['engine'] = 490
    w.last_restart['analyse'] = 490
    for _ in range(4):
        asyncio.run(w.check_once(sup, object()))
    assert not sup.stopped


def test_fresh_engine_and_analytics_do_not_restart_on_absent_live_feed():
    sup = FakeSupervisor()
    w = service_health.HealthWatchdog(clock=lambda: 500,
         checker=lambda _: {'paper_engine': 8, 'live_paper_window': 3})
    for _ in range(10):
        asyncio.run(w.check_once(sup, object()))
    assert not sup.stopped
    assert 'live' not in service_health.HEARTBEATS


def test_future_or_invalid_heartbeat_not_healthy():
    assert service_health.classify_heartbeat(-5, 120) == 'stale'
    assert service_health.classify_heartbeat(None, 120) == 'missing'
    assert service_health.classify_heartbeat(float('nan'), 120) == 'stale'
    assert service_health.classify_heartbeat(True, 120) == 'missing'
    assert service_health.classify_heartbeat(60, 120) == 'healthy'


def test_singleton_service_lease_prevents_second_instance(tmp_path):
    with service_lease.service_lease(tmp_path):
        with pytest.raises(RuntimeError, match='already running'):
            with service_lease.service_lease(tmp_path):
                pass
    # No PID-file stale state after process stops.
    with service_lease.service_lease(tmp_path):
        pass
    assert (tmp_path/'supervisor.instance.lock').exists()


def test_recovery_keeps_paper_pause_and_stops_no_unowned_children():
    code = Path(supervisor.__file__).read_text(encoding='utf-8')
    assert 'bot_control SET' not in code
    assert 'self.children.get(name)' not in code
    assert 'self._terminate(name, proc)' in code
    assert 'cleanup' not in supervisor.PERIODIC  # B retention marker protection pending


def test_telegram_owner_only_trade_id_and_no_sql_injection(monkeypatch):
    calls = []
    row = {'id': 123, 'state': 'closed', 'direction': 'long',
           'signal_ts': 'T0', 'opened_at': 'T1', 'closed_at': 'T2',
           'entry': 2500, 'exit': 2510, 'qty': 0.4,
           'stop': 2480, 'take': 2530, 'gross_pnl': 4, 'net_pnl': 3,
           'fee_paid': .7, 'funding_paid': .2, 'slippage_paid': .1,
           'exit_reason': 'take', 'reason': {'a': 'entry-time-only'}}
    class Conn:
        def execute(self, q, args):
            calls.append((q, args))
            return SimpleNamespace(fetchone=lambda: row)
    @contextmanager
    def db(_):
        yield Conn()
    monkeypatch.setattr(telegram_views, 'connection', db)
    settings = SimpleNamespace(symbol='ETHUSDT')
    answer = telegram_views.detail_trade(settings, 123)
    assert '#123' in answer and 'funding' in answer and 'entry-time-only' in answer
    assert calls[0][1] == ('ETHUSDT', 123)
    with pytest.raises(ValueError):
        telegram_views.detail_trade(settings, '123 OR 1=1')
    with pytest.raises(ValueError):
        telegram_views.detail_trade(settings, -2)


def test_telegram_pagination_keeps_all_evidence_and_flags_truncation():
    original = 'x' * 9000
    parts = telegram_views.pages(original, chunk_limit=1000)
    assert len(parts) == 8
    assert parts[-1].endswith('[Ответ ограничен: запросите конкретную запись]')
    assert telegram_views.pages('short') == ['short']
    assert all(len(p) < 3900 for p in parts)


def test_telegram_rate_limit_and_identity_cap_fail_closed():
    telegram_views._rate_times.clear()
    identity = (1, 2)
    assert all(telegram_views.allow_message(identity, now=100) for _ in range(20))
    assert not telegram_views.allow_message(identity, now=100)
    assert telegram_views.allow_message(identity, now=161)
    telegram_views._rate_times.clear()
    for i in range(1024):
        assert telegram_views.allow_message((i, i), now=100)
    assert not telegram_views.allow_message(('another', 1), now=101)
    telegram_views._rate_times.clear()


def test_telegram_only_safe_readonly_commands_and_no_delete():
    assert 'delete' not in telegram_views.COMMANDS
    assert 'risk' in telegram_views.COMMANDS
    assert 'research' in telegram_views.COMMANDS
    assert 'maintenance' in telegram_views.COMMANDS
    source = Path(telegram_views.__file__).read_text(encoding='utf-8')
    assert 'DELETE FROM' not in source and 'DROP TABLE' not in source
    assert 'UPDATE bot_control' not in source


def test_systemd_template_never_runs_as_root_or_grants_capabilities():
    path = Path(__file__).resolve().parents[1]/'install'/'eth-tester.service'
    config = path.read_text(encoding='utf-8')
    assert 'User=eth-tester' in config
    assert 'Restart=always' in config
    assert 'NoNewPrivileges=true' in config
    assert 'ProtectSystem=strict' in config
    assert 'CapabilityBoundingSet=' in config
    assert 'User=root' not in config
    assert 'ExecStart=/opt/eth-tester/.venv/bin/python -m eth_tester.cli run' in config


def test_risk_view_reports_waiting_feed_and_research_without_ml_claim(monkeypatch):
    responses = iter([
        {'enabled': True, 'paused': False, 'updated_at': 'T1'},
        {'equity': 10000, 'initial_equity': 10000},
        {'n': 1, 'notional': 1000},
        {'status': 'waiting_live_feed_open_position', 'detail': {'source': 'gap'},
         'updated_at': 'T2'},
        {'ts': 'T3', 'data': {'trade_id': 8, 'retention': 'preserve'}},
    ])
    @contextmanager
    def fake_db(_):
        class Query:
            def fetchone(self):
                return next(responses)
        class Conn:
            def execute(self, sql, args=None):
                assert 'DELETE ' not in sql.upper()
                return Query()
        yield Conn()
    monkeypatch.setattr(telegram_views, 'connection', fake_db)
    result = telegram_views.read_view(SimpleNamespace(symbol='ETHUSDT'), 'risk')
    assert 'waiting_live_feed_open_position' in result
    assert 'paper_trade_review_v1' in result
    assert 'НЕ ML' in result
    assert 'Реальные ордера запрещены' in result
