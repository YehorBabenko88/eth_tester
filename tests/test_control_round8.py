"""G eighth-round telemetry and offline schedule regressions; no Bybit/DB."""
import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest

from eth_tester import control_jobs, supervisor
from test_control_readiness_probe import NOW, complete, probe


@pytest.fixture
def recorded(monkeypatch):
    calls = []
    @contextmanager
    def fake_connection(_settings):
        yield SimpleNamespace(execute=lambda query, params: calls.append((query, params)))
    monkeypatch.setattr(control_jobs, 'connection', fake_connection)
    return calls


def test_label_worker_scanned_created_and_cursor_survive_telemetry(monkeypatch, recorded):
    monkeypatch.setattr(control_jobs, '_label_horizons', lambda _: {
        'status': 'labels_written', 'created': 3, 'scanned': 7,
        'next_after_id': 987, 'approved': False,
        'rejections': {'missing_or_ambiguous_historical_candle_witness': 2,
                       'private password=abc token=secret': 2}})
    result = control_jobs.run_control_job(object(), 'label-horizons')
    assert result['scanned'] == 7 and result['created'] == 3
    assert result['next_after_id'] == 987
    assert result['rejection_counts'] == {
        'missing_or_ambiguous_historical_candle_witness': 2, 'other': 2}
    assert result['rejected_total'] == 4
    persisted = recorded[0][1][2]
    assert 'secret' not in persisted and 'password' not in persisted


def test_training_no_eligible_data_preserves_watermark(monkeypatch, recorded):
    monkeypatch.setattr(control_jobs, '_causal_shadow', lambda _: {
        'status': 'insufficient_new_verified_data', 'usable': 0,
        'required': 180, 'previous_eligible_count': 17,
        'as_of_ms': 1764100000000, 'rejections': {'some exception /tmp/x': 1234}})
    result = control_jobs.run_control_job(object(), 'train-causal-shadow')
    assert result['previous_eligible_count'] == 17
    assert result['usable'] == 0
    assert result['rejection_counts'] == {'other': 1234}
    assert result['ml_certified_by_control'] is False
    assert recorded[0][1][1] == 'insufficient_new_verified_data'


def test_rejection_counter_rejects_malformed_source(monkeypatch, recorded):
    monkeypatch.setattr(control_jobs, '_label_horizons', lambda _: {
        'status': 'insufficient_verified_horizon',
        'rejections': {'some user string': True}})
    with pytest.raises(RuntimeError, match='invalid rejection count'):
        control_jobs.run_control_job(object(), 'label-horizons')
    assert recorded[0][1][1] == 'error'


def test_bounded_rejection_cardinality_does_not_emit_secrets():
    huge = {f'secret_{n}': 1 for n in range(257)}
    with pytest.raises(RuntimeError, match='invalid rejection map'):
        control_jobs._sanitize({'status': 'insufficient_verified_horizon',
                                'rejections': huge})


def test_arbitrary_status_does_not_exfiltrate_secrets():
    with pytest.raises(RuntimeError, match='invalid status'):
        control_jobs._sanitize({'status': 'token=SECRET secret path /tmp/creds'})


def test_auto_approval_still_blocked():
    with pytest.raises(RuntimeError, match='Automatic model approval'):
        control_jobs._sanitize({'status': 'shadow_registered', 'approved': True})


def test_unknown_rejection_categories_aggregate_with_limit():
    _, result = control_jobs._sanitize({
        'status': 'insufficient_verified_horizon',
        'rejections': {'secret one': 10**12-2, 'secret two': 10**12-2}})
    assert result['rejection_counts']['other'] == 2*(10**12-2)
    assert result['rejected_total'] == 2*(10**12-2)


def test_periodic_success_reports_future_wake_not_exited(monkeypatch, tmp_path):
    observations = []
    class FakeProcess:
        pid = 321
        returncode = 0
        async def wait(self):
            return 0
    async def spawn(*a, **kw):
        return FakeProcess()
    async def sleeping(seconds):
        observations.append((seconds, json.loads((tmp_path/'supervisor.json').read_text())))
        raise asyncio.CancelledError()
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', spawn)
    sup = supervisor.Supervisor(path=tmp_path, sleep=sleeping)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(sup.child('label-horizons', 300))
    seconds, doc = observations[0]
    waiting = doc['label-horizons']
    assert seconds == 300
    assert waiting['state'] == 'waiting_for_next_run'
    assert waiting['interval_seconds'] == 300
    assert waiting['exit_code'] == 0
    until = datetime.fromisoformat(waiting['next_run_at'])
    delta = until - datetime.now(timezone.utc)
    assert timedelta(seconds=290) <= delta <= timedelta(seconds=301)


def test_periodic_failure_uses_backoff_not_success_wait(monkeypatch, tmp_path):
    observations = []
    class FakeProcess:
        pid = 331
        returncode = 1
        async def wait(self):
            return 1
    async def spawn(*a, **kw):
        return FakeProcess()
    async def sleeping(seconds):
        observations.append((seconds, json.loads((tmp_path/'supervisor.json').read_text())))
        raise asyncio.CancelledError()
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', spawn)
    sup = supervisor.Supervisor(path=tmp_path, sleep=sleeping)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(sup.child('train-causal-shadow', 3600))
    seconds, doc = observations[0]
    assert seconds == 3
    assert doc['train-causal-shadow']['state'] == 'restarting'
    assert 'next_run_at' not in doc['train-causal-shadow']


def test_historical_ml_absence_not_a_paper_rejection():
    data = complete()
    data['certified_snapshot_count_24h'] = 0
    data['post_horizon_label_count'] = 0
    data['shadow_model_count'] = 0
    result = probe.assess(data, now=NOW)
    assert not result['bounded_paper']['reasons']
    assert len(result['historical_ml']['reasons']) == 3
    assert result['production_go'] is False


def test_paper_disconnect_does_not_relabel_historical_ml():
    data = complete()
    data['feed_status'] = 'disconnected'
    data['current_bounded_paper_evidence_count'] = 0
    result = probe.assess(data, now=NOW)
    assert 'feed_not_connected' in result['bounded_paper']['reasons']
    assert result['historical_ml']['reasons'] == []
    assert result['production_go'] is False


def test_publisher_staleness_blocks_paper_without_overwriting_ml():
    data = complete()
    data['paper_publisher_heartbeat'] = NOW - timedelta(seconds=25)
    result = probe.assess(data, now=NOW)
    assert 'paper_publisher_heartbeat_missing_or_stale' in result['bounded_paper']['reasons']
    assert result['historical_ml']['state'] == 'historical_evidence_present_not_accepted'


def test_current_paper_sql_never_converts_to_ml_proof():
    q = probe.QUERIES['current_bounded_paper_evidence_count'][0]
    assert 'paper_live_evidence' in q
    assert "ml_eligible IS FALSE" in q
    assert 'expires_at>=clock_timestamp()' in q
    assert 'UPDATE ' not in q and 'INSERT ' not in q
