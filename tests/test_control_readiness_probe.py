"""Pure, offline G read-only checks; never access PostgreSQL or an exchange."""
from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path

path = Path(__file__).resolve().parents[1] / "scripts" / "control_readiness_probe.py"
spec = importlib.util.spec_from_file_location("control_readiness_probe", path)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)

NOW = datetime(2026, 11, 25, tzinfo=timezone.utc)


def complete():
    return {
        "last_trade": NOW-timedelta(seconds=1),
        "last_candle": NOW-timedelta(seconds=60),
        "analytics_heartbeat": NOW-timedelta(seconds=100),
        "engine_heartbeat": NOW-timedelta(seconds=10),
        "feed_status": "connected",
        "unresolved_trade_gaps": 0,
        "certified_snapshot_count_24h": 2,
        "post_horizon_label_count": 1,
        "shadow_model_count": 1,
    }


def test_missing_all_evidence_blocks_go():
    result = probe.assess({}, now=NOW)
    assert result['status'] == 'insufficient_evidence'
    assert not result['production_go']
    assert 'no_verified_immutable_snapshot_v2' in result['reasons']


def test_live_feed_is_not_sufficient_for_ml():
    obs = complete()
    obs['certified_snapshot_count_24h'] = 0
    obs['post_horizon_label_count'] = 0
    obs['shadow_model_count'] = 0
    result = probe.assess(obs, now=NOW)
    assert result['status'] == 'insufficient_evidence'
    assert 'no_issued_post_horizon_labels' in result['reasons']


def test_missing_trade_stream_blocks():
    obs = complete()
    obs['last_trade'] = NOW-timedelta(minutes=3)
    result = probe.assess(obs, now=NOW)
    assert 'last_trade_missing_or_stale' in result['reasons']


def test_future_telegram_or_market_timestamp_cannot_look_fresh():
    obs = complete()
    obs['last_trade'] = NOW+timedelta(seconds=60)
    assert 'last_trade_missing_or_stale' in probe.assess(obs, now=NOW)['reasons']


def test_live_disconnected_blocks_even_with_recent_trade():
    obs = complete()
    obs['feed_status'] = 'disconnected'
    assert 'feed_not_connected' in probe.assess(obs, now=NOW)['reasons']


def test_unresolved_gaps_block():
    obs = complete()
    obs['unresolved_trade_gaps'] = 2
    assert 'trade_gap_not_cleared' in probe.assess(obs, now=NOW)['reasons']


def test_all_synthetic_metrics_still_never_mean_go():
    result = probe.assess(complete(), now=NOW)
    assert result['status'] == 'needs_independent_runtime_acceptance'
    assert result['production_go'] is False


def test_unaware_datetime_is_not_trusted():
    obs = complete()
    obs['last_candle'] = datetime(2026,11,25)
    assert 'last_candle_missing_or_stale' in probe.assess(obs, now=NOW)['reasons']


def test_readonly_queries_have_no_sql_writes():
    for query, _ in probe.QUERIES.values():
        assert query.lstrip().upper().startswith('SELECT ')
        assert not any(token in query.upper() for token in ('INSERT ', 'UPDATE ', 'DELETE ', 'DROP '))
