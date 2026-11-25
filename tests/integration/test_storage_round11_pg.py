"""Disposable real PostgreSQL 17/18 contract tests; never touch a user database.

Requires ETH_TEST_PG_DSN. Creates only eth_tester_ci_b11_<random> and drops
only that newly created DB. Synthetic receipts are not exchange-complete proof.
"""
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.retention import cleanup_diagnostics

pytestmark = pytest.mark.postgres


@pytest.fixture
def owned_b11(monkeypatch):
    dsn = os.getenv('ETH_TEST_PG_DSN')
    if not dsn:
        pytest.skip('Disposable PostgreSQL requires ETH_TEST_PG_DSN')
    monkeypatch.delenv('ETH_TESTER_OWNED_TEMP_DIR', raising=False)
    dbname = 'eth_tester_ci_b11_' + uuid4().hex[:12]
    target = make_conninfo(dsn, dbname=dbname)
    settings = Settings(_env_file=None, database_url=target)
    try:
        init_database(settings)
        yield settings
    finally:
        with psycopg.connect(make_conninfo(dsn, dbname='postgres'),
                             autocommit=True) as admin:
            if admin.execute('SELECT 1 FROM pg_database WHERE datname=%s',
                             (dbname,)).fetchone():
                admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(
                    sql.Identifier(dbname)))


def test_long_lived_session_and_outage_history_never_removed(owned_b11):
    old = datetime(2020,1,1,tzinfo=timezone.utc)
    with connection(owned_b11) as conn:
        for idx,status in enumerate(('connected','disconnected','connected')):
            conn.execute('''INSERT INTO feed_health(symbol,topic,ts,status,detail)
                VALUES('ETHUSDT','connection',%s,%s,%s::jsonb)''',
                         (old+timedelta(seconds=idx),status,'{"session_id":"test-session"}'))
        conn.execute('''INSERT INTO feed_health(symbol,topic,ts,status)
            VALUES('ETHUSDT','diagnostic-expired',%s,'error')''',(old,))
        conn.execute('''INSERT INTO bot_events(ts,severity,event,data)
            VALUES(%s,'info','paper_trade_review_v1',%s::jsonb)''',
                     (old,'{"trade_id":"test","retention":"preserve"}'))
        conn.execute('''INSERT INTO bot_events(ts,severity,event,data)
            VALUES(%s,'warning','old-diagnostic','{}'::jsonb)''',(old,))
    report = cleanup_diagnostics(owned_b11)
    assert report['status']=='ok'
    assert report['cleanup_complete'] is False
    assert report['feed_health_deleted']==1
    assert report['bot_events_deleted']==1
    with connection(owned_b11) as conn:
        assert conn.execute('''SELECT count(*) AS n FROM feed_health
            WHERE symbol='ETHUSDT' AND topic='connection' ''').fetchone()['n']==3
        assert conn.execute('''SELECT count(*) AS n FROM bot_events
            WHERE event='paper_trade_review_v1' ''').fetchone()['n']==1
        assert conn.execute('''SELECT count(*) AS n FROM bot_events
            WHERE event='old-diagnostic' ''').fetchone()['n']==0


def test_timeout_before_ownership_select_does_not_hang_forever(owned_b11):
    with connection(owned_b11) as blocker:
        blocker.execute('LOCK TABLE eth_storage_identity IN ACCESS EXCLUSIVE MODE')
        with pytest.raises(psycopg.errors.LockNotAvailable):
            cleanup_diagnostics(owned_b11)
    # After release, schema identity remains intact; no destructive fallback.
    result=cleanup_diagnostics(owned_b11)
    assert result['status']=='ok'


def test_cleanup_lease_competing_worker_fails_before_delete(owned_b11):
    old=datetime(2020,1,1,tzinfo=timezone.utc)
    with connection(owned_b11) as conn:
        conn.execute('''INSERT INTO bot_events(ts,severity,event)
            VALUES(%s,'warning','unprocessed')''',(old,))
    with connection(owned_b11) as blocker:
        blocker.execute('''SELECT pg_advisory_xact_lock(hashtext(%s))''',
                        ('eth_tester:storage:diagnostic_cleanup',))
        with pytest.raises(RuntimeError,match='Another'):
            cleanup_diagnostics(owned_b11)
        assert blocker.execute('''SELECT count(*) AS n FROM bot_events
            WHERE event='unprocessed' ''').fetchone()['n']==1
    assert cleanup_diagnostics(owned_b11)['bot_events_deleted']==1
