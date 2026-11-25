"""Round15 B source/SQL integration: UUID dedicated disposable DB only.

Authored for real PG17/18 CI. Never run against user database without explicit
ETH_TEST_PG_DSN and never assert market completeness from synthetic receipts.
"""
import os
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal as D
from uuid import uuid4
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.storage.cost_receipts import record_provisional_cost_observation
from eth_tester.storage.rehydration import require_original_archive_sha

pytestmark = pytest.mark.postgres

@pytest.fixture
def b15_db(monkeypatch):
    admin_dsn=os.getenv('ETH_TEST_PG_DSN')
    if not admin_dsn:
        pytest.skip('Real disposable PostgreSQL requires ETH_TEST_PG_DSN')
    monkeypatch.delenv('ETH_TESTER_OWNED_TEMP_DIR',raising=False)
    name='eth_tester_b15_'+uuid4().hex[:12]
    settings=Settings(_env_file=None,database_url=make_conninfo(admin_dsn,dbname=name))
    with psycopg.connect(make_conninfo(admin_dsn,dbname='postgres'),autocommit=True) as admin:
        if admin.execute('SELECT 1 FROM pg_database WHERE datname=%s',(name,)).fetchone():
            raise RuntimeError('Unexpected existing disposable database name')
    try:
        init_database(settings)
        yield settings
    finally:
        # The unique name was confirmed absent before init; even partial
        # init_database failures must be cleaned, never touching another DB.
        with psycopg.connect(make_conninfo(admin_dsn,dbname='postgres'),autocommit=True) as admin:
            if admin.execute('SELECT 1 FROM pg_database WHERE datname=%s',(name,)).fetchone():
                admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))

def _closed_trade(conn):
    t=datetime(2026,11,24,12,tzinfo=timezone.utc)
    return conn.execute('''INSERT INTO paper_trades(
        symbol,state,direction,signal_ts,opened_at,closed_at,entry,exit,
        qty,notional,stop,take,reason,strategy_version,funding_verified,
        funding_paid,fee_paid,net_pnl)
        VALUES('ETHUSDT','closed','long',%s,%s,%s,100,101,2,1000,98,105,
               '{}'::jsonb,'qa_storage_b15',false,0,1.1,0)
        RETURNING id''',(t,t,t+timedelta(hours=2))).fetchone()['id']

def test_provisional_zero_observations_append_only(b15_db):
    with connection(b15_db) as conn:
        ident=_closed_trade(conn)
    with connection(b15_db) as conn:
        first=record_provisional_cost_observation(conn,ident)
        second=record_provisional_cost_observation(conn,ident)
        assert first==second and first['funding_events_observed']==0
        row=conn.execute('''SELECT candidate_funding,schedule_complete,
            funding_verified,economics_verified,ml_eligible,reserve_usd,
            evidence,observed_at FROM paper_cost_observation_receipts
            WHERE receipt_key=%s''',(first['receipt_key'],)).fetchone()
        assert row['candidate_funding'] is None
        assert row['funding_verified'] is False
        assert row['economics_verified'] is False and row['ml_eligible'] is False
        assert row['reserve_usd'] is None
        assert row['evidence']['missing_event_periods_verified_zero'] is False
        assert conn.execute('SELECT count(*) AS n FROM paper_cost_observation_receipts').fetchone()['n']==1
    with pytest.raises(psycopg.Error):
        with connection(b15_db) as conn:
            conn.execute('''UPDATE paper_cost_observation_receipts
                SET recorded_net=123 WHERE receipt_key=%s''',(first['receipt_key'],))

def test_same_sha_archive_rehydration_readonly(b15_db):
    day=date(2026,11,24)
    original='a'*64
    with connection(b15_db) as conn:
        conn.execute('''INSERT INTO ingestion_job(source,symbol,day,state,row_count,sha256)
            VALUES('trades_archive','ETHUSDT',%s,'complete',4,%s)''',(day,original))
        ok=require_original_archive_sha(conn,'ETHUSDT',day,original)
        assert ok['rehydration_needed'] and ok['original_sha256']==original
        with pytest.raises(RuntimeError,match='differs'):
            require_original_archive_sha(conn,'ETHUSDT',day,'b'*64)
        assert conn.execute('''SELECT sha256 FROM ingestion_job
            WHERE source='trades_archive' AND symbol='ETHUSDT' AND day=%s''',(day,)).fetchone()['sha256']==original

def test_observed_cost_mark_asof_still_not_certified(b15_db):
    with connection(b15_db) as conn:
        ident=_closed_trade(conn)
        t=datetime(2026,11,24,13,tzinfo=timezone.utc)
        conn.execute('''INSERT INTO funding(symbol,ts,rate)
            VALUES('ETHUSDT',%s,0.001)''',(t,))
        conn.execute('''INSERT INTO market_ticker_events(
            symbol,exchange_ts,received_ts,mark_price,event_type,raw)
            VALUES('ETHUSDT',%s,%s,100,'ticker','{}'::jsonb)''',(t,t,))
        status=record_provisional_cost_observation(conn,ident)
        assert status['funding_events_observed']==1
        assert status['marks_missing']==0
        assert status['funding_verified'] is False and status['ml_eligible'] is False
        assert conn.execute('''SELECT funding_verified FROM paper_trades
             WHERE id=%s''',(ident,)).fetchone()['funding_verified'] is False
