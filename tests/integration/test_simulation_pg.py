"""Real PostgreSQL 17/18 disposable DB acceptance for Simulation E ledger.

Tests never contact exchanges or reuse a user's persistent database.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from eth_tester.config import Settings
from eth_tester.database import connection, init_database
from eth_tester.paper import close_paper, open_paper
from eth_tester.simulation.persistence import record_fill
from eth_tester.strategies import Parameters


pytestmark = pytest.mark.postgres


@pytest.fixture
def simulated_pg():
    admin_dsn = os.environ.get('ETH_TEST_PG_DSN')
    if not admin_dsn:
        pytest.skip('ETH_TEST_PG_DSN not set')
    name = 'eth_tester_sim_' + uuid4().hex[:12]
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    try:
        settings = Settings(_env_file=None, database_url=make_conninfo(admin_dsn, dbname=name))
        init_database(settings)
        with connection(settings) as conn:
            conn.execute('UPDATE bot_control SET enabled=true,paused=false WHERE singleton=true')
        yield settings
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))


def verified_reason(**extra):
    return {'strategy_version': Parameters().version,
            **Parameters().identity(), **extra}


def start_time():
    return datetime.now(timezone.utc) - timedelta(minutes=2)


def test_position_open_close_replay_and_cursor_version(simulated_pg):
    stamp = start_time()
    reason = verified_reason(pattern='qa')
    first = open_paper(simulated_pg, 'long', D(100), D(98), D(105),
                       reason, observed_at=stamp)
    with connection(simulated_pg) as conn:
        state = conn.execute('SELECT * FROM paper_position_state WHERE singleton=true').fetchone()
        assert state['paper_trade_id'] == first and state['version'] == 1
        rows = conn.execute('SELECT action,side,quality,qty FROM paper_order_events').fetchall()
        assert len(rows) == 1 and rows[0]['action'] == 'entry'
        assert rows[0]['side'] == 'Buy' and rows[0]['qty'] > 0
        assert rows[0]['quality'] == 'trade_print_proxy_no_liquidity'
        info = conn.execute('SELECT strategy_version,execution_quality FROM paper_trades WHERE id=%s',
                            (first,)).fetchone()
        assert info['strategy_version'] == 'eth_rules_v2'
        assert info['execution_quality'] == 'trade_print_proxy_no_liquidity'
    # Another simulated worker reconnects and must observe the persistent open state.
    with pytest.raises(RuntimeError):
        open_paper(simulated_pg, 'short', 100, 102, 95, reason,
                   observed_at=stamp + timedelta(seconds=1))
    assert close_paper(simulated_pg, 105, 'take', observed_at=stamp + timedelta(seconds=2),
                       expected_id=first) == first
    assert close_paper(simulated_pg, 105, 'duplicate', observed_at=stamp + timedelta(seconds=2),
                       expected_id=first) is None
    with connection(simulated_pg) as conn:
        state = conn.execute('SELECT * FROM paper_position_state WHERE singleton=true').fetchone()
        assert state['version'] == 2 and state['paper_trade_id'] is None
        events = conn.execute('SELECT action FROM paper_order_events ORDER BY event_ts').fetchall()
        assert [r['action'] for r in events] == ['entry', 'exit']


def test_event_conflict_rolls_back_without_advancing_cursor(simulated_pg):
    stamp = start_time()
    ident = open_paper(simulated_pg, 'short', 100, 102, 95,
                       verified_reason(), observed_at=stamp)
    with connection(simulated_pg) as conn:
        state = conn.execute('SELECT version,processed_cursor FROM paper_position_state').fetchone()
        original = conn.execute('SELECT * FROM paper_order_events').fetchone()
    with pytest.raises(psycopg.errors.UniqueViolation):
        with connection(simulated_pg) as conn:
            record_fill(conn, trade_id=ident, symbol='ETHUSDT',
                        event_id=original['event_id'], at=stamp,
                        observed_at=datetime.now(timezone.utc), side='Sell',
                        action='entry', qty=1, price=100, fee=0, slippage_cost=0)
    with connection(simulated_pg) as conn:
        new = conn.execute('SELECT version,processed_cursor FROM paper_position_state').fetchone()
        assert new == state
        assert conn.execute('SELECT count(*) AS n FROM paper_order_events').fetchone()['n'] == 1


def test_stale_settlement_rejected_and_open_unchanged(simulated_pg):
    stamp = start_time()
    ident = open_paper(simulated_pg, 'long', 100, 98, 105,
                       verified_reason(), observed_at=stamp)
    with pytest.raises(ValueError):
        close_paper(simulated_pg, 105, 'take',
                    observed_at=stamp-timedelta(seconds=1), expected_id=ident)
    with connection(simulated_pg) as conn:
        row = conn.execute('SELECT state FROM paper_trades WHERE id=%s', (ident,)).fetchone()
        state = conn.execute('SELECT version,paper_trade_id FROM paper_position_state').fetchone()
        assert row['state'] == 'open'
        assert state['version'] == 1 and state['paper_trade_id'] == ident


def test_funding_mark_breakdown_is_atomic(simulated_pg):
    stamp = start_time()
    ident = open_paper(simulated_pg, 'long', 100, 98, 105,
                       verified_reason(), observed_at=stamp)
    with connection(simulated_pg) as conn:
        qty = conn.execute('SELECT qty FROM paper_trades WHERE id=%s',
                           (ident,)).fetchone()['qty']
    event = {'ts': stamp + timedelta(seconds=20), 'rate': D('0.001'),
             'mark_price': D(100)}
    amount = qty * D('0.1')
    with pytest.raises(ValueError, match='breakdown'):
        close_paper(simulated_pg, 105, 'take', funding_paid=amount + D(1),
                    observed_at=stamp + timedelta(seconds=30), expected_id=ident,
                    funding_evidence=[event])
    with connection(simulated_pg) as conn:
        assert conn.execute('SELECT count(*) AS n FROM paper_funding_events').fetchone()['n'] == 0
        assert conn.execute('SELECT version FROM paper_position_state').fetchone()['version'] == 1
    assert close_paper(simulated_pg, 105, 'take', funding_paid=amount,
                       observed_at=stamp + timedelta(seconds=30), expected_id=ident,
                       funding_evidence=[event]) == ident
    with connection(simulated_pg) as conn:
        row = conn.execute('SELECT amount,mark_price,quality FROM paper_funding_events').fetchone()
        assert row['amount'] == amount and row['mark_price'] == D(100)
        assert 'unverified' in row['quality']
        assert conn.execute('SELECT version FROM paper_position_state').fetchone()['version'] == 2


def test_reject_unknown_new_strategy_but_close_legacy_open(simulated_pg):
    stamp=start_time()
    with pytest.raises(ValueError,match='identity'):
        open_paper(simulated_pg,'long',100,98,105,{'qa':True},observed_at=stamp)
    # Old open trade predates B migration 0005. Do not fabricate its entry.
    with connection(simulated_pg) as conn:
        old=conn.execute("""INSERT INTO paper_trades(
          symbol,state,direction,signal_ts,opened_at,entry,qty,notional,
          stop,take,reason,fee_paid,strategy_version)
          VALUES('ETHUSDT','open','long',%s,%s,100,10,1000,98,105,
                 '{"legacy":true}'::jsonb,0.55,'pivot_v1')
          RETURNING id""",(stamp,stamp)).fetchone()['id']
    assert close_paper(simulated_pg,105,'take',observed_at=stamp+timedelta(seconds=5),
                       expected_id=old)==old
    assert close_paper(simulated_pg,105,'duplicate',observed_at=stamp+timedelta(seconds=5),
                       expected_id=old) is None
    with connection(simulated_pg) as conn:
        state=conn.execute("SELECT paper_trade_id,version FROM paper_position_state").fetchone()
        events=conn.execute("SELECT action FROM paper_order_events WHERE paper_trade_id=%s",(old,)).fetchall()
        assert state['paper_trade_id'] is None and state['version']==1
        assert [r['action'] for r in events]==['exit']
