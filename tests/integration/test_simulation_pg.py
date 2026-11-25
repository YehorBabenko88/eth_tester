"""Real PostgreSQL 17/18 disposable DB acceptance for Simulation E ledger.

Tests never contact exchanges or reuse a user's persistent database.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import json
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
from eth_tester.strategies import Parameters, strategy_identity
from eth_tester.ml import FEATURE_NAMES
from eth_tester.simulation.identity import bind_snapshot_reference


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
    return {'strategy_version': strategy_identity(),
            'rules_version': Parameters().version,
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
        assert info['strategy_version'] == strategy_identity()
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
    # R11: a caller may NOT invent a funding rate/mark. Store the exact
    # disposable PostgreSQL market rows that the closing transaction will
    # independently re-read. Nothing here contacts an exchange.
    with connection(simulated_pg) as conn:
        conn.execute("""INSERT INTO funding(symbol,ts,rate)
           VALUES('ETHUSDT',%s,%s)""",(event['ts'],event['rate']))
        conn.execute("""INSERT INTO market_ticker_events(
           symbol,exchange_ts,event_type,mark_price,raw)
           VALUES('ETHUSDT',%s,'snapshot',%s,'{}'::jsonb)""",
           (event['ts'],event['mark_price']))
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


def test_atomic_exact_snapshot_binding_and_no_future_label(simulated_pg):
    """Disposable PG fixture proves E atomic identity, NOT market certification.

    The synthetic canonical row below must never be interpreted as an A/B/C
    production proof of completeness or executable quote liquidity.
    """
    bar=start_time().replace(second=0,microsecond=0)
    end=bar+timedelta(minutes=1)
    available=end+timedelta(seconds=10)
    computed=end+timedelta(seconds=20)
    decision=end+timedelta(seconds=40)
    key,checksum='a'*64,'b'*64
    evidence={'snapshot_key':key,'source_checksum':checksum,
              'contract':'eth_feature_snapshot_v2',
              'source_receipt_ms':int((end+timedelta(seconds=2)).timestamp()*1000),
              'source_checked_at_ms':int((end+timedelta(seconds=3)).timestamp()*1000),
              'available_at':available.isoformat(),
              'computed_at':computed.isoformat()}
    valid=verified_reason(
        source='closed_candles',signal_bar_ts=bar.isoformat(),
        orderflow={'admissible':True,'immutable_feature_snapshot':evidence})
    valid['market_trade_ts']=(decision-timedelta(seconds=2)).isoformat()
    bound=bind_snapshot_reference(valid,decision_at=decision,bar_ts=bar)
    checked_ms=int((end+timedelta(seconds=3)).timestamp()*1000)
    receipt_ms=int((end+timedelta(seconds=2)).timestamp()*1000)
    feature_ms=int((end+timedelta(seconds=4)).timestamp()*1000)
    source_ms=int(bar.timestamp()*1000)
    proof={'source':'trades_archive','instrument':'linear','interval':'',
           'checksum':checksum,'coverage_status':'verified',
           'evidence_ref':'synthetic_disposable_pg_fixture',
           'source_event_ms':source_ms,'source_checked_at_ms':checked_ms,
           'source_receipt_ms':receipt_ms}
    provenance={
        'contract':'eth_feature_snapshot_v2',
        'certified_by':'independent_source_proof',
        'source_coverage_verified':True,'coverage_status':'verified',
        'source_checked_at_ms':checked_ms,'source_receipt_ms':receipt_ms,
        'feature_sources':{name:dict(proof) for name in FEATURE_NAMES},
        'feature_available_ms':{name:feature_ms for name in FEATURE_NAMES},
        'fixture_only':True,
    }
    with connection(simulated_pg) as conn:
        conn.execute("""INSERT INTO feature_snapshots(
          snapshot_key,symbol,tf,bar_ts,available_at,observed_at,computed_at,
          source,source_checksum,quality,is_causal,values_json,provenance)
          VALUES(%s,'ETHUSDT','1',%s,%s,%s,%s,'fixture_only',%s,
                 'complete',true,'{}'::jsonb,%s::jsonb)""",
          (key,bar,available,computed,computed,checksum,json.dumps(provenance)))
    first=open_paper(simulated_pg,'long',100,98,105,bound,observed_at=decision)
    with connection(simulated_pg) as conn:
        trade=conn.execute("""SELECT signal_ts,reason FROM paper_trades
          WHERE id=%s""",(first,)).fetchone()
        state=conn.execute("SELECT paper_trade_id,version FROM paper_position_state").fetchone()
        assert trade['signal_ts']==decision
        assert trade['reason']['ml_snapshot_key']==key
        assert trade['reason']['ml_source_checksum']==checksum
        assert trade['reason']['ml_signal_decision_ms']==int(decision.timestamp()*1000)
        assert trade['reason']['market_trade_ts']==valid['market_trade_ts']
        assert 'ml_label' not in trade['reason']
        policy=trade['reason']['ml_label_policy']
        assert policy['anchor']=='first_full_bar_strictly_after_decision'
        assert policy['bar_interval_ms']==60000
        assert policy['horizon_bars']==3 and policy['barrier_bps']==20
        assert state['paper_trade_id']==first and state['version']==1
    assert close_paper(simulated_pg,105,'take',observed_at=decision+timedelta(seconds=1),
                       expected_id=first)==first


def test_missing_snapshot_rolls_back_without_paper_entry(simulated_pg):
    bar=start_time().replace(second=0,microsecond=0)
    end=bar+timedelta(minutes=1)
    available=end+timedelta(seconds=10)
    computed=end+timedelta(seconds=20)
    decision=end+timedelta(seconds=40)
    evidence={'snapshot_key':'a'*64,'source_checksum':'b'*64,
              'contract':'eth_feature_snapshot_v2',
              'source_receipt_ms':int((end+timedelta(seconds=2)).timestamp()*1000),
              'source_checked_at_ms':int((end+timedelta(seconds=3)).timestamp()*1000),
              'available_at':available.isoformat(),
              'computed_at':computed.isoformat()}
    reason=verified_reason(
        source='closed_candles',signal_bar_ts=bar.isoformat(),
        orderflow={'admissible':True,'immutable_feature_snapshot':evidence})
    bound=bind_snapshot_reference(reason,decision_at=decision,bar_ts=bar)
    with pytest.raises(ValueError,match='missing'):
        open_paper(simulated_pg,'long',100,98,105,bound,observed_at=decision)
    with connection(simulated_pg) as conn:
        assert conn.execute("SELECT count(*) AS n FROM paper_trades").fetchone()['n']==0
        assert conn.execute("SELECT count(*) AS n FROM paper_order_events").fetchone()['n']==0
        state=conn.execute("SELECT paper_trade_id,version FROM paper_position_state").fetchone()
        assert state['paper_trade_id'] is None and state['version']==0


def test_round6_paper_entry_and_strategy_decision_share_transaction(simulated_pg):
    """A mid-transaction crash leaves neither signal nor paper settlement."""
    stamp=start_time()
    identity=strategy_identity()
    reason=verified_reason(pattern='round6_transaction')
    with pytest.raises(RuntimeError,match='crash'):
        with connection(simulated_pg) as conn:
            conn.execute("""INSERT INTO strategy_signals(
              symbol,tf,bar_ts,version,decision,reason)
              VALUES('ETHUSDT','1',%s,%s,'candidate','{}'::jsonb)""",
              (stamp,identity))
            open_paper(simulated_pg,'long',100,98,105,reason,
                       observed_at=stamp,conn=conn)
            raise RuntimeError('crash before commit')
    with connection(simulated_pg) as conn:
        assert conn.execute("SELECT count(*) AS n FROM paper_trades").fetchone()['n']==0
        assert conn.execute("SELECT count(*) AS n FROM paper_order_events").fetchone()['n']==0
        assert conn.execute("SELECT count(*) AS n FROM strategy_signals").fetchone()['n']==0
        state=conn.execute("SELECT version,paper_trade_id FROM paper_position_state").fetchone()
        assert state['version']==0 and state['paper_trade_id'] is None
    # The same transaction can instead commit a terminal opened decision.
    with connection(simulated_pg) as conn:
        ident=open_paper(simulated_pg,'long',100,98,105,reason,
                         observed_at=stamp,conn=conn)
        conn.execute("""INSERT INTO strategy_signals(
          symbol,tf,bar_ts,version,decision,reason)
          VALUES('ETHUSDT','1',%s,%s,'opened',%s::jsonb)""",
          (stamp,identity,json.dumps({'paper_trade_id':ident})))
    with connection(simulated_pg) as conn:
        signals=conn.execute("SELECT decision,reason FROM strategy_signals").fetchall()
        assert len(signals)==1
        assert signals[0]['decision']=='opened'
        assert signals[0]['reason']['paper_trade_id']==ident
        assert conn.execute("SELECT count(*) AS n FROM paper_trades").fetchone()['n']==1
        assert conn.execute("SELECT count(*) AS n FROM paper_order_events").fetchone()['n']==1
    assert close_paper(simulated_pg,105,'take',
                       observed_at=stamp+timedelta(seconds=5),
                       expected_id=ident)==ident


def test_round6_savepoint_rolls_back_bad_entry_without_orphan(simulated_pg):
    """Paper admission rollback can coexist with a terminal strategy diagnostic."""
    stamp=start_time()
    with connection(simulated_pg) as conn:
        with pytest.raises(RuntimeError,match='synthetic failure'):
            with conn.transaction():
                open_paper(simulated_pg,'short',100,102,95,
                           verified_reason(),observed_at=stamp,conn=conn)
                raise RuntimeError('synthetic failure after entry')
        conn.execute("""INSERT INTO strategy_signals(
           symbol,tf,bar_ts,version,decision,reason)
           VALUES('ETHUSDT','1',%s,%s,'rejected_paper_admission',
                  '{"reason":"atomic_savepoint_rollback"}'::jsonb)""",
           (stamp,strategy_identity()))
    with connection(simulated_pg) as conn:
        assert conn.execute("SELECT count(*) AS n FROM paper_trades").fetchone()['n']==0
        assert conn.execute("SELECT count(*) AS n FROM paper_order_events").fetchone()['n']==0
        assert conn.execute("SELECT version FROM paper_position_state").fetchone()['version']==0
        row=conn.execute("SELECT decision FROM strategy_signals").fetchone()
        assert row['decision']=='rejected_paper_admission'


def test_round8_two_connection_terminal_conflict_rolls_back_trade(simulated_pg):
    """Connection B's terminal row wins after A's initial empty read.

    A must NOT commit a paper trade, fill event or cursor version if the
    competing final strategy_signals decision is already terminal. Real
    disposable PG, no internet and no claimed market-source completeness.
    """
    from eth_tester.runner import _persist_paper_decision
    stamp=start_time()
    version=strategy_identity()
    with pytest.raises(RuntimeError,match="atomic_signal_decision_conflict"):
        with connection(simulated_pg) as competing:
            assert competing.execute("""SELECT decision FROM strategy_signals
                WHERE symbol=%s AND tf='1' AND bar_ts=%s AND version=%s""",
                ("ETHUSDT",stamp,version)).fetchone() is None
            # Another worker commits a terminal decision after the above read.
            with connection(simulated_pg) as other:
                other.execute("""INSERT INTO strategy_signals(
                    symbol,tf,bar_ts,version,decision,reason)
                    VALUES('ETHUSDT','1',%s,%s,'no_signal','{}'::jsonb)""",
                    (stamp,version))
            # This QA-only direct paper fixture verifies the transactional
            # final signal guard; authentic live B proof is independently gated.
            paper_id=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                                verified_reason(),observed_at=stamp,
                                conn=competing)
            _persist_paper_decision(competing,simulated_pg,stamp,version,
                                     "opened",{"paper_trade_id":paper_id})
    with connection(simulated_pg) as conn:
        decision=conn.execute("SELECT decision FROM strategy_signals").fetchone()
        assert decision["decision"]=="no_signal"
        assert conn.execute("SELECT count(*) AS n FROM paper_trades").fetchone()["n"]==0
        assert conn.execute("SELECT count(*) AS n FROM paper_order_events").fetchone()["n"]==0
        state=conn.execute("SELECT version,paper_trade_id FROM paper_position_state").fetchone()
        assert state["version"]==0 and state["paper_trade_id"] is None


@pytest.mark.parametrize("overlaps_latest_window", [False, True])
def test_round8_historical_gap_preserved_but_only_current_overlap_blocks(
        simulated_pg, monkeypatch, overlaps_latest_window):
    """Do not silently 'resolve' old WS outages; healthy future bars can run."""
    from eth_tester import runner
    now=datetime.now(timezone.utc)
    last=now.replace(second=0,microsecond=0)-timedelta(minutes=1)
    with connection(simulated_pg) as conn:
        conn.execute("""INSERT INTO feed_health(symbol,topic,status)
           VALUES('ETHUSDT','connection','connected')""")
        conn.execute("""INSERT INTO trades(
            symbol,ts,trade_id,price,qty,side,notional,source)
            VALUES('ETHUSDT',%s,'e8-recent',100,1,'Buy',100,'websocket')""",
            (now-timedelta(seconds=1),))
        for i in range(35):
            ts=last-timedelta(minutes=34-i)
            conn.execute("""INSERT INTO candles(
                symbol,kind,tf,ts,open,high,low,close,volume,turnover,
                closed,source)
                VALUES('ETHUSDT','trade','1',%s,100,101,99,100,
                       1,100,true,'qa')""",(ts,))
        gap_start=(last if overlaps_latest_window
                   else now-timedelta(days=3))
        gap_end=(now if overlaps_latest_window
                 else now-timedelta(days=3,minutes=-1))
        conn.execute("""INSERT INTO data_gaps(
            source,symbol,start_ts,end_ts,explanation)
            VALUES('publicTrade','ETHUSDT',%s,%s,'synthetic_qagap')""",
            (gap_start,gap_end))
    monkeypatch.setattr(runner,"price_signal",lambda *args,**kw:None)
    expected=("blocked_unresolved_tick_gap" if overlaps_latest_window
              else "no_signal")
    assert runner.engine_tick(simulated_pg)==expected
    with connection(simulated_pg) as conn:
        gaps=conn.execute("""SELECT resolved_at FROM data_gaps
            WHERE explanation='synthetic_qagap'""").fetchall()
        assert len(gaps)==1 and gaps[0]["resolved_at"] is None


def test_round9_missing_ws_preserves_open_exposure_and_never_fills(simulated_pg):
    from eth_tester.runner import engine_tick
    opened=start_time()
    ident=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                     verified_reason(),observed_at=opened)
    assert engine_tick(simulated_pg)=="waiting_live_feed_open_position"
    with connection(simulated_pg) as conn:
        row=conn.execute("SELECT state,exit FROM paper_trades WHERE id=%s",
                         (ident,)).fetchone()
        assert row["state"]=="open" and row["exit"] is None
        assert conn.execute("SELECT count(*) AS n FROM paper_order_events").fetchone()["n"]==1
        assert conn.execute("SELECT paper_trade_id FROM paper_position_state").fetchone()["paper_trade_id"]==ident


def test_round9_asof_bid_ask_worsens_exit_without_fabricating_fill(simulated_pg):
    opened=start_time()
    ident=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                     verified_reason(),observed_at=opened)
    event=opened+timedelta(seconds=3)
    # Synthetic PG test-only DB ticker fixture; not an exchange receipt.
    with connection(simulated_pg) as conn:
        conn.execute("""INSERT INTO market_ticker_events(
            symbol,exchange_ts,received_ts,event_type,bid_price,ask_price,raw)
            VALUES('ETHUSDT',%s,%s,'snapshot',99.5,100.1,'{}'::jsonb)""",
            (event-timedelta(seconds=2),
             event-timedelta(milliseconds=150)))
    assert close_paper(simulated_pg,D(100),"manual",observed_at=event,
                       expected_id=ident)==ident
    with connection(simulated_pg) as conn:
        trade=conn.execute("SELECT exit,fee_paid,slippage_paid FROM paper_trades WHERE id=%s",
                           (ident,)).fetchone()
        fill=conn.execute("""SELECT quality,spread_cost,slippage_cost,
                         provenance FROM paper_order_events
                         WHERE paper_trade_id=%s AND action='exit'""",
                          (ident,)).fetchone()
        assert trade["exit"]<D("100")
        assert fill["quality"]=="observed_bbo_proxy_no_depth_or_executable_fill"
        assert fill["spread_cost"]>0 and fill["slippage_cost"]>0
        assert fill["provenance"]["exchange_fill_proven"] is False
        assert fill["provenance"]["paper_only"] is True
        assert trade["slippage_paid"]>0
    assert close_paper(simulated_pg,D(101),"duplicate",observed_at=event,
                       expected_id=ident) is None


def test_round9_recovery_review_is_durable_and_restart_idempotent(simulated_pg):
    from eth_tester.simulation.review import review_one_closed_trade
    stamp=start_time()
    ident=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                     verified_reason(),observed_at=stamp)
    close_paper(simulated_pg,D(102),"take",observed_at=stamp+timedelta(seconds=2),
                expected_id=ident)
    assert review_one_closed_trade(simulated_pg)=="review_recorded"
    assert review_one_closed_trade(simulated_pg)=="review_up_to_date"
    with connection(simulated_pg) as conn:
        reviews=conn.execute("""SELECT data FROM bot_events
           WHERE event='paper_trade_review_v1'""").fetchall()
        assert len(reviews)==1
        data=reviews[0]["data"]
        assert data["trade_id"]==ident
        assert data["paper_only"] is True and data["ml_eligible"] is False
        assert data["exchange_fill_proven"] is False
        assert data["drawdown_quality"]=="realized_closed_paper_only"
        assert data["observed_quality"] in (
            "unavailable_no_observed_prints",
            "observed_websocket_partial_not_exchange_complete")


def test_round9_research_never_certifies_excursions_over_tick_gap(simulated_pg):
    from eth_tester.simulation.review import review_one_closed_trade
    stamp=start_time()
    ident=open_paper(simulated_pg,"short",D(100),D(102),D(95),
                     verified_reason(),observed_at=stamp)
    close_paper(simulated_pg,D(101),"stop",observed_at=stamp+timedelta(seconds=4),
                expected_id=ident)
    with connection(simulated_pg) as conn:
        conn.execute("""INSERT INTO data_gaps(source,symbol,start_ts,
            end_ts,explanation) VALUES('publicTrade','ETHUSDT',%s,%s,
            'injected_round9_research_gap')""",
            (stamp+timedelta(seconds=1),stamp+timedelta(seconds=3)))
    assert review_one_closed_trade(simulated_pg)=="review_recorded"
    with connection(simulated_pg) as conn:
        data=conn.execute("""SELECT data FROM bot_events
            WHERE event='paper_trade_review_v1'""").fetchone()["data"]
        assert data["observed_quality"]=="unavailable_gap_or_over_24h_window"
        assert data["mae_fraction"] is None and data["mfe_fraction"] is None


def test_round9_research_insert_crash_rolls_back_and_retries(simulated_pg):
    from eth_tester.simulation.review import review_one_closed_trade
    stamp=start_time()
    ident=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                     verified_reason(),observed_at=stamp)
    close_paper(simulated_pg,D(102),"take",observed_at=stamp+timedelta(seconds=2),
                expected_id=ident)
    with connection(simulated_pg) as conn:
        conn.execute("""CREATE FUNCTION qa_review_crash() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
               IF NEW.event='paper_trade_review_v1'
               THEN RAISE EXCEPTION 'synthetic review commit crash';
               END IF;
               RETURN NEW;
             END; $$;""")
        conn.execute("""CREATE TRIGGER qa_review_crash_trigger
             BEFORE INSERT ON bot_events FOR EACH ROW
             EXECUTE FUNCTION qa_review_crash()""")
    with pytest.raises(psycopg.Error,match="synthetic review commit crash"):
        review_one_closed_trade(simulated_pg)
    with connection(simulated_pg) as conn:
        assert conn.execute("""SELECT count(*) AS n FROM bot_events
             WHERE event='paper_trade_review_v1'""").fetchone()["n"]==0
        assert conn.execute("""SELECT state FROM paper_trades
             WHERE id=%s""",(ident,)).fetchone()["state"]=="closed"
        conn.execute("DROP TRIGGER qa_review_crash_trigger ON bot_events")
        conn.execute("DROP FUNCTION qa_review_crash()")
    assert review_one_closed_trade(simulated_pg)=="review_recorded"
    assert review_one_closed_trade(simulated_pg)=="review_up_to_date"


def test_round9_pause_blocks_new_entry_but_not_verified_existing_exit(simulated_pg):
    from eth_tester.runner import engine_tick
    stamp=start_time()
    ident=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                     verified_reason(),observed_at=stamp)
    with connection(simulated_pg) as conn:
        conn.execute("UPDATE bot_control SET paused=true WHERE singleton=true")
        conn.execute("""INSERT INTO feed_health(symbol,topic,status)
            VALUES('ETHUSDT','connection','connected')""")
        conn.execute("""INSERT INTO trades(
            symbol,ts,trade_id,price,qty,side,notional,source)
            VALUES('ETHUSDT',clock_timestamp()-interval '1 second',
                   'e9-confirmed-stop',96,1,'Sell',96,'websocket')""")
    assert engine_tick(simulated_pg)==f"closed_{ident}"
    with connection(simulated_pg) as conn:
        assert conn.execute("SELECT paused FROM bot_control").fetchone()["paused"] is True
        trade=conn.execute("SELECT state,exit_reason FROM paper_trades WHERE id=%s",
                           (ident,)).fetchone()
        assert trade["state"]=="closed"
        assert trade["exit_reason"].startswith("stop")
        assert conn.execute("""SELECT count(*) AS n FROM paper_order_events
            WHERE paper_trade_id=%s AND action='exit'""",(ident,)).fetchone()["n"]==1
    assert engine_tick(simulated_pg)=="paused"


def test_round9_entry_costs_are_frozen_across_service_config_changes(simulated_pg):
    """Restart/config drift cannot rewrite the entry's taker-fee/slippage."""
    at=start_time()
    ident=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                     verified_reason(),observed_at=at)
    with connection(simulated_pg) as conn:
        reason=conn.execute("SELECT reason FROM paper_trades WHERE id=%s",
                            (ident,)).fetchone()["reason"]
        assert reason["paper_cost_policy"]["version"]=="paper_taker_cost_v1"
        assert D(reason["paper_cost_policy"]["taker_fee_rate"])==simulated_pg.taker_fee_rate
    mutated=simulated_pg.model_copy(update={
        "taker_fee_rate":D(".02"),"slippage_bps":D("100")})
    close_paper(mutated,D(101),"manual",observed_at=at+timedelta(seconds=2),
                expected_id=ident)
    with connection(simulated_pg) as conn:
        outcome=conn.execute("SELECT exit,fee_paid FROM paper_trades WHERE id=%s",
                            (ident,)).fetchone()
        assert outcome["exit"]>D("100")
        assert outcome["exit"]<D("101")
        assert outcome["fee_paid"]<D("5")
        provenance=conn.execute("""SELECT provenance FROM paper_order_events
           WHERE paper_trade_id=%s AND action='exit'""",(ident,)).fetchone()["provenance"]
        assert provenance["fee_quality"]=="entry_time_cost_policy_pinned"


def test_round11_forged_funding_rejected_with_atomic_no_exit(simulated_pg):
    """Caller-provided event JSON must correspond to original DB sources."""
    stamp=start_time()
    ident=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                     verified_reason(),observed_at=stamp)
    with connection(simulated_pg) as conn:
        qty=conn.execute("SELECT qty FROM paper_trades WHERE id=%s",
                         (ident,)).fetchone()["qty"]
    imaginary={"ts":stamp+timedelta(seconds=20),
               "rate":D(".002"),"mark_price":D("100")}
    with pytest.raises(ValueError,match="original DB"):
        close_paper(simulated_pg,D(105),"take",
                    funding_paid=qty*D(".2"),
                    observed_at=stamp+timedelta(seconds=25),
                    expected_id=ident,funding_evidence=[imaginary])
    with connection(simulated_pg) as conn:
        assert conn.execute("SELECT state FROM paper_trades WHERE id=%s",
                            (ident,)).fetchone()["state"]=="open"
        assert conn.execute("SELECT count(*) AS n FROM paper_order_events"
                           ).fetchone()["n"]==1
        assert conn.execute("SELECT version FROM paper_position_state"
                           ).fetchone()["version"]==1
    # No funding coverage proof: close safely and label provisional.
    assert close_paper(simulated_pg,D(105),"manual",
                       funding_paid=D(0),
                       observed_at=stamp+timedelta(seconds=25),
                       expected_id=ident)==ident
    with connection(simulated_pg) as conn:
        trade=conn.execute("""SELECT funding_verified,net_pnl,state
              FROM paper_trades WHERE id=%s""",(ident,)).fetchone()
        assert trade["funding_verified"] is False and trade["state"]=="closed"
        prov=conn.execute("""SELECT provenance FROM paper_order_events
             WHERE paper_trade_id=%s AND action='exit'""",
             (ident,)).fetchone()["provenance"]
        assert prov["funding_schedule_complete"] is False
        assert prov["funding_quality"]=="provisional_missing_funding_completeness"
        assert prov["net_pnl_quality"]=="provisional_paper_only"


def test_round11_canonical_funding_rate_mark_mismatch_fails_closed(simulated_pg):
    stamp=start_time()
    ident=open_paper(simulated_pg,"short",D(100),D(102),D(95),
                     verified_reason(),observed_at=stamp)
    rate_at=stamp+timedelta(seconds=5)
    with connection(simulated_pg) as conn:
        conn.execute("""INSERT INTO funding(symbol,ts,rate)
             VALUES('ETHUSDT',%s,.001)""",(rate_at,))
        conn.execute("""INSERT INTO market_ticker_events(
             symbol,exchange_ts,event_type,mark_price,raw)
             VALUES('ETHUSDT',%s,'snapshot',100,'{}'::jsonb)""",
             (rate_at,))
        qty=conn.execute("SELECT qty FROM paper_trades WHERE id=%s",
                         (ident,)).fetchone()["qty"]
    forged={"ts":rate_at,"rate":D(".002"),"mark_price":D("100")}
    with pytest.raises(ValueError,match="original DB"):
        close_paper(simulated_pg,D(99),"manual",funding_paid=-qty*D(".2"),
                    observed_at=rate_at+timedelta(seconds=1),
                    expected_id=ident,funding_evidence=[forged])
    correct={"ts":rate_at,"rate":D(".001"),"mark_price":D("100")}
    assert close_paper(simulated_pg,D(99),"manual",funding_paid=-qty*D(".1"),
                    observed_at=rate_at+timedelta(seconds=1),
                    expected_id=ident,funding_evidence=[correct])==ident
    with connection(simulated_pg) as conn:
        exit=conn.execute("""SELECT provenance FROM paper_order_events
             WHERE paper_trade_id=%s AND action='exit'""",
             (ident,)).fetchone()["provenance"]
        assert exit["funding_source_db_rechecked"] is True
        assert exit["funding_quality"]=="provisional_canonical_marks_schedule_unproven"
        assert exit["funding_schedule_complete"] is False


def test_round11_exit_source_gap_blocks_atomic_settlement(simulated_pg):
    """A source writer may publish a disconnect/gap before E acquires fence."""
    stamp=start_time()
    ident=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                     verified_reason(),observed_at=stamp)
    hit=stamp+timedelta(seconds=5)
    with connection(simulated_pg) as conn:
        conn.execute("""INSERT INTO feed_health(symbol,topic,status)
              VALUES('ETHUSDT','connection','connected')""")
        conn.execute("""INSERT INTO trades(
              symbol,ts,trade_id,price,qty,side,notional,source)
              VALUES('ETHUSDT',%s,'round11-exit-cross',96,1,'Sell',96,'websocket')""",
              (hit,))
        conn.execute("""INSERT INTO data_gaps(source,symbol,start_ts,end_ts,
              explanation) VALUES('publicTrade','ETHUSDT',%s,%s,
              'r11_between_first_read_and_exit_tx')""",
              (stamp+timedelta(seconds=1),hit+timedelta(seconds=1)))
    with pytest.raises(RuntimeError,match="paper_exit_source_gap"):
        close_paper(simulated_pg,D(96),"stop",observed_at=hit,
                    expected_id=ident,require_live_crossing=True)
    with connection(simulated_pg) as conn:
        assert conn.execute("SELECT state FROM paper_trades WHERE id=%s",
                            (ident,)).fetchone()["state"]=="open"
        assert conn.execute("SELECT version FROM paper_position_state"
                           ).fetchone()["version"]==1
        assert conn.execute("""SELECT count(*) AS n FROM paper_order_events
            WHERE paper_trade_id=%s""",(ident,)).fetchone()["n"]==1
        assert conn.execute("SELECT realized_pnl FROM paper_account WHERE id=1"
                           ).fetchone()["realized_pnl"]==0


def test_round11_exit_requires_first_persisted_crossing(simulated_pg):
    stamp=start_time()
    ident=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                     verified_reason(),observed_at=stamp)
    first=stamp+timedelta(seconds=3)
    with connection(simulated_pg) as conn:
        conn.execute("""INSERT INTO feed_health(symbol,topic,status)
                VALUES('ETHUSDT','connection','connected')""")
        conn.execute("""INSERT INTO trades(
              symbol,ts,trade_id,price,qty,side,notional,source)
              VALUES('ETHUSDT',%s,'r11-first-stop',97,1,'Sell',97,'websocket')""",
              (first,))
        conn.execute("""INSERT INTO trades(
              symbol,ts,trade_id,price,qty,side,notional,source)
              VALUES('ETHUSDT',%s,'r11-late-take',108,1,'Buy',108,'websocket')""",
              (first+timedelta(seconds=1),))
    with pytest.raises(RuntimeError,match="crossing_source_changed"):
        close_paper(simulated_pg,D(105),"take",
                    observed_at=first+timedelta(seconds=1),
                    expected_id=ident,require_live_crossing=True)
    assert close_paper(simulated_pg,D(97),"stop",observed_at=first,
                       expected_id=ident,require_live_crossing=True)==ident


def test_round11_review_drawdown_cannot_see_later_closed_trade(simulated_pg):
    from eth_tester.simulation.review import review_one_closed_trade
    stamp=start_time()
    first=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                     verified_reason(),observed_at=stamp)
    close_paper(simulated_pg,D(105),"take",
                observed_at=stamp+timedelta(seconds=2),expected_id=first)
    second=open_paper(simulated_pg,"short",D(100),D(102),D(95),
                      verified_reason(),observed_at=stamp+timedelta(seconds=3))
    close_paper(simulated_pg,D(102),"stop",
                observed_at=stamp+timedelta(seconds=4),expected_id=second)
    assert review_one_closed_trade(simulated_pg)=="review_recorded"
    with connection(simulated_pg) as conn:
        review=conn.execute("""SELECT data FROM bot_events
               WHERE event='paper_trade_review_v1'""").fetchone()["data"]
        assert review["trade_id"]==first
        assert D(review["max_realized_drawdown_fraction"])==0
        assert review["realized_drawdown_asof"]==(
            stamp+timedelta(seconds=2)).isoformat()
        assert review["retention"]=="preserve"
        assert review["net_pnl_certified"] is False


def test_round11_source_fence_concurrent_gap_before_exit(simulated_pg):
    """Two actual PG transactions, serialized by the exact A/E lock name."""
    from concurrent.futures import ThreadPoolExecutor
    stamp=start_time()
    ident=open_paper(simulated_pg,"long",D(100),D(98),D(105),
                     verified_reason(),observed_at=stamp)
    hit=stamp+timedelta(seconds=4)
    with connection(simulated_pg) as conn:
        conn.execute("""INSERT INTO feed_health(symbol,topic,status)
             VALUES('ETHUSDT','connection','connected')""")
        conn.execute("""INSERT INTO trades(
             symbol,ts,trade_id,price,qty,side,notional,source)
             VALUES('ETHUSDT',%s,'r11-fenced-stop',96,1,'Sell',96,'websocket')""",
             (hit,))
    # E starts after source writer has acquired lock. When A commits its gap
    # and unlocks, E MUST see it and refuse the already-selected crossing.
    with ThreadPoolExecutor(max_workers=1) as pool:
        with connection(simulated_pg) as writer:
            writer.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                           ("eth_paper_admission_source:ETHUSDT",))
            waiting=pool.submit(close_paper,simulated_pg,D(96),"stop",D(0),
                               hit,ident,require_live_crossing=True)
            writer.execute("""INSERT INTO data_gaps(source,symbol,start_ts,
              end_ts,explanation) VALUES('publicTrade','ETHUSDT',%s,%s,
              'r11_concurrent_gap_under_source_fence')""",
              (stamp+timedelta(seconds=1),hit+timedelta(seconds=1)))
        with pytest.raises(RuntimeError,match="paper_exit_source_gap"):
            waiting.result(timeout=15)
    with connection(simulated_pg) as conn:
        assert conn.execute("SELECT state FROM paper_trades WHERE id=%s",
                            (ident,)).fetchone()["state"]=="open"
        assert conn.execute("SELECT version FROM paper_position_state"
                           ).fetchone()["version"]==1


def test_round12_pg_two_connections_no_source_global_cycle(simulated_pg):
    """Two real PG connections: while exit owns global, admission must not
    take the source lease and then block waiting for the exit's global lease.
    A failed acquisition leaves the original signal eligible for future
    prospective retry, never a terminal paper decision.
    """
    from eth_tester.runner import _try_paper_admission_mutex
    source="eth_paper_admission_source:ETHUSDT"
    with connection(simulated_pg) as closer:
        closer.execute("SELECT pg_advisory_xact_lock(20991991)")
        with connection(simulated_pg) as opener:
            opener.execute("SET LOCAL lock_timeout = '300ms'")
            assert _try_paper_admission_mutex(opener) is False
            # The closer is free to acquire the exact shared source lock:
            # the losing admission did not acquire source first.
            row=closer.execute("""SELECT pg_try_advisory_xact_lock(
                hashtext(%s)) AS acquired""",(source,)).fetchone()
            assert row["acquired"] is True
    with connection(simulated_pg) as fresh:
        assert _try_paper_admission_mutex(fresh) is True


def test_round12_pg_source_wait_times_out_and_global_recovers(simulated_pg):
    """An A/C source writer holding its source lease cannot hang E
    indefinitely. When B/source lock waits beyond 200ms, PostgreSQL aborts
    that uncommitted paper admission and releases its global transaction lock.
    """
    from eth_tester.runner import _try_paper_admission_mutex
    source="eth_paper_admission_source:ETHUSDT"
    with connection(simulated_pg) as source_writer:
        source_writer.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                              (source,))
        with pytest.raises(psycopg.errors.LockNotAvailable):
            with connection(simulated_pg) as opener:
                opener.execute("SET LOCAL lock_timeout = '200ms'")
                assert _try_paper_admission_mutex(opener) is True
                opener.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                               (source,))
        # An aborted opener cannot poison the global mutex.
        with connection(simulated_pg) as closer:
            assert _try_paper_admission_mutex(closer) is True
