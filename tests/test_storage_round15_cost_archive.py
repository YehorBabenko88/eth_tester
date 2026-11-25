import importlib.util
from datetime import date,datetime,timezone,timedelta
from decimal import Decimal as D
from pathlib import Path
import pytest

SRC=Path(__file__).resolve().parents[1]/'src'/'eth_tester'/'storage'
def load(name):
    spec=importlib.util.spec_from_file_location('B15_'+name,SRC/(name+'.py'))
    obj=importlib.util.module_from_spec(spec);spec.loader.exec_module(obj);return obj
cost=load('cost_receipts');archive=load('rehydration')

class Result:
    def __init__(self, row=None,rows=None):self.row=row;self.rows=rows or []
    def fetchone(self):return self.row
    def fetchall(self):return self.rows

class FakeCost:
    def __init__(self,rate_rows=(),status='closed',funding_verified=False,paid=D('0'),existing=None):
        self.sql=[];self.rows=list(rate_rows);self.recorded={};self.existing=existing
        self.opened=datetime(2026,11,25,12,tzinfo=timezone.utc)
        self.closed=self.opened+timedelta(hours=2)
        self.trade={'id':8,'symbol':'ETHUSDT','state':status,'direction':'long',
        'opened_at':self.opened,'closed_at':self.closed,'qty':D('2'),'notional':D('1000'),
        'funding_paid':paid,'fee_paid':D('1.1'),'net_pnl':D('4'),
        'funding_verified':funding_verified}
    def execute(self,stmt,args=None):
        self.sql.append((stmt,args)); q=' '.join(stmt.split())
        if 'FROM paper_trades WHERE id=' in q:return Result(row=self.trade)
        if 'FROM funding AS f LEFT JOIN LATERAL' in q:return Result(rows=self.rows)
        if 'INSERT INTO paper_cost_observation_receipts' in q:
            key=args[0]; import json
            self.recorded[key]=json.loads(args[-1]);return Result()
        if 'SELECT receipt_key,evidence FROM paper_cost_observation_receipts' in q:
            key=args[0];return Result(row={'receipt_key':key,'evidence':self.recorded.get(key)} if key in self.recorded else self.existing)
        return Result()

class FakeArchive:
    def __init__(self,sha='a'*64,state='complete',rows=4,receipts=None,bincount=3):
        self.sql=[];self.job={'state':state,'sha256':sha,'row_count':rows}
        self.receipts=[] if receipts is None else receipts;self.bincount=bincount
    def execute(self,q,args=None):
        self.sql.append((q,args));flat=' '.join(q.split())
        if 'FROM ingestion_job' in flat:return Result(row=self.job)
        if 'FROM archive_stage_receipts' in flat:return Result(rows=self.receipts)
        if 'FROM archive_source_price_bins' in flat:return Result(row={'total':self.bincount})
        return Result()

def mark(ts,rate='0.001',mark='100',id=10,receipt=None):
    return {'ts':ts,'rate':D(rate),'mark_price':None if mark is None else D(mark),
            'mark_event_id':None if mark is None else id,'mark_received_at':receipt or ts}

def test_zero_funding_events_never_claim_zero_settlement():
    db=FakeCost()
    result=cost.record_provisional_cost_observation(db,8)
    assert result['funding_events_observed']==0
    assert result['zero_event_claim_certified'] is False
    assert result['reserve_status']=='unbounded_no_certified_capital_release'
    assert not result['funding_verified'] and not result['ml_eligible']
    statement=next(iter(db.recorded.values()))
    assert statement['candidate_funding'] is None and statement['reserve_usd'] is None
    assert statement['missing_event_periods_verified_zero'] is False

def test_complete_marks_still_unverified_no_capital_promotion():
    db=FakeCost(rate_rows=[mark(datetime(2026,11,25,13,tzinfo=timezone.utc))],paid=D('0.2'))
    result=cost.record_provisional_cost_observation(db,8)
    assert result['funding_events_observed']==1 and not result['costs_certified']
    evidence=next(iter(db.recorded.values()))
    assert evidence['candidate_funding']=='0.200' and evidence['funding_matches_observed_rows'] is True
    assert evidence['official_schedule_verified'] is False and evidence['economics_verified'] is False

def test_missing_mark_never_zeroed():
    db=FakeCost(rate_rows=[mark(datetime(2026,11,25,13,tzinfo=timezone.utc),mark=None)])
    result=cost.record_provisional_cost_observation(db,8)
    assert result['marks_missing']==1
    assert next(iter(db.recorded.values()))['candidate_funding'] is None

def test_mismatched_observed_amount_still_provisional():
    db=FakeCost(rate_rows=[mark(datetime(2026,11,25,13,tzinfo=timezone.utc))],paid=D('0'))
    cost.record_provisional_cost_observation(db,8)
    assert next(iter(db.recorded.values()))['funding_matches_observed_rows'] is False

def test_oversized_cost_window_fails_before_insert():
    db=FakeCost(rate_rows=[mark(datetime(2026,11,25,13,tzinfo=timezone.utc))]*1001)
    with pytest.raises(RuntimeError,match='unbounded'):cost.record_provisional_cost_observation(db,8)
    assert not db.recorded

def test_future_asof_mark_fails_closed():
    db=FakeCost(rate_rows=[mark(datetime(2026,11,25,13,tzinfo=timezone.utc),receipt=datetime(2026,11,25,16,tzinfo=timezone.utc))])
    with pytest.raises(ValueError,match='future'):cost.record_provisional_cost_observation(db,8)

@pytest.mark.parametrize('trade_id',[0,False,1.5,'1',-10])
def test_invalid_trade_id_rejected(trade_id):
    with pytest.raises(ValueError):cost.record_provisional_cost_observation(FakeCost(),trade_id)

def test_open_trade_rejected_and_paper_ledger_never_updated():
    db=FakeCost(status='open')
    with pytest.raises(ValueError):cost.record_provisional_cost_observation(db,8)
    assert not any('UPDATE paper_' in q for q,_ in db.sql)

def test_verified_requires_separate_path():
    with pytest.raises(RuntimeError,match='separate'):cost.record_provisional_cost_observation(FakeCost(funding_verified=True),8)

def test_archive_missing_membership_succeeds_only_identical_sha():
    db=FakeArchive()
    proof=archive.require_original_archive_sha(db,'ETHUSDT',date(2026,11,25),'a'*64)
    assert proof['rehydration_needed'] and proof['original_sha256']=='a'*64
    assert proof['exchange_complete'] is False and proof['ml_eligible'] is False
    assert all('UPDATE' not in q and 'DELETE' not in q and 'INSERT' not in q for q,_ in db.sql)

def test_archive_changed_source_sha_rejected_before_receipt_query():
    db=FakeArchive()
    with pytest.raises(RuntimeError,match='differs'):archive.require_original_archive_sha(db,'ETHUSDT',date(2026,11,25),'b'*64)
    assert not any('archive_stage_receipts' in q for q,_ in db.sql)

def test_archive_full_membership_checked():
    row={'archive_sha256':'a'*64,'original_rows':4,'price_bins':3}
    db=FakeArchive(receipts=[row])
    proof=archive.require_original_archive_sha(db,'ETHUSDT',date(2026,11,25),'a'*64)
    assert proof['rehydration_needed'] is False

def test_archive_conflicting_second_source_rejected():
    row={'archive_sha256':'b'*64,'original_rows':4,'price_bins':3}
    with pytest.raises(RuntimeError,match='Conflicting'):archive.require_original_archive_sha(FakeArchive(receipts=[row]),'ETHUSDT',date(2026,11,25),'a'*64)

def test_archive_corrupt_bins_rejected():
    row={'archive_sha256':'a'*64,'original_rows':4,'price_bins':3}
    with pytest.raises(RuntimeError,match='incomplete'):archive.require_original_archive_sha(FakeArchive(receipts=[row],bincount=2),'ETHUSDT',date(2026,11,25),'a'*64)

def test_archive_completed_without_sha_rejected():
    with pytest.raises(RuntimeError,match='missing/invalid'):archive.require_original_archive_sha(FakeArchive(sha=None),'ETHUSDT',date(2026,11,25),'a'*64)

@pytest.mark.parametrize('bad',[date(2026,11,25).isoformat(),None,'INVALID'])
def test_archive_invalid_day_rejected(bad):
    with pytest.raises(ValueError):archive.require_original_archive_sha(FakeArchive(),'ETHUSDT',bad,'a'*64)
