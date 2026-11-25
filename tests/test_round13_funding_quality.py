"""F R13: refuse provisional PAPER economics in certified-research path.

Disposable fake contracts only. No REST/WS, orders, or actual PostgreSQL.
"""
from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal
from types import ModuleType, SimpleNamespace
import sys
import pytest
from eth_tester.ml import extract

CUTOFF=1_800_000_000_000

@pytest.fixture
def verified_structural_label(monkeypatch):
    contract=ModuleType('eth_tester.ml.contract')
    calls=[]
    contract.milliseconds=lambda x:x
    contract.validate_snapshot=lambda record,reason:(tuple([.1]*9),tuple([1000]*9),2000)
    def label(record,reason,cutoff,closed,trade_id):
        calls.append(trade_id)
        return ('breakout',4000)
    contract.validate_label=label
    monkeypatch.setitem(sys.modules,'eth_tester.ml.contract',contract)
    return calls


def record():
    return {
       'paper_trade_id':21,
       'reason':{'paper_cost_policy':{'version':'paper_taker_cost_v1',
                                      'notional_usd':'100.00'},
                 'net_pnl_certified':True},
       'closed_at':3000,'notional':Decimal('100.0'),
       'gross_pnl':Decimal('4.00'), 'fees':Decimal('0.50'),
       'funding':Decimal('0.00'),'net_pnl':Decimal('3.50'),
       'funding_verified':True,
       'execution_quality':'trade_print_proxy_no_liquidity',
       'execution_events':[
           {'action':'entry','fee':'0.20','source':'paper_proxy',
            'quality':'trade_print_proxy_no_liquidity','provenance':{'reason':'synthetic'}},
           {'action':'exit','fee':'0.30','source':'paper_proxy',
            'quality':'trade_print_proxy_no_liquidity','provenance':{
             'fee_quality':'entry_time_cost_policy_pinned',
             'funding_source_db_rechecked':True,
             'funding_schedule_complete':True}}
       ]
    }


def decode(rows):
    return extract.from_snapshot_records(rows,CUTOFF)


def test_structural_label_is_checked_but_provisional_not_promoted(verified_structural_label):
    r=record()
    r['funding_verified']=False
    accepted,rejected=decode([r])
    assert accepted==[]
    assert rejected=={'provisional_paper_economics':1}
    assert verified_structural_label==[21]  # separate B label remains untouched


def test_e_round11_provisional_exit_blocks_even_with_funding_flag(verified_structural_label):
    r=record()
    r['execution_events'][1]['provenance']['funding_schedule_complete']=False
    accepted,rejected=decode([r])
    assert accepted==[] and rejected=={'provisional_paper_economics':1}

@pytest.mark.parametrize('mutate',[
    lambda r:r.update(execution_events=[]),
    lambda r:r['execution_events'].pop(),
    lambda r:r['execution_events'].append(r['execution_events'][-1]),
    lambda r:r['execution_events'].__setitem__(1, r['execution_events'][0]),
    lambda r:r['execution_events'][0].update(fee='0.60'),
    lambda r:r['execution_events'][0].update(fee='NaN'),
    lambda r:r['execution_events'][0].update(fee='-0.20'),
    lambda r:r['execution_events'][1].update(source='exchange'),
    lambda r:r['execution_events'][1].update(quality=''),
    lambda r:r['execution_events'][1].update(quality='unverified'),
    lambda r:r.update(execution_quality='unknown'),
    lambda r:r['execution_events'][1].update(provenance='untrusted'),
    lambda r:r['reason'].pop('paper_cost_policy'),
    lambda r:r['reason']['paper_cost_policy'].update(notional_usd='200'),
])
def test_missing_or_tampered_fee_receipt_blocks_money_label(verified_structural_label,mutate):
    r=record()
    mutate(r)
    accepted,rejected=decode([r])
    assert accepted==[] and rejected=={'incomplete_paper_fee_evidence':1}


def test_complete_explicit_synthetic_evidence_only_allows_observational_paper(verified_structural_label):
    accepted,rejected=decode([record()])
    assert rejected=={} and len(accepted)==1
    assert accepted[0].net_return==pytest.approx(.035)
    assert accepted[0].event_id=='paper:21'


def test_bulk_provisional_does_not_create_research_cohort(verified_structural_label):
    rows=[]
    for i in range(300):
        r=record()
        r['paper_trade_id']=i+1
        r['funding_verified']=False
        rows.append(r)
    accepted,rejected=decode(rows)
    assert not accepted and rejected=={'provisional_paper_economics':300}


def test_no_auto_approval_on_missing_economic_cohort(verified_structural_label,monkeypatch):
    from eth_tester import learning
    monkeypatch.setattr(extract,'load_causal_feature_examples',lambda *a,**kw:decode([
      {**record(),'funding_verified':False}]))
    # learning imports load_causal_feature_examples from extract at invocation.
    out=learning.train_causal_shadow(SimpleNamespace(symbol='ETHUSDT'),CUTOFF,min_rows=180)
    assert out['status']=='insufficient_new_verified_data'
    assert out['approved'] is False
    assert out['usable']==0
    assert out['rejections']=={'provisional_paper_economics':1}


def test_sql_requires_ordered_bounded_ledger_plus_funding_state(monkeypatch):
    class Response:
        def __init__(self,rows=None,row=None):self.rows=rows or [];self.row=row
        def fetchall(self):return self.rows
        def fetchone(self):return self.row
    class DB:
        def __init__(self):self.statements=[]
        @contextmanager
        def connection(self,settings):yield self
        def execute(self,query,params=None):
            self.statements.append((query,params))
            if 'to_regclass' in query:return Response(row={'installed':True})
            return Response()
    db=DB()
    monkeypatch.setattr(extract,'connection',db.connection)
    selected,_=extract.load_causal_feature_examples(SimpleNamespace(symbol='ETHUSDT'),CUTOFF,limit=100)
    assert selected==[]
    sql=' '.join(q for q,_ in db.statements)
    assert 'REPEATABLE READ READ ONLY' in sql
    assert 'p.funding_verified' in sql
    assert 'paper_order_events' in sql
    assert "'fee',ev.fee" in sql
    assert 'LIMIT 3' in sql
    assert 'LEFT JOIN paper_ml_labels' in sql
    assert 'WHERE p.symbol=%s' in sql


def test_malicious_financial_reason_cannot_override_false_db_state(verified_structural_label):
    r=record()
    r['reason']['funding_verified']=True
    r['reason']['funding_schedule_complete']=True
    r['reason']['net_pnl_certified']=True
    r['funding_verified']=False
    accepted,rejected=decode([r])
    assert accepted==[] and rejected=={'provisional_paper_economics':1}


def test_correlated_fee_read_sql_has_balanced_subquery_and_case():
    import ast
    import re
    from pathlib import Path
    tree=ast.parse(Path(extract.__file__).read_text())
    queries=[n.value for n in ast.walk(tree)
             if isinstance(n,ast.Constant) and isinstance(n.value,str)
             and n.value.startswith('SELECT p.id AS paper_trade_id')]
    assert len(queries)==1
    sql=queries[0]
    assert "LIMIT 3) ev\n                      )\n                      ELSE" in sql
    without_literals=re.sub(r"'(?:''|[^'])*'", "''", sql)
    depth=0
    for ch in without_literals:
        if ch=='(':depth+=1
        if ch==')':depth-=1
        assert depth>=0
    assert depth==0
