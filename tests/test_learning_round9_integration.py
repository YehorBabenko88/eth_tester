"""Offline fault-injected F scheduler/registry integration; no real PG claims."""
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
import json
import pytest

from eth_tester import learning
from eth_tester.ml import fit
from eth_tester.ml import extract, lifecycle
from eth_tester.ml.research import historical_shadow_report

from test_learning_round9_research import examples


class Result:
    def __init__(self,one=None):self.one=one
    def fetchone(self):return self.one


class FakeModelDB:
    def __init__(self):
        self.records=[]
        self.fail_once=False
        self.sql=[]
    @contextmanager
    def connection(self, settings):
        initial=len(self.records)
        try:
            yield self
        except Exception:
            self.records=self.records[:initial]
            raise
    def execute(self, query, params=None):
        self.sql.append((query,params))
        if "pg_advisory_xact_lock" in query:
            return Result()
        if "SELECT id FROM model_versions" in query:
            version=params[0]
            expected_sha=params[1] if len(params)>1 else None
            for old in reversed(self.records):
                if old["model"]["version"]==version and (
                        expected_sha is None or old["validation"]["research"]["fingerprint_sha256"]==expected_sha):
                    return Result({"id":old["id"]})
            return Result(None)
        if "INSERT INTO model_versions" in query:
            if self.fail_once:
                self.fail_once=False
                raise OSError("simulated DB death before insert")
            row={"id":len(self.records)+1,"model":json.loads(params[0]),
                 "validation":json.loads(params[1]),"approved":False}
            self.records.append(row)
            return Result({"id":row["id"]})
        raise AssertionError("unexpected SQL: "+query[:100])


@pytest.fixture
def fake_pg(monkeypatch):
    pg=FakeModelDB()
    monkeypatch.setattr(lifecycle,"connection",pg.connection)
    return pg


def _groups():
    rows=examples()
    return rows[:115],rows[115:155],rows[155:]


def test_registry_same_sha_crash_retry_is_idempotent(fake_pg):
    tr,val,shadow=_groups()
    model=fit(tr)
    report=historical_shadow_report(tr,val,shadow,model)
    settings=SimpleNamespace(symbol="ETHUSDT")
    fake_pg.fail_once=True
    with pytest.raises(OSError,match="simulated DB death"):
        lifecycle.store_shadow(settings,model,tr,val,shadow,eligible_count=195,research_report=report)
    assert fake_pg.records==[]
    id1, stored1=lifecycle.store_shadow(settings,model,tr,val,shadow,eligible_count=195,research_report=report)
    id2, stored2=lifecycle.store_shadow(settings,model,tr,val,shadow,eligible_count=195,research_report=report)
    assert id1==id2==1
    assert len(fake_pg.records)==1
    assert fake_pg.records[0]["validation"]["research"]["fingerprint_sha256"]==report["fingerprint_sha256"]
    assert fake_pg.records[0]["approved"] is False


def test_same_train_new_forward_cohort_is_not_lost(fake_pg):
    tr,val,shadow=_groups()
    model=fit(tr)
    original=historical_shadow_report(tr,val,shadow,model)
    changed=[replace(r,outcome="bounce" if r.outcome!="bounce" else "breakout") for r in shadow]
    new=historical_shadow_report(tr,val,changed,model)
    assert original["fingerprint_sha256"]!=new["fingerprint_sha256"]
    id1,_=lifecycle.store_shadow(SimpleNamespace(symbol="ETHUSDT"),model,tr,val,shadow,
                                  eligible_count=195,research_report=original)
    id2,_=lifecycle.store_shadow(SimpleNamespace(symbol="ETHUSDT"),model,tr,val,changed,
                                  eligible_count=196,research_report=new)
    assert id1==1 and id2==2 and len(fake_pg.records)==2
    assert fake_pg.records[1]["validation"]["eligible_count"]==196
    assert "fingerprint_sha256" in fake_pg.sql[2][0] or any(
        "fingerprint_sha256" in s for s, p in fake_pg.sql)


def test_no_promotion_even_if_metrics_improve(fake_pg):
    tr,val,shadow=_groups()
    model=fit(tr)
    report=historical_shadow_report(tr,val,shadow,model)
    assert report["promotion_eligible"] is False
    assert report["go"] is False
    id_, meta=lifecycle.store_shadow(SimpleNamespace(symbol="ETHUSDT"),model,tr,val,shadow,
                                      eligible_count=195,research_report=report)
    assert meta["research_go"] is False
    assert fake_pg.records[0]["approved"] is False
    assert lifecycle.admit_paper_candidate(None,id_,model,tr,val,shadow)[0] is False


def test_reject_caller_forced_go_flag(fake_pg):
    tr,val,shadow=_groups()
    model=fit(tr)
    research=historical_shadow_report(tr,val,shadow,model)
    research["go"]=True
    with pytest.raises(ValueError,match="promotable"):
        lifecycle.store_shadow(SimpleNamespace(symbol="ETHUSDT"),model,tr,val,shadow,
                               eligible_count=195,research_report=research)
    assert fake_pg.records==[]


def test_training_cycle_from_only_verified_ledger(monkeypatch,fake_pg):
    rows=examples()
    monkeypatch.setattr(extract,"load_causal_feature_examples",lambda *a,**kw:(rows,{}))
    result=learning.train_causal_shadow(SimpleNamespace(symbol="ETHUSDT"),rows[-1].exit_ms+1,
                                        min_rows=180,min_new=100,previous_count=0,
                                        embargo_ms=60000)
    assert result["status"]=="shadow_registered" and result["approved"] is False
    assert result["metrics"]["research"]["cohort"]=="eth_feature_snapshot_v2"
    assert len(fake_pg.records)==1


def test_truncated_population_blocks_db_write(monkeypatch,fake_pg):
    monkeypatch.setattr(extract,"load_causal_feature_examples",
                        lambda *a,**kw:([],{"scan_limit_reached":1}))
    outcome=learning.train_causal_shadow(SimpleNamespace(symbol="ETHUSDT"),1795000000000,
                                         min_rows=180,min_new=100,previous_count=0)
    assert outcome["status"]=="incomplete_verified_population_scan"
    assert fake_pg.records==[]


def test_source_is_observed_live_cannot_enter_training(monkeypatch,fake_pg):
    monkeypatch.setattr(extract,"load_causal_feature_examples",
                        lambda *a,**kw: (_ for _ in ()).throw(AssertionError("must not read")))
    outcome=learning.train_causal_shadow(SimpleNamespace(symbol="ETHUSDT"),1795000000000,
                                         source="eth_paper_live_evidence_v1")
    assert outcome["status"]=="uncertified_source_disabled" and outcome["approved"] is False
    assert fake_pg.records==[]


def test_no_three_classes_fails_without_db_write(monkeypatch,fake_pg):
    rows=[replace(x,outcome="uncertain") for x in examples()]
    monkeypatch.setattr(extract,"load_causal_feature_examples",lambda *a,**kw:(rows,{}))
    outcome=learning.train_causal_shadow(SimpleNamespace(symbol="ETHUSDT"),rows[-1].exit_ms+1,
                                         min_rows=180,min_new=100)
    assert outcome["status"]=="research_evidence_blocked" and outcome["approved"] is False
    assert fake_pg.records==[]


def test_stale_retraining_watermark_cannot_reregister(monkeypatch,fake_pg):
    rows=examples()
    monkeypatch.setattr(extract,"load_causal_feature_examples",lambda *a,**kw:(rows,{}))
    res=learning.train_causal_shadow(SimpleNamespace(symbol="ETHUSDT"),rows[-1].exit_ms+1,
                                     min_rows=180,min_new=100,previous_count=len(rows))
    assert res["status"]=="insufficient_new_verified_data"
    assert fake_pg.records==[]
