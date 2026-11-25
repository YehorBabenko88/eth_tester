"""Round10 prospective F source patch integration, fake DB only."""
from dataclasses import replace
from types import SimpleNamespace
import pytest

from eth_tester.ml import fit, lifecycle, research
from test_learning_round9_integration import FakeModelDB
from test_learning_round9_research import examples


def groups():
    data=examples()
    return data[:115],data[115:155],data[155:]


def patch_db(monkeypatch):
    db=FakeModelDB()
    monkeypatch.setattr(lifecycle,'connection',db.connection)
    return db


def report(tr,va,sh,model):
    return research.historical_shadow_report(tr,va,sh,model,embargo_ms=60000)


def test_original_dataset_fingerprint_matches(monkeypatch):
    db=patch_db(monkeypatch)
    tr,va,sh=groups()
    model=fit(tr)
    canonical=report(tr,va,sh,model)
    ident,result=lifecycle.store_shadow(SimpleNamespace(symbol='ETHUSDT'),model,tr,va,sh,
                                   eligible_count=195,research_report=canonical)
    assert ident==1 and len(db.records)==1
    assert result['research']['fingerprint_sha256']==canonical['fingerprint_sha256']
    assert result['research']['embargo_ms']==60000


@pytest.mark.parametrize('mutator',[
 lambda r:{**r,'fingerprint_sha256':'0'*64},
 lambda r:{**r,'shadow':{'net':999999}},
 lambda r:{**r,'validation_selected':'invalid'},
 lambda r:{**r,'embargo_ms':-1},
 lambda r:{**r,'embargo_ms':True},
 lambda r:{**r,'promotion_eligible':True},
 lambda r:{**r,'go':True},
])
def test_tampered_sha_metrics_and_policy_rejected(monkeypatch, mutator):
    db=patch_db(monkeypatch)
    tr,va,sh=groups()
    model=fit(tr)
    corrupted=mutator(report(tr,va,sh,model))
    with pytest.raises(ValueError):
        lifecycle.store_shadow(SimpleNamespace(symbol='ETHUSDT'),model,tr,va,sh,
                                eligible_count=195,research_report=corrupted)
    assert db.records==[]


def test_same_fingerprint_forged_metrics_can_no_longer_mask_persisted_report(monkeypatch):
    db=patch_db(monkeypatch)
    tr,va,sh=groups()
    model=fit(tr)
    honest=report(tr,va,sh,model)
    original,_=lifecycle.store_shadow(SimpleNamespace(symbol='ETHUSDT'),model,tr,va,sh,
                                     eligible_count=195,research_report=honest)
    modified={**honest,'shadow':{'invented_net':123456789}}
    with pytest.raises(ValueError):
        lifecycle.store_shadow(SimpleNamespace(symbol='ETHUSDT'),model,tr,va,sh,
                               eligible_count=195,research_report=modified)
    assert original==1 and len(db.records)==1
    assert db.records[0]['validation']['research']==honest


def test_trusted_production_research_generation_is_consistent(monkeypatch):
    db=patch_db(monkeypatch)
    tr,va,sh=groups()
    model=fit(tr)
    ident,metadata=lifecycle.store_shadow(SimpleNamespace(symbol='ETHUSDT'),model,tr,va,sh,
                                         eligible_count=195,research_required=True)
    assert ident==1 and metadata['research']['go'] is False
    assert metadata['research']['embargo_ms']==60000
    assert db.records[0]['validation']['research']['fingerprint_sha256']==metadata['research']['fingerprint_sha256']


def test_new_forward_cohort_creates_new_versioned_study(monkeypatch):
    db=patch_db(monkeypatch)
    tr,va,sh=groups()
    model=fit(tr)
    a,_=lifecycle.store_shadow(SimpleNamespace(symbol='ETHUSDT'),model,tr,va,sh,
                               eligible_count=195,research_required=True)
    alt=[replace(r,outcome='bounce' if r.outcome!='bounce' else 'breakout') for r in sh]
    b,_=lifecycle.store_shadow(SimpleNamespace(symbol='ETHUSDT'),model,tr,va,alt,
                               eligible_count=196,research_required=True)
    assert (a,b)==(1,2)
    assert len(db.records)==2


def test_crash_before_insert_keeps_db_unchanged_and_retry_single(monkeypatch):
    db=patch_db(monkeypatch)
    tr,va,sh=groups()
    model=fit(tr)
    db.fail_once=True
    with pytest.raises(OSError,match='simulated DB death'):
        lifecycle.store_shadow(SimpleNamespace(symbol='ETHUSDT'),model,tr,va,sh,
                               eligible_count=195,research_required=True)
    assert db.records==[]
    one,_=lifecycle.store_shadow(SimpleNamespace(symbol='ETHUSDT'),model,tr,va,sh,
                                 eligible_count=195,research_required=True)
    two,_=lifecycle.store_shadow(SimpleNamespace(symbol='ETHUSDT'),model,tr,va,sh,
                                 eligible_count=195,research_required=True)
    assert (one,two)==(1,1)
    assert len(db.records)==1


def test_research_respects_nondefault_two_minute_embargo(monkeypatch):
    db=patch_db(monkeypatch)
    tr,va,sh=groups()
    model=fit(tr)
    _,metadata=lifecycle.store_shadow(SimpleNamespace(symbol='ETHUSDT'),model,tr,va,sh,
                                      eligible_count=195,research_required=True,
                                      research_embargo_ms=120000)
    assert metadata['research']['embargo_ms']==120000
