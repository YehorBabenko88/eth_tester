"""F Round11: bounded, snapshot-consistent extraction and secret-free telemetry.

These are deterministic fake-DB fault tests, NOT live PostgreSQL acceptance.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace
import sys
import pytest
from eth_tester.ml import extract

AS_OF=1795600000000

class Result:
    def __init__(self, rows=None, row=None):
        self.rows=rows or []
        self.row=row
    def fetchall(self): return self.rows
    def fetchone(self): return self.row

class MockSnapshot:
    def __init__(self, size):
        start=datetime(2026,11,20,tzinfo=timezone.utc)
        self.rows=sorted(
            ({"decision_at":start.replace(second=i%60),"paper_trade_id":i+1}
             for i in range(size)),
            key=lambda row:(row["decision_at"],row["paper_trade_id"]),reverse=True)
        self.statements=[]
        self.snapshot=False
    @contextmanager
    def connection(self, settings):
        yield self
    def execute(self, sql, params=None):
        self.statements.append((sql,params))
        if "SET TRANSACTION" in sql:
            assert "REPEATABLE READ READ ONLY" in sql
            self.snapshot=True
            return Result()
        if "to_regclass" in sql:
            assert self.snapshot
            return Result(row={"installed":True})
        assert self.snapshot and "LEFT JOIN paper_ml_labels" in sql
        assert "ORDER BY p.signal_ts DESC,p.id DESC LIMIT %s" in sql
        _,_,_,last,last_b,last_id,limit=params
        assert last==last_b
        items=self.rows
        if last is not None:
            items=[r for r in items if (r["decision_at"],r["paper_trade_id"])<(last,last_id)]
        return Result(rows=items[:limit])

@pytest.mark.parametrize("size,limit,over",[
    (0,1,False),(1,1,False),(2,1,True),(999,1000,False),
    (1000,1000,False),(1001,1000,True),
    (1199,1250,False),(1250,1250,False),(1251,1250,True),
])
def test_limit_plus_one_finds_real_truncation_only(monkeypatch,size,limit,over):
    db=MockSnapshot(size)
    monkeypatch.setattr(extract,"connection",db.connection)
    monkeypatch.setattr(extract,"from_snapshot_records",lambda rows,as_of:([],{}))
    examples,rejections=extract.load_causal_feature_examples(
        SimpleNamespace(symbol="ETHUSDT"),AS_OF,limit=limit)
    assert examples==[]
    assert bool(rejections.get("scan_limit_reached")) is over
    assert db.statements[0][0].startswith("SET TRANSACTION")
    pages=[entry for entry in db.statements if "LEFT JOIN paper_ml_labels" in entry[0]]
    assert all(0<entry[1][-1]<=1001 for entry in pages)
    if size==1251:
        assert len(pages)==2 and pages[-1][1][-1]==251

def test_257_independent_rejections_do_not_break_g_telemetry(monkeypatch):
    contract=ModuleType("eth_tester.ml.contract")
    contract.milliseconds=lambda x:x
    def fail(row,reason):
        raise ValueError("secret-token-"+str(row["paper_trade_id"]))
    contract.validate_snapshot=fail
    contract.validate_label=lambda *a,**k:None
    monkeypatch.setitem(sys.modules,"eth_tester.ml.contract",contract)
    rows=[{"paper_trade_id":i,"reason":{}} for i in range(1,258)]
    accepted,rejected=extract.from_snapshot_records(rows,AS_OF)
    assert accepted==[] and rejected=={"other":257}
    assert len(rejected)<=256
    assert all("secret" not in k for k in rejected)

def test_source_failure_categories_are_finite(monkeypatch):
    contract=ModuleType("eth_tester.ml.contract")
    contract.milliseconds=lambda x:x
    def fail(row,reason):raise ValueError(row["failure"])
    contract.validate_snapshot=fail
    contract.validate_label=lambda *a,**k:None
    monkeypatch.setitem(sys.modules,"eth_tester.ml.contract",contract)
    rows=[{"paper_trade_id":i,"reason":{},"failure":reason} for i,reason in enumerate((
        "feature hindsight: oi","noncanonical source certification",
        "no independent receipt","user-specific secret"),1)]
    accepted,rejected=extract.from_snapshot_records(rows,AS_OF)
    assert accepted==[]
    assert rejected=={
        "late_or_backdated_horizon_source":1,
        "not_independently_certified_historical_snapshot":1,
        "historical_receipt_not_independently_witnessed":1,
        "other":1}

def test_legacy_diagnostics_never_expose_raw_reason():
    _,rejected=extract.from_trade_records(
        [{"reason":{"ml_snapshot":{"recorded_ms":0}},"event_id":"secret"}],
        AS_OF)
    assert rejected=={"source_not_immutable":1}

def test_overflow_does_not_silently_truncate_failure_counts(monkeypatch):
    db=MockSnapshot(1001)
    monkeypatch.setattr(extract,"connection",db.connection)
    monkeypatch.setattr(extract,"from_snapshot_records",
                        lambda rows,as_of:([],{"other":len(rows)}))
    _,result=extract.load_causal_feature_examples(
        SimpleNamespace(symbol="ETHUSDT"),AS_OF,limit=1000)
    assert result=={"scan_limit_reached":1,"other":1000}
