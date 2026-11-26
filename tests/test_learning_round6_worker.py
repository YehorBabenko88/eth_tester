"""Fail-closed independent label-worker preconditions with fake read-only DB."""
from datetime import datetime, timezone
from hashlib import sha256
import json
from types import SimpleNamespace
import pytest

from eth_tester.ml.label_worker import _reason_policy, _source_certificate
from eth_tester.ml.training_cursor import prior_eligible_count

POLICY={"version":"utc_full_bars_v1","method":"fixed_horizon_barrier_v1",
        "bar_interval_ms":60000,"horizon_bars":3,"barrier_bps":20,
        "anchor":"first_full_bar_strictly_after_decision"}

def valid_reason():
    return {"side":"resistance","level":"3200","ml_label_policy":dict(POLICY)}

def test_precommit_policy_exact():
    assert _reason_policy(valid_reason())[0]==POLICY

@pytest.mark.parametrize("tamper",[
    lambda r:r.pop("ml_label_policy"),
    lambda r:r["ml_label_policy"].update(horizon_bars=4),
    lambda r:r.update(ml_label="future"),
    lambda r:r.update(paper_evidence_quality="live_observed_bounded"),
    lambda r:r.update(ml_eligible=False),
    lambda r:r.update(side="unknown"),
    lambda r:r.update(level="-1"),
])
def test_uncommitted_bounded_live_and_tampered_policy_rejected(tamper):
    reason=valid_reason()
    tamper(reason)
    with pytest.raises(ValueError):
        _reason_policy(reason)

def _coverage_row(receipt,checked):
    start,end=60000,240000
    digest="a"*64
    evidence_ref=sha256(json.dumps([
        "ETHUSDT","linear","trade","1",start,end-60000,digest,receipt],
        separators=(",", ":")).encode()).hexdigest()
    return {"source":"kline:trade","interval":"1","instrument":"linear",
            "checksum":digest,"status":"verified", "start_ts":datetime.fromtimestamp(start/1000,timezone.utc),
            "end_ts":datetime.fromtimestamp(end/1000,timezone.utc),
            "observed_rows":3,"expected_rows":3,
            "quality":"exchange_closed_grid_checked",
            "evidence":{"kind":"trade","tf":"1",
              "receipt_is_independently_recorded":True,
              "closed_grid_complete":True,"source_receipt_ms":receipt,
              "source_receipt_kind":"postgres_clock_timestamp_after_http",
              "source_verified_as_of_receipt":True,"source_event_start_ms":start,
              "source_event_end_exclusive_ms":end,"requested_start_ms":start,
              "requested_end_ms":end-60000,"row_count":3,
              "rest_payload_sha256":digest,"evidence_ref":evidence_ref,
              "proves_all_trade_ids":False},
            "checked_at":datetime.fromtimestamp(checked/1000,timezone.utc)}

class FakeConn:
    def __init__(self,coverage, gap=None):
        self.coverage=coverage
        self.gap=gap
        self.sql=[]
    def execute(self,statement,params):
        self.sql.append((statement,params))
        return SimpleNamespace(fetchall=lambda:self.coverage,fetchone=lambda:self.gap)

def test_witness_is_pinned_to_real_checked_receipt_and_no_gap():
    conn=FakeConn([_coverage_row(240000,240000)])
    proof,_,rec,check=_source_certificate(conn,symbol="ETHUSDT",
        instrument="linear",decision_ms=60000,horizon_ms=180000,
        as_of_ms=300000)
    assert proof["source"]=="kline:trade" and rec==check==240000
    assert len(conn.sql)==2

@pytest.mark.parametrize("tamper",[
    lambda r:r["evidence"].pop("source_receipt_ms"),
    lambda r:r["evidence"].update(receipt_is_independently_recorded=False),
    lambda r:r["evidence"].update(closed_grid_complete=False),
    lambda r:r["evidence"].update(fixture="test_only"),
    lambda r:r.update(checksum="broken"),
    lambda r:r["evidence"].update(source_receipt_ms=170000),
    lambda r:r["evidence"].update(source_receipt_ms=310000),
    lambda r:r["evidence"].update(evidence_ref="0"*64),
    lambda r:r["evidence"].update(rest_payload_sha256="0"*64),
    lambda r:r["evidence"].update(row_count=2),
])
def test_incomplete_or_future_historical_proof_rejected(tamper):
    proof=_coverage_row(240000,240000)
    tamper(proof)
    conn=FakeConn([proof])
    with pytest.raises(ValueError):
        _source_certificate(conn,symbol="ETHUSDT",
        instrument="linear",decision_ms=60000,horizon_ms=180000,
        as_of_ms=300000)

def test_duplicate_witness_rejected():
    proof=_coverage_row(240000,240000)
    with pytest.raises(ValueError,match="missing_or_ambiguous"):
        _source_certificate(FakeConn([proof,proof]),symbol="ETHUSDT",
            instrument="linear",decision_ms=60000,horizon_ms=180000,
            as_of_ms=300000)

def test_unresolved_gap_rejected():
    proof=_coverage_row(240000,240000)
    with pytest.raises(ValueError,match="unresolved_candle_gap"):
        _source_certificate(FakeConn([proof],gap={"id":1}),symbol="ETHUSDT",
            instrument="linear",decision_ms=60000,horizon_ms=180000,
            as_of_ms=300000)

def test_persisted_shadow_watermark_is_verified():
    assert prior_eligible_count(None)==0
    assert prior_eligible_count({"validation":{"paper_only":True,"eligible_count":180}})==180
    with pytest.raises(ValueError):
        prior_eligible_count({"validation":{"eligible_count":200}})
    with pytest.raises(ValueError):
        prior_eligible_count({"validation":{"paper_only":True,"eligible_count":180.5}})
