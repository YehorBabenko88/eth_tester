"""F14 prospective every-closed-minute audit; fake unsigned receipts only."""
from datetime import datetime, timezone
from hashlib import sha256
import json

import pytest

from eth_tester.ml.prospective import audit_closed_minute_research

T0 = 1_795_680_000_000  # exact UTC minute, synthetic only
S = "ETHUSDT"


def digest(payload):
    return sha256(json.dumps(payload,sort_keys=True,separators=(",", ":"),
                             allow_nan=False).encode()).hexdigest()


def iso(ms):
    return datetime.fromtimestamp(ms/1000,timezone.utc).isoformat()


def minute(i, status="no_signal"):
    bar = T0 + 60_000*i
    observed = bar + 75_000
    sh = dict(contract="eth_strategy_ensemble_v1", version="1",
              mode="shadow_only", approved=False, paper_entry_authorized=False,
              ml_eligible=False, promoted=False,
              independent_of_incumbent=True,
              observation_receipt_authoritative=False,
              expected_net_lower_bound=None,
              source_identity_authoritative=False,
              bar_ts=iso(bar), input_window_closed_at=iso(bar+60_000),
              decision_cutoff_at=iso(observed), research_observed_at=iso(observed),
              input_sha256=digest(["past",i]),
              status="shadow_candidates" if status=="candidate" else "no_trade",
              reason=("await_independent_calibration_and_live_proof" if status=="candidate"
                      else "no_causal_pattern" if status=="no_signal"
                      else "open_candle_unavailable"),
              candidates=[{"pattern":"synthetic"}] if status=="candidate" else [],
              selected="synthetic" if status=="candidate" else None)
    sh["diagnostic_sha256"] = digest(sh)
    return {"symbol":S,"tf":"1","bar_ts_ms":bar,
            "observed_at_ms":observed,
            "source_receipt_ms":bar+70_000,
            "source_receipt_key":f"fake-original-{i}",
            "source_receipt_sha256":digest(["original",i]),
            "shadow":sh}


def forward(i, label="uncertain"):
    bar=T0+60_000*i
    h=bar+60_000+300_000
    return {"symbol":S,"tf":"1", "bar_ts_ms":bar,
            "horizon_end_ms":h, "source_receipt_ms":h+2,
            "observed_at_ms":h+3,"source_receipt_key":f"fake-future-{i}",
            "source_receipt_sha256":digest(["future",i]),"label":label}


def audit(obs,out, *, count=3,asof=None, **kwargs):
    return audit_closed_minute_research(obs,out,symbol=S,start_ms=T0,
                     end_ms=T0+60_000*count,
                     as_of_ms=asof or T0+60_000*count+500_000,
                     **kwargs)


def test_all_minutes_including_no_signal_have_denominator_and_no_pnl():
    report=audit([minute(0,"candidate"), minute(1), minute(2)],
                 [forward(0,"breakout"),forward(1,"bounce"),forward(2,"uncertain")])
    assert report["status"]=="structurally_complete_not_source_certified"
    assert report["expected_closed_minutes"]==3
    assert report["candidate_minutes"]==1
    assert report["no_signal_minutes"]==2
    assert report["future_outcomes_structurally_checked"]==3
    assert report["label_counts_by_original_decision"]["no_signal"]=={
           "breakout":0,"bounce":1,"uncertain":1}
    assert report["approved"] is False and report["go"] is False
    assert report["ml_eligible"] is False and report["economics_verified"] is False
    assert report["returns_computed"] is False
    assert report["original_source_attested_by_F"] is False
    assert report["selection_bias_excluded"] is False
    assert "net_profit" not in report and "win_rate" not in report


def test_missing_original_bar_never_converts_to_no_signal():
    report=audit([minute(0),minute(2)],[forward(0),forward(2)])
    assert report["status"].startswith("blocked_")
    assert report["no_signal_minutes"]==2
    assert report["missing_observations"]==1
    assert report["expected_closed_minutes"]==3
    assert report["future_outcomes_structurally_checked"]==2


def test_original_decision_without_future_outcome_is_matured_and_blocked():
    report=audit([minute(i) for i in range(3)], [forward(0)])
    assert report["rejection_counts"]["missing_future_outcome"]==2
    assert report["ml_eligible"] is False


def test_not_yet_matured_horizon_is_pending_not_fake_failure():
    report=audit([minute(0)],[ ],count=1,asof=T0+250_000)
    assert report["matured_expected_minutes"]==0
    assert report["rejection_counts"]["missing_future_outcome"]==0
    assert report["status"].startswith("blocked_")


def test_provisional_shadow_without_independent_B_receipt_does_not_count():
    observation=minute(0)
    observation.pop("source_receipt_sha256")
    report=audit([observation],[],count=1)
    assert report["missing_observations"]==1
    assert report["rejection_counts"]["invalid_original_observation"]==1
    assert report["no_signal_minutes"]==0


@pytest.mark.parametrize("change",[
 lambda x:x["shadow"].update(approved=True),
 lambda x:x["shadow"].update(paper_entry_authorized=True),
 lambda x:x["shadow"].update(ml_eligible=True),
 lambda x:x["shadow"].update(promoted=True),
 lambda x:x["shadow"].update(independent_of_incumbent=False),
 lambda x:x["shadow"].update(observation_receipt_authoritative=True),
 lambda x:x["shadow"].update(expected_net_lower_bound=10_000),
 lambda x:x["shadow"].update(research_observed_at="1900-01-01T00:00:00+00:00"),
 lambda x:x["shadow"].update(source_identity_authoritative=True),
 lambda x:x["shadow"].update(mode="live"),
 lambda x:x["shadow"].update(contract="eth_rules_v3"),
 lambda x:x["shadow"].update(input_sha256="0"),
 lambda x:x["shadow"].update(bar_ts="1990-01-01T00:00:00+00:00"),
 lambda x:x.update(observed_at_ms=x["bar_ts_ms"]+150_001),
 lambda x:x.update(source_receipt_ms=x["bar_ts_ms"]+150_001),
 lambda x:x.update(tf="5"),
 lambda x:x.update(source_receipt_sha256="x"*64),
 lambda x:x["shadow"].update(selected="unexpected"),
 lambda x:x["shadow"].update(status="shadow_candidates"),
])
def test_forged_or_late_D_paper_shadow_rejected(change):
    x=minute(0)
    change(x)
    r=audit([x],[],count=1)
    assert r["missing_observations"]==1
    assert r["ml_eligible"] is False
    assert r["rejection_counts"]["invalid_original_observation"]==1


def test_invalid_D_digest_rejected_even_if_paper_shadow_payload_self_proclaims_verified():
    x=minute(0)
    x["shadow"]["reason"]="modified_forecast"
    r=audit([x],[],count=1)
    assert r["rejection_counts"]["invalid_original_observation"]==1


@pytest.mark.parametrize("change",[
 lambda x:x.update(horizon_end_ms=x["horizon_end_ms"]-60_000),
 lambda x:x.update(source_receipt_ms=x["horizon_end_ms"]),
 lambda x:x.update(observed_at_ms=x["horizon_end_ms"]-1),
 lambda x:x.update(label="profitable"),
 lambda x:x.update(source_receipt_sha256="notsha"),
 lambda x:x.update(symbol="BTCUSDT"),
 lambda x:x.update(source_receipt_key=""),
])
def test_future_labels_must_have_new_matured_source_receipts(change):
    obs=minute(0)
    future=forward(0)
    change(future)
    report=audit([obs],[future],count=1)
    assert report["future_outcomes_structurally_checked"]==0
    assert report["rejection_counts"]["invalid_future_outcome"]==1
    assert report["ml_eligible"] is False


def test_reused_original_receipt_cannot_be_forward_witness():
    obs=minute(0)
    future=forward(0)
    future["source_receipt_key"]=obs["source_receipt_key"]
    report=audit([obs],[future],count=1)
    assert report["rejection_counts"]["invalid_future_outcome"]==1
    assert not report["ml_eligible"]


def test_reused_original_digest_cannot_be_future_witness():
    obs=minute(0)
    future=forward(0)
    future["source_receipt_sha256"]=obs["source_receipt_sha256"]
    report=audit([obs],[future],count=1)
    assert report["rejection_counts"]["invalid_future_outcome"]==1


def test_future_outcome_without_original_is_orphan_not_eligible():
    report=audit([], [forward(0)],count=1)
    assert report["rejection_counts"]["orphan_future_outcome"]==1
    assert report["future_outcomes_structurally_checked"]==0


def test_unavailable_is_not_no_signal():
    report=audit([minute(0,"unavailable")],[forward(0)],count=1)
    assert report["unavailable_minutes"]==1
    assert report["no_signal_minutes"]==0
    assert report["status"].startswith("blocked_")


def test_missing_one_minute_changes_complete_diagnostic_fingerprint():
    all_=audit([minute(i) for i in range(3)], [forward(i) for i in range(3)])
    broken=audit([minute(0),minute(2)],[forward(0),forward(2)])
    assert all_["diagnostic_fingerprint_sha256"] != broken["diagnostic_fingerprint_sha256"]


def test_altered_future_label_changes_report_without_pnl_claim():
    a=audit([minute(0)],[forward(0,"breakout")],count=1)
    b=audit([minute(0)],[forward(0,"bounce")],count=1)
    assert a["diagnostic_fingerprint_sha256"]!=b["diagnostic_fingerprint_sha256"]
    assert a["economics_verified"] is False and b["model_promotion_eligible"] is False


def test_any_duplicate_original_minute_fails_closed():
    with pytest.raises(ValueError,match="duplicate original minute receipt"):
        audit([minute(0),minute(0)],[],count=1)


def test_any_duplicate_future_receipt_fails_closed():
    with pytest.raises(ValueError,match="duplicate forward outcome receipt"):
        audit([minute(0)],[forward(0),forward(0)],count=1)


def test_large_synthetic_every_minute_no_signal_never_infers_profit_or_ML():
    n=500
    r=audit([minute(i) for i in range(n)],[forward(i) for i in range(n)],count=n)
    assert r["expected_closed_minutes"]==n
    assert r["no_signal_minutes"]==n
    assert r["missing_observations"]==0
    assert r["future_outcomes_structurally_checked"]==n
    assert r["ml_eligible"] is False
    assert r["go"] is False


@pytest.mark.parametrize("start,end,horizon",[
 (T0+1,T0+60000,300000),
 (T0,T0,300000),
 (T0,T0+60000,0),
 (T0,T0+60000,5),
 (T0,T0+60000,86_460_000),
 (T0,T0+60_000*10_081,300000),
])
def test_invalid_or_unbounded_window_is_rejected(start,end,horizon):
    with pytest.raises(ValueError):
        audit_closed_minute_research([],[],symbol=S,start_ms=start,end_ms=end,
                                     as_of_ms=T0+900_000_000,horizon_ms=horizon)


def test_diagnostic_counts_cannot_make_up_missing_history():
    # Caller cannot claim 3 original decisions by inventing only a total count.
    r=audit([minute(0)],[forward(0)],count=3)
    assert r["original_observations"]==1
    assert r["missing_observations"]==2
    assert r["selection_bias_excluded"] is False


def test_actual_D_empty_on_insufficient_history_is_unavailable_not_no_signal():
    x=minute(0,"unavailable")
    # D _empty() does not invent a bar identity or input hash when validation
    # fails; E's independent B original receipt may still witness the minute.
    sh=x["shadow"]
    sh["bar_ts"]=None
    for key in ("input_sha256","input_window_closed_at","decision_cutoff_at",
                "source_identity_authoritative"):
        sh.pop(key)
    sh["diagnostic_sha256"]=digest({k:v for k,v in sh.items()
                                    if k!="diagnostic_sha256"})
    result=audit([x],[],count=1)
    assert result["original_observations"]==1
    assert result["unavailable_minutes"]==1
    assert result["no_signal_minutes"]==0
    assert result["status"]=="blocked_incomplete_prospective_evidence"


def test_rehashing_unauthenticated_D_report_cannot_certify_source():
    x=minute(0)
    x["shadow"]["reason"]="conflicting_shadow_directions"
    x["shadow"]["diagnostic_sha256"]=digest({k:v for k,v in x["shadow"].items()
                                                if k!="diagnostic_sha256"})
    result=audit([x],[forward(0)],count=1)
    assert result["no_signal_minutes"]==1
    assert result["original_source_attested_by_F"] is False
    assert result["ml_eligible"] is False


def test_extremely_large_diagnostic_payload_does_not_enter_research():
    x=minute(0)
    x["shadow"]["context"]={"opaque":"x"*8_389_000}
    # A valid-looking signature does not permit unbounded payload encoding.
    x["shadow"]["diagnostic_sha256"]="0"*64
    result=audit([x],[],count=1)
    assert result["missing_observations"]==1
    assert result["rejection_counts"]["invalid_original_observation"]==1
