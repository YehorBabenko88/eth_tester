"""Reproducible offline fault and hypothesis research tests; NO market/PG claims."""
from dataclasses import replace
from math import isnan
import pytest
from eth_tester.ml import Example, fit, FEATURE_NAMES, CLASSES
from eth_tester.ml.research import (
    historical_shadow_report, verify_history_partition, fit_stump,
    prior_policy, FlowRulePolicy, moving_block_bootstrap_lower, calibration,
    score_oos, prospective_paper_cohort_status, walk_forward_train_only,
    CONTRACT,
)

BASE=1795000000000

def examples(n=195):
    data=[]
    for i in range(n):
        at=BASE+i*600_000
        # Reproducible, varied and causally available features, multiple labels.
        v=tuple(float(((i*7+j*13)%37)-18)/20 for j in range(len(FEATURE_NAMES)))
        outcome=CLASSES[i%3]
        gross=(i%7-3)/1000 + (0.006 if outcome=="bounce" else -0.002)
        costs=0.001 + (i%3)*0.0001
        data.append(Example("paper:"+str(i+1), at, at+180000, v,
                    tuple(at-2000-j for j in range(len(FEATURE_NAMES))), outcome,
                    gross, costs))
    return data

@pytest.fixture(scope="module")
def cohorts():
    all_rows=examples()
    train,val,sh=all_rows[:115],all_rows[115:155],all_rows[155:]
    model=fit(train)
    return train,val,sh,model

@pytest.fixture(scope="module")
def report(cohorts):
    return historical_shadow_report(*cohorts,embargo_ms=60000)


def test_reproducible_fingerprint_and_bootstrap(report,cohorts):
    next_report=historical_shadow_report(*cohorts,embargo_ms=60000)
    assert report==next_report
    assert len(report["fingerprint_sha256"])==64
    assert report["version"]=="historical_shadow_research_v1"
    assert report["cohort"]==CONTRACT
    assert report["go"] is False and report["promotion_eligible"] is False


def test_candidates_and_untouched_shadow(report):
    assert set(report["candidates"])=={"flow_rule","prior","stump","softmax3"}
    assert report["validation_selected"] in report["candidates"]
    assert report["selected_on"]=="validation_multiclass_brier_only"
    assert all(v["n"]==40 for v in report["shadow"].values())
    assert all(v["n"]==40 for v in report["validation"].values())
    assert report["shadow_reuse_not_independently_excluded"] is True


def test_block_bootstrap_not_iid_and_nonzero_band(cohorts):
    _, val, _, _=cohorts
    differences=[0.001]*20+[-0.002]*20
    bound=moving_block_bootstrap_lower(differences,seed=123)
    assert bound["block_size"]>1
    assert bound["repetitions"]==256
    assert bound["lower_p05"]<=bound["observed_mean"]<=bound["upper_p95"]
    assert bound==moving_block_bootstrap_lower(differences,seed=123)


def test_calibration_probabilities_and_reliability(cohorts):
    train,val,_,_ = cohorts
    result=calibration(prior_policy(train),val)
    assert result["multiclass_brier"]>=0
    assert 0<=result["expected_calibration_error"]<=1
    assert sum(x["count"] for x in result["reliability_bins"])==len(val)


def test_fit_stump_is_train_only(cohorts):
    train,val,shadow,_=cohorts
    one=fit_stump(train)
    altered_val=[replace(r, outcome="breakout") for r in val]
    altered_shadow=[replace(r, outcome="bounce") for r in shadow]
    assert one==fit_stump(train)
    assert len(altered_val)==len(val) and len(altered_shadow)==len(shadow)
    assert one.train_to_ms<val[0].decision_ms


def test_walkforward_train_only_and_purged(cohorts, report):
    train,val,shadow,model=cohorts
    folds=walk_forward_train_only(train)
    assert folds
    assert len(folds)==len(report["train_only_walk_forward"])
    assert all(f["first_test_ms"]<val[0].decision_ms for f in folds)
    assert all(f["test"]>=10 and f["train"]>=12 for f in folds)


def test_no_duplicate_ledger_ids(cohorts):
    tr,val,sh,_=cohorts
    with pytest.raises(ValueError,match="duplicate"):
        verify_history_partition(tr,[replace(val[0],event_id=tr[0].event_id)]+val[1:],sh)


def test_reject_future_feature(cohorts):
    tr,val,sh,_=cohorts
    bad=[replace(val[0],available_ms=tuple([val[0].decision_ms+1]*len(FEATURE_NAMES)))]+val[1:]
    with pytest.raises(ValueError):verify_history_partition(tr,bad,sh)


def test_reject_future_label_or_overlap(cohorts):
    tr,val,sh,_=cohorts
    bad=[replace(val[-1],exit_ms=sh[0].decision_ms) if i==len(val)-1 else v for i,v in enumerate(val)]
    with pytest.raises(ValueError):verify_history_partition(tr,bad,sh)


def test_reject_unordered_and_corrupt_event_id(cohorts):
    tr,val,sh,_=cohorts
    with pytest.raises(ValueError): verify_history_partition(tr,list(reversed(val)),sh)
    with pytest.raises(ValueError): verify_history_partition(tr,[replace(val[0],event_id="paper:fake")]+val[1:],sh)


def test_reject_nan_and_infinite(cohorts):
    tr,val,sh,_=cohorts
    for bad in (float("nan"),float("inf")):
        with pytest.raises(ValueError):
            verify_history_partition(tr,[replace(val[0],values=(bad,)+val[0].values[1:])]+val[1:],sh)


def test_mixed_historical_and_forward_paper_rejected(cohorts):
    tr,val,sh,model=cohorts
    with pytest.raises(ValueError,match="prospective"):
        historical_shadow_report(tr,val,sh,model,cohort="eth_paper_live_evidence_v1")
    status=prospective_paper_cohort_status(ledger_contract="eth_paper_live_evidence_v1")
    assert status["status"]=="blocked_missing_independent_prospective_outcome_ledger"
    assert status["historical_ml_eligible"] is False


def test_no_test_time_model_selection_even_when_shadow_changes(cohorts):
    tr,val,sh,model=cohorts
    baseline=historical_shadow_report(tr,val,sh,model)
    flipped=[replace(r,outcome="bounce" if r.outcome!="bounce" else "breakout") for r in sh]
    changed=historical_shadow_report(tr,val,flipped,model)
    assert baseline["validation_selected"]==changed["validation_selected"]
    assert baseline["validation"]!=changed["validation"] or baseline["shadow"]!=changed["shadow"]


def test_no_validity_if_model_weights_forged(cohorts):
    tr,val,sh,model=cohorts
    bad=replace(model,version="forged")
    with pytest.raises(ValueError,match="noncanonical"):
        historical_shadow_report(tr,val,sh,bad)


def test_bootstrap_short_series_cannot_claim_confidence():
    with pytest.raises(ValueError,match="insufficient"):
        moving_block_bootstrap_lower([0.2]*20)


def test_invalid_nonfinite_payoff_rejected(cohorts):
    tr,val,sh,_=cohorts
    with pytest.raises(ValueError):
        verify_history_partition(tr,[replace(val[0],gross_return=float("nan"))]+val[1:],sh)


def test_model_policy_requires_no_future_leakage(cohorts):
    tr,val,_,_=cohorts
    p=FlowRulePolicy((0.0,0.0,0.0),train_to_ms=val[0].decision_ms)
    with pytest.raises(ValueError,match="future leakage"):
        score_oos(p,val,evidence_fingerprint="test",seed_label="leak")


def test_moving_blocks_capture_serial_dependence_not_iid():
    import random
    series=[0.01]*32+[-0.01]*32
    blocked=moving_block_bootstrap_lower(series,seed=8)
    rng=random.Random(8)
    n=len(series)
    iid=sorted(sum(series[rng.randrange(n)] for _ in range(n))/n
               for _ in range(256))
    iid_lower=iid[int((len(iid)-1)*.05)]
    assert blocked["block_size"]==8
    assert blocked["lower_p05"] < iid_lower-0.002


def test_extreme_feature_magnitude_rejected_before_optimizer(cohorts):
    tr,val,sh,_=cohorts
    corrupt=[replace(tr[0],values=(1e308,)+tr[0].values[1:])]+tr[1:]
    with pytest.raises(ValueError,match="unbounded"):
        verify_history_partition(corrupt,val,sh)


def test_report_explicitly_blocks_neural_net_and_profit_inference(report):
    assert report["neural_net_status"]=="not_justified_without_independent_forward_cohort"
    assert "observed_trades_are_not_all_market_signals" in report["limitations"]
    assert "missing_B_append_only_prospective_outcome_ledger" in report["limitations"]
    assert report["actions"]=="research_only_no_orders_no_model_activation"
