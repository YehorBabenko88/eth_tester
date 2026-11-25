"""Round 4 immutable D→E snapshot contract tests, no real exchange APIs."""
from datetime import datetime, timedelta, timezone

import pytest

from eth_tester.simulation.identity import (
    bind_snapshot_reference, require_new_strategy_identity,
    verify_persisted_snapshot,
)
from eth_tester.strategies import Parameters, strategy_identity
from eth_tester.ml import FEATURE_NAMES


BASE = datetime(2026, 11, 20, tzinfo=timezone.utc)
KEY, CHECKSUM = "a"*64, "b"*64


def reason_for(bar=BASE, *, key=KEY, checksum=CHECKSUM, available=None,
               computed=None):
    end=bar+timedelta(minutes=1)
    available=available or end+timedelta(seconds=10)
    computed=computed or available+timedelta(seconds=10)
    snap={"snapshot_key":key,"source_checksum":checksum,
          "contract":"eth_feature_snapshot_v2",
          "source_receipt_ms":int((end+timedelta(seconds=2)).timestamp()*1000),
          "source_checked_at_ms":int((end+timedelta(seconds=3)).timestamp()*1000),
          "available_at":available.isoformat(),"computed_at":computed.isoformat()}
    return {"strategy_version":strategy_identity(),
            "rules_version":Parameters().version,**Parameters().identity(),
            "source":"closed_candles","signal_bar_ts":bar.isoformat(),
            "orderflow":{"admissible":True,"immutable_feature_snapshot":snap}}


def test_real_d_v3_identity_with_negative_tamper():
    valid=reason_for()
    assert require_new_strategy_identity(valid)["strategy_version"]==strategy_identity()
    for changed in (
        {**valid,"strategy_version":Parameters().version},
        {**valid,"strategy_version":"eth_rules_v2"},
        {**valid,"rules_version":"eth_rules_v2"},
        {**valid,"parameter_sha256":"0"*64},
        {**valid,"parameters":{**valid["parameters"],"max_distance":"999"}},
    ):
        with pytest.raises(ValueError,match="identity"):
            require_new_strategy_identity(changed)


def test_pins_original_snapshot_and_real_decision_not_print_event():
    decision=BASE+timedelta(minutes=1,seconds=40)
    original=reason_for()
    original["market_trade_ts"]=(decision-timedelta(seconds=3)).isoformat()
    pinned=bind_snapshot_reference(original,decision_at=decision,bar_ts=BASE)
    assert pinned["ml_snapshot_key"]==KEY
    assert pinned["ml_source_checksum"]==CHECKSUM
    assert pinned["ml_signal_decision_ms"]==int(decision.timestamp()*1000)
    assert pinned["ml_bar_end_ms"]==int((BASE+timedelta(minutes=1)).timestamp()*1000)
    assert original.get("ml_snapshot_key") is None
    assert "ml_label" not in pinned


@pytest.mark.parametrize("modification",[
    {"ml_snapshot_key":"c"*64},
    {"ml_source_checksum":"d"*64},
    {"ml_signal_decision_ms":1},
    {"ml_bar_end_ms":1},
    {"ml_label":{"outcome":"breakout"}},
])
def test_no_conflicting_or_future_binding(modification):
    with pytest.raises(ValueError):
        bind_snapshot_reference({**reason_for(),**modification},
                                decision_at=BASE+timedelta(minutes=1,seconds=40),
                                bar_ts=BASE)


def test_snapshot_cannot_materialize_after_decision():
    with pytest.raises(ValueError,match="after decision"):
        bind_snapshot_reference(reason_for(computed=BASE+timedelta(minutes=3)),
                                decision_at=BASE+timedelta(minutes=2),bar_ts=BASE)


def test_snapshot_must_be_canonical_and_pinned_in_same_transaction():
    decision=BASE+timedelta(minutes=1,seconds=40)
    context=bind_snapshot_reference(reason_for(),decision_at=decision,bar_ts=BASE)
    checked_ms=int((BASE+timedelta(minutes=1,seconds=3)).timestamp()*1000)
    receipt_ms=int((BASE+timedelta(minutes=1,seconds=2)).timestamp()*1000)
    at_ms=int((BASE+timedelta(minutes=1,seconds=4)).timestamp()*1000)
    event_ms=int(BASE.timestamp()*1000)
    proof={"source":"trades_archive","instrument":"linear","interval":"",
           "checksum":CHECKSUM,"coverage_status":"verified",
           "evidence_ref":"test:only","source_event_ms":event_ms,
           "source_checked_at_ms":checked_ms,"source_receipt_ms":receipt_ms}
    provenance={"contract":"eth_feature_snapshot_v2",
                "source_coverage_verified":True,
                "coverage_status":"verified",
                "certified_by":"independent_source_proof",
                "source_checked_at_ms":checked_ms,"source_receipt_ms":receipt_ms,
                "feature_sources":{name:dict(proof) for name in FEATURE_NAMES},
                "feature_available_ms":{name:at_ms for name in FEATURE_NAMES}}
    row={"snapshot_key":KEY,"source_checksum":CHECKSUM,"symbol":"ETHUSDT",
         "tf":"1","bar_ts":BASE,"quality":"complete","is_causal":True,
         "available_at":BASE+timedelta(minutes=1,seconds=10),
         "observed_at":BASE+timedelta(minutes=1,seconds=15),
         "computed_at":BASE+timedelta(minutes=1,seconds=20),
         "provenance":provenance}
    class FakeDB:
        def __init__(self): self.row=dict(row);self.queries=[]
        def execute(self,query,params=None):
            self.queries.append((query,params))
            return self
        def fetchone(self): return self.row
    db=FakeDB()
    assert verify_persisted_snapshot(db,symbol="ETHUSDT",reason=context,
                                     decision_at=decision)["snapshot_key"]==KEY
    assert "FOR SHARE" in db.queries[-1][0]
    assert db.queries[-1][1]==(KEY,)
    for name,value in [
        ("source_checksum","f"*64),("symbol","BTCUSDT"),("tf","5"),
        ("computed_at",decision+timedelta(seconds=1)),
        ("quality","partial_unverified"),("is_causal",False),
    ]:
        db.row={**row,name:value}
        with pytest.raises(ValueError):
            verify_persisted_snapshot(db,symbol="ETHUSDT",reason=context,
                                      decision_at=decision)
    db.row={**row,"provenance":{"contract":"eth_feature_snapshot_v1"}}
    with pytest.raises(ValueError):
        verify_persisted_snapshot(db,symbol="ETHUSDT",reason=context,
                                  decision_at=decision)
    out_of_order={**proof,"source_receipt_ms":checked_ms+1}
    bad_provenance={**provenance,
                    "feature_sources":{**provenance["feature_sources"],
                                       FEATURE_NAMES[0]:out_of_order}}
    db.row={**row,"provenance":bad_provenance}
    with pytest.raises(ValueError,match="Backdated"):
        verify_persisted_snapshot(db,symbol="ETHUSDT",reason=context,
                                  decision_at=decision)
