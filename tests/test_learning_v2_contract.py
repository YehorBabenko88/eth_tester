"""F round4: synthetic contract regressions; these are NOT market certificates."""
from datetime import datetime, timedelta, timezone
from copy import deepcopy

import pytest

from eth_tester.ml import FEATURE_NAMES
from eth_tester.ml.contract import validate_label, validate_snapshot
from eth_tester.ml.extract import from_snapshot_records


def sample():
    start = datetime(2026, 11, 20, 10, tzinfo=timezone.utc)
    def at(m):
        return start + timedelta(minutes=m)
    def ms(m):
        return int(at(m).timestamp() * 1000)

    source = {"source": "qa_archive_only", "instrument": "linear",
              "interval": "", "checksum": "c"*64,
              "coverage_evidence_sha256": "d"*64,
              "source_event_ms": ms(0), "source_receipt_ms": ms(1),
              "source_checked_at_ms": ms(2), "coverage_status": "verified",
              "evidence_ref": "QA-only:synthetic-verification"}
    provenance = {
        "contract": "eth_feature_snapshot_v2",
        "certified_by": "independent_source_proof",
        "coverage_status": "verified", "source_coverage_verified": True,
        "source_receipt_ms": ms(1), "source_checked_at_ms": ms(2),
        "feature_available_ms": dict.fromkeys(FEATURE_NAMES, ms(3)),
        "feature_sources": {name: dict(source) for name in FEATURE_NAMES},
    }
    reason = {
        "ml_snapshot_key": "a"*64, "ml_source_checksum": "b"*64,
        "ml_signal_decision_ms": ms(10),
        "orderflow": {"immutable_feature_snapshot": {
            "snapshot_key": "a"*64, "source_checksum": "b"*64}},
    }
    # Synthetic positive control only; never independently attested market evidence.
    reason["paper_cost_policy"] = {"version": "paper_taker_cost_v1", "notional_usd": "1000"}
    label = {
        "paper_trade_id": 17, "snapshot_key": "a"*64, "source_checksum": "b"*64,
        "method": "fixed_horizon_barrier_v1", "outcome": "bounce",
        "horizon_end_at": at(21).isoformat(), "issued_at": at(22).isoformat(),
        "label_source": "qa_closed_bars", "label_interval": "1",
        "label_instrument": "linear", "covered_from_at": at(10).isoformat(),
        "coverage_sha256": "e"*64,
        "source_receipt_at": at(21).isoformat(),
        "coverage_checked_at": at(22).isoformat(),
        "source_evidence": {"complete_closed_bar_grid": True,
                            "expected_bars": 5, "verified_bars": 5,
                            "barrier_evidence_sha256": "f"*64},
    }
    row = {
        "paper_trade_id": 17, "reason": reason,
        "snapshot_key": "a"*64, "source_checksum": "b"*64,
        "decision_at": at(10), "bar_ts": at(0),
        "available_at": at(4), "observed_at": at(5),
        "computed_at": at(6), "opened_at": at(11), "closed_at": at(16),
        "snapshot_quality": "complete", "is_causal": True,
        "values_json": dict.fromkeys(FEATURE_NAMES, "1.25"),
        "provenance": provenance,
        "label_ledger": label, "notional": 1000,
        "gross_pnl": 12, "fees": 2, "funding": 1, "net_pnl": 9,
        "funding_verified": True,
        "execution_quality": "trade_print_proxy_no_liquidity",
        "execution_events": [
            {"action": "entry", "fee": "1", "source": "paper_proxy",
             "quality": "trade_print_proxy_no_liquidity",
             "provenance": {"paper_only": True}},
            {"action": "exit", "fee": "1", "source": "paper_proxy",
             "quality": "trade_print_proxy_no_liquidity",
             "provenance": {"fee_quality": "entry_time_cost_policy_pinned",
                            "funding_source_db_rechecked": True,
                            "funding_schedule_complete": True}},
        ],
    }
    return row, ms(22)


def test_canonical_v2_snapshot_and_post_horizon_dataset():
    row, cutoff = sample()
    values, times, decision = validate_snapshot(row, row["reason"])
    assert len(values) == len(times) == 9
    assert decision == row["reason"]["ml_signal_decision_ms"]
    assert validate_label(row, row["reason"], cutoff, int(row["closed_at"].timestamp()*1000),
                          17)[0] == "bounce"
    accepted, reject = from_snapshot_records([row], cutoff)
    assert len(accepted) == 1 and reject == {}
    assert accepted[0].net_return == pytest.approx(.009)


@pytest.mark.parametrize("modification", [
    lambda r: r["provenance"].update(contract="eth_feature_snapshot_v1"),
    lambda r: r["provenance"].update(certified_by="none"),
    lambda r: r["provenance"].update(source_coverage_verified=False),
    lambda r: r["provenance"]["feature_sources"]["oi"].update(coverage_status="partial"),
    lambda r: r["provenance"]["feature_sources"]["cvd"].update(source_receipt_ms=True),
    lambda r: r["provenance"]["feature_sources"]["poc"].update(
        source_checked_at_ms=99999999999999),
    lambda r: r["reason"].update(ml_snapshot_key="f"*64),
    lambda r: r["reason"]["orderflow"]["immutable_feature_snapshot"].update(
        source_checksum="f"*64),
    lambda r: r.update(computed_at=r["decision_at"]+timedelta(seconds=1)),
])
def test_reject_uncertified_or_future_snapshot(modification):
    row, cutoff = sample()
    modification(row)
    accepted, rejects = from_snapshot_records([row], cutoff)
    assert accepted == [] and sum(rejects.values()) == 1


@pytest.mark.parametrize("modification", [
    lambda r: r.update(label_ledger=None),
    lambda r: r["label_ledger"].update(snapshot_key="f"*64),
    lambda r: r["label_ledger"].update(coverage_sha256="invalid"),
    lambda r: r["label_ledger"].update(outcome="trade_win"),
    lambda r: r["label_ledger"]["source_evidence"].update(verified_bars=4),
    lambda r: r["label_ledger"].update(issued_at="2030-01-01T00:00:00+00:00"),
    lambda r: r["label_ledger"].update(source_receipt_at="2020-01-01T00:00:00+00:00"),
])
def test_reject_incomplete_or_tampered_label(modification):
    row, cutoff = sample()
    modification(row)
    accepted, rejects = from_snapshot_records([row], cutoff)
    assert accepted == [] and sum(rejects.values()) == 1


def test_post_horizon_issue_time_and_missing_label_diagnostics():
    good, cutoff = sample()
    missing = deepcopy(good)
    missing["paper_trade_id"] = 18
    missing["label_ledger"] = None
    good_first, errors = from_snapshot_records([good, missing], cutoff)
    assert len(good_first) == 1 and sum(errors.values()) == 1
    earlier, errors = from_snapshot_records([good], cutoff-1)
    assert earlier == [] and sum(errors.values()) == 1


def test_paper_nominal_cost_proof_and_duplicate_event_ids():
    good, cutoff = sample()
    bad = deepcopy(good)
    bad["notional"] = 1001
    assert from_snapshot_records([bad], cutoff)[0] == []
    bad = deepcopy(good)
    bad["net_pnl"] = 10
    assert from_snapshot_records([bad], cutoff)[0] == []
    with pytest.raises(ValueError, match="duplicate"):
        from_snapshot_records([good, deepcopy(good)], cutoff)


def test_historic_exchange_timestamp_cannot_stand_in_for_decision():
    row, cutoff = sample()
    row["decision_at"] = row["bar_ts"] + timedelta(seconds=30)
    row["reason"]["ml_signal_decision_ms"] = int(row["decision_at"].timestamp()*1000)
    accepted, errors = from_snapshot_records([row], cutoff)
    assert accepted == [] and sum(errors.values()) == 1


@pytest.mark.parametrize("mutate", [
    lambda row: row.update(funding_verified=False),
    lambda row: row["execution_events"][1]["provenance"].update(
        funding_schedule_complete=False),
    lambda row: row["execution_events"][0].update(fee="NaN"),
    lambda row: row["reason"]["paper_cost_policy"].update(notional_usd="999"),
])
def test_synthetic_cost_control_rejects_provisional_and_inconsistent(mutate):
    row, cutoff = sample()
    mutate(row)
    accepted, rejected = from_snapshot_records([row], cutoff)
    assert accepted == [] and sum(rejected.values()) == 1
