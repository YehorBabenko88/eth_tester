"""Regression coverage: read-only F15 telemetry never promotes shadow observations."""
from datetime import datetime, timedelta, timezone
import pytest
from eth_tester.ml.observability import observe_research_window

NOW = datetime(2026, 11, 25, 22, 50, tzinfo=timezone.utc)
V = "eth_observed_minute_shadow_v1"


def make_row(offset=1, decision="research_no_trade"):
    bar = NOW - timedelta(minutes=offset)
    shadow = {
        "contract": "eth_strategy_ensemble_v1", "mode": "shadow_only",
        "approved": False, "paper_entry_authorized": False,
        "ml_eligible": False, "promoted": False,
        "status": "no_trade", "selected": None, "reason": "no_causal_pattern",
    }
    return {
        "symbol": "ETHUSDT", "tf": "1", "bar_ts": bar, "version": V,
        "decision": decision,
        "reason": {
            "contract": V, "bar_ts": bar.isoformat(),
            "observed_at": (bar + timedelta(seconds=75)).isoformat(),
            "independent_of_incumbent": True, "paper_only": True,
            "paper_entry_authorized": False, "ml_eligible": False,
            "research_cohort_complete": False,
            "prospective_source_certified": False,
            "source_identity_authoritative": False,
            "no_historical_backfill": True,
            "unavailable_reason": None,
            "shadow": shadow,
        },
    }


class Cursor:
    def __init__(self, rows):
        self.rows = rows
    def fetchone(self):
        return {"ts": NOW}
    def fetchall(self):
        return self.rows


class ReadOnlyDB:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []
    def execute(self, sql, args=None):
        self.calls.append((sql, args))
        if "clock_timestamp()" in sql:
            return Cursor(self.rows)
        assert sql.lstrip().startswith("SELECT ")
        assert "strategy_signals" in sql and args == (
            "ETHUSDT", NOW - timedelta(minutes=3), NOW)
        return Cursor(self.rows)


def test_every_expected_minute_in_denominator_not_just_candidate():
    db = ReadOnlyDB([make_row(1)])
    r = observe_research_window(db, minutes=3)
    assert (r["expected_closed_minutes"], r["recorded_minutes"],
            r["no_signal"], r["missing_minutes"]) == (3, 1, 1, 2)
    assert r["ml_eligible"] is False and r["oos_eligible"] is False
    assert r["model_promotion_eligible"] is False
    assert len(db.calls) == 2


@pytest.mark.parametrize("mutate", [
    lambda r: r["reason"].update(ml_eligible=True),
    lambda r: r["reason"].update(prospective_source_certified=True),
    lambda r: r["reason"].update(independent_of_incumbent=False),
    lambda r: r["reason"].update(no_historical_backfill=False),
    lambda r: r["reason"].update(observed_at="2000-01-01T00:00:00+00:00"),
    lambda r: r["reason"]["shadow"].update(approved=True),
    lambda r: r["reason"]["shadow"].update(status="shadow_candidates"),
])
def test_forged_or_noncanonical_row_is_malformed(mutate):
    row = make_row()
    mutate(row)
    r = observe_research_window(ReadOnlyDB([row]), minutes=3)
    assert r["malformed"] == 1 and r["no_signal"] == 0
    assert r["ml_eligible"] is False


def test_unavailable_not_no_signal():
    row = make_row(decision="research_unavailable")
    row["reason"]["unavailable_reason"] = "source_gap_in_observed_window"
    row["reason"]["shadow"] = None
    r = observe_research_window(ReadOnlyDB([row]), minutes=3)
    assert r["unavailable"] == 1 and r["no_signal"] == 0


def test_candidate_is_hypothesis_not_training_or_trade():
    row = make_row(decision="research_candidates_unverified")
    row["reason"]["shadow"].update(
        status="shadow_candidates", selected="hypothesis",
        candidates=[{"pattern": "synthetic"}])
    r = observe_research_window(ReadOnlyDB([row]), minutes=3)
    assert r["candidate"] == 1
    assert r["economic_cohort_verified"] is False
    assert r["model_promotion_eligible"] is False


def test_duplicate_minute_aborts_no_silent_dedup():
    with pytest.raises(ValueError, match="duplicate"):
        observe_research_window(ReadOnlyDB([make_row(), make_row()]), minutes=3)


@pytest.mark.parametrize("minutes", [0, -1, 1441, True, 1.5])
def test_unbounded_or_invalid_window_fails_closed(minutes):
    with pytest.raises(ValueError):
        observe_research_window(ReadOnlyDB([]), minutes=minutes)


def test_all_missing_not_converted_to_no_signal():
    r = observe_research_window(ReadOnlyDB([]), minutes=3)
    assert r["no_signal"] == r["candidate"] == 0
    assert r["missing_minutes"] == 3
    assert r["status"].startswith("ML_NO_GO")
