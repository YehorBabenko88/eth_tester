"""Round14 C safety tests: bounded live grouping and non-certifying volume parity."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from eth_tester import analytics


STAMP = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)
SETTINGS = SimpleNamespace(symbol="ETHUSDT", category="linear")


class Cursor:
    def __init__(self, *, one=None, rows=()):
        self.one, self.rows = one, list(rows)

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.rows


class FakeDb:
    def __init__(self, *, grouped=(), archive=None, existing_quality="partial_unverified",
                 candle_volume=Decimal("5"), observed_volume=Decimal("5")):
        self.grouped = list(grouped)
        self.archive = archive
        self.existing_quality = existing_quality
        self.candle_volume = candle_volume
        self.observed_volume = observed_volume
        self.calls = []
        self.updated = None
        self.mutations = []

    def execute(self, statement, params=None):
        self.calls.append((statement, params))
        sql = " ".join(statement.split())
        if "SET LOCAL" in sql or "pg_advisory_xact_lock" in sql:
            return Cursor()
        if "clock_timestamp() AS ts" in sql:
            return Cursor(one={"ts": STAMP + timedelta(minutes=1)})
        if "FROM trades t" in sql:
            assert params[-1] == analytics.MAX_LIVE_FOOTPRINT_GROUPS + 1
            return Cursor(rows=self.grouped[:params[-1]])
        if "SELECT 1 FROM aggregation_job" in sql:
            return Cursor(one=None)
        if "SELECT ts,delta,volume,quality FROM features" in sql:
            return Cursor(rows=[{"ts": STAMP, "delta": Decimal("1"),
                                 "volume": self.observed_volume,
                                 "quality": self.existing_quality}])
        if "SELECT ts,cvd FROM features" in sql or "SELECT 1 FROM features" in sql:
            return Cursor(one=None)
        if "FROM candles" in sql:
            return Cursor(one={"volume": self.candle_volume} if self.candle_volume is not None else None)
        if "FROM open_interest" in sql or "FROM funding" in sql:
            return Cursor(one=None)
        if "FROM market_book_state" in sql:
            return Cursor(one=None)
        if "FROM ingestion_job i" in sql:
            return Cursor(one=self.archive)
        if sql.startswith("UPDATE features SET cvd"):
            self.updated = params
            self.mutations.append(sql)
            return Cursor()
        if sql.startswith("DELETE") or sql.startswith("INSERT"):
            self.mutations.append(sql)
            return Cursor()
        raise AssertionError("Unexpected SQL: " + sql[:120])


def bind(monkeypatch, db):
    @contextmanager
    def fake_connection(settings):
        assert settings is SETTINGS
        yield db
    monkeypatch.setattr(analytics, "connection", fake_connection)


def group(price, side="Buy"):
    return {"bucket": STAMP, "price": Decimal(price), "side": side,
            "qty": Decimal("1"), "prints": 1}


def test_group_budget_fails_before_any_row_mutation(monkeypatch):
    db = FakeDb(grouped=[group("100"), group("101"), group("102")])
    bind(monkeypatch, db)
    monkeypatch.setattr(analytics, "MAX_LIVE_FOOTPRINT_GROUPS", 2)
    with pytest.raises(RuntimeError, match="live_footprint_group_budget_exceeded"):
        analytics.build_footprint(SETTINGS, tf="1", hours=2)
    assert not db.mutations
    assert any("SET LOCAL lock_timeout" in x for x, _ in db.calls)
    assert any("SET LOCAL statement_timeout" in x for x, _ in db.calls)


def test_no_group_truncation_at_budget(monkeypatch):
    db = FakeDb(grouped=[group("100"), group("101")])
    bind(monkeypatch, db)
    monkeypatch.setattr(analytics, "MAX_LIVE_FOOTPRINT_GROUPS", 2)
    assert analytics.build_footprint(SETTINGS, tf="1", hours=2) == 1
    assert sum("INSERT INTO footprint(" in x for x in db.mutations) == 2
    assert any("partial_unverified" in x for x in db.mutations)


@pytest.mark.parametrize("hours", [0, 25, 1.5, True, None])
def test_invalid_live_footprint_hours_rejected_before_db(monkeypatch, hours):
    def deny(_):
        raise AssertionError("Must not contact DB")
    monkeypatch.setattr(analytics, "connection", deny)
    with pytest.raises(ValueError, match="Live footprint hours"):
        analytics.build_footprint(SETTINGS, hours=hours)


@pytest.mark.parametrize("archive,prior,candle,observed,expected_quality,expected_status", [
    (None, "partial_unverified", Decimal("5"), Decimal("5"),
     "partial_unverified", "not_comparable"),
    ({"state": "complete", "sha256": "a"*64,
      "stage_sha_present": False, "rollup_sha_present": True},
     "partial_unverified", Decimal("5"), Decimal("5"),
     "partial_unverified", "archive_sha_membership_missing"),
    ({"state": "complete", "sha256": "a"*64,
      "stage_sha_present": True, "rollup_sha_present": False},
     "archive_rollup_partial", Decimal("5"), Decimal("5"),
     "partial_unverified", "archive_sha_membership_missing"),
    ({"state": "complete", "sha256": "a"*64,
      "stage_sha_present": True, "rollup_sha_present": True},
     "partial_unverified", Decimal("5"), Decimal("5"),
     "volume_parity_only_unverified", "parity_only_not_trade_id_completeness"),
    ({"state": "complete", "sha256": "a"*64,
      "stage_sha_present": True, "rollup_sha_present": True},
     "archive_rollup_partial", Decimal("5"), Decimal("5"),
     "archive_rollup_partial", "parity_only_not_trade_id_completeness"),
    ({"state": "complete", "sha256": "a"*64,
      "stage_sha_present": True, "rollup_sha_present": True},
     "archive_rollup_partial", Decimal("6"), Decimal("5"),
     "volume_mismatch", "mismatch"),
    ({"state": "complete", "sha256": "a"*64,
      "stage_sha_present": True, "rollup_sha_present": True},
     "partial_unverified", Decimal("NaN"), Decimal("5"),
     "volume_mismatch", "invalid_quantity"),
])
def test_parity_label_never_certifies_exchange_trade_ids(
        monkeypatch, archive, prior, candle, observed, expected_quality, expected_status):
    db = FakeDb(archive=archive, existing_quality=prior,
                candle_volume=candle, observed_volume=observed)
    bind(monkeypatch, db)
    assert analytics.enrich_features(SETTINGS, tf="1", hours=2) == 1
    assert db.updated[-5] == expected_quality
    import json
    meta = json.loads(db.updated[-4])
    assert meta["volume_check"] == expected_status
    assert meta["volume_exchange_complete"] is False
    assert expected_quality != "verified_volume"
