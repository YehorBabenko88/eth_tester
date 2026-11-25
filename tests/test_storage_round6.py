"""Storage B round-6 pure fail-closed regressions; no network or database DDL."""
from datetime import date, datetime, timedelta, timezone

import pytest

from eth_tester.storage.archive_receipts import (
    persist_archive_stage_bins, revalidate_archive_bins,
)
from eth_tester.storage.live_evidence import (
    record_trade_receipts, record_book_lineage,
    record_oi_observation, publish_bounded_paper_evidence,
    require_bounded_paper_evidence,
)
from eth_tester.storage.snapshots import _millis, _ceil_millis, _proof


class NoSql:
    def execute(self, *_args, **_kwargs):
        raise AssertionError("Rejected evidence must not reach PostgreSQL")


def test_historical_receipt_floor_ceil_do_not_backdate_submillisecond():
    moment = datetime(2026, 11, 25, 12, 34, 59, 999001, timezone.utc)
    shifted = moment.astimezone(timezone(timedelta(hours=1)))
    assert _millis(moment) == _millis(shifted)
    assert _ceil_millis(moment) == _millis(moment) + 1
    exact = moment.replace(microsecond=999000)
    assert _ceil_millis(exact) == _millis(exact)


def test_canonical_historical_proof_rejects_foreign_instrument_before_sql():
    args = dict(
        name="oi", symbol="ETHUSDT", instrument="linear",
        available_ms=1000000001000,
        claimed={
            "source": "oi", "interval": "5min", "checksum": "a"*64,
            "source_event_ms": 1000000000000,
            "source_receipt_ms": 1000000000100,
            "source_checked_at_ms": 1000000000200,
            "coverage_status": "verified", "evidence_ref": "pinned",
            "instrument": "inverse",
        },
    )
    with pytest.raises(ValueError, match="instrument|reference"):
        _proof(NoSql(), **args)


@pytest.mark.parametrize("table", ["trades", "eth_trade_stage", "other.eth_stage"])
def test_archive_source_rejects_anything_except_original_stage(table):
    with pytest.raises(ValueError, match="original eth_stage"):
        persist_archive_stage_bins(
            NoSql(), "ETHUSDT", date(2026, 11, 24),
            "a"*64, 100, stage_name=table,
        )


def test_archive_revalidation_rejects_invalid_sha_without_sql():
    with pytest.raises(ValueError, match="SHA256"):
        revalidate_archive_bins(NoSql(), "ETHUSDT", date(2026, 11, 24), "ab")


@pytest.mark.parametrize("ids", [[], ["same", "same"], [""]])
def test_trade_receipt_rejects_empty_or_replayed_packet_before_sql(ids):
    with pytest.raises(ValueError, match="unique"):
        record_trade_receipts(NoSql(), "ETHUSDT", "session-1", ids)


def test_live_book_requires_real_lineage_ids_before_sql():
    with pytest.raises(ValueError, match="lineage"):
        record_book_lineage(NoSql(), "ETHUSDT", "session-1", 5, 4)


def test_rest_oi_receipt_requires_content_sha_before_sql():
    now = datetime.now(timezone.utc)
    with pytest.raises(ValueError, match="SHA256"):
        record_oi_observation(NoSql(), "ETHUSDT", "5min", now, "not-sha")


def test_live_publication_rejects_non_aligned_or_naive_utc_bar():
    now = datetime.now(timezone.utc)
    with pytest.raises(ValueError, match="Natural UTC"):
        publish_bounded_paper_evidence(
            NoSql(), "ETHUSDT", now.replace(second=17), "session-1", 1, 1,
        )
    with pytest.raises(ValueError, match="Timezone-aware"):
        publish_bounded_paper_evidence(
            NoSql(), "ETHUSDT", now.replace(tzinfo=None), "session-1", 1, 1,
        )


def test_live_paper_lookup_rejects_forged_sha_before_sql():
    with pytest.raises(ValueError, match="SHA256"):
        require_bounded_paper_evidence(
            NoSql(), symbol="ETHUSDT", bar_ts=datetime.now(timezone.utc),
            evidence_key="forged", decision_at=datetime.now(timezone.utc),
        )


def test_live_quality_is_structurally_separate_from_historical_v2():
    from importlib.resources import files

    migration = files("eth_tester").joinpath(
        "migrations", "0007_bounded_live_and_archive_bins.sql"
    ).read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS paper_live_evidence" in migration
    assert "eth_paper_live_evidence_v1" in migration
    assert "quality='live_observed_bounded'" in migration
    assert "CHECK(paper_only)" in migration
    assert "CHECK(NOT ml_eligible)" in migration
    assert "CHECK(NOT exchange_complete)" in migration
    assert "CREATE TABLE IF NOT EXISTS archive_source_price_bins" in migration
    assert "CREATE TABLE IF NOT EXISTS archive_validation_receipts" in migration
