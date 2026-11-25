"""Round-2 snapshot publication certificate and causal availability tests."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from eth_tester.analytics_core import FRAMES, close_time
from eth_tester.snapshot_producer import (
    CertificationRejected, coverage_certificate, certified_values,
)


UTC = timezone.utc
SHA = "a" * 64


def example(first=None, minutes=1):
    first = first or datetime(2026, 1, 1, 23, 59, tzinfo=UTC)
    end = first + timedelta(minutes=minutes)
    midnight = first.replace(hour=0, minute=0, second=0, microsecond=0)
    checked = midnight + timedelta(days=1)
    coverage = [{
        "start_ts": midnight, "end_ts": midnight + timedelta(days=1),
        "status": "verified", "checksum": SHA, "observed_rows": 3,
        "expected_rows": 3, "checked_at": checked,
        "evidence": {
            "gzip_crc_checked": True, "trade_identity_checked": True,
            "volume_verified": True, "download_sha256": SHA,
        },
    }]
    jobs = {midnight.date(): {"state": "complete", "sha256": SHA}}
    checks = {(midnight.date(), SHA): {
        "total": minutes, "matched": minutes, "unique_minutes": minutes,
        "latest_checked": checked,
    }}
    return first, end, checked + timedelta(minutes=5), coverage, jobs, checks


def test_complete_source_proof_has_deterministic_manifest_hash():
    args = example()
    digest, manifest = coverage_certificate(*args)
    assert len(digest) == 64
    assert len(manifest) == 1
    assert digest == coverage_certificate(*args)[0]
    assert manifest[0]["minute_checks"] == 1


def test_reject_unverified_or_missing_identity_even_with_matched_volume():
    start, end, stamp, rows, jobs, checks = example()
    rows[0]["status"] = "partial"
    with pytest.raises(CertificationRejected, match="coverage_not_independently"):
        coverage_certificate(start, end, stamp, rows, jobs, checks)
    rows[0]["status"] = "verified"
    rows[0]["evidence"]["trade_identity_checked"] = False
    with pytest.raises(CertificationRejected, match="coverage_not_independently"):
        coverage_certificate(start, end, stamp, rows, jobs, checks)


def test_reject_minute_holes_and_stale_archive_checksum():
    start, end, stamp, rows, jobs, checks = example()
    checks[(start.date(), SHA)]["matched"] = 0
    with pytest.raises(CertificationRejected, match="minute_volume_grid"):
        coverage_certificate(start, end, stamp, rows, jobs, checks)
    checks[(start.date(), SHA)]["matched"] = 1
    jobs[start.date()]["sha256"] = "b" * 64
    with pytest.raises(CertificationRejected, match="archive_job_checksum_drift"):
        coverage_certificate(start, end, stamp, rows, jobs, checks)


def test_reject_future_check_and_day_that_has_not_closed():
    start, end, stamp, rows, jobs, checks = example()
    rows[0]["checked_at"] = stamp + timedelta(seconds=1)
    with pytest.raises(CertificationRejected, match="future_archive_evidence"):
        coverage_certificate(start, end, stamp, rows, jobs, checks)
    rows[0]["checked_at"] = stamp
    with pytest.raises(CertificationRejected, match="unclosed_or_future_source_window"):
        coverage_certificate(start, end, start + timedelta(seconds=30),
                             rows, jobs, checks)


def test_certificate_stitches_full_utc_week_without_backdating():
    start = datetime(2026, 1, 5, tzinfo=UTC)
    end = close_time(start, "W")
    sampled = end + timedelta(minutes=1)
    rows = []
    jobs = {}
    checks = {}
    for n in range(7):
        day = start + timedelta(days=n)
        digest = f"{n + 1:064x}"
        rows.append({
            "start_ts": day, "end_ts": day + timedelta(days=1),
            "status": "verified", "checksum": digest,
            "observed_rows": 100, "expected_rows": 100,
            "checked_at": sampled, "evidence": {
                "gzip_crc_checked": True, "trade_identity_checked": True,
                "volume_verified": True, "download_sha256": digest,
            },
        })
        jobs[day.date()] = {"state": "complete", "sha256": digest}
        checks[(day.date(), digest)] = {
            "total": 1440, "matched": 1440,
            "unique_minutes": 1440, "latest_checked": sampled,
        }
    digest, manifest = coverage_certificate(start, end, sampled, rows, jobs, checks)
    assert len(manifest) == 7
    assert len(digest) == 64
    rows.pop()
    with pytest.raises(CertificationRejected, match="unverified_or_overlapping"):
        coverage_certificate(start, end, sampled, rows, jobs, checks)


def test_trading_indicator_field_contract_and_fresh_l2():
    start, end, sampled, _, _, _ = example()
    source = "f" * 64
    feat = {
        "quality": "archive_rollup_partial", "poc": Decimal(100),
        "volume": Decimal(3), "delta": Decimal(1), "cvd": Decimal(9),
        "tape_tps": Decimal("0.05"), "market_close": Decimal("100.5"),
        "metadata": {
            "asof_close": end.isoformat(),
            "cvd_scope": "verified_window",
            "cvd_completeness": "verified",
            "cvd_source_sha256": source,
            "realized_volatility": "0.12",
        },
    }
    footprints = [{
        "price": Decimal(100), "buy_qty": Decimal(2),
        "sell_qty": Decimal(1), "trades": 3,
    }]
    level = {
        "id": 7, "source": "causal_closed_pivot",
        "level_price": Decimal(102), "confirmed_at": end,
    }
    book = {
        "valid": True, "bids": {"99": "10"}, "asks": {"101": "5"},
        "updated_at": sampled - timedelta(seconds=2),
    }
    oi = [
        {"ts": sampled - timedelta(minutes=3), "value": Decimal(100)},
        {"ts": sampled - timedelta(minutes=8), "value": Decimal(90)},
    ]
    values, provenance = certified_values(
        feat, footprints, level, book, oi, source, start, end, sampled)
    assert set(values) == {
        "oi", "cvd", "delta", "poc", "footprint", "tape_speed",
        "book_imbalance", "level_distance", "volatility",
    }
    assert values["oi"] == "10"
    assert values["level_distance"] == "1.5"
    assert Decimal(values["book_imbalance"]) == Decimal(1) / 3
    assert provenance["level_id"] == 7
    book["updated_at"] = sampled - timedelta(minutes=1)
    with pytest.raises(CertificationRejected, match="book_stale"):
        certified_values(feat, footprints, level, book, oi,
                         source, start, end, sampled)


def test_missing_cvd_certificate_never_becomes_ml_eligible():
    start, end, sampled, _, _, _ = example()
    feat = {"quality": "complete", "metadata": {"asof_close": end.isoformat()}}
    with pytest.raises(CertificationRejected, match="cvd_lineage_not_certified"):
        certified_values(feat, [], None, None, [], SHA, start, end, sampled)


@pytest.mark.parametrize("tf", FRAMES)
def test_all_thirteen_close_boundaries_are_utc(tf):
    start = datetime(2026, 2, 1, tzinfo=UTC)
    if tf == "W":
        start = datetime(2026, 2, 2, tzinfo=UTC)
    assert close_time(start, tf) > start
    assert close_time(start, tf).tzinfo == UTC



def test_round4_no_direct_snapshot_insert_code_path():
    import inspect
    from eth_tester import snapshot_producer as producer

    assert "INSERT INTO feature_snapshots" not in inspect.getsource(
        producer.certify_snapshot)
    assert "write_feature_snapshot(" in inspect.getsource(
        producer.certify_snapshot)


def test_round4_missing_independent_oi_or_book_witness_rejects():
    from eth_tester.snapshot_producer import _independent_sources

    start, end, sampled, _, _, _ = example()
    class NoSQL:
        def execute(self, *args, **kwargs):
            raise AssertionError("No certificate lookup from mutable flags")
    class Settings:
        symbol = "ETHUSDT"
        category = "linear"
    with pytest.raises(CertificationRejected, match="independent_nine_feature"):
        _independent_sources(
            NoSQL(), Settings(), {"source_coverage_verified": True},
            start, end, sampled, [], [], {}, {})


def test_round4_all_nine_witnesses_require_database_receipt():
    from eth_tester.snapshot_producer import _independent_sources
    from eth_tester.ml import FEATURE_NAMES

    start, end, sampled, _, _, _ = example()
    sampled += timedelta(minutes=10)
    seen = []
    witness_time = int((sampled-timedelta(minutes=2)).timestamp()*1000)
    checked = sampled-timedelta(minutes=1)
    proof_hash = "c"*64
    class DB:
        def execute(self, sql, args):
            seen.append((sql, args))
            return type("Cursor", (), {"fetchall": lambda self: [{
                "checked_at": checked, "status": "verified",
                "evidence": {
                    "source_receipt_ms": witness_time,
                    "evidence_ref": "original-ingest-0",
                    "receipt_is_independently_recorded": True,
                    "proves_all_trade_ids": True,
                    "orderbook_replay_complete": True,
                    "oi_receipt_and_grid_verified": True,
                },
            }]})()
    class Settings:
        symbol = "ETHUSDT"
        category = "linear"
    hints = {}
    for name in FEATURE_NAMES:
        source = {
            "oi": "open_interest", "book_imbalance": "orderbook",
            "level_distance": "candles", "volatility": "candles",
        }.get(name, "trades_archive")
        hints[name] = {
            "source": source, "interval": "" if source=="trades_archive" else "1",
            "checksum": proof_hash, "evidence_ref": "original-ingest-0",
            "source_receipt_ms": witness_time,
            "source_checked_at_ms": int(checked.timestamp()*1000),
        }
    oi = [{"ts": start, "value": Decimal(11)}]
    book = {"updated_at": start, "valid": True}
    level = {"confirmed_at": start}
    sources, available = _independent_sources(
        DB(), Settings(), {"independent_feature_sources": hints},
        start, end, sampled,
        [{"archive_sha256": proof_hash}], oi, book, level)
    assert set(sources) == set(FEATURE_NAMES)
    assert len(seen) == len(FEATURE_NAMES)
    assert all(available[name] >= int(end.timestamp()*1000)
               for name in FEATURE_NAMES)
    # An independently checked receipt cannot be backdated or fabricated from
    # a historical exchange timestamp.
    hints["oi"]["source_receipt_ms"] = int(start.timestamp()*1000)
    with pytest.raises(CertificationRejected, match="source_receipt_not"):
        _independent_sources(
            DB(), Settings(), {"independent_feature_sources": hints},
            start, end, sampled, [{"archive_sha256": proof_hash}],
            oi, book, level)


def test_round4_reject_legacy_writer_contract_even_if_coverage_boolean_true():
    from eth_tester.snapshot_producer import _verify_b_writer_v2

    start, _, _, _, _, _ = example()
    class DB:
        def execute(self, *args, **kwargs):
            return type("Cursor", (), {"fetchone": lambda self: {
                "source_checksum": SHA, "quality": "complete",
                "is_causal": True, "available_at": start,
                "observed_at": start, "computed_at": start,
                "provenance": {
                    "contract": "eth_feature_snapshot_v1",
                    "source_coverage_verified": True,
                },
            }})()
    with pytest.raises(CertificationRejected, match="canonical_storage_v2"):
        _verify_b_writer_v2(DB(), "historic-key", SHA)


def test_round4_canonical_writer_postcondition_enforces_real_clock_order():
    from eth_tester.snapshot_producer import _verify_b_writer_v2
    from eth_tester.ml import FEATURE_NAMES

    start, _, _, _, _, _ = example()
    state = {
        "source_checksum": SHA, "quality": "complete", "is_causal": True,
        "available_at": start, "observed_at": start+timedelta(seconds=1),
        "computed_at": start+timedelta(seconds=2),
        "provenance": {
            "contract": "eth_feature_snapshot_v2",
            "source_coverage_verified": True, "certified_by": "storage_b",
            "source_receipt_ms": int(start.timestamp()*1000),
            "source_checked_at_ms": int(start.timestamp()*1000),
            "feature_sources": {name: {"checksum": SHA}
                                for name in FEATURE_NAMES},
        },
    }
    class DB:
        def execute(self, *args, **kwargs):
            return type("Cursor", (), {"fetchone": lambda self: state})()
    assert _verify_b_writer_v2(DB(), "key", SHA) is state
    state["computed_at"] = start-timedelta(microseconds=1)
    with pytest.raises(CertificationRejected, match="snapshot_observation_clock"):
        _verify_b_writer_v2(DB(), "key", SHA)
