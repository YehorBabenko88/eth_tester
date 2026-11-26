"""D paper-only live evidence contract tests. Synthetic rows are NOT Bybit proof."""
from copy import deepcopy
import sys
from types import ModuleType
from datetime import datetime, timedelta, timezone
import pytest

from eth_tester.strategies.paper_live import read_live_evidence, CONTRACT, QUALITY

START = datetime(2026, 11, 25, 12, 34, tzinfo=timezone.utc)
END = START + timedelta(minutes=1)
NOW = END + timedelta(seconds=10)


def _ms(t):
    return int(t.timestamp() * 1000)


def _proof(event, receipt, checked):
    return {
        "source_checksum": "1" * 64, "evidence_ref": "synthetic-only",
        "source_event_ms": _ms(event), "source_receipt_ms": _ms(receipt),
        "source_checked_at_ms": _ms(checked),
    }


def live_row():
    trade = _proof(END - timedelta(seconds=1),
                   END + timedelta(seconds=5), END + timedelta(seconds=6))
    trade.update(session_continuous=True, no_unresolved_gaps=True,
                 session_id="synthetic-session")
    candle = _proof(END, END + timedelta(seconds=1),
                    END + timedelta(seconds=2))
    candle.update(closed=True, base_volume_reconciled=True,
                  quote_volume_reconciled=True)
    book = _proof(END + timedelta(seconds=3),
                  END + timedelta(seconds=5), END + timedelta(seconds=6))
    book.update(snapshot_lineage_verified=True, snapshot_id="synthetic-snapshot")
    oi = _proof(START.replace(minute=30),
                END + timedelta(seconds=1), END + timedelta(seconds=2))
    oi.update(receipt_and_grid_verified=True, interval="5min")
    return {
        "snapshot_key": "2" * 64, "source_checksum": "3" * 64,
        "symbol": "ETHUSDT", "tf": "1", "bar_ts": START,
        "observed_at": END + timedelta(seconds=8),
        "computed_at": END + timedelta(seconds=8),
        "expires_at": END + timedelta(seconds=90),
        "quality": QUALITY, "paper_only": True, "ml_eligible": False,
        "evidence": {
            "contract": CONTRACT, "quality": QUALITY, "paper_only": True,
            "ml_eligible": False, "proves_all_trade_ids": False,
            "source_receipt_ms": _ms(END + timedelta(seconds=5)),
            "source_checked_at_ms": _ms(END + timedelta(seconds=6)),
            "source_proofs": {
                "public_trade": trade, "closed_candle": candle,
                "orderbook": book, "open_interest": oi,
            },
        },
    }


@pytest.fixture(autouse=True)
def b_source_verification_standin(monkeypatch):
    """Strict B-helper stand-in; pure contract tests, not actual source proof."""
    module = ModuleType("eth_tester.storage.live_evidence")

    def require_bounded_paper_evidence(conn, *, symbol, bar_ts, evidence_key,
                                       decision_at):
        row = conn.row
        if row is None or row["snapshot_key"] != evidence_key:
            raise ValueError("no matching B-issued immutable source")
        evidence = row["evidence"]
        proofs = evidence.get("source_proofs", {})
        required = {"public_trade", "closed_candle", "orderbook", "open_interest"}
        if set(proofs) != required:
            raise ValueError("missing original sources")
        for proof in proofs.values():
            event = proof.get("source_event_ms")
            receipt = proof.get("source_receipt_ms")
            checked = proof.get("source_checked_at_ms")
            if (any(type(v) is not int for v in (event, receipt, checked))
                    or not 0 < event <= receipt <= checked <= _ms(row["observed_at"])
                    or not isinstance(proof.get("source_checksum"), str)
                    or len(proof["source_checksum"]) != 64
                    or not proof.get("evidence_ref")):
                raise ValueError("invalid original source")
        trade, candle, book, oi = (proofs[name] for name in (
            "public_trade", "closed_candle", "orderbook", "open_interest"))
        if (trade.get("session_continuous") is not True
                or trade.get("no_unresolved_gaps") is not True
                or not trade.get("session_id")
                or not _ms(START) <= trade["source_event_ms"] < _ms(END)
                or candle.get("closed") is not True
                or candle.get("base_volume_reconciled") is not True
                or candle.get("quote_volume_reconciled") is not True
                or book.get("snapshot_lineage_verified") is not True
                or not book.get("snapshot_id")
                or oi.get("receipt_and_grid_verified") is not True
                or oi.get("interval") != "5min"
                or oi["source_event_ms"] % 300000):
            raise ValueError("incomplete bounded paper sources")
        if (abs(_ms(decision_at) - trade["source_receipt_ms"]) > 15000
                or abs(_ms(decision_at) - book["source_receipt_ms"]) > 10000
                or _ms(decision_at) - oi["source_event_ms"] > 600000):
            raise ValueError("stale observed source")
        if (evidence.get("source_receipt_ms") != max(
                p["source_receipt_ms"] for p in proofs.values())
                or evidence.get("source_checked_at_ms") != max(
                    p["source_checked_at_ms"] for p in proofs.values())):
            raise ValueError("source aggregate mismatch")
        return {
            "snapshot_key": row["snapshot_key"],
            "source_checksum": row["source_checksum"],
            "contract": evidence["contract"], "quality": row["quality"],
            "bar_ts": row["bar_ts"],
            "observed_at": row["observed_at"],
            "computed_at": row["computed_at"],
            "expires_at": row["expires_at"],
            "paper_only": row["paper_only"], "ml_eligible": row["ml_eligible"],
        }

    module.require_bounded_paper_evidence = require_bounded_paper_evidence
    monkeypatch.setitem(sys.modules, module.__name__, module)


class Response:
    def __init__(self, data):
        self.data = data

    def fetchone(self):
        return self.data


class FakeConn:
    def __init__(self, row=None, *, installed=True):
        self.row = deepcopy(row) if row is not None else None
        self.installed = installed
        self.statements = []

    def execute(self, sql, args=None):
        self.statements.append(sql)
        if "to_regclass('public.paper_live_evidence')" in sql:
            return Response({"present": "paper_live_evidence" if self.installed else None})
        if "FROM paper_live_evidence" in sql:
            return Response(self.row)
        raise AssertionError("unexpected source proof SQL")


class NoSql:
    def execute(self, *_args, **_kwargs):
        raise AssertionError("past-deadline decision must not touch database")


def accept(row=None, *, now=NOW):
    return read_live_evidence(
        FakeConn(row if row is not None else live_row()),
        "ETHUSDT", START, END, now)


def test_accepted_row_is_paper_only_without_ml_or_backdating():
    identity = accept()
    assert identity is not None
    assert identity["snapshot_key"] == "2" * 64
    assert identity["contract"] == CONTRACT
    assert identity["paper_only"] is True
    assert identity["ml_eligible"] is False
    assert "ml_snapshot_key" not in identity
    assert "immutable_feature_snapshot" not in identity
    assert identity["computed_at"].startswith("2026-11-25T12:35:08")


def test_no_sql_on_future_or_expired_evaluation():
    for decision in (START, END - timedelta(microseconds=1),
                     END + timedelta(seconds=91),
                     END + timedelta(days=1)):
        assert read_live_evidence(NoSql(), "ETHUSDT", START, END, decision) is None


def test_absent_b_table_and_unissued_row_are_denied():
    assert read_live_evidence(FakeConn(live_row(), installed=False),
                              "ETHUSDT", START, END, NOW) is None
    assert read_live_evidence(FakeConn(), "ETHUSDT", START, END, NOW) is None


@pytest.mark.parametrize("field,value", [
    ("snapshot_key", "not-a-sha"), ("source_checksum", "x"*64),
    ("symbol", "BTCUSDT"), ("tf", "5"),
    ("quality", "complete"), ("paper_only", False), ("ml_eligible", True),
    ("computed_at", NOW + timedelta(microseconds=1)),
    ("expires_at", END + timedelta(seconds=91)),
    ("expires_at", END + timedelta(seconds=9)),
    ("bar_ts", START - timedelta(minutes=1)),
])
def test_invalid_row_identity_quality_and_clocks_are_denied(field, value):
    row = live_row()
    row[field] = value
    assert accept(row) is None


@pytest.mark.parametrize("field,value", [
    ("contract", "eth_feature_snapshot_v2"),
    ("quality", "complete"), ("paper_only", False),
    ("ml_eligible", True), ("proves_all_trade_ids", True),
    ("complete_trade_ids_proven", True),
    ("is_historical_certified", True),
    ("source_receipt_ms", _ms(END + timedelta(seconds=9))),
    ("source_checked_at_ms", _ms(NOW + timedelta(seconds=10))),
])
def test_mutated_aggregate_quality_and_receipt_fail_closed(field, value):
    row = live_row()
    row["evidence"][field] = value
    assert accept(row) is None


@pytest.mark.parametrize("kind,field,value", [
    ("public_trade", "session_continuous", False),
    ("public_trade", "no_unresolved_gaps", False),
    ("public_trade", "session_id", ""),
    ("closed_candle", "closed", False),
    ("closed_candle", "base_volume_reconciled", False),
    ("closed_candle", "quote_volume_reconciled", False),
    ("orderbook", "snapshot_lineage_verified", False),
    ("orderbook", "snapshot_id", ""),
    ("open_interest", "receipt_and_grid_verified", False),
    ("open_interest", "interval", "1"),
    ("orderbook", "source_checksum", ""),
    ("public_trade", "source_receipt_ms", _ms(NOW + timedelta(seconds=1))),
    ("open_interest", "source_checked_at_ms", _ms(NOW + timedelta(seconds=1))),
])
def test_missing_independent_receipt_gap_candle_book_oi_rejected(kind, field, value):
    row = live_row()
    row["evidence"]["source_proofs"][kind][field] = value
    assert accept(row) is None


def test_missing_source_witness_does_not_fallback_to_archive_or_ml():
    row = live_row()
    del row["evidence"]["source_proofs"]["orderbook"]
    assert accept(row) is None
    row = live_row()
    row["evidence"]["source_proofs"]["orderbook"]["evidence_ref"] = ""
    assert accept(row) is None


def test_freshness_gates_l2_and_recent_trade():
    row = live_row()
    row["evidence"]["source_proofs"]["orderbook"]["source_receipt_ms"] = _ms(
        END - timedelta(seconds=20))
    row["evidence"]["source_proofs"]["orderbook"]["source_checked_at_ms"] = _ms(
        END - timedelta(seconds=19))
    row["evidence"]["source_proofs"]["orderbook"]["source_event_ms"] = _ms(
        END - timedelta(seconds=21))
    assert accept(row) is None
    row = live_row()
    assert read_live_evidence(FakeConn(row), "BTCUSDT", START, END, NOW) is None
