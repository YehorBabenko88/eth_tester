"""Round 8 prospective bounded-paper evidence gate: deterministic unit regressions.

Fixtures are fabricated solely to test rejecting stale/forged receipts and
never claim to be real exchange evidence. PG17/18 CI integration is separate.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from eth_tester.storage.admission import _digest, recheck_admission_sources
from eth_tester.storage.live_evidence import _sha, require_bounded_paper_evidence

UTC = timezone.utc
BAR = datetime(2026, 11, 25, 12, 34, tzinfo=UTC)
END = BAR + timedelta(minutes=1)
NOW = END + timedelta(seconds=10)
PUB = NOW - timedelta(seconds=2)
DECISION = NOW - timedelta(seconds=1)
SESSION = "session-actual-ws-r8"
SYMBOL = "ETHUSDT"


class Result:
    def __init__(self, value):
        self.value = value

    def fetchone(self):
        return self.value


class AdmittedDB:
    """Minimal SQL dispatch, with a real sequence of two sampled DB clocks."""
    def __init__(self):
        self.clock = NOW
        self.disconnected = False
        self.new_disconnect = False
        self.gap = False
        self.new_gap = False
        self.book_valid = True
        self.book_stale = False
        self.oi_stale = False
        self.changed_candle = False
        self.changed_tape = False
        self.changed_oi = False
        self.clock_calls = 0
        self.queries = []
        self.candle = {
            "open": Decimal("100"), "high": Decimal("101"),
            "low": Decimal("99"), "close": Decimal("100"),
            "volume": Decimal("1"), "turnover": Decimal("100"),
            "source": "bybit_v5",
        }
        prov = {
            "contract": "eth_paper_live_evidence_v1",
            "quality": "live_observed_bounded",
            "paper_only": True, "ml_eligible": False,
            "exchange_complete": False,
            "closed_candle_sha256": _digest([
                str(self.candle[k]) for k in
                ("open", "high", "low", "close", "volume", "turnover", "source")
            ]),
            "oi_source_checksum": "c"*64,
        }
        checksum = _sha(prov)
        key = _sha(["eth_paper_live_evidence_v1", SYMBOL,
                    BAR.isoformat(), checksum])
        self.key = key
        self.row = {
            "snapshot_key": key,
            "source_checksum": checksum,
            "contract": "eth_paper_live_evidence_v1",
            "symbol": SYMBOL, "tf": "1", "bar_ts": BAR,
            "bar_close_at": END, "observed_at": PUB,
            "computed_at": PUB, "expires_at": END+timedelta(seconds=90),
            "quality": "live_observed_bounded", "paper_only": True,
            "ml_eligible": False, "exchange_complete": False,
            "is_ml_certified": False, "is_historical_certified": False,
            "trade_session": SESSION, "trade_count": 2,
            "book_receipt_id": 7, "oi_receipt_id": 8,
            "base_volume": Decimal("1"), "quote_turnover": Decimal("100"),
            "trade_last_receipt_at": PUB,
            "evidence": prov,
        }

    def execute(self, query, params=()):
        self.queries.append((query, params))
        if "pg_advisory_xact_lock" in query:
            assert params == ("eth_paper_admission_source:" + SYMBOL,)
            return Result(None)
        if "SELECT clock_timestamp()" in query:
            self.clock_calls += 1
            return Result({"observed_at": self.clock})
        if "FROM paper_live_evidence" in query:
            return Result(self.row)
        if "topic='connection' AND status='disconnected'" in query:
            return Result({"one": 1} if self.new_disconnect else None)
        if "FROM feed_health" in query:
            return Result({
                "status": "disconnected" if self.disconnected else "connected",
                "ts": PUB, "detail": {"session_id": SESSION},
            })
        if "FROM data_gaps" in query:
            if "detected_at" in query:
                return Result({"one": 1} if self.new_gap else None)
            return Result({"one": 1} if self.gap else None)
        if "FROM candles" in query:
            bar = dict(self.candle)
            if self.changed_candle:
                bar["high"] += Decimal("0.01")
            return Result(bar)
        if "FROM live_trade_receipts" in query:
            if "max(source_event_at)" in query:
                return Result({"source_at": NOW, "received_at": NOW})
            return Result({
                "n": 3 if self.changed_tape else 2,
                "base": Decimal("1"), "quote": Decimal("100"),
                "sessions": 1, "session": SESSION,
            })
        if "FROM trades" in query:
            return Result({"n": 2, "base": Decimal("1"),
                           "quote": Decimal("100")})
        if "FROM live_book_receipts WHERE id=" in query:
            return Result({"symbol": SYMBOL, "session_id": SESSION,
                           "event_id": 10, "received_at": PUB})
        if "FROM live_book_receipts WHERE symbol=" in query:
            return Result({"symbol": SYMBOL, "session_id": SESSION,
                           "event_id": 11,
                           "received_at": NOW-timedelta(seconds=11)
                              if self.book_stale else NOW})
        if "FROM market_book_state" in query:
            return Result({"last_event_id": 11,
                           "valid": self.book_valid,
                           "updated_at": NOW-timedelta(seconds=11)
                             if self.book_stale else NOW})
        if "FROM live_oi_receipts" in query:
            return Result({
                "symbol": SYMBOL, "instrument": "linear", "interval": "5min",
                "source_event_at": NOW-timedelta(minutes=11)
                    if self.oi_stale else NOW-timedelta(minutes=1),
                "received_at": PUB, "oi_value": Decimal("1234"),
                "response_sha256": "c"*64,
            })
        if "FROM open_interest" in query:
            return Result({"value": Decimal("0") if self.changed_oi
                           else Decimal("1234")})
        raise AssertionError("Unrecognized SQL: " + query)


def test_original_b_receipt_with_fresh_live_sources_is_paper_only():
    db = AdmittedDB()
    result = require_bounded_paper_evidence(
        db, symbol=SYMBOL, bar_ts=BAR, evidence_key=db.key,
        decision_at=DECISION, at_admission=True,
    )
    assert result["snapshot_key"] == db.key
    assert result["quality"] == "live_observed_bounded"
    assert result["ml_eligible"] is False
    assert result["exchange_complete"] is False
    assert db.clock_calls >= 2
    assert any("FOR SHARE" in q for q, _ in db.queries)
    assert all(not q.strip().upper().startswith(("INSERT","UPDATE","DELETE"))
               for q, _ in db.queries)


@pytest.mark.parametrize("mutant,reason", [
    ("disconnected", "WebSocket"),
    ("new_disconnect", "disconnected"),
    ("gap", "gap"),
    ("new_gap", "interruption"),
    ("book_valid", "L2"),
    ("book_stale", "L2"),
    ("oi_stale", "OI"),
    ("changed_candle", "candle"),
    ("changed_tape", "minute"),
    ("changed_oi", "OI"),
])
def test_stored_immutable_proof_cannot_outrun_live_source_changes(mutant, reason):
    db = AdmittedDB()
    setattr(db, mutant, False if mutant == "book_valid" else True)
    with pytest.raises(ValueError, match=reason):
        require_bounded_paper_evidence(
            db, symbol=SYMBOL, bar_ts=BAR, evidence_key=db.key,
            decision_at=DECISION, at_admission=True,
        )


def test_at_admission_rejects_future_or_old_original_decision():
    db = AdmittedDB()
    with pytest.raises(ValueError, match="clock"):
        recheck_admission_sources(
            db, row=db.row, symbol=SYMBOL, bar_start=BAR,
            decision=NOW+timedelta(seconds=2), at_admission=True,
        )
    with pytest.raises(ValueError, match="clock"):
        recheck_admission_sources(
            db, row=db.row, symbol=SYMBOL, bar_start=BAR,
            decision=NOW-timedelta(seconds=6), at_admission=True,
        )


def test_expired_window_is_not_reopened_by_new_ws_data():
    db = AdmittedDB()
    db.clock = END + timedelta(seconds=91)
    with pytest.raises(ValueError, match="clock"):
        require_bounded_paper_evidence(
            db, symbol=SYMBOL, bar_ts=BAR, evidence_key=db.key,
            decision_at=DECISION, at_admission=True,
        )


def test_retrospective_source_evidence_cannot_modify_key_or_checksum():
    db = AdmittedDB()
    db.row["evidence"]["closed_candle_sha256"] = "f"*64
    with pytest.raises(ValueError, match="Tampered"):
        require_bounded_paper_evidence(
            db, symbol=SYMBOL, bar_ts=BAR, evidence_key=db.key,
            decision_at=DECISION, at_admission=True,
        )
