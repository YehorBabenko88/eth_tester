"""Simulation E round 8: prospective source and atomic decision regressions.

Synthetic fixtures are negative contract tests, NOT captured exchange evidence.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import types

import pytest

from test_simulation_runner import Result, load_runner
from test_simulation_live_round6 import (
    BAR, END, DECISION, HASH, KEY, EvidenceDB, d_reason,
)
from eth_tester.simulation.live_evidence import (
    bind_live_paper_reference, verify_live_paper_row,
)
from eth_tester.strategies import Parameters, strategy_identity


def _row():
    return {
        "snapshot_key": KEY, "source_checksum": HASH, "symbol": "ETHUSDT",
        "tf": "1", "bar_ts": BAR,
        "observed_at": END + timedelta(seconds=10),
        "computed_at": END + timedelta(seconds=15),
        "expires_at": END + timedelta(seconds=90),
        "quality": "live_observed_bounded", "paper_only": True,
        "ml_eligible": False,
        "evidence": {"contract": "eth_paper_live_evidence_v1"},
    }


def _b_verified(**changes):
    result = dict(_row())
    result.update({
        "contract": "eth_paper_live_evidence_v1",
        "exchange_complete": False,
    })
    result.update(changes)
    return result


@pytest.mark.parametrize("failure", [
    "new_ws_disconnect", "new_public_trade_gap",
    "new_book_gap", "stale_oi_source",
])
def test_admission_rechecks_B_current_source_not_only_immutable_row(monkeypatch, failure):
    from eth_tester.storage import live_evidence as b
    bound = bind_live_paper_reference(d_reason(),bar_ts=BAR,decision_at=DECISION)
    db = EvidenceDB(_row())
    called = []
    def require(conn, *, symbol, bar_ts, evidence_key, decision_at):
        called.append((symbol,bar_ts,evidence_key,decision_at))
        if failure == "stale_oi_source":
            raise ValueError("stale source receipt after publication")
        return _b_verified()
    def continuity(*args):
        if failure in ("new_ws_disconnect","new_public_trade_gap","new_book_gap"):
            raise ValueError("new WS outage after publication")
    monkeypatch.setattr(b,"require_bounded_paper_evidence",require)
    monkeypatch.setattr(b,"_no_overlap",continuity)
    with pytest.raises(ValueError):
        verify_live_paper_row(db,symbol="ETHUSDT",reason=bound,
                              decision_at=DECISION)
    assert called == [("ETHUSDT",BAR,KEY,DECISION)]
    assert "pg_advisory_xact_lock" in str(db.query) or db.evidence_query is not None


@pytest.mark.parametrize("changed", [
    {"source_checksum": "f"*64},
    {"quality": "complete"},
    {"ml_eligible": True},
    {"exchange_complete": True},
])
def test_B_current_identity_or_quality_change_fails_closed(monkeypatch, changed):
    from eth_tester.storage import live_evidence as b
    bound = bind_live_paper_reference(d_reason(),bar_ts=BAR,decision_at=DECISION)
    db = EvidenceDB(_row())
    monkeypatch.setattr(b,"require_bounded_paper_evidence",
                        lambda *a,**kw: _b_verified(**changed))
    monkeypatch.setattr(b,"_no_overlap",lambda *a: None)
    with pytest.raises(ValueError,match="B prospective"):
        verify_live_paper_row(db,symbol="ETHUSDT",reason=bound,
                              decision_at=DECISION)


def test_live_book_is_valid_at_admission_not_only_at_publication(monkeypatch):
    from eth_tester.storage import live_evidence as b
    bound=bind_live_paper_reference(d_reason(),bar_ts=BAR,decision_at=DECISION)
    db=EvidenceDB(_row())
    db.book={"valid":False,"updated_at":DECISION}
    execute=db.execute
    def with_invalid_book(sql,params=None):
        if "FROM market_book_state" in sql:
            return Result(db.book)
        return execute(sql,params)
    db.execute=with_invalid_book
    monkeypatch.setattr(b,"require_bounded_paper_evidence",
                        lambda *a,**k: _b_verified())
    monkeypatch.setattr(b,"_no_overlap",lambda *a: None)
    with pytest.raises(ValueError,match="L2 book became invalid"):
        verify_live_paper_row(db,symbol="ETHUSDT",reason=bound,
                              decision_at=DECISION)


def test_conflicting_terminal_row_cannot_commit_paper_admission(monkeypatch):
    runner=load_runner(monkeypatch)
    class ConflictDB:
        def execute(self,sql,params=None):
            assert "RETURNING decision,reason" in sql
            return Result(None)
    with pytest.raises(RuntimeError,match="atomic_signal_decision_conflict"):
        runner._persist_paper_decision(
            ConflictDB(),types.SimpleNamespace(symbol="ETHUSDT"),BAR,
            strategy_identity(),"opened",{"paper_trade_id":44})


def test_nonretryable_D_rejection_is_terminal(monkeypatch):
    runner=load_runner(monkeypatch)
    clock=datetime.now(timezone.utc)
    bar=clock.replace(second=0,microsecond=0)-timedelta(minutes=1)
    candles=[{"ts":bar-timedelta(minutes=34-i),"open":Decimal(100),
              "high":Decimal(101),"low":Decimal(99),"close":Decimal(100),
              "volume":Decimal(20)} for i in range(35)]
    class DB:
        saved=None
        def __enter__(self):return self
        def __exit__(self,*a):return None
        def execute(self,sql,params=None):
            if "INSERT INTO strategy_signals" in sql:
                self.saved={"decision":params[3],"reason":json.loads(params[4])}
                return Result(self.saved)
            if "feed_health" in sql:
                return Result({"status":"connected"})
            if "ORDER BY ts DESC,trade_id DESC LIMIT 1" in sql:
                return Result({"ts":datetime.now(timezone.utc)
                               -timedelta(milliseconds=100),"price":Decimal(100)})
            if "paper_trades WHERE state='open'" in sql:
                return Result(None)
            if "bot_control" in sql:
                return Result({"enabled":True,"paused":False})
            if "data_gaps" in sql:
                return Result(None)
            if "FROM candles" in sql:
                return Result(list(reversed(candles)))
            if "pg_try_advisory_xact_lock(20991991)" in sql:
                return Result({"acquired": True})
            if sql.startswith("SET LOCAL"):
                return Result(None)
            if "pg_advisory_xact_lock" in sql:
                return Result(None)
            if "FROM strategy_signals" in sql:
                return Result(self.saved)
            raise AssertionError(sql)
    db=DB()
    monkeypatch.setattr(runner,"connection",lambda *a:db)
    monkeypatch.setattr(runner,"price_signal",lambda *a,**kw:
        types.SimpleNamespace(
            direction="long",stop=Decimal(98),take=Decimal(105),
            reason={"strategy_version":strategy_identity(),
                    "rules_version":Parameters().version,
                    **Parameters().identity(),
                    "signal_bar_ts":bar.isoformat(),"source":"closed_candles"}))
    monkeypatch.setattr(runner,"confirm_rejection",lambda *a,**kw:
                        (False,{"admissible":False,"reason":"stale_book"}))
    monkeypatch.setattr(runner,"open_paper",lambda *a,**kw:
                        pytest.fail("terminal D rejection must never open"))
    settings=types.SimpleNamespace(symbol="ETHUSDT",
        taker_fee_rate=Decimal(".00055"),slippage_bps=Decimal(2))
    assert runner.engine_tick(settings)=="rejected_orderflow"
    assert db.saved["reason"]["proof_rejected_reason"]=="stale_book"
    assert runner.engine_tick(settings)=="already_evaluated"
