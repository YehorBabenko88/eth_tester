"""E round-6 tests: bounded LIVE paper provenance is not historical ML proof."""
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import sys
import types

import pytest

from eth_tester.simulation.live_evidence import (
    bind_live_paper_reference, verify_live_paper_row,
)
from eth_tester.strategies import Parameters, strategy_identity

BAR = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)
END = BAR + timedelta(minutes=1)
DECISION = END + timedelta(seconds=35)
KEY, HASH = "a" * 64, "b" * 64


def d_reason(*, bar=BAR, observed=None, computed=None, expires=None):
    end = bar + timedelta(minutes=1)
    observed = observed or end + timedelta(seconds=10)
    computed = computed or end + timedelta(seconds=15)
    expires = expires or end + timedelta(seconds=90)
    return {
        "strategy_version": strategy_identity(),
        "rules_version": Parameters().version,
        **Parameters().identity(),
        "source": "closed_candles",
        "signal_bar_ts": bar.isoformat(),
        "orderflow": {
            "admissible": True,
            "immutable_live_evidence": {
                "snapshot_key": KEY, "source_checksum": HASH,
                "contract": "eth_paper_live_evidence_v1",
                "quality": "live_observed_bounded", "paper_only": True,
                "ml_eligible": False, "bar_ts": bar.isoformat(),
                "observed_at": observed.isoformat(),
                "computed_at": computed.isoformat(),
                "expires_at": expires.isoformat(),
            },
        },
    }


def test_live_paper_binding_never_becomes_training_snapshot():
    original = d_reason()
    accepted = bind_live_paper_reference(original, bar_ts=BAR, decision_at=DECISION)
    assert accepted["paper_evidence_tier"] == "live_observed_bounded"
    assert accepted["paper_live_snapshot_key"] == KEY
    assert accepted["paper_live_source_checksum"] == HASH
    assert accepted["paper_live_decision_ms"] == int(DECISION.timestamp() * 1000)
    assert accepted["ml_eligible"] is False and accepted["paper_only"] is True
    assert not any(name.startswith("ml_") and name != "ml_eligible"
                   for name in accepted)
    assert not any(name.startswith("ml_") for name in original)


def test_live_binding_is_strictly_idempotent_for_actual_runner_to_paper_path():
    raw = d_reason()
    once = bind_live_paper_reference(raw, bar_ts=BAR, decision_at=DECISION)
    twice = bind_live_paper_reference(once, bar_ts=BAR, decision_at=DECISION)
    assert twice == once
    assert twice["ml_eligible"] is False
    assert twice["paper_only"] is True
    assert "ml_snapshot_key" not in twice and "ml_label" not in twice


@pytest.mark.parametrize("change", [
    {"ml_eligible": 0},
    {"ml_eligible": True},
    {"ml_eligible": None},
    {"paper_only": 1},
    {"paper_live_snapshot_key": "c" * 64},
    {"paper_live_decision_ms": int(DECISION.timestamp() * 1000) - 1},
    {"paper_live_bar_end_ms": int(END.timestamp() * 1000) + 1},
    {"paper_evidence_tier": "historical_ml"},
    {"ml_snapshot_key": KEY},
    {"ml_label": {"future": True}},
])
def test_rebinding_never_accepts_tampered_or_ml_promoted_reason(change):
    bound = bind_live_paper_reference(d_reason(), bar_ts=BAR, decision_at=DECISION)
    with pytest.raises(ValueError):
        bind_live_paper_reference({**bound, **change}, bar_ts=BAR,
                                  decision_at=DECISION)


def test_partial_paper_binding_cannot_be_completed_by_trusted_binder():
    raw = d_reason()
    for name, value in (
        ("ml_eligible", False), ("paper_only", True),
        ("paper_live_snapshot_key", KEY),
    ):
        with pytest.raises(ValueError, match="Incomplete"):
            bind_live_paper_reference({**raw, name: value},
                                      bar_ts=BAR, decision_at=DECISION)


@pytest.mark.parametrize("mutation", [
    {"contract": "eth_feature_snapshot_v2"},
    {"quality": "complete"},
    {"paper_only": False}, {"ml_eligible": True},
    {"source_checksum": "unverified"},
    {"snapshot_key": "f" * 63},
    {"bar_ts": (BAR - timedelta(minutes=1)).isoformat()},
    {"computed_at": (DECISION + timedelta(seconds=1)).isoformat()},
    {"observed_at": (END - timedelta(seconds=1)).isoformat()},
])
def test_reject_forged_incomplete_or_future_live_proof(mutation):
    reason = d_reason()
    reason["orderflow"]["immutable_live_evidence"].update(mutation)
    with pytest.raises(ValueError):
        bind_live_paper_reference(reason, bar_ts=BAR, decision_at=DECISION)


def test_expired_window_never_backdates_decision():
    with pytest.raises(ValueError, match="Late"):
        bind_live_paper_reference(
            d_reason(), bar_ts=BAR, decision_at=END + timedelta(seconds=91))


def test_bounded_paper_rejects_mixed_historical_and_future_ml_keys():
    with pytest.raises(ValueError, match="ML"):
        bind_live_paper_reference({**d_reason(), "ml_snapshot_key": KEY},
                                  bar_ts=BAR, decision_at=DECISION)
    with pytest.raises(ValueError, match="ML"):
        bind_live_paper_reference({**d_reason(), "ml_label": {"outcome": "bounce"}},
                                  bar_ts=BAR, decision_at=DECISION)


class RowResult:
    def __init__(self, row):
        self.row = row
    def fetchone(self):
        return self.row


class EvidenceDB:
    def __init__(self, row):
        self.row = row
        self.query = None
        self.params = None
        self.evidence_query = None
        self.evidence_params = None
    def execute(self, query, params=None):
        self.query, self.params = query, params
        if "FROM paper_live_evidence" in query:
            self.evidence_query, self.evidence_params = query, params
        if "clock_timestamp()" in query:
            return RowResult({"ts":DECISION})
        if "market_book_state" in query:
            return RowResult({"valid":True,"updated_at":DECISION})
        return RowResult(self.row)


def test_rechecks_exact_immutable_b_row_for_share(monkeypatch):
    from eth_tester.storage import live_evidence as b
    # Unit fixture explicitly mocks B's original-source gate; NOT actual WS.
    def original_source_gate(conn, *, symbol, bar_ts, evidence_key, decision_at):
        assert symbol=="ETHUSDT" and evidence_key==KEY and bar_ts==BAR
        return {
            "snapshot_key":KEY,"source_checksum":HASH,
            "contract":"eth_paper_live_evidence_v1",
            "quality":"live_observed_bounded", "paper_only":True,
            "ml_eligible":False,"exchange_complete":False,
            "bar_ts":BAR,
            "observed_at":END+timedelta(seconds=10),
            "computed_at":END+timedelta(seconds=15),
            "expires_at":END+timedelta(seconds=90),
        }
    monkeypatch.setattr(b,"require_bounded_paper_evidence",original_source_gate)
    monkeypatch.setattr(b,"_no_overlap",lambda *args: None)
    context = bind_live_paper_reference(d_reason(), bar_ts=BAR, decision_at=DECISION)
    row = {
        "snapshot_key": KEY, "symbol": "ETHUSDT", "tf": "1",
        "bar_ts": BAR, "source_checksum": HASH,
        "observed_at": END + timedelta(seconds=10),
        "computed_at": END + timedelta(seconds=15),
        "expires_at": END + timedelta(seconds=90),
        "quality": "live_observed_bounded", "paper_only": True,
        "ml_eligible": False,
        "evidence": {"contract": "eth_paper_live_evidence_v1",
                     "fixture_only_not_market_certification": True},
    }
    db = EvidenceDB(row)
    assert verify_live_paper_row(db, symbol="ETHUSDT", reason=context,
                                 decision_at=DECISION)["snapshot_key"] == KEY
    assert "FOR SHARE" in db.evidence_query and db.evidence_params == (KEY,)
    for field, bad in [
        ("symbol", "BTCUSDT"), ("tf", "5"),
        ("quality", "complete"), ("paper_only", False),
        ("ml_eligible", True), ("source_checksum", "c" * 64),
        ("computed_at", DECISION + timedelta(seconds=1)),
        ("expires_at", DECISION - timedelta(seconds=1)),
        ("evidence", {"contract": "eth_feature_snapshot_v2"}),
    ]:
        db.row = {**row, field: bad}
        with pytest.raises(ValueError):
            verify_live_paper_row(db, symbol="ETHUSDT", reason=context,
                                  decision_at=DECISION)
    db.row = None
    with pytest.raises(ValueError, match="missing"):
        verify_live_paper_row(db, symbol="ETHUSDT", reason=context,
                              decision_at=DECISION)


def test_original_paper_window_is_never_retroactively_extended():
    from eth_tester.runner import _paper_window
    assert not _paper_window(BAR, END - timedelta(microseconds=1))
    assert _paper_window(BAR, END)
    assert _paper_window(BAR, END + timedelta(seconds=90))
    assert not _paper_window(BAR, END + timedelta(seconds=90, microseconds=1))


def test_missing_live_proof_can_retry_until_opened_but_not_replay(monkeypatch):
    from test_simulation_runner import load_runner, Result
    runner = load_runner(monkeypatch)
    clock = datetime.now(timezone.utc)
    bar = clock.replace(second=0, microsecond=0) - timedelta(minutes=1)
    bars = [{"ts": bar - timedelta(minutes=34 - i),
             "open": Decimal(100), "high": Decimal(101), "low": Decimal(99),
             "close": Decimal(100), "volume": Decimal(20)}
            for i in range(35)]
    class DB:
        record = None
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return None
        def transaction(self):
            return nullcontext()
        def execute(self, sql, params=None):
            if "INSERT INTO strategy_signals" in sql:
                self.record = {"decision": params[3],
                               "reason": json.loads(params[4])}
                return Result(self.record)
            if "feed_health" in sql:
                return Result({"status": "connected"})
            if "ORDER BY ts DESC,trade_id DESC LIMIT 1" in sql:
                return Result({"ts": datetime.now(timezone.utc)
                                      - timedelta(milliseconds=50),
                               "price": Decimal(100)})
            if "paper_trades WHERE state='open'" in sql:
                return Result(None)
            if "bot_control" in sql:
                return Result({"enabled": True, "paused": False})
            if "data_gaps" in sql:
                return Result(None)
            if "FROM candles" in sql:
                return Result(list(reversed(bars)))
            if "FROM strategy_signals" in sql:
                return Result(self.record)
            if "clock_timestamp()" in sql:
                return Result({"ts": datetime.now(timezone.utc)})
            if "pg_try_advisory_xact_lock(20991991)" in sql:
                return Result({"acquired": True})
            if sql.startswith("SET LOCAL"):
                return Result(None)
            if "pg_advisory_xact_lock(hashtext" in sql:
                return Result(None)
            raise AssertionError(sql)
    db = DB()
    monkeypatch.setattr(runner, "connection", lambda settings: db)
    reasons = []
    def candidate(*args, **kwargs):
        return types.SimpleNamespace(
            direction="long", stop=Decimal(98), take=Decimal(105),
            reason={"strategy_version": strategy_identity(),
                    "rules_version": Parameters().version,
                    **Parameters().identity(),
                    "signal_bar_ts": bar.isoformat(),
                    "source": "closed_candles"})
    def confirm(conn, settings, signal, last, *, observed_at=None):
        reasons.append("check")
        if len(reasons) == 1:
            return False, {"admissible":False,
                           "reason":"pending_bounded_live_evidence",
                           "retryable":True}
        now = datetime.now(timezone.utc)
        return True, d_reason(bar=bar, observed=now, computed=now)[
            "orderflow"]
    opened = []
    def fake_open(settings, side, price, stop, take, reason, observed_at=None, *, conn=None):
        assert conn is db and reason["ml_eligible"] is False
        assert "ml_snapshot_key" not in reason
        opened.append((observed_at, reason["paper_live_snapshot_key"]))
        return 52
    monkeypatch.setattr(runner, "price_signal", candidate)
    monkeypatch.setattr(runner, "confirm_rejection", confirm)
    monkeypatch.setattr(runner, "open_paper", fake_open)
    fake_charts = types.ModuleType("eth_tester.charts")
    fake_charts.render_trade = lambda *a: None
    monkeypatch.setitem(sys.modules, "eth_tester.charts", fake_charts)
    settings = types.SimpleNamespace(symbol="ETHUSDT",
        taker_fee_rate=Decimal("0.00055"), slippage_bps=Decimal(2))
    assert runner.engine_tick(settings) == "pending_missing_proof"
    assert db.record["decision"] == "pending_missing_proof"
    assert runner.engine_tick(settings) == "opened_52"
    assert db.record["decision"] == "opened"
    assert db.record["reason"]["paper_trade_id"] == 52
    assert len(opened) == 1
    assert runner.engine_tick(settings) == "already_evaluated"
    assert len(opened) == 1


def test_old_bar_is_terminal_no_late_archive_reentry(monkeypatch):
    from test_simulation_runner import load_runner, Result
    runner = load_runner(monkeypatch)
    now = datetime.now(timezone.utc)
    bar = now.replace(second=0, microsecond=0) - timedelta(minutes=3)
    bars = [{"ts": bar-timedelta(minutes=34-i), "open": Decimal(100),
             "high": Decimal(101), "low": Decimal(99), "close": Decimal(100),
             "volume": Decimal(20)} for i in range(35)]
    class DB:
        def __enter__(self): return self
        def __exit__(self,*args): return None
        def execute(self,sql,params=None):
            if "feed_health" in sql:
                return Result({"status":"connected"})
            if "ORDER BY ts DESC,trade_id DESC LIMIT 1" in sql:
                return Result({"ts":now-timedelta(seconds=1),"price":Decimal(100)})
            if "paper_trades WHERE state='open'" in sql:
                return Result(None)
            if "bot_control" in sql:
                return Result({"enabled":True,"paused":False})
            if "data_gaps" in sql:
                return Result(None)
            if "FROM candles" in sql:
                return Result(list(reversed(bars)))
            if "FROM strategy_signals" in sql:
                return Result(None)
            if "INSERT INTO strategy_signals" in sql:
                assert params[3] == "expired_missing_proof"
                return Result({"decision":params[3],
                               "reason":json.loads(params[4])})
            if "pg_try_advisory_xact_lock(20991991)" in sql:
                return Result({"acquired": True})
            if sql.startswith("SET LOCAL"):
                return Result(None)
            if "pg_advisory_xact_lock(hashtext" in sql:
                return Result(None)
            raise AssertionError(sql)
    monkeypatch.setattr(runner, "connection", lambda _: DB())
    monkeypatch.setattr(runner, "price_signal", lambda *a,**k:
                        types.SimpleNamespace(
                          direction="long", stop=Decimal(98), take=Decimal(105),
                          reason={"strategy_version":strategy_identity(),
                                  "rules_version":Parameters().version,
                                  **Parameters().identity()}))
    monkeypatch.setattr(runner, "confirm_rejection", lambda *a,**k:
                        pytest.fail("Expired bar cannot request later proof"))
    monkeypatch.setattr(runner, "open_paper", lambda *a,**k:
                        pytest.fail("Expired bar cannot open paper position"))
    result = runner.engine_tick(types.SimpleNamespace(
        symbol="ETHUSDT", taker_fee_rate=Decimal(".00055"),slippage_bps=Decimal(2)))
    assert result == "expired_missing_proof"
