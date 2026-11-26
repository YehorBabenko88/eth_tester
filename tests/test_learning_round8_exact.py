"""Exact parity with B SQL NUMERIC, not an exchange data quality certification."""
from decimal import Decimal
from hashlib import sha256
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from contextlib import contextmanager
import pytest
from eth_tester.ml.labels import full_future_grid, label_level_outcome
from eth_tester.ml.label_worker import _reason_policy, _source_certificate, _sql_label_rejection, run_label_cycle

DECISION = 1795610060123
STEP = 60_000


def classify(side, level, high, low, bps=20):
    start, _, end = full_future_grid(DECISION, bar_interval_ms=STEP, horizon_bars=1)
    return label_level_outcome(side, level, [{"start_ms": start, "closed_ms": end, "high": high, "low": low}],
                               decision_ms=DECISION, horizon_ms=end, as_of_ms=end,
                               bar_interval_ms=STEP, barrier_bps=bps)


def test_sql_decimal_exact_resistance_upper_touch_is_breakout():
    # SQL numeric 392.282749 * 1.002 == 393.067314498, exactly.
    assert classify("resistance", "392.282749", "393.067314498", "392.272749") == "breakout"


def test_sql_decimal_exact_support_lower_touch_is_breakout():
    # SQL numeric 392.282749 * .998 == 391.498183502, exactly.
    assert classify("support", "392.282749", "392.282749", "391.498183502") == "breakout"


def test_same_bar_two_exact_boundaries_uncertain():
    assert classify("resistance", "392.282749", "393.067314498", "391.498183502") == "uncertain"


def test_previously_misrounded_float_boundaries_preserve_raw_level():
    reason={"side":"resistance", "level":"392.282749", "ml_label_policy":{
      "version":"utc_full_bars_v1", "method":"fixed_horizon_barrier_v1", "bar_interval_ms":60000,
      "horizon_bars":3,"barrier_bps":20,"anchor":"first_full_bar_strictly_after_decision"}}
    _,value=_reason_policy(reason)
    assert isinstance(value, Decimal) and value == Decimal("392.282749")


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1", "0", True, "invalid"])
def test_invalid_structural_barrier_rejected(value):
    with pytest.raises(ValueError):
        classify("resistance", value, "500", "100")


def test_exclusive_coverage_end_equal_to_horizon_is_admissible_with_receipt():
    class Conn:
        def execute(self, sql, params):
            if "FROM ingestion_coverage" in sql:
                assert "end_ts>=%s" in sql
                digest="a"*64
                receipt=240000
                ref=sha256(json.dumps(["ETHUSDT","linear","trade","1",60000,180000,digest,receipt],
                         separators=(",", ":")).encode()).hexdigest()
                record={"source":"kline:trade","checksum":digest,"quality":"exchange_closed_grid_checked",
                        "start_ts":datetime.fromtimestamp(60,timezone.utc),
                        "end_ts":datetime.fromtimestamp(240,timezone.utc),
                        "observed_rows":3,"expected_rows":3,
                        "evidence":{"kind":"trade", "tf":"1", "receipt_is_independently_recorded":True,
                                    "closed_grid_complete":True,"source_receipt_ms":receipt,
                                    "source_receipt_kind":"postgres_clock_timestamp_after_http",
                                    "source_verified_as_of_receipt":True,
                                    "source_event_start_ms":60000,"source_event_end_exclusive_ms":240000,
                                    "requested_start_ms":60000,"requested_end_ms":180000,
                                    "row_count":3,"rest_payload_sha256":digest,
                                    "evidence_ref":ref,"proves_all_trade_ids":False},
                        "checked_at":datetime.fromtimestamp(240,timezone.utc)}
                return SimpleNamespace(fetchall=lambda:[record])
            return SimpleNamespace(fetchone=lambda:None)
    proof,_,rec,checked=_source_certificate(Conn(),symbol="ETHUSDT",instrument="linear",
                decision_ms=60000,horizon_ms=180000,as_of_ms=300000)
    assert proof["source"] == "kline:trade" and rec == checked == 240000


class RaisedByPg(Exception):
    sqlstate="P0001"
    diag=SimpleNamespace(message_primary="Missing verified full future candle grid")

class OtherPg(Exception):
    sqlstate="42P01"
    diag=SimpleNamespace(message_primary="relation does not exist")


def test_known_trigger_error_category_only():
    assert _sql_label_rejection(RaisedByPg()) == "incomplete_future_grid"
    assert _sql_label_rejection(OtherPg()) is None


class Conn:
    def __init__(self):
        self.calls=[]
        self.rollbacks=0
        self.saves=0
        self.cursor_updated=False
    @contextmanager
    def transaction(self):
        self.saves+=1
        try:
            yield
        except Exception:
            self.rollbacks+=1
            raise
    def execute(self, sql, params=None):
        self.calls.append(sql)
        if "pg_try_advisory_xact_lock" in sql:
            return SimpleNamespace(fetchone=lambda:{"ok":True})
        if "clock_timestamp()" in sql:
            return SimpleNamespace(fetchone=lambda:{"ts":datetime(2026,11,25,tzinfo=timezone.utc)})
        if "SELECT details FROM analytics_cursor" in sql:
            return SimpleNamespace(fetchone=lambda:None)
        if "FROM paper_trades p" in sql:
            return SimpleNamespace(fetchall=lambda:[{"paper_trade_id":1},{"paper_trade_id":2},{"paper_trade_id":3}])
        if "INSERT INTO analytics_cursor" in sql:
            self.cursor_updated=True
        return SimpleNamespace(fetchone=lambda:None)


def test_known_sql_trigger_denial_does_not_abort_batch(monkeypatch):
    import eth_tester.ml.label_worker as module
    db=Conn()
    @contextmanager
    def provide(_): yield db
    def label_one(conn,row,**kwargs):
        if row["paper_trade_id"]==2:
            raise RaisedByPg()
        return "breakout"
    monkeypatch.setattr(module,"connection",provide)
    monkeypatch.setattr(module,"_label_one",label_one)
    r=run_label_cycle(SimpleNamespace(symbol="ETHUSDT"),limit=10)
    assert r["scanned"]==3 and r["created"]==2
    assert r["rejection_counts"]=={"incomplete_future_grid":1}
    assert db.saves==3 and db.rollbacks==1 and db.cursor_updated
    assert r["approved"] is False


def test_unknown_pg_sql_failure_aborts_batch_not_masked(monkeypatch):
    import eth_tester.ml.label_worker as module
    db=Conn()
    @contextmanager
    def provide(_): yield db
    def label_one(conn,row,**kwargs): raise OtherPg()
    monkeypatch.setattr(module,"connection",provide)
    monkeypatch.setattr(module,"_label_one",label_one)
    with pytest.raises(OtherPg):
        run_label_cycle(SimpleNamespace(symbol="ETHUSDT"),limit=10)
    assert not db.cursor_updated and db.rollbacks==1
