"""R12 E regression: global paper lease must precede D/B source admission.

This tests the actual runner.engine_tick with an isolated SQL state machine.
It does not replace the real PostgreSQL two-connection integration below.
"""
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from eth_tester import runner


NOW=datetime(2026,11,25,21,0,30,tzinfo=timezone.utc)
LAST=NOW.replace(second=0,microsecond=0)-timedelta(minutes=1)


class One:
    def __init__(self, row):
        self.row=row
    def fetchone(self):
        return self.row
    def fetchall(self):
        return self.row


class FakePg:
    def __init__(self,*,admission_lease=True,active_late=False,paused_late=False,
                 disable_late=False):
        self.history=[]
        self.admission_lease=admission_lease
        self.active_late=active_late
        self.paused_late=paused_late
        self.disable_late=disable_late
        self.control_reads=0
        self.published=[]
    def __enter__(self):
        return self
    def __exit__(self,typ,value,tb):
        return False
    def execute(self,sql,params=None):
        q=" ".join(sql.split())
        self.history.append((q,params))
        if "FROM feed_health" in q:
            return One({"status":"connected"})
        if "SELECT ts,price FROM trades" in q:
            return One({"ts":NOW-timedelta(seconds=1),"price":Decimal(100)})
        if "SELECT * FROM paper_trades WHERE state='open'" in q:
            return One(None)
        if "SELECT 1 FROM paper_trades WHERE state='open'" in q:
            return One({"one":1} if self.active_late else None)
        if "FROM bot_control" in q:
            self.control_reads+=1
            return One({"enabled":not (self.disable_late and self.control_reads>=2),
                        "paused":self.paused_late and self.control_reads>=2})
        if "FROM candles" in q:
            bars=[{"ts":LAST-timedelta(minutes=i),"open":100,"high":101,
                   "low":99,"close":100,"volume":Decimal(100)}
                  for i in range(30,-1,-1)]
            # SQL requests DESC; engine reverses rows to causal ASC order.
            return One(list(reversed(bars)))
        if "FROM data_gaps" in q:
            return One(None)
        if "pg_advisory_xact_lock(hashtext" in q:
            return One(None)
        if "FROM strategy_signals" in q:
            return One(None)
        if q.startswith("SET LOCAL"):
            return One(None)
        if "pg_try_advisory_xact_lock(20991991)" in q:
            return One({"acquired":self.admission_lease})
        raise AssertionError("Unexpected SQL: "+q[:120])


class Clock(datetime):
    @classmethod
    def now(cls,tz=None):
        return NOW if tz else NOW.replace(tzinfo=None)


def boot(monkeypatch,db,*,positive=True):
    monkeypatch.setattr(runner,"datetime",Clock)
    monkeypatch.setattr(runner,"connection",lambda settings:db)
    monkeypatch.setattr(runner,"rule_identity",
                        lambda:("test_v3",{"parameters":{"version":"test"},
                                         "parameter_sha256":"a"*64}))
    signal=(SimpleNamespace(reason={"strategy_version":"test_v3"},
                            direction="long",stop=Decimal(98),take=Decimal(105))
            if positive else None)
    monkeypatch.setattr(runner,"price_signal",lambda *a,**k:signal)
    monkeypatch.setattr(runner,"require_new_strategy_identity",lambda reason:reason)
    def verify(conn,settings,chosen,bar,**kw):
        assert chosen is signal
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                     ("eth_paper_admission_source:ETHUSDT",))
        return False,{"reason":"pending_bounded_live_evidence","retryable":True}
    monkeypatch.setattr(runner,"confirm_rejection",verify)
    def persist(conn,settings,bar_ts,version,decision,payload):
        db.published.append(decision)
        return {"decision":decision,"reason":payload}
    monkeypatch.setattr(runner,"_persist_paper_decision",persist)
    return SimpleNamespace(symbol="ETHUSDT",taker_fee_rate=Decimal(".00055"),
                           slippage_bps=Decimal("2"))


def has(db,fragment):
    return [i for i,(sql,_) in enumerate(db.history) if fragment in sql]


def test_positive_signal_lease_precedes_source_and_rechecks_control(monkeypatch):
    db=FakePg()
    settings=boot(monkeypatch,db)
    assert runner.engine_tick(settings)=="pending_missing_proof"
    assert db.published==["pending_missing_proof"]
    global_at=has(db,"pg_try_advisory_xact_lock(20991991)")[0]
    signal_at=next(i for i,(q,params) in enumerate(db.history)
                   if params is not None and
                   isinstance(params,tuple) and
                   str(params[0]).startswith("paper_signal:"))
    source_at=next(i for i,(q,params) in enumerate(db.history)
                   if params==("eth_paper_admission_source:ETHUSDT",))
    assert global_at < signal_at < source_at
    assert db.control_reads==2
    assert has(db,"SET LOCAL lock_timeout")
    assert has(db,"SET LOCAL statement_timeout")


def test_global_mutex_busy_is_transient_no_source_or_terminal(monkeypatch):
    db=FakePg(admission_lease=False)
    settings=boot(monkeypatch,db)
    assert runner.engine_tick(settings)=="paper_admission_busy"
    assert db.published==[]
    assert not any(params==("eth_paper_admission_source:ETHUSDT",)
                   for _,params in db.history)


@pytest.mark.parametrize("state,expected",[
    ({"active_late":True},"paper_position_already_open"),
    ({"paused_late":True},"paused"),
    ({"disable_late":True},"paused"),
])
def test_after_lease_source_never_called_if_position_or_control_changes(
        monkeypatch,state,expected):
    db=FakePg(**state)
    settings=boot(monkeypatch,db)
    assert runner.engine_tick(settings)==expected
    assert db.published==[]
    assert not any(params==("eth_paper_admission_source:ETHUSDT",)
                   for _,params in db.history)


def test_no_signal_bar_diagnostic_obeys_global_then_signal(monkeypatch):
    db=FakePg()
    settings=boot(monkeypatch,db,positive=False)
    assert runner.engine_tick(settings)=="no_signal"
    assert db.published==["no_signal"]
    assert has(db,"pg_try_advisory_xact_lock(20991991)")
    signal_at=next(i for i,(q,params) in enumerate(db.history)
                   if params is not None and
                   isinstance(params,tuple) and
                   str(params[0]).startswith("paper_signal:"))
    assert has(db,"pg_try_advisory_xact_lock(20991991)")[0] < signal_at
    assert not any(params==("eth_paper_admission_source:ETHUSDT",)
                   for _,params in db.history)
