"""Simulation E R14: original-minute shadow observation & funding fail-closed.

No exchange orders. Isolated DB models below execute the actual E runner
entry point; dedicated disposable PostgreSQL tests cover SQL behavior.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
import inspect

import pytest
from eth_tester import runner
import eth_tester.strategies.ensemble as ensemble


NOW=datetime(2026,11,25,21,0,30,tzinfo=timezone.utc)
TARGET=NOW.replace(second=0,microsecond=0)-timedelta(minutes=1)
VERSION="eth_observed_minute_shadow_v1"


class One:
    def __init__(self,row): self.row=row
    def fetchone(self): return self.row
    def fetchall(self): return self.row


class ResearchFakePg:
    def __init__(self,*,connected=True,gap=False,present=True,closed=True,
                 count=65,lease=True,duplicate=False,slow=False):
        self.connected,self.gap=connected,gap
        self.present,self.closed=present,closed
        self.count,self.lease=count,lease
        self.duplicate,self.slow=duplicate,slow
        self.history = []
        self.stored = []
        self.clocks = 0
        self.candles=[{"ts":TARGET-timedelta(minutes=i),
                       "open":Decimal(100),"high":Decimal(101),
                       "low":Decimal(99),"close":Decimal(100),
                       "volume":Decimal(7),"closed":closed}
                      for i in range(count-1,-1,-1)]
        if not present:
            self.candles=self.candles[:-1]
    def __enter__(self): return self
    def __exit__(self,typ,val,tb): return False
    def execute(self,sql,params=None):
        q=" ".join(sql.split())
        self.history.append((q,params))
        if q.startswith("SET LOCAL"):
            return One(None)
        if "SELECT clock_timestamp() AS ts" in q:
            self.clocks+=1
            return One({"ts":NOW+timedelta(seconds=91) if self.slow and self.clocks>1 else NOW})
        if "pg_try_advisory_xact_lock(hashtext" in q:
            return One({"acquired":self.lease})
        if "FROM strategy_signals" in q:
            return One({"one":1} if self.duplicate else None)
        if "FROM candles" in q:
            return One(list(reversed(self.candles)))
        if "FROM feed_health" in q:
            return One({"status":"connected" if self.connected else "disconnected"})
        if "FROM data_gaps" in q:
            return One({"one":1} if self.gap else None)
        if "INSERT INTO strategy_signals" in q:
            self.stored.append((params[3],params[4]))
            return One({"decision":params[3]})
        raise AssertionError("Unexpected research SQL "+q[:150])


@pytest.fixture
def setup(monkeypatch):
    def build(*,status="no_trade",**kwargs):
        db=ResearchFakePg(**kwargs)
        settings=SimpleNamespace(symbol="ETHUSDT",
                                 taker_fee_rate=Decimal(".00055"),
                                 slippage_bps=Decimal(2))
        monkeypatch.setattr(runner,"connection",lambda _settings:db)
        calls=[]
        def evaluate(bars,*,observed_at,taker_fee_rate,slippage_bps):
            calls.append((bars,observed_at,taker_fee_rate,slippage_bps))
            return {"status":status,"bar_ts":bars[-1]["ts"].isoformat(),
                    "input_bar_count":len(bars),
                    "independent_of_incumbent":True,
                    "paper_entry_authorized":False,
                    "ml_eligible":False}
        monkeypatch.setattr(ensemble,"evaluate_closed_bar_research",evaluate)
        return db,settings,calls
    return build


def test_every_closed_minute_no_trade_is_persisted_independently(setup):
    db,settings,calls=setup()
    assert runner.record_closed_bar_research(settings)=="research_no_trade"
    assert len(calls)==1 and calls[0][1]==NOW
    assert len(db.stored)==1 and db.stored[0][0]=="research_no_trade"
    sql,params=next((q,p) for q,p in db.history if "INSERT INTO strategy_signals" in q)
    assert "ON CONFLICT(symbol,tf,bar_ts,version) DO NOTHING" in sql
    assert params[:4]==("ETHUSDT",TARGET,VERSION,"research_no_trade")
    import json
    report=json.loads(params[4])
    assert report["ml_eligible"] is False
    assert report["research_cohort_complete"] is False
    assert report["paper_entry_authorized"] is False
    assert report["shadow"]["status"]=="no_trade"


def test_independent_candidate_is_non_executable(setup):
    db,settings,calls=setup(status="shadow_candidates")
    assert runner.record_closed_bar_research(settings)=="research_candidates_unverified"
    assert len(calls)==1


@pytest.mark.parametrize("kwargs,reason",[
    ({"connected":False},"source_not_connected_at_observation"),
    ({"gap":True},"source_gap_in_observed_window"),
    ({"present":False},"current_minute_candle_not_observed"),
    ({"closed":False},"unavailable_or_invalid_closed_bar_window"),
    ({"count":59},"unavailable_or_invalid_closed_bar_window"),
])
def test_bad_source_or_incomplete_bar_window_never_certifies(setup,kwargs,reason):
    import json
    db,settings,calls=setup(**kwargs)
    assert runner.record_closed_bar_research(settings)=="research_unavailable"
    report=json.loads(db.stored[0][1])
    assert report["unavailable_reason"]==reason
    assert report["prospective_source_certified"] is False
    assert report["ml_eligible"] is False
    if reason in ("source_not_connected_at_observation","source_gap_in_observed_window",
                  "current_minute_candle_not_observed"):
        assert calls==[]


def test_prior_observation_skipped_without_modification(setup):
    db,settings,calls=setup(duplicate=True)
    assert runner.record_closed_bar_research(settings)=="research_already_observed"
    assert db.stored==[] and calls==[]


def test_busy_research_lease_no_double_write(setup):
    db,settings,calls=setup(lease=False)
    assert runner.record_closed_bar_research(settings)=="research_busy"
    assert db.stored==[] and calls==[]


def test_slow_evaluation_does_not_backdate_a_receipt(setup):
    db,settings,calls=setup(slow=True)
    assert runner.record_closed_bar_research(settings)=="research_expired_no_replay"
    assert len(calls)==1 and db.stored==[]


def test_fails_closed_on_noncanonical_d_output(setup,monkeypatch):
    db,settings,calls=setup()
    monkeypatch.setattr(ensemble,"evaluate_closed_bar_research",
                        lambda *a,**kw:{"status":"shadow_candidates",
                                       "paper_entry_authorized":True})
    with pytest.raises(ValueError,match="Noncanonical"):
        runner.record_closed_bar_research(settings)
    assert db.stored==[]


def test_separate_loop_invocation_and_database_closed_projection():
    source=inspect.getsource(runner.run_engine)
    assert source.index("record_closed_bar_research") < source.index("engine_tick")
    assert "SELECT ts,open,high,low,close,volume,closed FROM candles" in inspect.getsource(runner.engine_tick)
    assert "SELECT ts,open,high,low,close,volume,closed" in inspect.getsource(runner.record_closed_bar_research)
    assert "pg_advisory_xact_lock(20991991)" not in inspect.getsource(runner.record_closed_bar_research)
