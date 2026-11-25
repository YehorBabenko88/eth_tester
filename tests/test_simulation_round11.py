"""Simulation E Round11 source-fenced PAPER-only exit negative tests.

These are synthetic SQL adapters, not PostgreSQL isolation certification.
Real two-connection tests are in integration/test_simulation_pg.py.
"""
from datetime import datetime,timedelta,timezone
from decimal import Decimal as D
import pytest

from eth_tester.paper import _require_live_exit_source,_canonical_funding_rows

NOW=datetime(2026,11,25,21,0,10,tzinfo=timezone.utc)
BAR=NOW-timedelta(seconds=2)


class Result:
    def __init__(self,value=None):
        self.value=value
    def fetchone(self):
        return self.value
    def fetchall(self):
        return self.value


class SourceDB:
    def __init__(self,*,status="connected",disconnected=False,gap=False,
                 low=D(97),high=None):
        self.status=status
        self.disconnected=disconnected
        self.gap=gap
        self.low=low
        self.high=high
        self.sql=[]
        self.values=[]
    def execute(self,sql,params=None):
        self.sql.append(sql)
        self.values.append(params)
        if "pg_advisory_xact_lock" in sql:
            return Result(None)
        if "clock_timestamp()" in sql:
            return Result({"ts":NOW})
        if "FROM feed_health" in sql and "status='disconnected'" in sql:
            return Result({"one":1} if self.disconnected else None)
        if "FROM feed_health" in sql:
            return Result({"status":self.status})
        if "FROM data_gaps" in sql:
            return Result({"one":1} if self.gap else None)
        if "GROUP BY ts" in sql:
            return Result({"ts":BAR,"low_cross":self.low,"high_cross":self.high})
        raise AssertionError("unexpected SQL "+sql[:80])


TRADE={"symbol":"ETHUSDT","opened_at":BAR-timedelta(seconds=15),
       "direction":"long","stop":D(98),"take":D(105)}


def test_source_lock_is_first_and_exact_shared_a_b_e_name():
    conn=SourceDB()
    assert _require_live_exit_source(conn,TRADE,BAR,D(97),"stop")["reason"]=="stop"
    assert "pg_advisory_xact_lock" in conn.sql[0]
    assert conn.values[0]==("eth_paper_admission_source:ETHUSDT",)
    assert sum("FROM data_gaps" in q for q in conn.sql)==1
    assert any("GROUP BY ts ORDER BY ts LIMIT 1" in q for q in conn.sql)


@pytest.mark.parametrize("kwargs,expected",[
    ({"status":"disconnected"},"paper_exit_source_not_connected"),
    ({"disconnected":True},"paper_exit_disconnected_since_open"),
    ({"gap":True},"paper_exit_source_gap"),
])
def test_source_failure_never_confirms_crossing(kwargs,expected):
    with pytest.raises(RuntimeError,match=expected):
        _require_live_exit_source(SourceDB(**kwargs),TRADE,BAR,D(97),"stop")


@pytest.mark.parametrize("price,reason",[
    (D(97),"take"),(D(98),"stop"),(D(105),"take"),
])
def test_forged_fill_price_or_side_rejected(price,reason):
    with pytest.raises(RuntimeError,match="crossing_source_changed"):
        _require_live_exit_source(SourceDB(),TRADE,BAR,price,reason)


def test_ambiguous_simultaneous_stop_and_take_still_stop_first():
    conn=SourceDB(low=D(96),high=D(110))
    cross=_require_live_exit_source(
        conn,TRADE,BAR,D(96),"stop_same_timestamp_ambiguous_stop_first")
    assert cross["ambiguous_same_timestamp"] is True
    assert cross["reason"]=="stop" and cross["price"]==D(96)


def test_missing_earliest_observed_crossing_fails_closed():
    with pytest.raises(RuntimeError,match="crossing_source_changed"):
        _require_live_exit_source(SourceDB(low=None,high=None),
                                  TRADE,BAR,D(97),"stop")


def test_canonical_funding_selects_mark_within_1s_and_original_db_rows():
    class FundingDB:
        def execute(self,sql,params):
            assert "FROM funding f" in sql and "market_ticker_events m" in sql
            assert "m.exchange_ts>=f.ts-interval '1 second'" in sql
            assert "ORDER BY m.exchange_ts DESC,m.id DESC" in sql
            assert params==("ETHUSDT",TRADE["opened_at"],BAR)
            return Result([{"ts":BAR,"rate":D(".0001"),"mark_price":D(100)}])
    rows=_canonical_funding_rows(FundingDB(),"ETHUSDT",TRADE["opened_at"],BAR)
    assert rows[0]["rate"]==D(".0001")
