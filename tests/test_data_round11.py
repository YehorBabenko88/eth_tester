"""Data Guardian round11; all database writes are mocked in unit scope."""
from datetime import date,datetime,timedelta,timezone
from types import SimpleNamespace
import pytest
from eth_tester import live,archives
from eth_tester.data_quality import DataIntegrityError

class Row:
    def __init__(self,value=None): self.value=value
    def fetchone(self): return self.value

class Conn:
    def __init__(self): self.queries=[]
    def execute(self,sql,args=None):
        self.queries.append(sql)
        if "INSERT INTO trades" in sql:
            return Row({"trade_id":"tx"})
        if "INSERT INTO book_events" in sql:
            return Row({"id":1,"recv_ts":datetime.now(timezone.utc)})
        return Row()

def print_row(qty="1"):
    return {"i":"tx","T":1767225600000,"p":"100","v":qty,
            "S":"Buy","s":"ETHUSDT"}

def test_live_all_trade_rows_under_source_lock(monkeypatch):
    conn=Conn()
    monkeypatch.setattr(live,"_record_ws_trade_receipts",lambda *args:None)
    live.ingest_ws(conn,"ETHUSDT",{"topic":"publicTrade.ETHUSDT",
                   "data":[print_row(),print_row()]},session_id="s")
    assert "pg_advisory_xact_lock" in conn.queries[0]
    assert any("INSERT INTO trades" in q for q in conn.queries[1:])

def test_live_conflict_before_db(monkeypatch):
    conn=Conn()
    with pytest.raises(DataIntegrityError,match="Conflicting duplicate"):
        live.ingest_ws(conn,"ETHUSDT",{"topic":"publicTrade.ETHUSDT",
                  "data":[print_row(),print_row("2")]},session_id="s")
    assert conn.queries==[]

@pytest.mark.parametrize("topic,data",[
    ("orderbook.200.ETHUSDT",{"u":1,"seq":1,"a":[],"b":[]}),
    ("tickers.ETHUSDT",{"lastPrice":"100"}),
    ("allLiquidation.ETHUSDT",[]),
])
def test_other_ws_frame_lock(topic,data):
    conn=Conn()
    live.ingest_ws(conn,"ETHUSDT",{"topic":topic,"data":data,
              "ts":1767225600000,"type":"snapshot"})
    assert "pg_advisory_xact_lock" in conn.queries[0]

def test_archive_day_quota_retry_and_dedup():
    start=date(2026,1,1)
    end=start+timedelta(days=200)
    selected=archives.plan_archive_days(start,end,start+timedelta(days=20),
        [start,start+timedelta(days=20),start+timedelta(days=2)])
    assert len(selected)<=32
    assert len(selected)==len(set(selected))
    assert selected[0]==start
    assert start+timedelta(days=21) in selected

def test_archive_cursor_is_bounded_and_future_excluded():
    start=date(2026,1,1)
    assert archives.plan_archive_days(start,start+timedelta(days=2),
             start+timedelta(days=300),[start+timedelta(days=400)])==[]
    with pytest.raises(ValueError):
        archives.plan_archive_days(start,start-timedelta(days=1),start,[])

def test_failed_archive_day_does_not_starve_later_day(monkeypatch):
    import asyncio
    async def scenario():
        start=date(2026,1,1)
        processed=[]
        advances=[]
        class Client:
            async def __aenter__(self):return self
            async def __aexit__(self,*args):pass
        class DB:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def execute(self,sql,args=None):
                if "count(*)" in sql:
                    return Row({"n":1})
                return Row({"cursor_ts":None})
        async def download(settings,day,client):
            processed.append(day)
            if day==start:
                raise DataIntegrityError("corrupt gzip")
            return "complete"
        async def nothing(_):return None
        monkeypatch.setattr(archives,"archive_cycle_plan",lambda s:[("fresh",start),("fresh",start+timedelta(days=1))])
        monkeypatch.setattr(archives.httpx,"AsyncClient",lambda **kwargs:Client())
        monkeypatch.setattr(archives,"download_archive",download)
        monkeypatch.setattr(archives,"quarantine_archive_day",lambda *x:processed.append("quarantine"))
        monkeypatch.setattr(archives,"advance_archive_cursor",lambda s,d,**kw:advances.append(d))
        monkeypatch.setattr(archives,"revalidate_completed_archive",lambda *x:{"status":"pending_official_1m"})
        monkeypatch.setattr(archives,"connection",lambda *x:DB())
        monkeypatch.setattr(archives.asyncio,"sleep",nothing)
        result=await archives.backfill_archives(SimpleNamespace(
            symbol="ETHUSDT",category="linear",archive_start=start,
            archive_downloads_per_second=1))
        assert processed[:3]==[start,"quarantine",start+timedelta(days=1)]
        assert advances==[start,start+timedelta(days=1)]
        assert result["status"]=="partial" and result["failed"]==1
        assert result["ml_eligible"] is False
    asyncio.run(scenario())
