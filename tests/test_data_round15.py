"""A R15 fault regressions: SHA-pinned recovery, rate-bounded catch-up, no live orders."""
import asyncio
from datetime import date, timedelta
from types import SimpleNamespace
import gzip

import pytest

from eth_tester import archives
from eth_tester.data_quality import DataIntegrityError

START = date(2026, 1, 1)
END = START + timedelta(days=100)


def test_rehydration_priority_limit_and_dedup():
    days = [START+timedelta(days=i) for i in range(20)]
    plan = archives.plan_archive_work(
        START, END, START, [], START, days, rehydrate=days)
    assert sum(mode == "rehydrate" for mode, _ in plan) == 4
    assert plan[0] == ("rehydrate", START)
    assert len(plan) == len({day for mode, day in plan})


@pytest.mark.parametrize("state,reason,expected", [
    ("complete", None, "recovered_same_sha"),
    ("cached", None, "recovered_same_sha"),
    ("failed", OSError("network offline"), "pending"),
    ("failed", ValueError("Previously completed archive SHA256 changed"),
     "blocked_original_sha"),
])
def test_rehydration_ledger_preserves_source_sha(monkeypatch, state, reason, expected):
    calls = []
    class DB:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, args=None):
            calls.append((sql, args))
            class Result:
                def fetchone(self): return {"state": "complete", "sha256": "a"*64}
            return Result()
    monkeypatch.setattr(archives, "connection", lambda _: DB())
    value = archives.record_archive_rehydration(
        SimpleNamespace(symbol="ETHUSDT", category="linear"),
        START, state, reason)
    assert value == expected
    assert len(calls) == 2
    assert calls[-1][1][3] == expected
    assert "a"*64 in calls[-1][1][-1]
    assert "UPDATE ingestion_job" not in calls[-1][0]


def test_rehydration_rejects_unknown_original_sha(monkeypatch):
    class DB:
        def __enter__(self): return self
        def __exit__(self,*a): return False
        def execute(self,*a):
            class Result:
                def fetchone(self): return {"state":"complete","sha256":None}
            return Result()
    monkeypatch.setattr(archives,"connection",lambda _:DB())
    with pytest.raises(DataIntegrityError, match="Missing immutable original SHA"):
        archives.record_archive_rehydration(
            SimpleNamespace(symbol="ETHUSDT",category="linear"),START,"complete")


def _completed_archive_db(monkeypatch, old_sha="a"*64):
    calls = []
    class DB:
        def __enter__(self): return self
        def __exit__(self,*a): return False
        def execute(self,sql,args=None):
            calls.append(sql)
            class Result:
                def fetchone(self):
                    if "SELECT state,sha256 FROM ingestion_job" in sql:
                        return {"state":"complete","sha256":old_sha}
                    return None
            return Result()
    monkeypatch.setattr(archives,"connection",lambda _:DB())
    monkeypatch.setattr(archives,"disk_usage",lambda _:SimpleNamespace(free=100*1024**3))
    return calls


def test_changed_remote_gzip_never_overwrites_original_sha(monkeypatch):
    calls = _completed_archive_db(monkeypatch)
    payload = gzip.compress(b"timestamp,trdMatchID,side,price,size\n")
    class Response:
        status_code=200
        async def __aenter__(self):return self
        async def __aexit__(self,*a):return False
        def raise_for_status(self):pass
        async def aiter_bytes(self):yield payload
    class Client:
        def stream(self,*a,**kw):return Response()
    settings=SimpleNamespace(symbol="ETHUSDT",category="linear",minimum_free_gb=0)
    with pytest.raises(DataIntegrityError, match="SHA256 changed"):
        asyncio.run(archives.download_archive(settings,START,Client()))
    assert not any("UPDATE ingestion_job" in sql for sql in calls)


def test_404_completed_archive_keeps_original_metadata(monkeypatch):
    calls = _completed_archive_db(monkeypatch)
    class Response:
        status_code=404
        async def __aenter__(self):return self
        async def __aexit__(self,*a):return False
    class Client:
        def stream(self,*a,**kw):return Response()
    settings=SimpleNamespace(symbol="ETHUSDT",category="linear",minimum_free_gb=0)
    with pytest.raises(DataIntegrityError, match="HTTP 404"):
        asyncio.run(archives.download_archive(settings,START,Client()))
    assert not any("UPDATE ingestion_job" in sql for sql in calls)


def test_catchup_adapts_and_stops_when_caught_up(monkeypatch):
    calls=[]
    async def cycle(settings):
        calls.append(1)
        return {"attempted":24,"unattempted_days":max(0,48-len(calls)*24),
                "sha_membership_missing_days":0}
    async def no_sleep(_):return None
    monkeypatch.setattr(archives,"backfill_archives",cycle)
    monkeypatch.setattr(archives.asyncio,"sleep",no_sleep)
    result=asyncio.run(archives.run_archive_catchup(object()))
    assert len(calls)==2 and result["unattempted_days"]==0


def test_catchup_keeps_rehydrating_after_fresh_cursor(monkeypatch):
    calls=[]
    async def cycle(settings):
        calls.append(1)
        return {"attempted":4,"unattempted_days":0,
                "sha_membership_missing_days":max(0,12-4*len(calls))}
    async def no_sleep(_):return None
    monkeypatch.setattr(archives,"backfill_archives",cycle)
    monkeypatch.setattr(archives.asyncio,"sleep",no_sleep)
    asyncio.run(archives.run_archive_catchup(object()))
    assert len(calls)==3


def test_catchup_never_exceeds_eight_cycles(monkeypatch):
    calls=[]
    async def cycle(settings):
        calls.append(1)
        return {"attempted":24,"unattempted_days":9999,
                "sha_membership_missing_days":9999}
    async def no_sleep(_):return None
    monkeypatch.setattr(archives,"backfill_archives",cycle)
    monkeypatch.setattr(archives.asyncio,"sleep",no_sleep)
    asyncio.run(archives.run_archive_catchup(object()))
    assert len(calls)==8


@pytest.mark.parametrize("cycles,seconds", [
    (0,1800),(9,1800),(8,1801),(8,20),(True,1800)])
def test_invalid_catchup_limits_rejected(cycles,seconds):
    with pytest.raises(ValueError):
        asyncio.run(archives.run_archive_catchup(
            object(),max_cycles=cycles,budget_seconds=seconds))
