"""A Round13 regression: statuses, rotating recovery, no pytest-asyncio dependency."""
from datetime import date,timedelta
from types import SimpleNamespace
import pytest
from eth_tester import archives

START=date(2026,1,1)
END=START+timedelta(days=300)


def test_bounded_fresh_and_past_recheck():
    jobs=archives.plan_archive_work(START,END,None,[],None,[])
    assert 0 < len(jobs) <= 32
    assert jobs[0]==("fresh",START)
    assert len({day for _,day in jobs})==len(jobs)


def test_recheck_after_fresh_cursor_caught_up():
    jobs=archives.plan_archive_work(START,END,END,[],START,[])
    assert jobs and all(mode=="recheck" for mode,_ in jobs)


def test_prior_pending_recheck_first():
    old=START+timedelta(days=150)
    jobs=archives.plan_archive_work(START,END,END,[],START,[old])
    assert jobs[0]==("recheck",old)


def test_failed_retry_is_not_fresh_cursor():
    old=START+timedelta(days=150)
    jobs=archives.plan_archive_work(START,END,START,[old],START,[])
    assert jobs[0]==("retry",old)
    assert jobs[1]==("fresh",START)


def test_recheck_utc_wrap_and_excludes_today():
    jobs=archives.plan_archive_work(START,END,END,[],END-timedelta(days=3),[])
    assert [day for _,day in jobs][:4] == [
        END-timedelta(days=3),END-timedelta(days=2),
        END-timedelta(days=1),START]
    assert all(day < END for _,day in jobs)


@pytest.mark.parametrize("bad",[
    {"status":"volume_only_reconciled","minutes_checked":1439,
     "minutes_matched":1439,"exchange_complete":False},
    {"status":"volume_only_reconciled","minutes_checked":1440,
     "minutes_matched":1440,"exchange_complete":True},
    {"status":"mismatch","minutes_checked":1440,
     "minutes_matched":1440,"exchange_complete":False},
    {"status":"pending_official_1m"},
    {"status":"volume_match","minutes_checked":1440,
     "minutes_matched":1440,"exchange_complete":False},
])
def test_comparison_never_promotes_unproven_volume(bad):
    assert archives.archive_volume_reconciled(bad) is False


def test_official_1440_volume_only_reconciliation():
    proof={"status":"volume_only_reconciled","minutes_checked":1440,
           "minutes_matched":1440,"exchange_complete":False}
    assert archives.archive_volume_reconciled(proof) is True


def test_no_unbounded_cycle():
    with pytest.raises(ValueError):
        archives.plan_archive_work(START,END,START,[],START,[],fresh_limit=0)


def test_durable_pending_then_volume_only_reconciled(monkeypatch):
    statuses=[]
    class Connection:
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def execute(self,statement,params=None):
            statuses.append(params[3])
    monkeypatch.setattr(archives,"connection",lambda settings:Connection())
    settings=SimpleNamespace(symbol="ETHUSDT",category="linear")
    assert archives.record_archive_recheck(
        settings,START,{"status":"pending_official_1m"}) is False
    assert archives.record_archive_recheck(
        settings,START,{"status":"volume_only_reconciled",
        "minutes_checked":1440,"minutes_matched":1440,
        "exchange_complete":False}) is True
    assert statuses==["pending","volume_only_reconciled"]


def test_failed_database_recheck_receipt_fails_closed(monkeypatch):
    class BadConnection:
        def __enter__(self): raise OSError("db offline")
        def __exit__(self,*args): pass
    monkeypatch.setattr(archives,"connection",lambda _:BadConnection())
    with pytest.raises(OSError,match="db offline"):
        archives.record_archive_recheck(
            SimpleNamespace(symbol="ETHUSDT",category="linear"),
            START,{"status":"pending_official_1m"})
