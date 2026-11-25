from datetime import datetime, timedelta, timezone
import pytest
from eth_tester.naked_poc import NakedPOCBook

UTC=timezone.utc


def test_naked_poc_not_touched_before_availability():
    t=datetime(2026,1,1,tzinfo=UTC)
    book=NakedPOCBook()
    assert book.register("D",t,100,available_at=t+timedelta(days=1),
                         quality="partial") is None
    state=book.register("D",t,100,available_at=t+timedelta(days=2),
                        observed_at=t+timedelta(days=2),
                        quality="verified")
    assert book.observe("D",{"ts":t+timedelta(days=1),"low":90,"high":110})==[]
    assert state.status=="naked"
    assert book.observe("D",{"ts":t+timedelta(days=2),"low":90,"high":110})==[state]
    assert state.status=="touched"
    assert state.touched_at==t+timedelta(days=3)
    assert book.nearest("D",100)==[]


def test_naked_poc_out_of_order_fail_closed():
    t=datetime(2026,1,1,tzinfo=UTC)
    book=NakedPOCBook()
    with pytest.raises(ValueError):
        book.register("D",t,100,available_at=t,quality="verified")
    book.observe("D",{"ts":t,"low":90,"high":110})
    with pytest.raises(ValueError):
        book.observe("D",{"ts":t,"low":90,"high":110})


def test_nearest_naked_poc_is_price_distance():
    t=datetime(2026,1,1,tzinfo=UTC)
    book=NakedPOCBook()
    for price in (90,110,120):
        book.register("D",t,price,available_at=t+timedelta(days=1),
                      quality="verified")
    assert [x["state"].price for x in book.nearest("D",100,limit=2)]==[90,110]
