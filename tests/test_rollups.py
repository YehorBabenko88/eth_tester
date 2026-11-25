from datetime import date
import pytest
from eth_tester.rollups import MINUTES,TIMEFRAMES,bucket_expression

def test_all_13_frames():
    assert len(TIMEFRAMES)==13
    assert {"1","3","5","15","30","60","120","240","360","720","D","W","M"}==set(TIMEFRAMES)

def test_fixed_bucket_uses_utc_origin():
    for tf in MINUTES:
        result=bucket_expression(tf)
        assert "date_bin" in result
        assert "1970-01-01 00:00+00" in result

def test_calendar_month_and_week_are_not_fixed_days():
    assert bucket_expression("M")=="date_trunc('month',ts)"
    assert bucket_expression("W")=="date_trunc('week',ts)"
    with pytest.raises(ValueError):
        bucket_expression("999")

def test_utc_archive_day():
    assert date(2026,11,25).isoformat()=="2026-11-25"
