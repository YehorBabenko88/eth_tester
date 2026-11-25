from datetime import datetime, timezone
from decimal import Decimal

import pytest

from eth_tester.data_quality import (
    DataIntegrityError, next_open, open_time, reconcile_minute_volume,
    validate_candle_page, validate_period_page, window_grid,
)

UTC = timezone.utc


def ms(year, month, day, hour=0, minute=0):
    return int(datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp() * 1000)


def row(stamp, close="100"):
    return [str(stamp), "100", "102", "99", close, "1", "100"]


def test_calendar_month_leap():
    jan = ms(2024, 1, 1)
    assert next_open("M", jan) == ms(2024, 2, 1)
    assert next_open("M", ms(2024, 2, 1)) == ms(2024, 3, 1)


def test_week_starts_monday():
    assert open_time("W", ms(2026, 11, 25)) == ms(2026, 11, 23)


def test_monthly_closed_grid():
    assert list(window_grid("M", ms(2024, 1, 1), ms(2024, 3, 1),
                            ms(2024, 3, 20))) == [ms(2024, 1, 1), ms(2024, 2, 1)]


def test_valid_complete_candle_window():
    start = ms(2026, 1, 1)
    records, status = validate_candle_page(
        [row(start + i * 60000) for i in (2, 1, 0)], "1",
        start, start + 2 * 60000, start + 3 * 60000)
    assert status == "verified"
    assert len(records) == 3


def test_conflicting_duplicate_rejected():
    start = ms(2026, 1, 1)
    with pytest.raises(DataIntegrityError, match="Conflicting"):
        validate_candle_page([row(start), row(start, "101")], "1", start,
                             start, start + 60000)


def test_identical_duplicate_accepted():
    start = ms(2026, 1, 1)
    assert validate_candle_page([row(start), row(start)], "1", start,
                                start, start + 60000)[1] == "verified"


def test_missing_middle_candle_rejected():
    start = ms(2026, 1, 1)
    with pytest.raises(DataIntegrityError, match="Missing"):
        validate_candle_page([row(start), row(start + 2 * 60000)], "1",
                             start, start + 2 * 60000, start + 3 * 60000)


def test_off_grid_candle_rejected():
    start = ms(2026, 1, 1)
    with pytest.raises(DataIntegrityError, match="Off-grid"):
        validate_candle_page([row(start + 1000)], "1",
                             start, start + 60000, start + 2 * 60000)


def test_unavailable_not_verified():
    start = ms(2026, 1, 1)
    assert validate_candle_page([], "1", start, start, start + 60000)[1] == "unavailable"


def test_open_candle_not_verified():
    start = ms(2026, 1, 1)
    assert validate_candle_page([row(start)], "1", start, start, start)[1] == "no_closed_bars"


def test_oi_gap_rejected():
    start = ms(2026, 1, 1)
    with pytest.raises(DataIntegrityError, match="Missing"):
        validate_period_page([{"timestamp": start, "openInterest": "1"},
                              {"timestamp": start + 600000, "openInterest": "2"}],
                             "5min", start, start + 600000, values=("openInterest",))


def test_oi_conflicting_duplicate_rejected():
    start = ms(2026, 1, 1)
    with pytest.raises(DataIntegrityError, match="Conflicting"):
        validate_period_page([{"timestamp": start, "openInterest": "1"},
                              {"timestamp": start, "openInterest": "2"}],
                             "5min", start, start, values=("openInterest",))


def test_ratio_partial_not_complete():
    start = ms(2026, 1, 1)
    result, status = validate_period_page(
        [{"timestamp": start, "buyRatio": "0.6", "sellRatio": "0.4"}],
        "5min", start, start + 600000, values=("buyRatio", "sellRatio"))
    assert len(result) == 1
    assert status == "observed_partial"


def test_minute_volume_requires_full_day():
    day = datetime(2026, 1, 1, tzinfo=UTC)
    assert reconcile_minute_volume([(day, Decimal("2"))],
                                   [(day, Decimal("2"))])["status"] == "incomplete_candles"


def test_volume_mismatch_is_not_verified():
    day = datetime(2026, 1, 1, tzinfo=UTC)
    from datetime import timedelta
    candles = [(day + timedelta(minutes=i), Decimal("1")) for i in range(1440)]
    result = reconcile_minute_volume([(day, Decimal("3"))], candles)
    assert result["status"] == "volume_mismatch"
    assert result["mismatches"] == 1440


def test_volume_matching_all_minutes():
    day = datetime(2026, 1, 1, tzinfo=UTC)
    from datetime import timedelta
    candles = [(day + timedelta(minutes=i), Decimal("1")) for i in range(1440)]
    assert reconcile_minute_volume(candles, candles)["status"] == "verified"
