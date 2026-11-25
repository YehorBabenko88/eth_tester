from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from eth_tester.analytics_core import (
    FRAMES, LevelEngine, bucket, close_time, delta_open_interest, footprint,
    market_indicators, quality_flags, tape_metrics, value_profile,
)


UTC = timezone.utc


def stamp(day):
    return datetime(2026, 1, day, tzinfo=UTC)


def bar(ts, high, low, close):
    return {"ts": ts, "high": Decimal(high), "low": Decimal(low),
            "close": Decimal(close), "closed": True}


def test_all_thirteen_bucket_boundaries():
    assert len(FRAMES) == 13
    assert bucket(datetime(2026, 1, 7, 11, 30, tzinfo=UTC), "W") == stamp(5)
    assert bucket(datetime(2026, 2, 28, tzinfo=UTC), "M") == datetime(
        2026, 2, 1, tzinfo=UTC)
    assert close_time(datetime(2026, 2, 1, tzinfo=UTC), "M") == datetime(
        2026, 3, 1, tzinfo=UTC)
    assert close_time(stamp(5), "W") == stamp(12)
    for tf in FRAMES:
        assert bucket(stamp(6), tf).tzinfo is not None


def test_value_area_tie_break_and_nodes():
    result = value_profile({100: 3, 101: 10, 102: 1, 103: 8, 104: 2})
    assert result["poc"] == Decimal(101)
    assert result["vpoc"] == Decimal(101)
    assert result["val"] == Decimal(100)
    assert result["vah"] == Decimal(103)
    assert result["hvn"] == [Decimal(101), Decimal(103)]
    assert result["lvn"] == [Decimal(102)]
    assert value_profile({})["poc"] is None
    assert value_profile({100: 5, 101: 5})["poc"] == 100


def test_footprint_delta_and_diagonal_stack():
    trades = [
        {"price": str(p), "qty": "10", "side": "Buy"}
        for p in (100, 101, 102, 103)
    ] + [{"price": "100", "qty": "1", "side": "Sell"}]
    result = footprint(trades, min_stack=3)
    assert result["buy"] == 40
    assert result["sell"] == 1
    assert result["delta"] == 39
    assert result["stacked_imbalance"]["buy"] == [[Decimal(101), Decimal(102),
                                                  Decimal(103)]]
    with pytest.raises(ValueError):
        footprint([{"price": 100, "qty": -1, "side": "Buy"}])


def test_wilder_atr_realized_volatility_vwap_and_oi():
    bars = [
        {"high": "11", "low": "9", "close": "10", "volume": "5"},
        {"high": "12", "low": "10", "close": "11", "volume": "5"},
        {"high": "13", "low": "11", "close": "12", "volume": "10"},
    ]
    result = market_indicators(bars, atr_period=2)
    assert result["atr"] == 2
    assert result["range"] == 4
    assert result["vwap"] is not None
    assert result["realized_volatility"] > 0
    assert delta_open_interest(120, 100) == 20
    assert delta_open_interest(None, 100) is None
    with pytest.raises(ValueError):
        market_indicators([dict(bars[0], closed=False)])


def test_quality_requires_grid_and_comparable_volume():
    grid = [stamp(1), stamp(2)]
    assert quality_flags(archive_complete=True, expected_candles=grid,
                         actual_candles=grid, trade_volume=10,
                         candle_volume=10)["complete"]
    assert quality_flags(archive_complete=True, expected_candles=grid,
                         actual_candles=grid[:1], trade_volume=10,
                         candle_volume=10)["reason"] == "candle_grid_mismatch"
    assert quality_flags(archive_complete=True, expected_candles=grid,
                         actual_candles=grid)["reason"] == "volume_unavailable"
    assert quality_flags(archive_complete=False, expected_candles=grid,
                         actual_candles=grid)["reason"] == "archive_incomplete"


def test_tape_signals_are_proxies():
    trades = [{"price": "100", "qty": "30", "side": "Buy"},
              {"price": "100", "qty": "2", "side": "Sell"}]
    result = tape_metrics(trades, duration_seconds=4, large_trade_qty=20,
                          reference_tps=2)
    assert result["large_count"] == 1
    assert result["tps"] == Decimal("0.5")
    assert result["absorption_proxy"] is True
    assert result["exhaustion_proxy"] is True


def test_causal_confirmation_and_lifecycle():
    engine = LevelEngine("D", flank=1)
    assert engine.update(bar(stamp(1), "10", "6", "8")) == []
    assert engine.update(bar(stamp(2), "12", "7", "10")) == []
    emitted = engine.update(bar(stamp(3), "11", "8", "9"))
    level = next(level for level, event in emitted if event == "confirmed"
                 and level.side == "resistance")
    assert level.price == 12
    assert level.origin_ts == stamp(2)
    assert level.confirmation_ts == stamp(4)
    assert level.touches == 0
    events = engine.update(bar(stamp(4), "12", "9", "11"))
    assert any(event == "touch" for _, event in events)
    assert level.touches == 1
    events = engine.update(bar(stamp(5), "14", "11", "13"))
    assert any(event == "break" for _, event in events)
    assert level.broken_at == stamp(6)
    events = engine.update(bar(stamp(6), "14", "12", "13"))
    assert any(event == "retest" for _, event in events)
    events = engine.update(bar(stamp(7), "13", "10", "11"))
    assert any(event == "invalidation" for _, event in events)
    assert level.status == "invalidated"


def test_level_nearest_and_no_future_data():
    engine = LevelEngine("1", flank=1)
    for i, high in enumerate((10, 20, 10)):
        engine.update(bar(stamp(1) + timedelta(minutes=i), str(high),
                          "5", "8"))
    assert len(engine.nearest(10)) == 1
    assert engine.nearest(10)[0]["distance"] == 10
    with pytest.raises(ValueError):
        engine.update(bar(stamp(1), "12", "5", "7"))
    with pytest.raises(ValueError):
        LevelEngine("M").update(dict(bar(stamp(1), "12", "5", "7"), closed=False))
