import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from eth_tester.data_quality import DataIntegrityError
from eth_tester.recent_recovery import normalize_recent_trade, recover_recent_public_trades

UTC = timezone.utc


def trade(identifier="id-1", qty="1", symbol="ETHUSDT"):
    return {
        "symbol": symbol, "execId": identifier,
        "time": str(int((datetime.now(UTC) - timedelta(seconds=3)).timestamp() * 1000)),
        "price": "3000.5", "size": qty, "side": "Sell", "seq": "21",
    }


def test_recent_trade_parser_preserves_utc_and_units():
    result = normalize_recent_trade(trade(), "ETHUSDT")
    assert result[2] == "id-1"
    assert result[3] == Decimal("3000.5")
    assert result[4] == Decimal("1")
    assert result[5] == "Sell"
    assert result[6] == Decimal("3000.5")
    assert result[1].tzinfo == UTC


def test_recent_trade_rejects_symbol_and_future():
    with pytest.raises(DataIntegrityError, match="symbol mismatch"):
        normalize_recent_trade(trade(symbol="BTCUSDT"), "ETHUSDT")
    future = trade()
    future["time"] = str(int((datetime.now(UTC) + timedelta(minutes=5))
                             .timestamp() * 1000))
    with pytest.raises(DataIntegrityError, match="Future"):
        normalize_recent_trade(future, "ETHUSDT")


def test_recent_trade_rejects_invalid_number():
    invalid = trade()
    invalid["price"] = "NaN"
    with pytest.raises(DataIntegrityError, match="Malformed"):
        normalize_recent_trade(invalid, "ETHUSDT")


def test_recent_trade_rejects_identity_collision_within_page(monkeypatch):
    import eth_tester.recent_recovery as recovery

    class Conn:
        def execute(self, *_args, **_kw):
            class Cursor:
                def fetchall(self):
                    return [{"id": 7, "start_ts": datetime.now(UTC), "end_ts": None}]
            return Cursor()

    @contextmanager
    def conn(_):
        yield Conn()
    monkeypatch.setattr(recovery, "connection", conn)

    class Client:
        async def get(self, *_args, **_kwargs):
            return {"list": [trade("same", "1"), trade("same", "2")]}

    with pytest.raises(DataIntegrityError, match="Conflicting REST recent"):
        asyncio.run(recover_recent_public_trades(
            SimpleNamespace(symbol="ETHUSDT", category="linear"), Client()))


def test_recent_trade_never_resolves_gap_from_short_rest_page(monkeypatch):
    import eth_tester.recent_recovery as recovery
    writes = []

    class Conn:
        def execute(self, *_args, **_kw):
            class Cursor:
                def fetchall(self):
                    return [{"id": 7, "start_ts": datetime.now(UTC), "end_ts": None}]
            return Cursor()

    @contextmanager
    def conn(_):
        yield Conn()

    monkeypatch.setattr(recovery, "connection", conn)
    def copy(conn, rows):
        writes.append(("copy", len(rows)))
        return {"input_rows": len(rows), "inserted_rows": len(rows)}
    monkeypatch.setattr(recovery, "copy_trades", copy)
    monkeypatch.setattr(recovery, "write_coverage",
                        lambda _conn, **kw: writes.append(("coverage", kw)))

    class Client:
        async def get(self, path, **kw):
            assert path == "recent-trade"
            assert kw["limit"] == 1000
            return {"list": [trade()]}

    result = asyncio.run(recover_recent_public_trades(
        SimpleNamespace(symbol="ETHUSDT", category="linear"), Client()))
    assert result["status"] == "partial"
    assert writes[0] == ("copy", 1)
    assert writes[1][1]["status"] == "partial"
    assert writes[1][1]["evidence"]["cannot_certify_full_coverage"] is True


def test_recent_api_skips_without_recent_gap(monkeypatch):
    import eth_tester.recent_recovery as recovery

    class Conn:
        def execute(self, *_args, **_kw):
            class Cursor:
                def fetchall(self):
                    return []
            return Cursor()
    @contextmanager
    def conn(_):
        yield Conn()
    monkeypatch.setattr(recovery, "connection", conn)

    class Client:
        async def get(self, *_a, **_kw):
            raise AssertionError("No recent gap: must not call Bybit")

    assert asyncio.run(recover_recent_public_trades(
        SimpleNamespace(symbol="ETHUSDT", category="linear"), Client())
    ) == {"status": "not_needed", "rows": 0}
