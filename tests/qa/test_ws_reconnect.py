"""Mock WebSocket connection drops and retry backoff (no external socket)."""
import asyncio
from types import SimpleNamespace
import pytest
from eth_tester import live

class StopMockLoop(BaseException):
    pass

class FakeRows:
    def fetchone(self):
        return None

class FakeDb:
    def __init__(self, queries):
        self.queries = queries
    def __enter__(self):
        return self
    def __exit__(self, *_):
        return False
    def execute(self, query, *_args):
        self.queries.append(query)
        return FakeRows()

def test_disconnect_health_event_and_reconnect(monkeypatch):
    attempts, queries = [], []
    class BrokenWebsocket:
        async def __aenter__(self):
            raise ConnectionResetError("mock exchange disconnect")
        async def __aexit__(self, *_):
            return False
    def mocked_connect(uri, **kwargs):
        attempts.append(uri)
        return BrokenWebsocket()
    async def short_circuit_sleep(_duration):
        if len(attempts) >= 2:
            raise StopMockLoop()
    monkeypatch.setattr(live.websockets, "connect", mocked_connect)
    monkeypatch.setattr(live, "connection", lambda _: FakeDb(queries))
    monkeypatch.setattr(live.asyncio, "sleep", short_circuit_sleep)
    cfg = SimpleNamespace(book_depth=50, symbol="ETHUSDT", bybit_ws="wss://mock.invalid")
    with pytest.raises(StopMockLoop):
        asyncio.run(live.live_feed(cfg))
    assert attempts == ["wss://mock.invalid"] * 2
    assert sum("INSERT INTO feed_health" in q for q in queries) == 2
