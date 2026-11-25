"""Deterministic Bybit REST error/retry contracts; no external network."""
import asyncio

import httpx
import pytest

from eth_tester import bybit


def run(coro):
    return asyncio.run(coro)


async def mock_request(replies):
    seen = []
    iterator = iter(replies)

    def handler(request):
        seen.append((request.method, request.url.path, dict(request.url.params)))
        status, payload = next(iterator)
        return httpx.Response(status, json=payload, request=request)

    async with bybit.MarketClient("https://offline.invalid", rps=100000) as client:
        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        result = await client.get("kline", symbol="ETHUSDT", category="linear")
    return result, seen


def test_market_only_get_and_params():
    result, seen = run(mock_request([(200, {"retCode": 0, "result": {"list": []}})]))
    assert result == {"list": []}
    assert seen == [("GET", "/v5/market/kline", {"symbol": "ETHUSDT", "category": "linear"})]


def test_429_and_5xx_retries(monkeypatch):
    async def no_wait(_seconds):
        return None

    monkeypatch.setattr(bybit.asyncio, "sleep", no_wait)
    replies = [
        (429, {"retCode": 10006}),
        (503, {"retCode": 10016}),
        (200, {"retCode": 0, "result": {"list": ["recovered"]}}),
    ]
    result, seen = run(mock_request(replies))
    assert result["list"] == ["recovered"]
    assert len(seen) == 3


def test_retryable_exchange_retcode(monkeypatch):
    async def no_wait(_seconds):
        return None

    monkeypatch.setattr(bybit.asyncio, "sleep", no_wait)
    result, seen = run(mock_request([
        (200, {"retCode": 10006, "retMsg": "rate limited"}),
        (200, {"retCode": 0, "result": {"list": []}}),
    ]))
    assert result == {"list": []}
    assert len(seen) == 2


def test_auth_block_is_terminal():
    with pytest.raises(bybit.BybitError, match="403"):
        run(mock_request([(403, {"retCode": 10003})]))


def test_retry_budget_is_bounded(monkeypatch):
    async def no_wait(_seconds):
        return None

    monkeypatch.setattr(bybit.asyncio, "sleep", no_wait)
    with pytest.raises(bybit.BybitError, match="Retry budget exhausted"):
        run(mock_request([(503, {})] * 9))
