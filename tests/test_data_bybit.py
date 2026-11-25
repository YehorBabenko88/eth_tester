import asyncio
import json

import httpx
import pytest

from eth_tester.bybit import BybitError, MarketClient, SharedRateBudget


def test_shared_rate_budget_across_independent_instances(tmp_path):
    path = tmp_path / "budget.json"
    one = SharedRateBudget("https://api.bybit.com", 2, path=path)
    two = SharedRateBudget("https://api.bybit.com", 2, path=path)
    first = one._update()
    second = two._update()
    assert first >= 0
    assert second > 0.4
    assert json.loads(path.read_text())["next"] > 0


def test_corrupt_budget_fails_closed(tmp_path):
    path = tmp_path / "budget.json"
    path.write_text("NOT JSON")
    with pytest.raises(ValueError):
        SharedRateBudget("https://api.bybit.com", 1, path=path)._update()


def test_http_403_is_not_retried(tmp_path):
    requests = []

    async def scenario():
        def handler(request):
            requests.append(request)
            return httpx.Response(403, request=request)

        client = MarketClient("https://api.bybit.com")
        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client._budget = SharedRateBudget("test403", 5000, path=tmp_path / "b")
        async with client:
            with pytest.raises(BybitError, match="403"):
                await client.get("kline", interval="1")

    asyncio.run(scenario())
    assert len(requests) == 1


def test_http_429_retries_and_propagates_budget(tmp_path, monkeypatch):
    requests = []
    sleeps = []

    async def instant_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", instant_sleep)

    async def scenario():
        def handler(request):
            requests.append(request)
            if len(requests) == 1:
                return httpx.Response(429, headers={"Retry-After": "2"},
                                      request=request)
            return httpx.Response(200, json={"retCode": 0, "result": {"list": []}},
                                  request=request)

        client = MarketClient("https://api.bybit.com")
        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client._budget = SharedRateBudget("test429", 10000, path=tmp_path / "b")
        async with client:
            assert (await client.get("kline")) == {"list": []}

    asyncio.run(scenario())
    assert len(requests) == 2
    assert max(sleeps) >= 2


def test_http_400_is_not_retried(tmp_path):
    requests = []

    async def scenario():
        def handler(request):
            requests.append(request)
            return httpx.Response(400, request=request)

        client = MarketClient("https://api.bybit.com")
        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client._budget = SharedRateBudget("test400", 10000, path=tmp_path / "b")
        async with client:
            with pytest.raises(BybitError, match="400"):
                await client.get("kline")

    asyncio.run(scenario())
    assert len(requests) == 1


def test_non_retryable_bybit_retcode(tmp_path):
    requests = []

    async def scenario():
        def handler(request):
            requests.append(request)
            return httpx.Response(200, json={
                "retCode": 10001, "retMsg": "Invalid params"}, request=request)

        client = MarketClient("https://api.bybit.com")
        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client._budget = SharedRateBudget("testcode", 10000, path=tmp_path / "b")
        async with client:
            with pytest.raises(BybitError, match="10001"):
                await client.get("kline")

    asyncio.run(scenario())
    assert len(requests) == 1
