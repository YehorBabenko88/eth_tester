import asyncio
import random
import time
import httpx

class BybitError(RuntimeError):
    pass

class MarketClient:
    def __init__(self, base, rps=3.0):
        if not base.startswith("https://"):
            raise ValueError("HTTPS required")
        self.base = base.rstrip("/")
        self.spacing = 1/max(0.1,rps)
        self.next_call = 0.0
        self._rate_lock = asyncio.Lock()
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(35), follow_redirects=False)

    async def __aenter__(self):
        return self

    async def __aexit__(self,*_):
        await self.client.aclose()

    async def get(self,path,**params):
        params = {k:v for k,v in params.items() if v is not None}
        for attempt in range(9):
            async with self._rate_lock:
                delay = self.next_call-time.monotonic()
                if delay>0:
                    await asyncio.sleep(delay)
                self.next_call = time.monotonic()+self.spacing
            try:
                response = await self.client.get(self.base+"/v5/market/"+path,params=params)
                if response.status_code in (401,403):
                    raise BybitError(f"Bybit geo/auth block HTTP {response.status_code}")
                if response.status_code==429 or response.status_code>=500:
                    raise httpx.HTTPStatusError("Retryable HTTP",request=response.request,response=response)
                if not 200 <= response.status_code < 300:
                    raise BybitError(f"Nonretryable HTTP {response.status_code}")
                data = response.json()
                if data.get("retCode")==0:
                    return data["result"]
                if data.get("retCode") not in (10006,10016):
                    raise BybitError(f"{path}: {data.get('retCode')} {data.get('retMsg')}")
                raise httpx.HTTPStatusError("Retryable retCode",request=response.request,response=response)
            except (httpx.TransportError,httpx.TimeoutException,httpx.HTTPStatusError) as exc:
                last=exc
                if attempt == 8:
                    break
                wait = min(120,2**attempt)+random.uniform(0,1)
                if isinstance(exc, httpx.HTTPStatusError):
                    headers = exc.response.headers
                    try:
                        wait = max(wait, float(headers.get('Retry-After', '0')))
                    except ValueError:
                        pass
                    try:
                        wait = max(wait, float(headers.get('X-Bapi-Limit-Reset-Timestamp', '0'))/1000-time.time())
                    except ValueError:
                        pass
                async with self._rate_lock:
                    self.next_call = max(self.next_call, time.monotonic()+wait)
                await asyncio.sleep(wait)
        raise BybitError(f"Retry budget exhausted for {path}") from last

    async def instrument(self,symbol,category="linear"):
        items=(await self.get("instruments-info",category=category,symbol=symbol))["list"]
        if not items:
            raise BybitError(f"Unknown instrument {symbol}")
        return items[0]
