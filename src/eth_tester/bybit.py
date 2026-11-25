import asyncio
import random
import time
import httpx

import hashlib
import json
import os
from pathlib import Path
import tempfile


class SharedRateBudget:
    """Crash-safe host-local REST budget shared by independent Python processes.

    Separate machines need an external coordination service; this does not assert
    exchange-global limits. All cooperating clients must use the same path.
    """

    def __init__(self, base, rps, path=None):
        key = hashlib.sha256(base.encode()).hexdigest()[:20]
        self.path = Path(path) if path else Path(tempfile.gettempdir()) / (
            f"eth_tester_bybit_budget_{key}.json")
        self.spacing = 1 / max(float(rps), 0.1)

    def _lock(self, file):
        if os.name == "nt":
            import msvcrt
            file.seek(0)
            msvcrt.locking(file.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(file.fileno(), fcntl.LOCK_EX)

    def _unlock(self, file):
        if os.name == "nt":
            import msvcrt
            file.seek(0)
            msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(file.fileno(), fcntl.LOCK_UN)

    def _update(self, cooldown=0):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a+b") as file:
            self._lock(file)
            try:
                file.seek(0)
                raw = file.read()
                state = json.loads(raw) if raw else {"next": 0}
                now = time.time()
                next_slot = max(now, float(state.get("next", 0)))
                if cooldown:
                    next_slot = max(next_slot, now + cooldown)
                    delay = 0
                else:
                    delay = next_slot - now
                    next_slot += self.spacing
                file.seek(0)
                file.truncate()
                file.write(json.dumps({"next": next_slot}).encode())
                file.flush()
                os.fsync(file.fileno())
                return delay
            finally:
                self._unlock(file)

    async def reserve(self):
        return await asyncio.to_thread(self._update)

    async def defer(self, seconds):
        return await asyncio.to_thread(self._update, max(0, seconds))


class BybitError(RuntimeError):
    pass

class MarketClient:
    def __init__(self, base, rps=3.0):
        if not base.startswith("https://"):
            raise ValueError("HTTPS required")
        self.base = base.rstrip("/")
        self._budget = SharedRateBudget(self.base, rps)
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(35), follow_redirects=False)

    async def __aenter__(self):
        return self

    async def __aexit__(self,*_):
        await self.client.aclose()

    async def get(self,path,**params):
        params = {k:v for k,v in params.items() if v is not None}
        for attempt in range(9):
            delay = await self._budget.reserve()
            if delay > 0:
                await asyncio.sleep(delay)
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
                await self._budget.defer(wait)
                await asyncio.sleep(wait)
        raise BybitError(f"Retry budget exhausted for {path}") from last

    async def instrument(self,symbol,category="linear"):
        items=(await self.get("instruments-info",category=category,symbol=symbol))["list"]
        if not items:
            raise BybitError(f"Unknown instrument {symbol}")
        return items[0]
