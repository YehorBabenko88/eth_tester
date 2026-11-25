import asyncio
import random
import time
import httpx

import hashlib
import json
import os
import math
import stat
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
        request_rate = float(rps)
        if not math.isfinite(request_rate) or request_rate <= 0:
            raise ValueError("REST rate limit must be finite and positive")
        self.spacing = 1 / max(request_rate, 0.1)

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
        """Recover truncated shared budget under OS lock without rate-limit bypass.

        A terminated writer can leave malformed JSON. Instead of crashing every
        worker indefinitely or resetting to zero requests, preserve a conservative
        60s cooldown and atomically rewrite a canonical bounded next timestamp.
        The rate file contains no keys, but is protected from symlink/file attacks.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_symlink():
            raise BybitError("Shared REST budget must not be a symlink")
        flags = (os.O_CREAT | os.O_RDWR |
                 getattr(os, "O_BINARY", 0) |
                 getattr(os, "O_CLOEXEC", 0) |
                 getattr(os, "O_NOFOLLOW", 0))
        # Exclusive creation distinguishes first use from a crashed writer
        # leaving an EMPTY existing file; the latter must cool down instead
        # of silently resetting the global rate budget.
        try:
            fd = os.open(self.path, flags | os.O_EXCL, 0o600)
            created = True
        except FileExistsError:
            fd = os.open(self.path, flags, 0o600)
            created = False
        with os.fdopen(fd, "r+b") as file:
            if not stat.S_ISREG(os.fstat(file.fileno()).st_mode):
                raise BybitError("Shared REST budget must be a regular file")
            if os.name != "nt":
                if os.fstat(file.fileno()).st_uid != os.geteuid():
                    raise BybitError("Shared REST budget has a different owner")
                os.fchmod(file.fileno(), 0o600)
            self._lock(file)
            try:
                file.seek(0)
                raw = file.read(8193)
                now = time.time()
                corrupted = False
                try:
                    if len(raw) > 8192:
                        raise ValueError("Oversized shared rate-budget file")
                    if not raw and not created:
                        raise ValueError("Truncated existing REST budget file")
                    state = json.loads(raw) if raw else {"next": 0}
                    if not isinstance(state, dict):
                        raise ValueError("Malformed REST budget state")
                    if type(state["next"]) not in (int, float):
                        raise ValueError("Non-numeric REST budget timestamp")
                    saved = float(state["next"])
                    if not math.isfinite(saved) or saved < 0:
                        raise ValueError("Non-finite shared budget timestamp")
                except (ValueError, KeyError, TypeError, OverflowError):
                    # Never turn a corrupt on-disk budget into a free request.
                    saved = now + 60
                    corrupted = True
                # A nonsensical future timestamp indicates clock corruption
                # or a malicious file, not an unlimited multi-day lease.
                if saved > now + 86400:
                    saved = now + 60
                    corrupted = True
                delay = max(0, saved - now)
                wanted = float(cooldown)
                if not math.isfinite(wanted) or wanted < 0:
                    raise ValueError("Invalid REST cooldown")
                if wanted:
                    next_slot = max(saved, now + wanted)
                    delay = 0
                else:
                    next_slot = max(now, saved) + self.spacing
                file.seek(0)
                file.truncate()
                file.write(json.dumps({"next": next_slot}, allow_nan=False).encode())
                file.flush()
                os.fsync(file.fileno())
                if corrupted:
                    # Consumer sees the timeout; never misreport a good
                    # exchange response or pretend the old budget was valid.
                    return max(delay, 60 if not wanted else 0)
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
                        seconds = float(headers.get('Retry-After', '0'))
                        if math.isfinite(seconds):
                            wait = max(wait, max(0, seconds))
                    except (ValueError, OverflowError):
                        pass
                    try:
                        reset = float(headers.get(
                            'X-Bapi-Limit-Reset-Timestamp', '0'))
                        if math.isfinite(reset):
                            wait = max(wait, max(0, reset/1000-time.time()))
                    except (ValueError, OverflowError):
                        pass
                # Bounded cooldown prevents hostile/invalid retry metadata
                # from hanging the 24/7 observer for days. Still obey normal
                # Bybit REST backoff and never downgrade TLS/HTTPS.
                wait = min(300, wait)
                await self._budget.defer(wait)
                await asyncio.sleep(wait)
        raise BybitError(f"Retry budget exhausted for {path}") from last

    async def instrument(self,symbol,category="linear"):
        items=(await self.get("instruments-info",category=category,symbol=symbol))["list"]
        if not items:
            raise BybitError(f"Unknown instrument {symbol}")
        return items[0]
