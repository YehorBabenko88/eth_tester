from datetime import datetime,timezone
import json
from .bybit import MarketClient
from .database import connection,cursor_ts,save_cursor
from .data_quality import FIXED_MS
from .history_integrity import collect_klines
from .history_series import collect_funding, collect_oi, collect_account_ratios

UTC=timezone.utc
INTERVAL_MS = {**FIXED_MS, "M": None}  # calendar months are not fixed durations
KIND_ENDPOINTS={"trade":"kline","mark":"mark-price-kline",
 "index":"index-price-kline","premium":"premium-index-price-kline"}
OI_INTERVALS=("5min","15min","30min","1h","4h","1d")

def dtms(ms):
    return datetime.fromtimestamp(int(ms)/1000,UTC)
def toms(dt):
    return int(dt.timestamp()*1000)

async def backfill_rest(settings):
    async with MarketClient(settings.bybit_rest,settings.rest_rps) as client:
        instrument=await client.instrument(settings.symbol,settings.category)
        with connection(settings) as conn:
            conn.execute("""INSERT INTO instrument_metadata(symbol,category,payload)
             VALUES(%s,%s,%s::jsonb) ON CONFLICT(symbol)
             DO UPDATE SET payload=EXCLUDED.payload,updated_at=now()""",
             (settings.symbol,settings.category,json.dumps(instrument)))
        errors=[]
        for kind in KIND_ENDPOINTS:
            for tf in INTERVAL_MS:
                try:
                    await collect_klines(settings,client,kind,tf)
                    print(f"completed {kind} {tf}",flush=True)
                except Exception as exc:
                    errors.append((kind,tf,str(exc)))
                    print(f"failed {kind} {tf}: {exc}",flush=True)
        try:
            await collect_funding(settings,client)
        except Exception as exc:
            errors.append(("funding","",str(exc)))
        for interval in OI_INTERVALS:
            try:
                await collect_oi(settings,client,interval)
            except Exception as exc:
                errors.append(("oi",interval,str(exc)))
        for period in OI_INTERVALS:
            try:
                await collect_account_ratios(settings,client,period)
            except Exception as exc:
                errors.append(('account_ratio',period,str(exc)))
        if errors:
            raise RuntimeError(f"Incomplete backfill: {errors}")

