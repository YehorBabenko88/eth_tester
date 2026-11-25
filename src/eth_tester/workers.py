"""Independent periodic research feature generation; run under local supervisor."""
import asyncio
import json
from .analytics import build_footprint,build_levels,enrich_features
from .database import connection
from .book_analysis import replay_orderbook

async def analytics_worker(settings):
    cycle=0
    while True:
        try:
            await asyncio.to_thread(replay_orderbook,settings)
            if cycle%5==0:
                for tf in ("1","3","5","15","30","60","120","240","360","720","D","W","M"):
                    await asyncio.to_thread(build_levels,settings,tf)
            for tf in ("1","3","5","15","30","60","120","240","360","720","D"):
                await asyncio.to_thread(build_footprint,settings,tf,2)
                if tf in ("1","5","15","60"):
                    await asyncio.to_thread(enrich_features,settings,tf,2)
            with connection(settings) as conn:
                conn.execute("""INSERT INTO bot_heartbeat(component,status)
                  VALUES('analytics','ok') ON CONFLICT(component)
                  DO UPDATE SET status='ok',updated_at=now()""")
        except Exception as exc:
            with connection(settings) as conn:
                conn.execute("""INSERT INTO bot_events(severity,event,data)
                  VALUES('error','analytics_failure',%s::jsonb)""",
                  (json.dumps({"error":str(exc)[:500]}),))
        cycle+=1
        await asyncio.sleep(180)
