"""Local multi-process supervisor; independently restarts failed workers."""
import asyncio
import logging
import sys
from .database import init_database

async def child(command,recurring_seconds=None):
    delay=3.0
    while True:
        proc=await asyncio.create_subprocess_exec(sys.executable,"-m","eth_tester.cli",command)
        status=await proc.wait()
        if status==0:
            delay=3.0
            if recurring_seconds is None:
                return
            await asyncio.sleep(recurring_seconds)
        else:
            logging.error("%s stopped (%s), retry in %.0f seconds",command,status,delay)
            await asyncio.sleep(delay)
            delay=min(delay*2,300)

async def supervise(settings):
    init_database(settings)
    tasks=[("live",None),("analyse",None),("engine",None),
           ("backfill",86400),("archives",86400)]
    if settings.tg_bot_token and settings.tg_chat_id:
        tasks.append(("telegram",None))
    await asyncio.gather(*(child(name,interval) for name,interval in tasks))
