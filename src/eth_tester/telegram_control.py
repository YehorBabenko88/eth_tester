import asyncio
from telegram import Update
from telegram.ext import Application,CommandHandler,ContextTypes
from .database import connection

def authorise(settings,update):
    return bool(update.effective_chat and settings.tg_chat_id
      and str(update.effective_chat.id)==settings.tg_chat_id)

async def run_telegram(settings):
    if not settings.tg_bot_token or not settings.tg_chat_id:
        raise RuntimeError("Missing Telegram token/chat ID")
    async def command(update:Update,context:ContextTypes.DEFAULT_TYPE):
        if not authorise(settings,update):
            return
        name=update.message.text.split()[0].split("@")[0].lower()
        with connection(settings) as conn:
            if name=="/pause":
                conn.execute("UPDATE bot_control SET paused=true,updated_at=now() WHERE singleton=true")
                msg="Paper bot paused, existing position remains open"
            elif name=="/resume":
                conn.execute("UPDATE bot_control SET enabled=true,paused=false,updated_at=now() WHERE singleton=true")
                msg="Paper bot enabled (no real orders)"
            elif name=="/stop":
                conn.execute("UPDATE bot_control SET enabled=false,paused=true,updated_at=now() WHERE singleton=true")
                msg="Paper bot disabled; data retained"
            elif name=="/stats":
                acc=conn.execute("SELECT * FROM paper_account WHERE id=1").fetchone()
                row=conn.execute("""SELECT count(*) n,count(*) FILTER (WHERE net_pnl>0) wins,
                   coalesce(sum(net_pnl),0) pnl FROM paper_trades WHERE state='closed'""").fetchone()
                msg=f"Closed: {row['n']}; wins: {row['wins']}; net: {row['pnl']}; equity: {acc['equity']}"
            elif name=="/disk":
                size=conn.execute("SELECT pg_size_pretty(pg_database_size(current_database())) AS size").fetchone()
                msg=f"PostgreSQL: {size['size']} (excluding external archives)"
            elif name=="/progress":
                rows=conn.execute("SELECT state,count(*) n FROM ingestion_job GROUP BY state").fetchall()
                msg=", ".join(f"{r['state']}: {r['n']}" for r in rows) or "No jobs"
            elif name=="/levels":
                rows=conn.execute("""SELECT tf,side,level_price FROM levels WHERE symbol=%s
                  AND status='unbroken' ORDER BY origin_ts DESC LIMIT 12""",(settings.symbol,)).fetchall()
                msg="\n".join(f"{r['tf']} {r['side']} {r['level_price']}" for r in rows) or "No levels"
            elif name=="/trade":
                rows=conn.execute("""SELECT id,direction,entry,exit,net_pnl FROM paper_trades
                  ORDER BY id DESC LIMIT 5""").fetchall()
                msg="\n".join(str(dict(r)) for r in rows) or "No trades"
            elif name=="/chart":
                if not context.args or not context.args[0].isdigit():
                    msg="Usage: /chart 123"
                else:
                    from .charts import render_trade
                    try:
                        image_path=await asyncio.to_thread(render_trade,settings,int(context.args[0]))
                        with open(image_path,"rb") as photo:
                            await update.message.reply_photo(photo=photo,caption="Paper trade chart")
                        return
                    except (ValueError,RuntimeError) as exc:
                        msg=f"Chart unavailable: {exc}"
            elif name=="/gaps":
                gaps=conn.execute("SELECT source,start_ts FROM data_gaps WHERE resolved_at IS NULL LIMIT 5").fetchall()
                msg=str([dict(g) for g in gaps]) if gaps else "No marked data gaps"
            elif name=="/delete":
                msg="Safety restriction: no remote database erasure; use a local administrator"
            else:
                msg="/pause /resume /stop /stats /disk /progress /levels /trade /delete"
        await update.message.reply_text(msg[:4000])
    app=Application.builder().token(settings.tg_bot_token).build()
    for cmd in ("start","help","pause","resume","stop","stats","disk",
                "progress","levels","trade","chart","gaps","delete"):
        app.add_handler(CommandHandler(cmd,command))
    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        try:
            await asyncio.Event().wait()
        finally:
            await app.updater.stop()
            await app.stop()
