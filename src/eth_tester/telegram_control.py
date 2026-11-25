"""Paper-only ETH Tester control: private Telegram, Russian buttons and audit.

Remote database deletion is fail-closed until Storage Guardian #2 approves a contract.
"""

import asyncio
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil

from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from .database import connection

LOGGER = logging.getLogger(__name__)
MENU = (
    ("▶️ Запуск", "⏸ Пауза", "⏯ Продолжить", "⏹ Стоп"),
    ("📈 Позиция", "📜 Сделки", "📊 Статистика"),
    ("📥 Загрузка", "💾 Диск", "🖥 Система"),
    ("⚙️ Процессы", "🔌 WebSocket", "⚠️ Ошибки", "🕳 Пропуски"),
    ("📉 График", "🧱 Уровни", "❓ Помощь"),
    ("🩺 Здоровье", "🛡 Риск", "📚 Модели", "🧪 Исследования"),
    ("📡 Покрытие", "📌 Сигналы", "🧹 Обслуживание"),
)
ALIASES = {
    "▶️ Запуск": "startsim", "⏸ Пауза": "pause", "⏯ Продолжить": "resume",
    "⏹ Стоп": "stop", "📈 Позиция": "position", "📜 Сделки": "history",
    "📊 Статистика": "stats", "📥 Загрузка": "progress", "💾 Диск": "disk",
    "🖥 Система": "system", "⚙️ Процессы": "processes", "🔌 WebSocket": "ws",
    "⚠️ Ошибки": "errors", "🕳 Пропуски": "gaps", "📉 График": "chart",
    "🧱 Уровни": "levels", "❓ Помощь": "help",
    "🩺 Здоровье": "health", "🛡 Риск": "risk",
    "📚 Модели": "models", "🧪 Исследования": "research",
    "📡 Покрытие": "coverage", "📌 Сигналы": "signals",
    "🧹 Обслуживание": "maintenance",
}
COMMANDS = (
    "start", "help", "startsim", "pause", "resume", "stop", "position",
    "history", "trade", "stats", "progress", "disk", "system", "processes",
    "ws", "errors", "gaps", "chart", "levels", "delete",
    "health", "risk", "models", "research", "coverage", "signals", "maintenance",
)


def authorise(settings, update):
    """Fail closed: match chat ID, human user ID AND private chat."""
    user = getattr(update, "effective_user", None)
    chat = getattr(update, "effective_chat", None)
    owner = os.environ.get("ETH_TESTER_TG_USER_ID", "").strip()
    return bool(
        user is not None and getattr(user, "is_bot", True) is False
        and chat is not None and getattr(chat, "type", None) == "private"
        and bool(getattr(settings, "tg_chat_id", None)) and owner.isdecimal()
        and str(getattr(chat, "id", "")) == str(settings.tg_chat_id).strip()
        and str(getattr(user, "id", "")) == owner
    )


def _audit(settings, update, name, accepted, outcome):
    """Only IDs, command names and outcomes are stored; no text or token."""
    chat = getattr(update, "effective_chat", None)
    user = getattr(update, "effective_user", None)
    detail = {
        "command": name[:48], "accepted": accepted, "outcome": outcome[:48],
        "chat_id": str(getattr(chat, "id", "")) if chat is not None else None,
        "user_id": str(getattr(user, "id", "")) if user is not None else None,
    }
    try:
        with connection(settings) as conn:
            conn.execute(
                """INSERT INTO bot_events(severity,event,data)
                   VALUES(%s,'telegram_command',%s::jsonb)""",
                ("info" if accepted else "warning", json.dumps(detail)),
            )
    except Exception:
        LOGGER.exception("Telegram command audit unavailable")
        if accepted:
            raise RuntimeError("Refusing command without audit") from None


def _control(settings, name):
    states = {
        "startsim": (True, False, "Бумажная симуляция запущена"),
        "resume": (True, False, "Бумажная симуляция продолжена"),
        "pause": (True, True, "Новые входы приостановлены; позиция остаётся под наблюдением"),
        "stop": (False, True, "Новые входы остановлены; позиция остаётся под наблюдением"),
    }
    enabled, paused, result = states[name]
    with connection(settings) as conn:
        conn.execute(
            """UPDATE bot_control SET enabled=%s,paused=%s,updated_at=now()
               WHERE singleton=true""",
            (enabled, paused),
        )
    return result + ". Реальные ордера не отправляются."


def _read(settings, name):
    with connection(settings) as conn:
        if name == "position":
            row = conn.execute(
                """SELECT id,direction,entry,qty,stop,take,opened_at,reason
                   FROM paper_trades WHERE state='open' ORDER BY id DESC LIMIT 1"""
            ).fetchone()
            return (
                f"Бумажная позиция #{row['id']} {row['direction']}\n"
                f"Вход {row['entry']}, qty {row['qty']}, stop {row['stop']}, take {row['take']}\n"
                f"Открыта {row['opened_at']}\nПричина: {str(row['reason'])[:900]}"
                if row else "Открытых позиций нет"
            )
        if name in ("history", "trade"):
            rows = conn.execute(
                """SELECT id,state,direction,entry,exit,net_pnl,exit_reason,reason
                   FROM paper_trades ORDER BY id DESC LIMIT 8"""
            ).fetchall()
            return "\n".join(
                f"#{r['id']} {r['direction']} ({r['state']}) {r['entry']} → {r['exit']} "
                f"net {r['net_pnl']} | {r['exit_reason'] or '-'} | {str(r['reason'])[:110]}"
                for r in rows
            ) or "Сделок пока нет"
        if name == "stats":
            acc = conn.execute(
                "SELECT equity FROM paper_account WHERE id=1"
            ).fetchone()
            r = conn.execute(
                """SELECT count(*) n,count(*) FILTER (WHERE net_pnl>0) wins,
                   coalesce(sum(gross_pnl),0) gross,coalesce(sum(net_pnl),0) net,
                   coalesce(sum(fee_paid),0) fees,coalesce(sum(funding_paid),0) funding,
                   coalesce(sum(slippage_paid),0) slippage
                   FROM paper_trades WHERE state='closed'"""
            ).fetchone()
            rate = 100 * r["wins"] / r["n"] if r["n"] else 0
            return (
                f"Закрыто {r['n']}, прибыльных {r['wins']} ({rate:.1f}%)\n"
                f"Gross {r['gross']}, Net {r['net']}, Equity {acc['equity'] if acc else '-'}\n"
                f"Комиссии {r['fees']}, funding {r['funding']}, slippage {r['slippage']}"
            )
        if name == "progress":
            jobs = conn.execute(
                """SELECT source,state,count(*) n,coalesce(sum(bytes_downloaded),0) bytes,
                   coalesce(sum(row_count),0) rows FROM ingestion_job
                   GROUP BY source,state ORDER BY source,state"""
            ).fetchall()
            cursors = conn.execute(
                """SELECT source,interval,status,cursor_ts FROM ingestion_cursor
                   ORDER BY source,interval LIMIT 60"""
            ).fetchall()
            lines = [
                f"{r['source']}: {r['state']}: {r['n']} дней, {r['rows']} строк, {r['bytes']} B"
                for r in jobs
            ] + [
                f"{r['source']}/{r['interval']}: {r['status']} до {r['cursor_ts']}"
                for r in cursors
            ]
            return "Загрузка по источникам:\n" + "\n".join(lines) if lines else "Нет загрузок"
        if name == "disk":
            row = conn.execute(
                """SELECT current_database() db,
                   pg_size_pretty(pg_database_size(current_database())) size"""
            ).fetchone()
            disk = shutil.disk_usage(Path.cwd())
            return (
                f"PostgreSQL {row['db']}: {row['size']}\n"
                f"Свободно {disk.free / 1024**3:.2f} из {disk.total / 1024**3:.2f} GiB "
                "(архивы не входят в размер БД)"
            )
        if name == "processes":
            rows = conn.execute(
                """SELECT component,status,updated_at FROM bot_heartbeat
                   ORDER BY component LIMIT 30"""
            ).fetchall()
            lines = [f"{r['component']}: {r['status']} ({r['updated_at']})" for r in rows]
            from .supervisor import runtime_path
            path = runtime_path() / "supervisor.json"
            if path.is_file():
                try:
                    state = json.loads(path.read_text(encoding="utf-8"))
                    lines += [
                        f"Supervisor {key}: {value.get('state')} pid={value.get('pid', '-')}"
                        for key, value in state.items()
                    ]
                except (ValueError, OSError):
                    lines.append("Состояние Supervisor недоступно")
            return "\n".join(lines) or "Heartbeat отсутствует"
        if name == "ws":
            rows = conn.execute(
                """SELECT topic,status,ts FROM feed_health WHERE symbol=%s
                   ORDER BY ts DESC LIMIT 12""", (settings.symbol,)
            ).fetchall()
            latest = conn.execute(
                """SELECT ts FROM trades WHERE symbol=%s AND source='websocket'
                   ORDER BY ts DESC LIMIT 1""", (settings.symbol,)
            ).fetchone()
            if latest and latest["ts"]:
                age = (datetime.now(timezone.utc) - latest["ts"]).total_seconds()
                freshness = "✅ поток сделок активен" if -2 <= age <= 45 else "⚠️ поток сделок устарел"
                prefix = f"{freshness}; последняя сделка {latest['ts']}"
            else:
                prefix = "⚠️ нет подтверждённых WebSocket-сделок"
            details = "\n".join(
                f"{r['topic']}: {r['status']} ({r['ts']})" for r in rows
            ) or "Телеметрия WebSocket отсутствует"
            return prefix + "\n" + details
        if name == "errors":
            rows = conn.execute(
                """SELECT ts,severity,event FROM bot_events
                   WHERE severity IN ('error','warning') ORDER BY ts DESC LIMIT 12"""
            ).fetchall()
            return "\n".join(
                f"{r['ts']}: {r['severity']} {r['event']}" for r in rows
            ) or "Ошибок не отмечено"
        if name == "gaps":
            rows = conn.execute(
                """SELECT source,start_ts,end_ts FROM data_gaps
                   WHERE resolved_at IS NULL ORDER BY start_ts DESC LIMIT 12"""
            ).fetchall()
            return "\n".join(
                f"{r['source']}: {r['start_ts']} — {r['end_ts']}" for r in rows
            ) or "Открытых пропусков нет"
        if name == "levels":
            rows = conn.execute(
                """SELECT tf,side,level_price FROM levels WHERE symbol=%s
                   AND status='unbroken' ORDER BY origin_ts DESC LIMIT 12""",
                (settings.symbol,)
            ).fetchall()
            return "\n".join(
                f"{r['tf']} {r['side']} {r['level_price']}" for r in rows
            ) or "Нет уровней"
    return "Неизвестная команда"


def _system():
    try:
        import psutil  # optional: dependency coordination belongs to MASTER
    except ImportError:
        return "CPU/RAM недоступны: установите необязательный пакет psutil"
    ram = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.1)
    return (
        f"CPU {cpu:.1f}% | RAM {ram.percent:.1f}%\n"
        f"Доступно RAM {ram.available / 1024**3:.2f} из {ram.total / 1024**3:.2f} GiB"
    )


async def dispatch(settings, update, context, name):
    from .telegram_views import allow_message, pages
    chat = getattr(update, "effective_chat", None)
    user = getattr(update, "effective_user", None)
    if not allow_message((str(getattr(chat, "id", "")),
                          str(getattr(user, "id", "")))):
        return  # Per-process burst limit, including unauthenticated requests.
    if not authorise(settings, update):
        _audit(settings, update, name, False, "denied")
        return
    if name not in COMMANDS:
        _audit(settings, update, name, False, "unknown")
        return
    if name == "start":
        name = "help"  # /start displays help, never starts a simulation
    try:
        _audit(settings, update, name, True, "requested")
        if name == "help":
            answer = (
                "ETH Tester — бумажная симуляция, без реальных ордеров.\n"
                "/startsim /pause /resume /stop — управление входами\n"
                "/position /history /stats — позиция, сделки, расходы\n"
                "/chart ID — график сделки\n"
                "/progress /disk /system /processes /ws /errors /gaps /levels\n"
                "/trade ID — подробности конкретной бумажной сделки\n"
                "/health /risk /signals /models /research /coverage /maintenance\n"
                "/delete — удаление по Telegram отключено"
            )
        elif name in ("startsim", "pause", "resume", "stop"):
            answer = await asyncio.to_thread(_control, settings, name)
        elif name == "delete":
            answer = "Удаление БД по Telegram заблокировано до согласования со Storage Guardian."
        elif name == "system":
            answer = await asyncio.to_thread(_system)
        elif name == "trade":
            args = context.args or []
            if len(args) != 1 or not args[0].isdecimal() or not 0 < int(args[0]) < 2**63:
                answer = "Укажите ID бумажной сделки: /trade 123"
            else:
                from .telegram_views import detail_trade
                answer = await asyncio.to_thread(detail_trade, settings, int(args[0]))
        elif name in ("health", "risk", "signals", "models", "research",
                      "coverage", "maintenance"):
            from .telegram_views import read_view
            answer = await asyncio.to_thread(read_view, settings, name)
        elif name == "chart":
            args = context.args or []
            if not args or not args[0].isdigit() or int(args[0]) <= 0:
                answer = "Укажите ID сделки: /chart 123. Список: /history"
            else:
                from .charts import render_trade
                path = await asyncio.to_thread(render_trade, settings, int(args[0]))
                with open(path, "rb") as photo:
                    await update.effective_message.reply_photo(
                        photo=photo, caption=f"Бумажная сделка #{args[0]}"
                    )
                _audit(settings, update, name, True, "completed")
                return
        else:
            answer = await asyncio.to_thread(_read, settings, name)
        for page_number, page in enumerate(pages(answer)):
            await update.effective_message.reply_text(
                page, reply_markup=(ReplyKeyboardMarkup(MENU, resize_keyboard=True)
                                    if page_number == 0 else None)
            )
        _audit(settings, update, name, True, "completed")
    except Exception:
        LOGGER.exception("Telegram command %s failed", name)
        try:
            _audit(settings, update, name, True, "failed")
        except RuntimeError:
            pass
        await update.effective_message.reply_text(
            "Не удалось выполнить запрос. Проверьте журнал ошибок."
        )


async def run_telegram(settings):
    if not settings.tg_bot_token or not settings.tg_chat_id or not os.environ.get(
        "ETH_TESTER_TG_USER_ID"
    ):
        raise RuntimeError("Telegram requires TG_BOT_TOKEN, TG_CHAT_ID and ETH_TESTER_TG_USER_ID")
    app = Application.builder().token(settings.tg_bot_token).build()

    async def command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        name = update.effective_message.text.split()[0].split("@")[0].lstrip("/").lower()
        await dispatch(settings, update, context, name)

    async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
        name = ALIASES.get(update.effective_message.text)
        if name:
            await dispatch(settings, update, context, name)
        elif not authorise(settings, update):
            _audit(settings, update, "unknown_text", False, "denied")

    for item in COMMANDS:
        app.add_handler(CommandHandler(item, command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu))
    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        try:
            await asyncio.Event().wait()
        finally:
            await app.updater.stop()
            await app.stop()
