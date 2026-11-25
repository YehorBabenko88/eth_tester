"""Bounded, owner-authorized, SELECT-only paper control views.

Telegram auth and auditing belong to telegram_control; this module never opens
paper positions, changes bot_control, promotes a model, or deletes data.
"""

import json
import os
import re
import stat
import time
from pathlib import Path

from .database import connection

COMMANDS = frozenset(("trade", "signals", "models", "research", "risk",
                      "health", "coverage", "maintenance"))


_SECRET_KEY = re.compile(r"token|secret|password|passwd|credential|api.?key|private.?key|"
                         r"authorization|cookie|session.?key|database.?url|dsn", re.I)
_BEARER = re.compile(r"(?i)\bBearer\s+[^\s,;]+")
_INLINE_SECRET = re.compile(r"(?i)\b(password|secret|token|api_key)\s*[:=]\s*[^\s,;]+")
_DATABASE_URL = re.compile(r"(?i)\b(postgresql?://)[^\s]+")


def _redact(value, depth=0):
    if depth >= 8:
        return "[nested content omitted]"
    if isinstance(value, dict):
        result = {}
        for index, (key, item) in enumerate(list(value.items())[:256]):
            key = str(key)[:100]
            if _SECRET_KEY.search(key):
                result[f"[redacted field {index}]"] = "[REDACTED]"
            else:
                result[key] = _redact(item, depth+1)
        if len(value) > 256:
            result["_truncated"] = True
        return result
    if isinstance(value, (list, tuple)):
        return [_redact(item, depth+1) for item in value[:256]]
    if isinstance(value, str):
        text = _BEARER.sub("Bearer [REDACTED]", value)
        text = _INLINE_SECRET.sub(lambda m: m.group(1)+"=[REDACTED]", text)
        return _DATABASE_URL.sub(r"\1[REDACTED]", text)[:4000]
    if isinstance(value, (bool, type(None), int, float)):
        return value
    return str(value)[:100]


def _json(value, limit=900):
    raw = json.dumps(_redact(value), ensure_ascii=False, default=str, sort_keys=True)
    return raw if len(raw) <= limit else raw[:limit] + " [truncated: query specific record]"


def sanitize_message(message):
    """A last-resort output filter, including legacy /history and /position."""
    if not isinstance(message, str):
        raise ValueError("Invalid Telegram output")
    value = _BEARER.sub("Bearer [REDACTED]", message)
    value = _INLINE_SECRET.sub(lambda m: m.group(1) + "=[REDACTED]", value)
    return _DATABASE_URL.sub(r"\1[REDACTED]", value)


def existing_trade_chart(settings, trade_id, *, root=None):
    """Read only a previously stored chart from an exact local owned directory.

    In particular NEVER call charts.render_trade: that function changes the
    paper_trades ledger and overwrites PNG bytes when Telegram merely reads.
    """
    if type(trade_id) is not int or not 0 < trade_id < 2**63:
        raise ValueError("Valid positive paper trade ID required")
    with connection(settings) as conn:
        row = conn.execute("""SELECT id,closed_at,entry_chart,exit_chart
            FROM paper_trades WHERE id=%s AND symbol=%s""",
            (trade_id, settings.symbol)).fetchone()
    if row is None:
        raise FileNotFoundError("Paper trade not found")
    stage = "exit" if row.get("closed_at") is not None else "entry"
    recorded = row.get(stage + "_chart")
    trusted = Path(root) if root is not None else Path.cwd() / "runtime" / "charts"
    if not trusted.is_dir() or trusted.is_symlink():
        raise FileNotFoundError("Owned paper chart root unavailable")
    expected = trusted / f"paper_trade_{trade_id}_{stage}.png"
    if not recorded or expected.is_symlink() or not expected.is_file():
        raise FileNotFoundError("Previously stored paper chart unavailable")
    # The recorded path must match the exact predetermined trade/stage name.
    if Path(recorded).resolve(strict=True) != expected.resolve(strict=True):
        raise FileNotFoundError("Unexpected paper chart identity")
    # No FOLLOW on the leaf, and verify PNG signature on the opened descriptor.
    # The trusted parent must be ACL-protected against path substitution.
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(expected, flags)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise FileNotFoundError("Paper chart is not a regular file")
        if os.read(fd, 8) != b"\x89PNG\r\n\x1a\n":
            raise FileNotFoundError("Stored paper chart has invalid PNG header")
        os.lseek(fd, 0, os.SEEK_SET)
        # Keep the SAME verified descriptor through Telegram send; never
        # reopen by pathname after a race-prone check.
        return os.fdopen(fd, "rb")
    except BaseException:
        os.close(fd)
        raise


def detail_trade(settings, trade_id):
    if type(trade_id) is not int or not 0 < trade_id < 2**63:
        raise ValueError("Valid positive paper trade ID required")
    with connection(settings) as conn:
        row = conn.execute("""SELECT id,symbol,state,direction,signal_ts,opened_at,
                 closed_at,entry,exit,qty,stop,take,gross_pnl,net_pnl,
                 fee_paid,funding_paid,slippage_paid,exit_reason,reason
                 FROM paper_trades WHERE symbol=%s AND id=%s""",
                           (settings.symbol, trade_id)).fetchone()
    if row is None:
        return "Бумажная сделка не найдена"
    return (f"Бумажная сделка #{row['id']} [{row['state']}] {row['direction']}\n"
            f"Решение {row['signal_ts']}; открыта {row['opened_at']}; закрыта {row['closed_at']}\n"
            f"Вход {row['entry']} → выход {row['exit']}; qty {row['qty']}\n"
            f"Stop {row['stop']}; Take {row['take']}; причина выхода {row['exit_reason']}\n"
            f"Gross {row['gross_pnl']}; Net {row['net_pnl']}; комиссии {row['fee_paid']}; "
            f"funding {row['funding_paid']}; slippage {row['slippage_paid']}\n"
            f"Причины и доказательства (не сертификат ML):\n{_json(row['reason'], limit=24000)}")


def read_view(settings, name):
    if name not in COMMANDS - {"trade"}:
        raise ValueError("Unknown read-only Telegram view")
    with connection(settings) as conn:
        if name == "health":
            rows = conn.execute("""SELECT component,status,updated_at
                FROM bot_heartbeat ORDER BY component LIMIT 40""").fetchall()
            gaps = conn.execute("""SELECT count(*) AS n FROM data_gaps
                WHERE symbol=%s AND resolved_at IS NULL""", (settings.symbol,)).fetchone()
            live = conn.execute("""SELECT status,ts FROM feed_health
                WHERE symbol=%s AND topic='connection' ORDER BY ts DESC LIMIT 1""",
                (settings.symbol,)).fetchone()
            return (f"Здоровье (НЕ разрешение торговать)\nИсточник: {live or 'неизвестен'}\n"
                    f"Неустранённых пропусков: {gaps['n']}\n" + "\n".join(
                        f"{r['component']}: {r['status']} ({r['updated_at']})" for r in rows))
        if name == "risk":
            ctrl = conn.execute("""SELECT enabled,paused,updated_at
                FROM bot_control WHERE singleton=true""").fetchone()
            acc = conn.execute("""SELECT equity,initial_equity
                FROM paper_account WHERE id=1""").fetchone()
            positions = conn.execute("""SELECT count(*) AS n,
                coalesce(sum(qty*entry),0) AS notional FROM paper_trades
                WHERE state='open' AND symbol=%s""", (settings.symbol,)).fetchone()
            engine = conn.execute("""SELECT status,detail,updated_at
                FROM bot_heartbeat WHERE component='paper_engine'""").fetchone()
            review = conn.execute("""SELECT ts,data FROM bot_events
                WHERE event='paper_trade_review_v1'
                ORDER BY ts DESC LIMIT 1""").fetchone()
            return (f"Риск только бумажной торговли\nУправление: {_json(ctrl)}\n"
                    f"Счёт: {_json(acc)}\nОткрытых позиций: {positions['n']}; "
                    f"номинал: {positions['notional']}\n"
                    f"Engine/разрыв потока: {_json(engine, 600)}\n"
                    f"Последний paper_trade_review_v1 (НЕ ML): {_json(review, 600)}\n"
                    "Реальные ордера запрещены")
        if name == "signals":
            rows = conn.execute("""SELECT bar_ts,tf,decision,version
                FROM strategy_signals WHERE symbol=%s
                ORDER BY bar_ts DESC LIMIT 20""", (settings.symbol,)).fetchall()
            return "Последние бумажные сигналы:\n" + (
                "\n".join(f"{r['bar_ts']} {r['tf']} {r['decision']} {r['version']}"
                          for r in rows) or "нет")
        if name == "models":
            rows = conn.execute("""SELECT id,created_at,model_name,approved,validation
                FROM model_versions ORDER BY id DESC LIMIT 10""").fetchall()
            return "Исследовательские версии (не автодопуск):\n" + (
                "\n".join(f"#{r['id']} {r['model_name']} approved={r['approved']} "
                          f"{r['created_at']}\n{_json(r['validation'], 300)}"
                          for r in rows) or "нет")
        if name == "research":
            rows = conn.execute("""SELECT component,status,detail,updated_at
                FROM bot_heartbeat WHERE component IN
                ('control_label_horizons','control_train_causal_shadow')
                ORDER BY component""").fetchall()
            return "Исследовательская выборка НЕПОЛНАЯ: нет всех no_signal/no_trade bars; это не OOS и не сертификат ML.\n" + (
                "\n".join(f"{r['component']}: {r['status']} {r['updated_at']} "
                          f"{_json(r['detail'], 400)}" for r in rows) or "нет")
        if name == "coverage":
            rows = conn.execute("""SELECT source,interval,status,quality,
                checked_at FROM ingestion_coverage WHERE symbol=%s
                ORDER BY checked_at DESC LIMIT 24""", (settings.symbol,)).fetchall()
            return "Покрытие источников (quality — не допуск к ML):\n" + (
                "\n".join(f"{r['source']}/{r['interval']} {r['status']} "
                          f"{r['quality']} ({r['checked_at']})" for r in rows) or "нет")
        if name == "maintenance":
            rows = conn.execute("""SELECT count(*) AS n FROM bot_events
                WHERE ts < clock_timestamp()-interval '180 days'
                AND event <> 'paper_trade_review_v1'
                AND coalesce(data->>'retention','') <> 'preserve'""").fetchone()
            feed = conn.execute("""SELECT count(*) AS n FROM feed_health
                WHERE ts < clock_timestamp()-interval '90 days'""").fetchone()
            return ("Обслуживание: ТОЛЬКО диагностика\n"
                    f"Старых bot_events: {rows['n']}, feed_health: {feed['n']}\n"
                    "Периодическая очистка максимум 10000 строк/таблицу; "
                    "сделки, свечи, архивы, ML-метки не удаляются.\n"
                    "Дистанционное удаление БД отключено.")
    raise ValueError("Unreachable view")


def pages(message, *, chunk_limit=3500, max_chunks=8):
    """Telegram UTF-16 safe paging, explicit omission marker, never silent loss."""
    if not isinstance(message, str):
        raise ValueError("Invalid Telegram output")
    if not 400 <= chunk_limit <= 3900 or not 1 <= max_chunks <= 32:
        raise ValueError("Unsafe Telegram page limits")
    # Reserve UTF-16 units for page labels and truncation indicator.
    budget = chunk_limit - 90
    chunks = []
    position = 0
    while position < len(message) and len(chunks) < max_chunks:
        units = 0
        end = position
        while end < len(message):
            needed = 2 if ord(message[end]) > 0xffff else 1
            if units + needed > budget:
                break
            units += needed
            end += 1
        chunks.append(message[position:end])
        position = end
    if position < len(message):
        chunks[-1] += "\n[Ответ ограничен: запросите конкретную запись]"
    total = len(chunks)
    result = [f"({i}/{total})\n{part}" if total > 1 else part
              for i, part in enumerate(chunks, 1)] or ["Нет данных"]
    assert all(len(value.encode("utf-16-le"))//2 <= chunk_limit for value in result)
    return result


# Per-process *admission* throttle. B owns the durable alert/outbox ledger;
# this map is not presented as a durable or cross-process rate limit.
_RATE_WINDOW_S = 60
_RATE_PER_WINDOW = 20
_RATE_LIMIT_TRACKED = 1024
_rate_times = {}


def allow_message(identity, *, now=None):
    if not isinstance(identity, tuple) or len(identity) != 2:
        return False
    clock = time.monotonic() if now is None else now
    # Expire old identities before accepting any new memory allocation.
    stale = [key for key, entries in _rate_times.items()
             if not entries or entries[-1] <= clock - _RATE_WINDOW_S]
    for key in stale:
        _rate_times.pop(key, None)
    if identity not in _rate_times and len(_rate_times) >= _RATE_LIMIT_TRACKED:
        return False
    entries = _rate_times.setdefault(identity, [])
    entries[:] = [stamp for stamp in entries if stamp > clock - _RATE_WINDOW_S]
    if len(entries) >= _RATE_PER_WINDOW:
        return False
    entries.append(clock)
    return True
