"""Telegram-команды бота: /status, /stats, /last, /settings, /help."""

import time
from datetime import datetime

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

router = Router()

# Через сколько секунд тишины поток считается неживым
STREAM_ALIVE_THRESHOLD = 30

FILTER_ROWS = (
    ("concentration", "отсеяно"),
    ("human", "отсеяно"),
    ("bundle", "отсеяно"),
    ("dev", "отсеяно"),
    ("score", "отсеяно"),
    ("passed", "прошло"),
)


def _allowed(message: Message, ctx) -> bool:
    return str(message.chat.id) == str(ctx.settings.telegram_chat_id)


def _uptime(seconds: float) -> str:
    total_minutes = int(seconds // 60)
    days, rest = divmod(total_minutes, 1440)
    hours, minutes = divmod(rest, 60)
    if days:
        return f"{days}д {hours}ч {minutes}м"
    return f"{hours}ч {minutes}м"


def _pct(part: int, total: int) -> str:
    if total <= 0:
        return "—"
    return f"{part / total * 100:.0f}%"


@router.message(Command("status"))
async def cmd_status(message: Message, ctx) -> None:
    if not _allowed(message, ctx):
        return
    counters = await ctx.stats.counters()

    if ctx.stats.last_event_at is None:
        stream = "🔴 событий ещё не было"
    else:
        ago = time.time() - ctx.stats.last_event_at
        if ago <= STREAM_ALIVE_THRESHOLD:
            stream = f"🟢 живой ({ago:.0f} сек назад)"
        else:
            stream = f"🔴 тишина ({ago:.0f} сек)"

    text = (
        "🩺 <b>Статус бота</b>\n"
        f"⏱ Аптайм: {_uptime(time.time() - ctx.stats.started_at)}\n"
        f"📡 Поток рынка: {stream}\n"
        f"📥 Событий получено: {counters.get('events_received', 0):,}\n"
        f"🛒 Покупок замечено: {counters.get('buys_seen', 0):,}\n"
        f"🔍 Транзакций проверено: {counters.get('tx_checked', 0):,}\n"
        f"📈 Расчётов капы: {counters.get('mc_calcs', 0):,}\n"
        f"🔔 Алертов отправлено: {counters.get('alerts_sent', 0):,}"
    )
    await message.answer(text)


@router.message(Command("stats"))
async def cmd_stats(message: Message, ctx) -> None:
    if not _allowed(message, ctx):
        return
    counters = await ctx.stats.counters()
    lines = [
        "📊 <b>Статистика скринера</b>",
        f"Всего алертов: {counters.get('alerts_sent', 0):,}",
    ]

    for window in ("1h", "6h"):
        data = await ctx.redis.hgetall(f"outcome:{window}")
        checked = int(data.get("checked", 0))
        grad = int(data.get("grad", 0))
        x13 = int(data.get("x13", 0))
        x2 = int(data.get("x2", 0))
        dead = int(data.get("dead", 0))
        lines += [
            "",
            f"Через {window} (проверено {checked}):",
            f"  🎓 Градуация: {grad} ({_pct(grad, checked)})",
            f"  📈 ≥1.3x: {x13} ({_pct(x13, checked)})",
            f"  🚀 ≥2x: {x2} ({_pct(x2, checked)})",
            f"  💀 Умерло: {dead} ({_pct(dead, checked)})",
        ]

    lines += ["", "🔬 <b>Работа фильтров</b> (градуации за 24ч среди отсеянных):"]
    for filter_name, verb in FILTER_ROWS:
        total = counters.get(f"filtered:{filter_name}", 0)
        grad_data = await ctx.redis.hgetall(f"gradstats:{filter_name}")
        checked = int(grad_data.get("checked", 0))
        graduated = int(grad_data.get("graduated", 0))
        lines.append(
            f"  {filter_name}: {verb} {total:,}, "
            f"градуировало {graduated}/{checked} ({_pct(graduated, checked)})"
        )
    await message.answer("\n".join(lines))


@router.message(Command("last"))
async def cmd_last(message: Message, ctx) -> None:
    if not _allowed(message, ctx):
        return
    alerts = await ctx.stats.last_alerts(5)
    if not alerts:
        await message.answer("Алертов пока не было.")
        return
    lines = ["🕐 <b>Последние алерты</b>"]
    for alert in alerts:
        when = datetime.fromtimestamp(alert["ts"]).strftime("%d.%m %H:%M")
        lines.append(
            f"• {when} — <b>{alert.get('name', '?')}</b> ({alert.get('symbol', '?')}), "
            f"MC ${alert.get('mc', 0):,.0f}\n  <code>{alert['mint']}</code>"
        )
    await message.answer("\n".join(lines))


@router.message(Command("settings"))
async def cmd_settings(message: Message, ctx) -> None:
    if not _allowed(message, ctx):
        return
    s = ctx.settings
    text = (
        "⚙️ <b>Текущие пороги</b>\n"
        f"Капа для анализа: ≥ ${s.mc_analyze_min:,.0f}\n"
        f"Диапазон алерта: ${s.alert_mc_min:,.0f}–${s.alert_mc_max:,.0f}\n"
        f"Guard при отправке: ${s.send_guard_mc_min:,.0f}–${s.send_guard_mc_max:,.0f}\n"
        f"Холдер: ≤ {s.holder_max_percent}% | Топ-10: &lt; {s.top10_max_percent}%\n"
        f"HUMAN: ≥ {s.human_min_percent:.0f}% | UNKNOWN: ≤ {s.unknown_max_percent:.0f}%\n"
        f"MSR: ≥ {s.msr_min_percent:.0f}% (мин. {s.dev_min_tokens} токенов, "
        f"{s.dev_history_days} дн.)\n"
        f"Бандлы: ≤ {s.max_bundle_percent:.0f}%\n"
        f"Мин. скор: {s.min_score} (веса H {s.score_weight_human:.2f} / "
        f"MSR {s.score_weight_msr:.2f} / C {s.score_weight_concentration:.2f} / "
        f"B {s.score_weight_bundle:.2f})\n"
        f"Выживший токен: объём > ${s.survivor_min_volume_usd:,.0f}"
    )
    await message.answer(text)


@router.message(Command("help", "start"))
async def cmd_help(message: Message, ctx) -> None:
    if not _allowed(message, ctx):
        return
    await message.answer(
        "🤖 <b>Команды</b>\n"
        "/status — аптайм, поток событий, счётчики\n"
        "/stats — статистика алертов и работы фильтров\n"
        "/last — последние 5 алертов\n"
        "/settings — текущие пороги фильтров\n"
        "/help — эта справка"
    )
