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
    ("age", "отсеяно"),
    ("inactive", "отсеяно"),
    ("concentration", "отсеяно"),
    ("dev", "отсеяно"),
    ("human_strict", "отсеяно"),
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

    calls_24h = ctx.helius.calls_last_24h()
    helius_k = calls_24h // 1000
    text = (
        "🩺 <b>Статус бота</b>\n"
        f"⏱ Аптайм: {_uptime(time.time() - ctx.stats.started_at)}\n"
        f"📡 Поток рынка: {stream}\n"
        f"📥 Событий получено: {counters.get('events_received', 0):,}\n"
        f"🛒 Покупок замечено: {counters.get('buys_seen', 0):,}\n"
        f"🔍 Транзакций проверено: {counters.get('tx_checked', 0):,}\n"
        f"📈 Расчётов капы: {counters.get('mc_calcs', 0):,}\n"
        f"🔔 Алертов отправлено: {counters.get('alerts_sent', 0):,}\n"
        f"💳 Кредиты Helius: расход ~{helius_k} тыс./сутки (лимит 1M/мес)"
    )
    await message.answer(text)


FILTER_LABELS = {
    "age":         ("⏳", "Возраст"),
    "inactive":    ("💤", "Активность"),
    "concentration": ("🏦", "Концентрация"),
    "dev":         ("👨‍💻", "Дев / Early buy"),
    "human_strict":("👥", "HUMAN strict"),
    "score":       ("🎯", "Score / Vol / Buyers"),
    "passed":      ("✅", "Прошло все фильтры"),
}


@router.message(Command("stats"))
async def cmd_stats(message: Message, ctx) -> None:
    if not _allowed(message, ctx):
        return
    counters = await ctx.stats.counters()
    alerts   = counters.get("alerts_sent", 0)
    no_win   = counters.get("no_window", 0)
    guard    = counters.get("guard_out", 0)

    lines = [
        "📊 <b>Статистика скринера</b>",
        "",
        f"🔔 Алертов отправлено: <b>{alerts:,}</b>",
        f"⌛ Не вошли в окно капы: {no_win:,}  ·  🚫 Отменено guard: {guard:,}",
    ]

    for window in ("1h", "6h"):
        data    = await ctx.redis.hgetall(f"outcome:{window}")
        checked = int(data.get("checked", 0))
        grad    = int(data.get("grad", 0))
        x13     = int(data.get("x13", 0))
        x2      = int(data.get("x2", 0))
        dead    = int(data.get("dead", 0))
        lines += [
            "",
            f"<b>📈 Исходы через {window}</b>  (проверено: {checked})",
            f"  🎓 Graduated  — {grad} ({_pct(grad, checked)})",
            f"  📊 Рост ≥1.3x — {x13} ({_pct(x13, checked)})",
            f"  🚀 Рост ≥2x   — {x2} ({_pct(x2, checked)})",
            f"  💀 Умерло     — {dead} ({_pct(dead, checked)})",
        ]

    lines += ["", "<b>🔬 Воронка фильтров</b>  (grad = градуировало за 24ч)"]
    for filter_name, verb in FILTER_ROWS:
        total     = counters.get(f"filtered:{filter_name}", 0)
        grad_data = await ctx.redis.hgetall(f"gradstats:{filter_name}")
        checked   = int(grad_data.get("checked", 0))
        graduated = int(grad_data.get("graduated", 0))
        icon, label = FILTER_LABELS.get(filter_name, ("·", filter_name))
        grad_str = _pct(graduated, checked) if checked else "—"
        action   = "прошло" if filter_name == "passed" else "отсеяно"
        lines.append(f"  {icon} {label}  —  {action} {total:,}  ·  grad {grad_str}")

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
    early_buy = "✅ вкл" if s.dev_early_buy_required else "❌ откл"
    lines = [
        "⚙️ <b>Настройки скринера</b>",
        "",
        "🕐 <b>Торговые сессии</b>",
        f"  🇪🇺 Европа:   {s.eu_session_start:02d}:00 – {s.eu_session_end:02d}:00  ({s.timezone})",
        f"  🇺🇸 Америка:  {s.us_session_start:02d}:00 – {s.us_session_end:02d}:00  ({s.timezone})",
        "",
        "💰 <b>Market Cap</b>",
        f"  Порог анализа:    ≥ ${s.mc_analyze_min:,.0f}",
        f"  Диапазон алерта:  ${s.alert_mc_min:,.0f} – ${s.alert_mc_max:,.0f}",
        f"  Guard при отправке: ${s.send_guard_mc_min:,.0f} – ${s.send_guard_mc_max:,.0f}",
        "",
        "🔎 <b>Предфильтры</b>",
        f"  Возраст:           ≤ {s.max_token_age_hours:.0f}ч",
        f"  Последняя сделка:  ≤ {s.max_inactive_seconds:.0f}с назад",
        f"  Покупок / 2 мин:   ≥ {s.min_buy_count_last_2min}",
        f"  Объём / 5 мин:     ≥ ${s.min_volume_usd_5min:,.0f}",
        f"  Покупателей / 5 мин: ≥ {s.min_unique_buyers_5min}",
        "",
        "🎯 <b>Качество токена</b>",
        f"  Макс. холдер:  {'отключён' if s.holder_max_percent >= 100 else f'≤ {s.holder_max_percent}%'}"
        f"  ·  Топ-10: {'отключён' if s.top10_max_percent >= 100 else f'≤ {s.top10_max_percent}%'}",
        f"  HUMAN:         ≥ {s.human_min_percent:.0f}%  ·  UNKNOWN: ≤ {s.unknown_max_percent:.0f}%",
        f"  Бандлы:        ≤ {s.max_bundle_percent:.0f}%",
        f"  MSR:           ≥ {s.msr_min_percent:.0f}%  (мин. {s.dev_min_tokens} токена, {s.dev_history_days} дн.)",
        f"  Выжившие токены: объём > ${s.survivor_min_volume_usd:,.0f}",
        f"  Ранняя покупка дева: {early_buy}  (окно {s.dev_early_buy_window}с)",
        "",
        "🧮 <b>Скоринг</b>",
        f"  Мин. балл: {s.min_score} / 10",
        f"  Веса: HUMAN {s.score_weight_human:.2f} · MSR {s.score_weight_msr:.2f}"
        f" · Conc {s.score_weight_concentration:.2f} · Bundle {s.score_weight_bundle:.2f}",
        "",
        "🛡 <b>Защита</b>",
        f"  Тренд роста:   ≥ {s.min_trend_percent:.1f}% за {s.stability_check_seconds:.0f}с"
        f"  (таймаут {s.trend_wait_timeout}с, проверка каждые {s.trend_recheck_interval}с)",
        f"  Антиволат. A:  рост ≤ {s.max_price_increase_percent:.0f}% за {s.stability_check_seconds:.0f}с",
        f"  Антиволат. B:  разворот ≥ {s.sharp_reversal_drop_percent:.0f}% от пика за {s.stability_check_seconds:.0f}с",
        "",
        "⚙️ <b>Инфраструктура</b>",
        f"  Helius rate limit:  {s.helius_rate_limit:.1f}с/запрос",
        f"  DAS retry:          {s.das_retries} повтор, задержка {s.das_retry_delay:.0f}с",
        f"  Параллельный старт (sigs + asset): ✅",
        f"  429 защита:         1 повтор → SKIPPED",
    ]
    await message.answer("\n".join(lines))


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
