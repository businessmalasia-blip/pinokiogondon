"""Точка входа: WebSocket-подписка на Pump.fun, пайплайн анализа, алерты,
Telegram-команды и фоновая проверка исходов.

Запуск: python -m bot.main
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp
import websockets
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand
from redis.asyncio import Redis

from .alerts import build_alert_text, extract_token_meta, send_alert, send_startup_message
from .commands import router as commands_router
from .config import Settings, load_settings
from .filters import (
    calculate_bundle_percent,
    calculate_human_percent,
    check_concentration,
    check_dev,
)
from .scoring import calculate_score
from .helius import HeliusClient
from .jupiter import get_token_info, token_price
from .outcomes import outcomes_loop
from .prices import get_market_cap, get_sol_price, sol_price_loop
from .pump import find_buy_accounts, has_buy_log
from .stats import Stats

log = logging.getLogger("bot")


@dataclass
class Context:
    settings: Settings
    redis: Redis
    helius: HeliusClient
    http: aiohttp.ClientSession
    tg_bot: Bot
    stats: Stats
    analysis_semaphore: asyncio.Semaphore


async def market_cap(ctx: Context, bonding_curve: str) -> Optional[float]:
    ctx.stats.bump("mc_calcs")
    return await get_market_cap(ctx.helius, ctx.redis, bonding_curve)


# ---------------------------------------------------------------------------
# Ожидание входа капы в целевой диапазон
# ---------------------------------------------------------------------------

async def wait_for_mc_range(
    ctx: Context, mint: str, bonding_curve: str, timeout: Optional[float] = None
) -> Optional[float]:
    """Каждые MC_POLL_INTERVAL секунд опрашивает bonding curve, пока капа
    не войдёт в диапазон [ALERT_MC_MIN, ALERT_MC_MAX] или не истечёт таймаут."""
    s = ctx.settings
    if timeout is None:
        timeout = s.mc_wait_timeout
    if timeout <= 0:
        return None
    deadline = time.monotonic() + timeout
    poll_count = 0
    progress_every = max(1, int(30 / s.mc_poll_interval))
    while time.monotonic() < deadline:
        mc = await market_cap(ctx, bonding_curve)
        if mc is not None and s.alert_mc_min <= mc <= s.alert_mc_max:
            log.info("[%s] 🎯 капа вошла в диапазон: $%.0f", mint, mc)
            return mc
        if (
            mc is not None
            and s.mc_wait_abort_below > 0
            and mc < s.mc_wait_abort_below
        ):
            log.info(
                "[%s] 🛑 ожидание прервано: капа $%.0f упала ниже $%.0f — токен слит",
                mint, mc, s.mc_wait_abort_below,
            )
            return None
        poll_count += 1
        if poll_count % progress_every == 0:
            log.info(
                "[%s] ⏳ жду диапазон $%.0f–$%.0f, сейчас MC %s",
                mint, s.alert_mc_min, s.alert_mc_max,
                f"${mc:,.0f}" if mc is not None else "н/д",
            )
        await asyncio.sleep(s.mc_poll_interval)
    log.info("[%s] капа не вошла в диапазон за %.0f сек", mint, s.mc_wait_timeout)
    return None


# ---------------------------------------------------------------------------
# Пайплайн анализа токена
# ---------------------------------------------------------------------------

async def analyze_token(ctx: Context, mint: str, bonding_curve: str) -> None:
    s = ctx.settings
    log.info("[%s] 🔎 запускаю анализ (bonding curve %s)", mint, bonding_curve)

    # Фильтр 1: концентрация
    conc_ok, holders, top10_percent = await check_concentration(
        mint, ctx.helius, s, bonding_curve
    )
    if not conc_ok:
        log.info("[%s] ❌ ОТСЕЯН фильтром concentration", mint)
        await ctx.stats.record_filter_result(mint, "concentration")
        return
    log.info(
        "[%s] ✔ concentration пройден (%d холдеров, топ-10 %.1f%%)",
        mint, len(holders), top10_percent,
    )

    # Дальше жёстких отсечек нет: метрики только считаются,
    # решение принимает общий скоринг
    _, human_percent = await calculate_human_percent(
        holders, ctx.redis, ctx.helius, s, mint
    )
    log.info("[%s] 👥 HUMAN %.0f%%", mint, human_percent)

    # Сигнатуры минта: один запрос, используется бандл-метрикой и check_dev
    mint_signatures = await ctx.helius.get_signatures(mint, limit=1000)

    # Бандлы: доля покупок в слоте создания токена (снайперы на запуске)
    bundle_percent = calculate_bundle_percent(mint_signatures, s.bundle_slot_window)
    log.info("[%s] 📦 бандлы на запуске: %.1f%%", mint, bundle_percent)

    # История дева (Bad не отсеивает — даёт 0 баллов за MSR в скоринге)
    dev = await check_dev(
        mint, ctx.redis, ctx.helius, ctx.http, s, mint_signatures=mint_signatures
    )
    log.info("[%s] 👨‍💻 dev: %s, MSR %s", mint, dev["status"], dev["msr"])

    # Скоринг по посчитанным метрикам — единственный решающий порог
    score = calculate_score(
        {
            "human_percent": human_percent,
            "msr": dev["msr"],
            "top10_percent": top10_percent,
            "bundle_percent": bundle_percent,
        },
        s,
    )
    log.info(
        "[%s] ⭐ Score %.1f/10 (human %d, msr %d, conc %d, bundle %d)",
        mint, score["total"], score["human"], score["msr"],
        score["concentration"], score["bundle"],
    )
    if score["total"] < s.min_score:
        log.info(
            "[%s] ❌ ОТСЕЯН по скору (%.1f < %.1f)", mint, score["total"], s.min_score
        )
        await ctx.stats.record_filter_result(mint, "score")
        return

    # Все фильтры и скоринг пройдены
    log.info(
        "[%s] 🎯 ВСЕ ФИЛЬТРЫ ПРОЙДЕНЫ (score %.1f) — жду капу $%.0f–$%.0f",
        mint, score["total"], s.alert_mc_min, s.alert_mc_max,
    )
    await ctx.stats.record_filter_result(mint, "passed")

    alert_data = {
        "bonding_curve": bonding_curve,
        "human_percent": human_percent,
        "dev_status": dev["status"],
        "msr": dev["msr"],
        "top10_percent": top10_percent,
        "bundle_percent": bundle_percent,
        "score": score,
        "passed_at": time.time(),
    }
    # Сохраняем состояние ожидания: переживает рестарт бота
    await ctx.redis.set(
        f"pending_alert:{mint}",
        json.dumps(alert_data),
        ex=int(s.mc_wait_timeout) + 60,
    )
    await wait_and_alert(ctx, mint, bonding_curve, alert_data)


async def wait_and_alert(
    ctx: Context, mint: str, bonding_curve: str, alert_data: dict
) -> None:
    """Ожидание окна капы и отправка алерта. Состояние хранится в Redis,
    поэтому после рестарта ожидание возобновляется, а не пропадает."""
    s = ctx.settings
    remaining = s.mc_wait_timeout - (time.time() - alert_data["passed_at"])
    try:
        mc = await wait_for_mc_range(ctx, mint, bonding_curve, timeout=remaining)
        if mc is None:
            ctx.stats.bump("no_window")
            return

        # Контрольная проверка капы прямо перед отправкой
        final_mc = await market_cap(ctx, bonding_curve)
        if final_mc is None or not (
            s.send_guard_mc_min <= final_mc <= s.send_guard_mc_max
        ):
            log.info("[%s] алерт отменён: капа %s вне guard-диапазона", mint, final_mc)
            ctx.stats.bump("guard_out")
            return

        asset = await ctx.helius.get_asset(mint)
        name, symbol, image_url = extract_token_meta(asset)
        text = build_alert_text(
            name=name,
            symbol=symbol,
            mint=mint,
            human_percent=alert_data["human_percent"],
            dev_status=alert_data["dev_status"],
            msr=alert_data["msr"],
            top10_percent=alert_data["top10_percent"],
            bundle_percent=alert_data["bundle_percent"],
            score=alert_data["score"],
            market_cap=final_mc,
        )
        await send_alert(ctx.tg_bot, s.telegram_chat_id, text, image_url)
        log.info("[%s] 🔔 алерт отправлен, MC $%.0f", mint, final_mc)

        # Фиксируем алерт для /stats и /last; цену берём из Jupiter,
        # при её отсутствии — оценку из капы (supply Pump.fun = 1 млрд)
        info = await get_token_info(ctx.http, mint)
        price = token_price(info) or final_mc / 1e9
        await ctx.stats.record_alert(mint, name, symbol, final_mc, price)
    finally:
        await ctx.redis.delete(f"pending_alert:{mint}")


async def resume_pending_alerts(ctx: Context) -> None:
    """После рестарта возобновляет ожидания окна капы, прерванные остановкой."""
    async for key in ctx.redis.scan_iter("pending_alert:*"):
        raw = await ctx.redis.get(key)
        if not raw:
            continue
        try:
            alert_data = json.loads(raw)
            mint = key.split(":", 1)[1]
            bonding_curve = alert_data["bonding_curve"]
        except (json.JSONDecodeError, KeyError):
            await ctx.redis.delete(key)
            continue
        log.info("[%s] ♻️ возобновляю ожидание окна капы после рестарта", mint)
        asyncio.create_task(wait_and_alert(ctx, mint, bonding_curve, alert_data))


async def handle_buy_signature(ctx: Context, signature: str) -> None:
    """Обработка транзакции с инструкцией buy: извлечение mint/bonding curve,
    проверка капы и запуск анализа."""
    s = ctx.settings
    async with ctx.analysis_semaphore:
        tx = await ctx.helius.get_transaction(signature)
        if not tx or (tx.get("meta") or {}).get("err"):
            return
        ctx.stats.bump("tx_checked")

        parsed = find_buy_accounts(
            tx, s.pump_program, s.buy_mint_index, s.buy_bonding_curve_index
        )
        if not parsed:
            return
        mint, bonding_curve = parsed

        # Дедупликация: один mint анализируем один раз за SEEN_MINT_TTL
        seen_key = f"seen:{mint}"
        if not await ctx.redis.set(seen_key, "1", ex=s.seen_mint_ttl, nx=True):
            return

        mc = await market_cap(ctx, bonding_curve)
        if mc is None or mc < s.mc_analyze_min:
            # Капа ещё мала — снимаем метку, чтобы следующий buy проверил снова
            log.debug("[%s] MC $%.0f ниже порога $%.0f", mint, mc or 0, s.mc_analyze_min)
            await ctx.redis.delete(seen_key)
            return
        if s.mc_analyze_max > 0 and mc > s.mc_analyze_max:
            # Токен уже улетел выше guard-диапазона — анализ не окупится,
            # алерт всё равно не отправится (метку не снимаем)
            log.debug("[%s] MC $%.0f выше потолка $%.0f", mint, mc, s.mc_analyze_max)
            return
        log.info("[%s] 💵 MC $%.0f ≥ $%.0f — токен идёт на анализ", mint, mc, s.mc_analyze_min)

        try:
            await analyze_token(ctx, mint, bonding_curve)
        except Exception:
            log.exception("[%s] ошибка анализа", mint)


# ---------------------------------------------------------------------------
# WebSocket-подписка на логи программы Pump.fun
# ---------------------------------------------------------------------------

# Бэкофф переподключения: 1с -> 2с -> 4с -> 8с -> 16с -> максимум 30с
WS_RECONNECT_DELAY_INITIAL = 1.0
WS_RECONNECT_DELAY_MAX = 30.0
# Если за это время не пришло ни одного события — подписка "тихо" умерла,
# соединение принудительно пересоздаётся (поток Pump.fun не молчит так долго)
WS_IDLE_TIMEOUT = 60.0


async def pump_logs_loop(ctx: Context) -> None:
    s = ctx.settings
    subscribe_request = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [
                {"mentions": [s.pump_program]},
                {"commitment": "confirmed"},
            ],
        }
    )
    delay = WS_RECONNECT_DELAY_INITIAL
    attempt = 0
    while True:
        attempt += 1
        try:
            log.info("WebSocket: попытка подключения #%d...", attempt)
            async with websockets.connect(
                s.rpc_ws_url,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=15,
                close_timeout=5,
                max_size=None,
            ) as ws:
                await ws.send(subscribe_request)
                log.info(
                    "WebSocket подключён (попытка #%d), подписка на %s",
                    attempt, s.pump_program,
                )
                # Успешное подключение — сбрасываем бэкофф и счётчик попыток
                delay = WS_RECONNECT_DELAY_INITIAL
                attempt = 0

                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=WS_IDLE_TIMEOUT)
                    message = json.loads(raw)
                    if message.get("method") != "logsNotification":
                        continue
                    ctx.stats.mark_event()
                    ctx.stats.bump("events_received")
                    value = message["params"]["result"]["value"]
                    if value.get("err"):
                        continue
                    if not has_buy_log(value.get("logs") or []):
                        continue
                    ctx.stats.bump("buys_seen")
                    asyncio.create_task(
                        handle_buy_signature(ctx, value["signature"])
                    )
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            log.warning(
                "WebSocket: нет событий %d сек — подписка зависла, "
                "reconnecting in %.0f seconds...",
                WS_IDLE_TIMEOUT, delay,
            )
        except Exception as exc:
            log.warning(
                "WebSocket disconnected (%s: %s), reconnecting in %.0f seconds...",
                type(exc).__name__, exc, delay,
            )
        await asyncio.sleep(delay)
        delay = min(delay * 2, WS_RECONNECT_DELAY_MAX)


# ---------------------------------------------------------------------------
# Минутный "пульс" в лог: цена SOL и статистика за минуту
# ---------------------------------------------------------------------------

HEARTBEAT_INTERVAL = 60


async def heartbeat_loop(ctx: Context) -> None:
    # Стартуем от текущих значений, иначе первый пульс покажет
    # накопленные за всё время суммы вместо статистики за минуту
    try:
        prev: dict[str, int] = await ctx.stats.counters()
    except Exception:
        prev = {}
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            counters = await ctx.stats.counters()
            sol_price = await get_sol_price(ctx.redis)

            def delta(field: str) -> int:
                return counters.get(field, 0) - prev.get(field, 0)

            log.info(
                "💓 SOL $%s | за минуту: событий %d, покупок %d, tx %d, "
                "расчётов капы %d | отсеяно: conc %d, human %d, dev %d | "
                "прошло %d | алертов всего %d",
                f"{sol_price:.2f}" if sol_price else "?",
                delta("events_received"),
                delta("buys_seen"),
                delta("tx_checked"),
                delta("mc_calcs"),
                delta("filtered:concentration"),
                delta("filtered:human"),
                delta("filtered:dev"),
                delta("filtered:passed"),
                counters.get("alerts_sent", 0),
            )
            prev = counters
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Ошибка heartbeat")


# ---------------------------------------------------------------------------
# Запуск
# ---------------------------------------------------------------------------

BOT_COMMANDS = [
    BotCommand(command="status", description="Аптайм и счётчики"),
    BotCommand(command="stats", description="Статистика алертов и фильтров"),
    BotCommand(command="last", description="Последние 5 алертов"),
    BotCommand(command="settings", description="Текущие пороги"),
    BotCommand(command="help", description="Справка"),
]


async def run() -> None:
    settings = load_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    tg_bot = Bot(
        token=settings.telegram_bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    async with aiohttp.ClientSession() as http:
        helius = HeliusClient(http, settings.rpc_http_url, settings.helius_rate_limit)
        ctx = Context(
            settings=settings,
            redis=redis_client,
            helius=helius,
            http=http,
            tg_bot=tg_bot,
            stats=Stats(redis_client),
            analysis_semaphore=asyncio.Semaphore(settings.max_concurrent_analyses),
        )

        await tg_bot.set_my_commands(BOT_COMMANDS)
        await send_startup_message(tg_bot, settings.telegram_chat_id)
        await resume_pending_alerts(ctx)

        dispatcher = Dispatcher()
        dispatcher.include_router(commands_router)
        dispatcher["ctx"] = ctx

        tasks = [
            asyncio.create_task(
                sol_price_loop(http, redis_client, settings.sol_price_interval)
            ),
            asyncio.create_task(pump_logs_loop(ctx)),
            asyncio.create_task(outcomes_loop(ctx)),
            asyncio.create_task(heartbeat_loop(ctx)),
            asyncio.create_task(
                dispatcher.start_polling(tg_bot, handle_signals=False)
            ),
        ]

        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await ctx.stats.flush()
            await tg_bot.session.close()
            await redis_client.aclose()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Остановка по Ctrl+C")


if __name__ == "__main__":
    main()
