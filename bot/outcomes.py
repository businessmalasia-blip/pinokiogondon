"""Фоновая проверка исходов: судьба алертов через 1ч/6ч и градуации
отсеянных токенов через 24ч. Результаты копятся в Redis для /stats."""

import asyncio
import logging
import time

from .jupiter import get_token_info, is_graduated, token_price
from .stats import GRADCHECK_PENDING_KEY, OUTCOME_PENDING_KEY

log = logging.getLogger(__name__)

CYCLE_INTERVAL = 30
BATCH_LIMIT = 15
# Пауза между запросами к Jupiter, чтобы не упереться в лимиты lite-api
JUPITER_PAUSE = 1.0

# Порог "смерти" токена: цена упала до 10% от цены алерта и ниже
DEAD_RATIO = 0.1


async def _check_alert_outcomes(ctx) -> None:
    now = time.time()
    due = await ctx.redis.zrangebyscore(
        OUTCOME_PENDING_KEY, "-inf", now, start=0, num=BATCH_LIMIT
    )
    for member in due:
        window, _ts, mint, price_str = member.split("|", 3)
        info = await get_token_info(ctx.http, mint)
        price_now = token_price(info)
        try:
            price_alert = float(price_str)
        except ValueError:
            price_alert = 0.0

        key = f"outcome:{window}"
        pipe = ctx.redis.pipeline()
        pipe.hincrby(key, "checked", 1)
        if is_graduated(info):
            pipe.hincrby(key, "grad", 1)
        if price_alert > 0 and price_now > 0:
            ratio = price_now / price_alert
            if ratio >= 1.3:
                pipe.hincrby(key, "x13", 1)
            if ratio >= 2.0:
                pipe.hincrby(key, "x2", 1)
            if ratio <= DEAD_RATIO:
                pipe.hincrby(key, "dead", 1)
        elif price_now <= 0:
            # Токена нет в Jupiter или цена нулевая — считаем умершим
            pipe.hincrby(key, "dead", 1)
        pipe.zrem(OUTCOME_PENDING_KEY, member)
        await pipe.execute()
        await asyncio.sleep(JUPITER_PAUSE)


async def _check_graduations(ctx) -> None:
    now = time.time()
    due = await ctx.redis.zrangebyscore(
        GRADCHECK_PENDING_KEY, "-inf", now, start=0, num=BATCH_LIMIT
    )
    for member in due:
        filter_name, mint = member.split("|", 1)
        info = await get_token_info(ctx.http, mint)
        key = f"gradstats:{filter_name}"
        pipe = ctx.redis.pipeline()
        pipe.hincrby(key, "checked", 1)
        if is_graduated(info):
            pipe.hincrby(key, "graduated", 1)
        pipe.zrem(GRADCHECK_PENDING_KEY, member)
        await pipe.execute()
        await asyncio.sleep(JUPITER_PAUSE)


async def outcomes_loop(ctx) -> None:
    """Каждые CYCLE_INTERVAL секунд: сброс счётчиков в Redis и обработка
    подошедших по времени проверок."""
    while True:
        try:
            await ctx.stats.flush()
            await _check_alert_outcomes(ctx)
            await _check_graduations(ctx)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Ошибка в цикле проверки исходов")
        await asyncio.sleep(CYCLE_INTERVAL)
