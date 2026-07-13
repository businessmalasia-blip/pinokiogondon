"""Цена SOL из Jupiter с кэшем в Redis и расчёт market cap."""

import asyncio
import base64
import logging
from typing import Optional

import aiohttp
from redis.asyncio import Redis

from .helius import HeliusClient
from .pump import parse_bonding_curve_state

log = logging.getLogger(__name__)

SOL_MINT = "So11111111111111111111111111111111111111112"
JUPITER_PRICE_URL = "https://lite-api.jup.ag/price/v3"
SOL_PRICE_KEY = "sol_price_usd"
# Кэш живёт заметно дольше интервала обновления, чтобы пережить сбои Jupiter
SOL_PRICE_TTL = 30


async def sol_price_loop(
    session: aiohttp.ClientSession,
    redis_client: Redis,
    interval: float,
    price_holder=None,
) -> None:
    """Фоновая задача: раз в `interval` секунд обновляет цену SOL в Redis
    и в price_holder.sol_price (для расчётов без обращения к Redis)."""
    while True:
        try:
            async with session.get(
                JUPITER_PRICE_URL,
                params={"ids": SOL_MINT},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                data = await resp.json()
            price = float(data[SOL_MINT]["usdPrice"])
            await redis_client.set(SOL_PRICE_KEY, price, ex=SOL_PRICE_TTL)
            if price_holder is not None:
                price_holder.sol_price = price
        except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, TypeError, ValueError) as exc:
            log.warning("Не удалось обновить цену SOL: %s", exc)
        await asyncio.sleep(interval)


async def get_sol_price(redis_client: Redis) -> Optional[float]:
    raw = await redis_client.get(SOL_PRICE_KEY)
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


async def get_market_cap(
    helius: HeliusClient, redis_client: Redis, bonding_curve: str
) -> Optional[float]:
    """Market cap токена по свежему состоянию bonding curve.

    Цена токена берётся из виртуальных резервов кривой
    (virtual_sol / virtual_token), market cap = цена * полный supply * курс SOL.
    Если данные кривой прочитать не удалось — fallback на старую формулу
    (lamports / 1e9) * цена SOL.
    """
    value = await helius.get_account_info(bonding_curve)
    if not value:
        return None
    sol_price = await get_sol_price(redis_client)
    if sol_price is None:
        return None

    data_field = value.get("data")
    if isinstance(data_field, list) and data_field and data_field[0]:
        try:
            state = parse_bonding_curve_state(base64.b64decode(data_field[0]))
        except (ValueError, TypeError):
            state = None
        if not state or state["virtual_token_reserves"] <= 0:
            # Аккаунт с данными, но это не bonding curve — адрес распарсен неверно
            return None
        if state["complete"]:
            # Кривая завершена — токен градуировал, скрининг не нужен
            return None
        # Резервы: SOL в лампортах (1e9), токены в мин. единицах (1e6);
        # decimals в формуле сокращаются, поэтому 1e6 корректен и для иных decimals
        price_sol = (state["virtual_sol_reserves"] / 1e9) / (
            state["virtual_token_reserves"] / 1e6
        )
        supply_tokens = state["token_total_supply"] / 1e6
        return price_sol * supply_tokens * sol_price

    # Фоллбэк по формуле из ТЗ, если у аккаунта нет данных
    lamports = value.get("lamports")
    if lamports is None:
        return None
    return lamports / 1e9 * sol_price
