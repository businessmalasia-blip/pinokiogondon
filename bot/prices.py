"""Цена SOL из Jupiter с кэшем в Redis и расчёт market cap."""

import asyncio
import logging
from typing import Optional

import aiohttp
from redis.asyncio import Redis

from .helius import HeliusClient

log = logging.getLogger(__name__)

SOL_MINT = "So11111111111111111111111111111111111111112"
JUPITER_PRICE_URL = "https://lite-api.jup.ag/price/v3"
SOL_PRICE_KEY = "sol_price_usd"
# Кэш живёт заметно дольше интервала обновления, чтобы пережить сбои Jupiter
SOL_PRICE_TTL = 30


async def sol_price_loop(
    session: aiohttp.ClientSession, redis_client: Redis, interval: float
) -> None:
    """Фоновая задача: раз в `interval` секунд обновляет цену SOL в Redis."""
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
    """Market cap по свежему балансу bonding curve: (lamports / 1e9) * цена SOL."""
    lamports = await helius.get_account_lamports(bonding_curve)
    if lamports is None:
        return None
    sol_price = await get_sol_price(redis_client)
    if sol_price is None:
        return None
    return lamports / 1e9 * sol_price
