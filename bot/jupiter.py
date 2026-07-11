"""Общие запросы к Jupiter Token API."""

import asyncio
import logging
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

JUPITER_TOKEN_SEARCH_URL = "https://lite-api.jup.ag/tokens/v2/search"


async def get_token_info(session: aiohttp.ClientSession, mint: str) -> Optional[dict]:
    """Карточка токена из Jupiter tokens v2 (цена, объёмы, градуация)."""
    try:
        async with session.get(
            JUPITER_TOKEN_SEARCH_URL,
            params={"query": mint},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data = await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        log.warning("Jupiter: ошибка запроса по %s: %s", mint, exc)
        return None
    if not isinstance(data, list):
        return None
    return next((item for item in data if item.get("id") == mint), None)


def token_price(info: Optional[dict]) -> float:
    if not info:
        return 0.0
    try:
        return float(info.get("usdPrice") or 0)
    except (TypeError, ValueError):
        return 0.0


def token_volume_24h(info: Optional[dict]) -> float:
    if not info:
        return 0.0
    stats = info.get("stats24h") or {}
    try:
        return float(stats.get("buyVolume") or 0) + float(stats.get("sellVolume") or 0)
    except (TypeError, ValueError):
        return 0.0


def is_graduated(info: Optional[dict]) -> bool:
    """Токен градуировал с bonding curve (ушёл в пул)."""
    if not info:
        return False
    return bool(info.get("graduatedPool") or info.get("graduatedAt"))
