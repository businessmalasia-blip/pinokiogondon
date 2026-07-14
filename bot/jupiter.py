"""Общие запросы к Jupiter Token API."""

import asyncio
import logging
from datetime import datetime, timezone
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


def _num(info: Optional[dict], key: str) -> Optional[float]:
    if not info or info.get(key) is None:
        return None
    try:
        return float(info[key])
    except (TypeError, ValueError):
        return None


def token_holder_count(info: Optional[dict]) -> Optional[int]:
    value = _num(info, "holderCount")
    return int(value) if value is not None else None


def token_liquidity(info: Optional[dict]) -> Optional[float]:
    return _num(info, "liquidity")


def token_trade_counts(info: Optional[dict]) -> Optional[tuple[int, int]]:
    """(покупок, продаж) за 24ч — для свежего токена это вся его история."""
    if not info:
        return None
    stats = info.get("stats24h") or {}
    if stats.get("numBuys") is None and stats.get("numSells") is None:
        return None
    try:
        return int(stats.get("numBuys") or 0), int(stats.get("numSells") or 0)
    except (TypeError, ValueError):
        return None


def token_age_seconds(info: Optional[dict]) -> Optional[float]:
    """Возраст токена в секундах по createdAt (ISO 8601)."""
    if not info:
        return None
    created = info.get("createdAt")
    if not created:
        pool = info.get("firstPool") or {}
        created = pool.get("createdAt")
    if not created:
        return None
    try:
        dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except (ValueError, AttributeError):
        return None
