"""Три фильтра анализа токена: концентрация, HUMAN-процент, история дева."""

import logging
import time
from typing import Optional

import aiohttp
from redis.asyncio import Redis

from .config import Settings
from .helius import HeliusClient
from .jupiter import get_token_info, token_price, token_volume_24h
from .pump import fee_payer, find_created_mint

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Фильтр 1: концентрация холдеров
# ---------------------------------------------------------------------------

async def check_concentration(
    mint: str, helius: HeliusClient, settings: Settings
) -> tuple[bool, list[str]]:
    """Проверка распределения supply по топ-100 держателей.

    Возвращает (прошёл ли фильтр, список адресов холдеров без bonding curve).
    Список холдеров отдаётся наружу, чтобы фильтр HUMAN не запрашивал их повторно.
    """
    supply_info = await helius.get_token_supply(mint)
    if not supply_info or supply_info[0] <= 0:
        log.info("[%s] concentration: не удалось получить supply", mint)
        return False, []
    raw_supply, _decimals = supply_info

    accounts = await helius.get_token_accounts(mint, limit=100)
    if not accounts:
        log.info("[%s] concentration: нет токен-аккаунтов", mint)
        return False, []

    # DAS getTokenAccounts и getTokenSupply оба отдают amount в сырых единицах,
    # поэтому доля считается напрямую от полного supply.
    holders: list[tuple[str, float]] = []
    for acc in accounts:
        owner = acc.get("owner")
        raw_amount = acc.get("amount")
        if owner is None or raw_amount is None:
            continue
        holders.append((owner, float(raw_amount)))

    if not holders:
        return False, []

    shares = [(owner, amount / raw_supply * 100.0) for owner, amount in holders]
    shares.sort(key=lambda item: item[1], reverse=True)

    # Исключаем bonding curve: аккаунт с долей выше порога (по ТЗ >50%)
    filtered = [
        (owner, share)
        for owner, share in shares
        if share <= settings.bonding_curve_exclude_percent
    ]
    holder_addresses = [owner for owner, _ in filtered]

    if not filtered:
        return False, []

    # Ни у одного холдера нет больше HOLDER_MAX_PERCENT
    max_share = max(share for _, share in filtered)
    if max_share > settings.holder_max_percent:
        log.info("[%s] concentration: холдер держит %.2f%%", mint, max_share)
        return False, holder_addresses

    # Сумма топ-10 меньше TOP10_MAX_PERCENT
    top10_sum = sum(share for _, share in filtered[:10])
    if top10_sum >= settings.top10_max_percent:
        log.info("[%s] concentration: топ-10 держат %.2f%%", mint, top10_sum)
        return False, holder_addresses

    log.info(
        "[%s] concentration OK: max %.2f%%, топ-10 %.2f%%",
        mint, max_share, top10_sum,
    )
    return True, holder_addresses


# ---------------------------------------------------------------------------
# Фильтр 2: процент "живых" кошельков
# ---------------------------------------------------------------------------

async def calculate_human_percent(
    holders: list[str],
    redis_client: Redis,
    helius: HeliusClient,
    settings: Settings,
    mint: str = "",
) -> tuple[bool, float]:
    """HUMAN — у адреса есть история транзакций; UNKNOWN — истории нет.

    Возвращает (прошёл ли фильтр, HUMAN-процент для алерта).
    """
    if not holders:
        return False, 0.0

    human = 0
    unknown = 0
    for address in holders:
        cache_key = f"human:{address}"
        cached = await redis_client.get(cache_key)
        if cached is not None:
            if cached == "1":
                human += 1
            else:
                unknown += 1
            continue

        signatures = await helius.get_signatures(address, limit=1)
        if signatures:
            human += 1
            await redis_client.set(cache_key, "1", ex=settings.human_cache_ttl)
        else:
            unknown += 1
            await redis_client.set(cache_key, "0", ex=settings.unknown_cache_ttl)

    total = human + unknown
    human_percent = human / total * 100.0
    unknown_percent = unknown / total * 100.0

    passed = (
        human_percent >= settings.human_min_percent
        and unknown_percent <= settings.unknown_max_percent
    )
    log.info(
        "[%s] human: HUMAN %.1f%% / UNKNOWN %.1f%% (%d холдеров, пороги ≥%.0f/≤%.0f) -> %s",
        mint, human_percent, unknown_percent, total,
        settings.human_min_percent, settings.unknown_max_percent,
        "OK" if passed else "FAIL",
    )
    return passed, human_percent


# ---------------------------------------------------------------------------
# Фильтр 3: история дева (MSR)
# ---------------------------------------------------------------------------

async def _find_creator(mint: str, helius: HeliusClient) -> Optional[str]:
    """Создатель токена: feePayer самой первой транзакции по mint."""
    signatures = await helius.get_signatures(mint, limit=1000)
    if not signatures:
        return None
    oldest = signatures[-1]["signature"]
    tx = await helius.get_transaction(oldest)
    if not tx:
        return None
    return fee_payer(tx)


async def _token_survived(
    session: aiohttp.ClientSession, mint: str, min_volume_usd: float
) -> bool:
    """Выживший токен: цена в Jupiter > 0 и суточный объём > порога."""
    info = await get_token_info(session, mint)
    return token_price(info) > 0 and token_volume_24h(info) > min_volume_usd


async def check_dev(
    mint: str,
    redis_client: Redis,
    helius: HeliusClient,
    session: aiohttp.ClientSession,
    settings: Settings,
) -> dict:
    """Оценка дева по выживаемости его прошлых токенов (MSR).

    Возвращает {"status": "Clean" | "Bad" | "Unknown", "msr": float | None}.
    """
    creator = await _find_creator(mint, helius)
    if not creator:
        log.info("[%s] dev: создатель не найден", mint)
        return {"status": "Unknown", "msr": None}

    if await redis_client.exists(f"bad_dev:{creator}"):
        log.info("[%s] dev %s: Bad (из кэша)", mint, creator)
        return {"status": "Bad", "msr": None}

    cached_msr = await redis_client.get(f"dev_msr:{creator}")
    if cached_msr is not None:
        return {"status": "Clean", "msr": float(cached_msr)}

    # История дева: до DEV_TX_LIMIT транзакций за DEV_HISTORY_DAYS дней
    signatures = await helius.get_signatures(creator, limit=settings.dev_tx_limit)
    cutoff = time.time() - settings.dev_history_days * 86400
    recent = [
        sig for sig in signatures
        if sig.get("blockTime") and sig["blockTime"] >= cutoff and not sig.get("err")
    ]

    created_mints: list[str] = []
    for sig_info in recent:
        tx = await helius.get_transaction(sig_info["signature"])
        if not tx:
            continue
        logs = (tx.get("meta") or {}).get("logMessages") or []
        if not any(settings.pump_program in line for line in logs):
            continue
        created = find_created_mint(tx, settings.pump_program)
        if created and created != mint and created not in created_mints:
            created_mints.append(created)

    if len(created_mints) < settings.dev_min_tokens:
        log.info("[%s] dev %s: Unknown (%d токенов)", mint, creator, len(created_mints))
        return {"status": "Unknown", "msr": None}

    to_check = created_mints[: settings.dev_tokens_check_max]
    survived = 0
    for token_mint in to_check:
        if await _token_survived(session, token_mint, settings.survivor_min_volume_usd):
            survived += 1

    msr = survived / len(to_check) * 100.0
    log.info("[%s] dev %s: MSR %.1f%% (%d/%d)", mint, creator, msr, survived, len(to_check))

    if msr >= settings.msr_min_percent:
        await redis_client.set(f"dev_msr:{creator}", msr)
        return {"status": "Clean", "msr": msr}

    await redis_client.set(f"bad_dev:{creator}", "1")
    return {"status": "Bad", "msr": msr}
