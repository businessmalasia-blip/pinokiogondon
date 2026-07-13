"""Три фильтра анализа токена: концентрация, HUMAN-процент, история дева."""

import asyncio
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
    mint: str,
    helius: HeliusClient,
    settings: Settings,
    bonding_curve: str = "",
    raw_supply: int = 0,
) -> tuple[str, list[tuple[str, float]], float]:
    """Проверка распределения supply по крупнейшим держателям.

    Холдеры берутся через getTokenLargestAccounts (топ-20 аккаунтов) —
    это текущее состояние сети без задержки DAS-индексации, работает
    для токенов возрастом в секунды. Supply передаётся снаружи (он уже
    прочитан из аккаунта bonding curve).

    Возвращает (статус, список холдеров без bonding curve, доля топ-10 в %).
    Статус: "ok" — прошёл, "fail" — отсеян, "incomplete" — данных пока
    мало и анализ надо повторить позже.
    Холдеры — пары (адрес владельца, доля в %) по убыванию доли; отдаются
    наружу, чтобы фильтр HUMAN не запрашивал их повторно.

    Bonding curve исключается в первую очередь по адресу владельца:
    при капе выше ~$7k кривая держит меньше 50% supply и порог по доле
    её уже не ловит. Порог >50% остаётся страховкой.
    """
    if raw_supply <= 0:
        return "incomplete", [], 0.0

    accounts = await helius.get_token_largest_accounts(mint)
    attempts = 0
    while len(accounts) < settings.das_min_accounts and attempts < settings.das_retries:
        attempts += 1
        log.info(
            "[%s] concentration: только %d токен-аккаунтов — жду покупателей (%d/%d)",
            mint, len(accounts), attempts, settings.das_retries,
        )
        await asyncio.sleep(settings.das_retry_delay)
        accounts = await helius.get_token_largest_accounts(mint)
    if len(accounts) < settings.das_min_accounts:
        log.info(
            "[%s] concentration: всего %d токен-аккаунтов — данных мало",
            mint, len(accounts),
        )
        return "incomplete", [], 0.0

    owners_map = await helius.get_accounts_owners(
        [acc.get("address") for acc in accounts if acc.get("address")]
    )
    holders: list[tuple[str, float]] = []
    for acc in accounts:
        owner = owners_map.get(acc.get("address"))
        raw_amount = acc.get("amount")
        if owner is None or raw_amount is None:
            continue
        holders.append((owner, float(raw_amount)))

    if not holders:
        return "incomplete", [], 0.0

    shares = [(owner, amount / raw_supply * 100.0) for owner, amount in holders]
    shares.sort(key=lambda item: item[1], reverse=True)

    # Исключаем bonding curve: по адресу владельца и (страховкой) по доле >50%
    filtered = [
        (owner, share)
        for owner, share in shares
        if owner != bonding_curve and share <= settings.bonding_curve_exclude_percent
    ]

    if not filtered:
        return "incomplete", [], 0.0

    max_share = max(share for _, share in filtered)
    top10_sum = sum(share for _, share in filtered[:10])

    # Ни у одного холдера нет больше HOLDER_MAX_PERCENT
    if max_share > settings.holder_max_percent:
        log.info("[%s] concentration: холдер держит %.2f%%", mint, max_share)
        return "fail", filtered, top10_sum

    # Сумма топ-10 меньше TOP10_MAX_PERCENT
    if top10_sum >= settings.top10_max_percent:
        log.info("[%s] concentration: топ-10 держат %.2f%%", mint, top10_sum)
        return "fail", filtered, top10_sum

    log.info(
        "[%s] concentration OK: max %.2f%%, топ-10 %.2f%%",
        mint, max_share, top10_sum,
    )
    return "ok", filtered, top10_sum


# ---------------------------------------------------------------------------
# Фильтр 2: процент "живых" кошельков
# ---------------------------------------------------------------------------

async def calculate_human_percent(
    holders: list[tuple[str, float]],
    redis_client: Redis,
    helius: HeliusClient,
    settings: Settings,
    mint: str = "",
) -> tuple[bool, float]:
    """HUMAN — у адреса есть история транзакций; UNKNOWN — истории нет.

    Для экономии кредитов Helius проверяются только топ-HUMAN_CHECK_TOP
    холдеров по балансу, адреса с долей меньше HUMAN_MIN_HOLDER_SHARE
    игнорируются. Некэшированные адреса запрашиваются batch-запросами
    по 10 штук. Возвращает (прошёл ли фильтр, HUMAN-процент для алерта).
    """
    top = holders[: settings.human_check_top]
    candidates = [
        address for address, share in top
        if share >= settings.human_min_holder_share
    ]
    # У хорошо распределённых токенов почти все доли ниже отсечки, и выборка
    # вырождается в 2-4 кошелька. Добираем следующими по размеру холдерами,
    # чтобы процент считался минимум по HUMAN_MIN_CANDIDATES адресам.
    if len(candidates) < settings.human_min_candidates:
        seen = set(candidates)
        for address, _share in top:
            if len(candidates) >= settings.human_min_candidates:
                break
            if address not in seen:
                candidates.append(address)
                seen.add(address)
    if not candidates:
        log.info("[%s] human: нет холдеров для проверки", mint)
        return False, 0.0

    human = 0
    unknown = 0
    to_query: list[str] = []
    for address in candidates:
        cached = await redis_client.get(f"human:{address}")
        if cached == "1":
            human += 1
        elif cached == "0":
            unknown += 1
        else:
            to_query.append(address)

    for start in range(0, len(to_query), 10):
        chunk = to_query[start : start + 10]
        signatures_by_address = await helius.get_signatures_batch(chunk, limit=1)
        pipe = redis_client.pipeline()
        for address in chunk:
            if signatures_by_address.get(address):
                human += 1
                pipe.set(f"human:{address}", "1", ex=settings.human_cache_ttl)
            else:
                unknown += 1
                pipe.set(f"human:{address}", "0", ex=settings.unknown_cache_ttl)
        await pipe.execute()

    total = human + unknown
    human_percent = human / total * 100.0
    unknown_percent = unknown / total * 100.0

    passed = (
        human_percent >= settings.human_min_percent
        and unknown_percent <= settings.unknown_max_percent
    )
    log.info(
        "[%s] human: HUMAN %.1f%% / UNKNOWN %.1f%% (%d холдеров)",
        mint, human_percent, unknown_percent, total,
    )
    return passed, human_percent


# ---------------------------------------------------------------------------
# Доля bundle-покупок (считается по уже загруженным сигнатурам минта)
# ---------------------------------------------------------------------------

def calculate_bundle_percent(signatures: list[dict], window_slots: int) -> float:
    """Доля покупок, забандленных на запуске токена.

    Бандл — транзакции, севшие в слот создания токена (+window_slots
    следующих слотов): органическая покупка не может попасть в слот
    создания, туда попадают только скоординированные снайперы (Jito-бандлы).
    Считается по ответу getSignaturesForAddress(mint) — новых запросов нет.
    У активных токенов несколько органических покупок в одном слоте — норма
    (слот 400 мс), поэтому совпадение слотов вне запуска бандлом НЕ считается.
    """
    valid = [sig for sig in signatures if not sig.get("err") and sig.get("slot")]
    if len(valid) < 2:
        return 0.0
    if len(valid) >= 1000:
        # История обрезана лимитом запроса — слот создания не виден,
        # метрика недостоверна; токен не наказываем
        return 0.0
    slots = [sig["slot"] for sig in valid]
    creation_slot = min(slots)
    # Минус 1 — сама транзакция создания
    bundled = sum(1 for slot in slots if slot <= creation_slot + window_slots) - 1
    total = len(slots) - 1
    if total <= 0:
        return 0.0
    return max(bundled, 0) / total * 100.0


# ---------------------------------------------------------------------------
# Фильтр 3: история дева (MSR)
# ---------------------------------------------------------------------------

async def _find_creator(
    mint: str, helius: HeliusClient, signatures: list[dict]
) -> Optional[str]:
    """Создатель токена: feePayer самой первой транзакции по mint."""
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
    mint_signatures: Optional[list[dict]] = None,
) -> dict:
    """Оценка дева по выживаемости его прошлых токенов (MSR).

    mint_signatures — заранее загруженные сигнатуры минта (переиспользуются
    из пайплайна, чтобы не делать повторный запрос).
    Возвращает {"status": "Clean" | "Bad" | "Unknown", "msr": float | None}.
    """
    if mint_signatures is None:
        mint_signatures = await helius.get_signatures(mint, limit=1000)
    creator = await _find_creator(mint, helius, mint_signatures)
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
