"""Точка входа: WebSocket-подписка на Pump.fun, пайплайн анализа, алерты,
Telegram-команды и фоновая проверка исходов.

Запуск: python -m bot.main
"""

import asyncio
import base64
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

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
    get_creator,
)
from .scoring import calculate_score
from .helius import HeliusClient
from .jupiter import get_token_info, token_price
from .outcomes import outcomes_loop
from .prices import get_market_cap, get_sol_price, sol_price_loop
from .pump import (
    derive_bonding_curve,
    find_buy_accounts,
    iter_trade_events,
    market_cap_from_reserves,
    parse_bonding_curve_state,
)
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
    candidate_queue: asyncio.Queue
    sol_price: Optional[float] = None
    # {mint: [(wall_time, usd_amount), ...]} — накапливается из WebSocket-событий
    vol_tracker: dict = field(default_factory=dict)


async def market_cap(ctx: Context, bonding_curve: str) -> Optional[float]:
    ctx.stats.bump("mc_calcs")
    return await get_market_cap(ctx.helius, ctx.redis, bonding_curve)


# ---------------------------------------------------------------------------
# Вспомогательные проверки перед алертом
# ---------------------------------------------------------------------------

def _trading_session(s: Settings) -> Optional[str]:
    """Возвращает 'EU', 'US' или None если вне торговых сессий."""
    hour = datetime.now(ZoneInfo(s.timezone)).hour
    if s.eu_session_start <= hour < s.eu_session_end:
        return "EU"
    if s.us_session_start <= hour < s.us_session_end:
        return "US"
    return None


async def _check_sell_pressure(ctx: Context, mint: str, bonding_curve: str) -> bool:
    """True если обнаружен дамп (продажа > 1% суплая) в последних 10 транзакциях."""
    from .pump import parse_trade_event, TOKEN_TOTAL_SUPPLY_RAW

    threshold = TOKEN_TOTAL_SUPPLY_RAW // 100  # 1% суплая в raw-единицах
    sigs = await ctx.helius.get_signatures(bonding_curve, limit=10)
    if not sigs:
        return False
    sig_list = [sig["signature"] for sig in sigs if sig.get("signature")]
    txs = await ctx.helius.get_transactions_batch(sig_list)
    for tx in txs:
        if not tx:
            continue
        logs = (tx.get("meta") or {}).get("logMessages") or []
        for line in logs:
            event = parse_trade_event(line)
            if event and not event["is_buy"] and event["mint"] == mint:
                if event["token_amount"] > threshold:
                    log.info(
                        "[%s] SELL_PRESSURE: detected dump from large holder "
                        "(%.1f%% supply sold)",
                        mint,
                        event["token_amount"] / TOKEN_TOTAL_SUPPLY_RAW * 100,
                    )
                    return True
    return False


# ---------------------------------------------------------------------------
# Новые фильтры: Volume, Unique Buyers, Dev Early Buy
# ---------------------------------------------------------------------------

def _check_volume(ctx: "Context", mint: str) -> tuple[bool, float]:
    """Проверка USD-объёма покупок за последние 5 мин из WebSocket-событий."""
    cutoff = time.time() - 300
    bucket = ctx.vol_tracker.get(mint) or []
    fresh = [(t, v) for t, v in bucket if t >= cutoff]
    ctx.vol_tracker[mint] = fresh
    total = sum(v for _, v in fresh)
    return total >= ctx.settings.min_volume_usd_5min, total


async def _check_unique_buyers(
    ctx: "Context", mint: str, mint_signatures: list[dict]
) -> tuple[bool, int]:
    """Кол-во уникальных feePayer-адресов среди транзакций минта за последние 5 мин."""
    from .pump import fee_payer as _fee_payer

    cutoff = time.time() - 300
    recent_sigs = [
        sig["signature"]
        for sig in mint_signatures
        if not sig.get("err")
        and sig.get("blockTime", 0) >= cutoff
        and sig.get("signature")
    ]
    if not recent_sigs:
        return False, 0
    txs = await ctx.helius.get_transactions_batch(recent_sigs[:25])
    unique: set[str] = set()
    for tx in txs:
        if not tx:
            continue
        fp = _fee_payer(tx)
        if fp:
            unique.add(fp)
    return len(unique) >= ctx.settings.min_unique_buyers_5min, len(unique)


async def _check_dev_early_buy(
    ctx: "Context", mint: str, creator: str, mint_signatures: list[dict]
) -> bool:
    """True если дев купил токен в первые DEV_EARLY_BUY_WINDOW сек после создания."""
    from .pump import parse_trade_event

    oldest_time = next(
        (float(sig["blockTime"]) for sig in reversed(mint_signatures) if sig.get("blockTime")),
        None,
    )
    if oldest_time is None:
        return False
    early_cutoff = oldest_time + ctx.settings.dev_early_buy_window
    creator_sigs = await ctx.helius.get_signatures(creator, limit=5)
    early_sigs = [
        sig["signature"]
        for sig in creator_sigs
        if sig.get("blockTime") and sig["blockTime"] <= early_cutoff
        and not sig.get("err") and sig.get("signature")
    ]
    if not early_sigs:
        return False
    txs = await ctx.helius.get_transactions_batch(early_sigs)
    for tx in txs:
        if not tx:
            continue
        for line in (tx.get("meta") or {}).get("logMessages") or []:
            event = parse_trade_event(line)
            if event and event["is_buy"] and event["mint"] == mint:
                return True
    return False


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
    # Момент, когда капа впервые ушла ниже порога слива. Пока она ниже —
    # ждём камбэк MC_COMEBACK_WAIT секунд; если восстановилась выше порога,
    # таймер сбрасывается. Так дамп-и-возврат ловится, а мёртвый токен всё
    # равно бросается через 5 минут, а не висит весь таймаут.
    dumped_since: Optional[float] = None
    last_fresh_at = time.monotonic()
    mc_history: deque = deque()  # (monotonic_time, mc) для проверки стабильности
    while time.monotonic() < deadline:
        mc = await market_cap(ctx, bonding_curve)
        if mc is not None:
            last_fresh_at = time.monotonic()
            mc_history.append((time.monotonic(), mc))
        elif time.monotonic() - last_fresh_at > s.mc_stale_timeout:
            # getAccountInfo завис/не отдаёт баланс дольше порога — не рискуем
            # отправлять алерт по устаревшей капе
            log.info(
                "[%s] 🛑 капа не обновлялась > %.0f сек — прекращаю ожидание",
                mint, s.mc_stale_timeout,
            )
            return None
        if mc is not None and s.alert_mc_min <= mc <= s.alert_mc_max:
            # Проверка торговых сессий (EU / US)
            _session = _trading_session(s)
            if _session is None:
                log.info(
                    "[%s] TRADING_HOURS: outside EU/US sessions — ожидаю",
                    mint,
                )
                await asyncio.sleep(s.mc_poll_interval)
                continue
            log.info("[%s] TRADING_HOURS: %s session активна", mint, _session)
            # Anti-Volatility: два условия блокировки (любое из них → ждём)
            _now_mono = time.monotonic()
            _av_blocked = False

            # Условие A: слишком быстрый рост (манипулятивный памп)
            cutoff = _now_mono - s.stability_check_seconds
            old_mc = next((m for t, m in mc_history if t >= cutoff), None)
            if old_mc is not None and old_mc > 0:
                increase_pct = (mc - old_mc) / old_mc * 100
                if increase_pct > s.max_price_increase_percent:
                    log.info(
                        "[%s] ANTI_VOLATILITY: price increased %.1f%% in %.0fs — ожидаю",
                        mint, increase_pct, s.stability_check_seconds,
                    )
                    _av_blocked = True

            # Условие B: мгновенный разворот — пик в 120 сек и уже падаем
            if not _av_blocked and s.sharp_reversal_drop_percent > 0:
                _window_120 = [(t, m) for t, m in mc_history if _now_mono - t <= 120]
                if _window_120:
                    _peak_t, _peak_mc = max(_window_120, key=lambda x: x[1])
                    if _peak_mc > 0:
                        drop_pct = (_peak_mc - mc) / _peak_mc * 100
                        time_since_peak = _now_mono - _peak_t
                        if (drop_pct >= s.sharp_reversal_drop_percent
                                and time_since_peak < s.stability_check_seconds):
                            log.info(
                                "[%s] ANTI_VOLATILITY: sharp reversal detected "
                                "(peak: $%.0f, current: $%.0f, drop: %.1f%% in %.0fs) — ожидаю",
                                mint, _peak_mc, mc, drop_pct, time_since_peak,
                            )
                            _av_blocked = True

            if _av_blocked:
                await asyncio.sleep(s.mc_poll_interval)
                continue
            # Контрольный выстрел: немедленная повторная проверка капы без
            # ожидания следующей итерации цикла — сокращает задержку алерта.
            log.info("[%s] ⚡ контрольный выстрел (MC $%.0f в диапазоне)…", mint, mc)
            confirm_mc = await market_cap(ctx, bonding_curve)
            if confirm_mc is not None:
                mc_history.append((time.monotonic(), confirm_mc))
            if confirm_mc is None or not (
                s.send_guard_mc_min <= confirm_mc <= s.send_guard_mc_max
            ):
                log.info(
                    "[%s] ⚡ контрольный выстрел: MC %s вне guard-диапазона — продолжаю",
                    mint,
                    f"${confirm_mc:,.0f}" if confirm_mc is not None else "н/д",
                )
                await asyncio.sleep(s.mc_poll_interval)
                continue
            log.info(
                "[%s] 🎯 капа подтверждена контрольным выстрелом: $%.0f",
                mint, confirm_mc,
            )
            # Фильтр TREND DIRECTION: рост капы ≥ MIN_TREND_PERCENT за STABILITY_CHECK_SECONDS
            if s.min_trend_percent > 0:
                trend_cutoff = time.monotonic() - s.stability_check_seconds
                oldest_mc = next((m for t, m in mc_history if t >= trend_cutoff), None)
                if oldest_mc is not None and oldest_mc > 0:
                    growth_pct = (confirm_mc - oldest_mc) / oldest_mc * 100
                    if growth_pct < s.min_trend_percent:
                        log.info(
                            "[%s] TREND: рост %.1f%% за %.0fс < MIN_TREND_PERCENT %.1f%% — жду",
                            mint, growth_pct, s.stability_check_seconds, s.min_trend_percent,
                        )
                        await asyncio.sleep(s.mc_poll_interval)
                        continue
            return confirm_mc
        if mc is not None and s.mc_wait_abort_below > 0:
            if mc < s.mc_wait_abort_below:
                if dumped_since is None:
                    dumped_since = time.monotonic()
                    log.info(
                        "[%s] 📉 капа $%.0f ниже $%.0f — жду камбэк %.0f сек",
                        mint, mc, s.mc_wait_abort_below, s.mc_comeback_wait,
                    )
                elif time.monotonic() - dumped_since > s.mc_comeback_wait:
                    log.info(
                        "[%s] 🛑 ожидание прервано: капа $%.0f не вернулась за %.0f сек — токен слит",
                        mint, mc, s.mc_comeback_wait,
                    )
                    return None
            elif dumped_since is not None:
                # Капа восстановилась выше порога слива — камбэк, ждём дальше
                log.info("[%s] 📈 капа вернулась к $%.0f — продолжаю ждать окно", mint, mc)
                dumped_since = None
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

def _last_tx_time(signatures: list[dict]) -> Optional[float]:
    """blockTime самой свежей транзакции (getSignaturesForAddress отдаёт
    список от новых к старым)."""
    for sig in signatures:
        bt = sig.get("blockTime")
        if bt:
            return float(bt)
    return None


def _token_age_seconds(
    asset: Optional[dict], signatures: list[dict]
) -> Optional[float]:
    """Возраст токена в секундах.

    Приоритет — created_at из getAsset (у pump-токенов обычно отсутствует).
    Иначе — blockTime самой старой из полученных сигнатур минта. Если
    история обрезана лимитом (1000), это НИЖНЯЯ граница возраста: если уже
    она > порога, токен точно старый и отсекается; если меньше — токен
    активный и молодой, пропускаем. Так старые дохлые токены (мало
    транзакций → история полная → точный возраст) ловятся надёжно.
    """
    from datetime import datetime, timezone

    created = None
    if asset:
        # DAS кладёт время создания в разных местах в зависимости от версии
        created = asset.get("created_at")
        content = asset.get("content") or {}
        metadata = content.get("metadata") or {}
        created = created or metadata.get("created_at")
    if created:
        try:
            if isinstance(created, (int, float)):
                return max(0.0, datetime.now(timezone.utc).timestamp() - float(created))
            dt = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
            return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())
        except (ValueError, AttributeError):
            pass

    # Фоллбэк по сигнатурам: blockTime самой старой из полученных.
    # Полная история (< лимита) -> точный возраст; обрезанная -> нижняя граница.
    if signatures:
        oldest = next(
            (s.get("blockTime") for s in reversed(signatures) if s.get("blockTime")),
            None,
        )
        if oldest:
            return max(0.0, time.time() - float(oldest))
    return None


async def analyze_token(
    ctx: Context, mint: str, bonding_curve: str, raw_supply: int
) -> None:
    s = ctx.settings
    log.info("[%s] 🔎 запускаю анализ (bonding curve %s)", mint, bonding_curve)

    # Сигнатуры минта: один запрос, дальше переиспользуется предпроверками,
    # бандл-метрикой и check_dev (никаких повторных вызовов)
    mint_signatures = await ctx.helius.get_signatures(mint, limit=1000)

    # Имя/тикер/картинка + created_at одним getAsset (кэшируем для алерта)
    asset = await ctx.helius.get_asset(mint)
    name, symbol, image_url = extract_token_meta(asset)

    # --- Предварительная проверка 2.1: возраст токена ---
    age = _token_age_seconds(asset, mint_signatures)
    if age is not None and age > s.max_token_age_hours * 3600:
        log.info(
            "[%s] ❌ ОТСЕЯН: возраст %.1fч > %.0fч",
            mint, age / 3600, s.max_token_age_hours,
        )
        await ctx.stats.record_filter_result(mint, "age")
        await ctx.redis.set(f"seen:{mint}", "1", ex=s.seen_mint_ttl)
        return

    # --- Предварительная проверка 2.2: активность (последняя сделка) ---
    last_ts = _last_tx_time(mint_signatures)
    if last_ts is not None and (time.time() - last_ts) > s.max_inactive_seconds:
        log.info(
            "[%s] ❌ ОТСЕЯН: последняя сделка %.0f сек назад > %.0f",
            mint, time.time() - last_ts, s.max_inactive_seconds,
        )
        await ctx.stats.record_filter_result(mint, "inactive")
        await ctx.redis.set(f"seen:{mint}", "1", ex=s.analysis_retry_ttl)
        return

    # --- Предварительная проверка 2.3: плотность покупок за последние 2 мин ---
    now_ts = time.time()
    recent_tx_count = sum(
        1 for sig in mint_signatures
        if not sig.get("err") and sig.get("blockTime")
        and (now_ts - sig["blockTime"]) <= 120
    )
    if recent_tx_count < s.min_buy_count_last_2min:
        log.info(
            "[%s] ❌ ОТСЕЯН: %d tx за последние 2 мин < %d (MIN_BUY_COUNT_LAST_2MIN)",
            mint, recent_tx_count, s.min_buy_count_last_2min,
        )
        await ctx.stats.record_filter_result(mint, "inactive")
        await ctx.redis.set(f"seen:{mint}", "1", ex=s.analysis_retry_ttl)
        return

    # --- Предварительная проверка 2.4: USD-объём покупок за 5 мин ---
    vol_ok, vol_usd = _check_volume(ctx, mint)
    if not vol_ok:
        log.info(
            "[%s] ❌ ОТСЕЯН VOLUME: $%.0f за 5 мин < $%.0f (MIN_VOLUME_USD_5MIN)",
            mint, vol_usd, s.min_volume_usd_5min,
        )
        await ctx.stats.record_filter_result(mint, "score")
        await ctx.redis.set(f"seen:{mint}", "1", ex=s.analysis_retry_ttl)
        return
    log.info("[%s] ✔ volume OK: $%.0f за 5 мин", mint, vol_usd)

    # --- Предварительная проверка 2.5: уникальных покупателей за 5 мин ---
    buyers_ok, unique_buyers = await _check_unique_buyers(ctx, mint, mint_signatures)
    if not buyers_ok:
        log.info(
            "[%s] ❌ ОТСЕЯН UNIQUE_BUYERS: %d покупателей за 5 мин < %d (MIN_UNIQUE_BUYERS_5MIN)",
            mint, unique_buyers, s.min_unique_buyers_5min,
        )
        await ctx.stats.record_filter_result(mint, "score")
        await ctx.redis.set(f"seen:{mint}", "1", ex=s.analysis_retry_ttl)
        return
    log.info("[%s] ✔ unique buyers OK: %d за 5 мин", mint, unique_buyers)

    # Фильтр 1: концентрация
    conc_status, holders, top10_percent = await check_concentration(
        mint, ctx.helius, s, bonding_curve, raw_supply
    )
    if conc_status == "incomplete":
        # Helius ещё не проиндексировал токен — это не вердикт фильтра.
        # Укорачиваем метку seen: следующая покупка вернёт токен на анализ
        log.info(
            "[%s] 🔁 данные ещё не проиндексированы — повтор через ~%d сек",
            mint, s.analysis_retry_ttl,
        )
        await ctx.redis.set(f"seen:{mint}", "1", ex=s.analysis_retry_ttl)
        return
    if conc_status != "ok":
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

    # Бандлы: доля покупок в слоте создания токена (снайперы на запуске)
    bundle_percent = calculate_bundle_percent(mint_signatures, s.bundle_slot_window)
    log.info("[%s] 📦 бандлы на запуске: %.1f%%", mint, bundle_percent)

    # Жёсткий порог по бандлам: >MAX_BUNDLE_PERCENT → мгновенный отсев до скоринга
    if bundle_percent > s.max_bundle_percent:
        log.info(
            "[%s] ❌ ОТСЕЯН: бандлы %.1f%% > %.0f%% (MAX_BUNDLE_PERCENT)",
            mint, bundle_percent, s.max_bundle_percent,
        )
        await ctx.stats.record_filter_result(mint, "score")
        return

    # История дева
    dev = await check_dev(
        mint, ctx.redis, ctx.helius, ctx.http, s, mint_signatures=mint_signatures
    )
    log.info("[%s] 👨‍💻 dev: %s, MSR %s", mint, dev["status"], dev["msr"])

    # Bad-дев — мгновенный отсев, до скоринга
    if dev["status"] == "Bad":
        log.info("[%s] ❌ ОТСЕЯН: дев в чёрном списке (Bad)", mint)
        await ctx.stats.record_filter_result(mint, "dev")
        await ctx.redis.set(f"seen:{mint}", "1", ex=s.seen_mint_ttl)
        return

    # --- Фильтр DEV_EARLY_BUY: дев купил в первые 60 сек ---
    if s.dev_early_buy_required:
        creator = await get_creator(mint, ctx.helius, mint_signatures)
        if creator:
            if not await _check_dev_early_buy(ctx, mint, creator, mint_signatures):
                log.info(
                    "[%s] ❌ ОТСЕЯН DEV_EARLY_BUY: дев %s не купил в первые 60 сек",
                    mint, creator,
                )
                await ctx.stats.record_filter_result(mint, "dev")
                await ctx.redis.set(f"seen:{mint}", "1", ex=s.analysis_retry_ttl)
                return
            log.info("[%s] ✔ dev early buy подтверждён (%s)", mint, creator)

    # Unknown-дев (нет истории) — ужесточаем требования к HUMAN/UNKNOWN.
    # unknown_percent = 100 - human_percent (третьего состояния нет)
    if dev["status"] == "Unknown":
        unknown_percent = 100.0 - human_percent
        if (
            human_percent < s.human_min_percent_unknown
            or unknown_percent > s.unknown_max_percent_unknown
        ):
            log.info(
                "[%s] ❌ ОТСЕЯН: Unknown-дев + HUMAN %.0f%%/UNKNOWN %.0f%% "
                "(нужно ≥%.0f/≤%.0f)",
                mint, human_percent, unknown_percent,
                s.human_min_percent_unknown, s.unknown_max_percent_unknown,
            )
            await ctx.stats.record_filter_result(mint, "human_strict")
            await ctx.redis.set(f"seen:{mint}", "1", ex=s.analysis_retry_ttl)
            return

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
        "name": name,
        "symbol": symbol,
        "image_url": image_url,
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
            # Токен мог слиться и вернуться (камбек) — даём шанс на повторный
            # цикл анализа вместо часового бана
            await ctx.redis.set(f"seen:{mint}", "1", ex=s.analysis_retry_ttl)
            return

        # Контрольная проверка капы прямо перед отправкой
        final_mc = await market_cap(ctx, bonding_curve)
        if final_mc is None or not (
            s.send_guard_mc_min <= final_mc <= s.send_guard_mc_max
        ):
            log.info("[%s] алерт отменён: капа %s вне guard-диапазона", mint, final_mc)
            ctx.stats.bump("guard_out")
            await ctx.redis.set(f"seen:{mint}", "1", ex=s.analysis_retry_ttl)
            return

        # Проверка давления продаж: если крупный холдер дампит — отменяем алерт
        if await _check_sell_pressure(ctx, mint, bonding_curve):
            await ctx.redis.set(f"seen:{mint}", "1", ex=s.analysis_retry_ttl)
            return

        # Дедуп: одна монета — один алерт (в пределах ALERT_DEDUP_TTL).
        # Иначе токен, зависший в окне капы, перезаливается каждый час.
        if not await ctx.redis.set(
            f"alerted:{mint}", "1", ex=s.alert_dedup_ttl, nx=True
        ):
            log.info("[%s] алерт пропущен: уже отправляли недавно", mint)
            return

        # Имя/тикер/картинка кэшированы на этапе анализа (getAsset не повторяем)
        name = alert_data.get("name", "Unknown")
        symbol = alert_data.get("symbol", "?")
        image_url = alert_data.get("image_url")
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


def screen_buy_events(ctx: Context, logs: list[str]) -> list[tuple[str, float]]:
    """Скрининг покупок прямо из логов события — ноль RPC-запросов.

    Из строк "Program data:" достаются TradeEvent'ы (mint + резервы кривой),
    капа считается по резервам и кэшированной цене SOL. Возвращает список
    (mint, mc) токенов, чья капа попала в рабочий диапазон.
    """
    s = ctx.settings
    sol_price = ctx.sol_price
    if sol_price is None:
        return []
    candidates = []
    seen_in_tx: set[str] = set()
    now_wall = time.time()
    for event in iter_trade_events(logs):
        if not event["is_buy"]:
            continue
        # Накапливаем USD-объём покупок для фильтра VOLUME (все покупки, до дедупа)
        usd = event["sol_amount"] / 1e9 * sol_price
        ctx.vol_tracker.setdefault(event["mint"], []).append((now_wall, usd))
        if not seen_in_tx:
            ctx.stats.bump("buys_seen")
        if event["mint"] in seen_in_tx:
            continue
        seen_in_tx.add(event["mint"])
        mc = market_cap_from_reserves(
            event["virtual_sol_reserves"], event["virtual_token_reserves"], sol_price
        )
        ctx.stats.bump("mc_calcs")
        if mc is None or mc < s.mc_analyze_min:
            continue
        if s.mc_analyze_max > 0 and mc > s.mc_analyze_max:
            continue
        candidates.append((event["mint"], mc))
    return candidates


async def _fetch_curve_state(ctx: Context, address: str) -> Optional[dict]:
    value = await ctx.helius.get_account_info(address)
    data_field = (value or {}).get("data")
    if not (isinstance(data_field, list) and data_field and data_field[0]):
        return None
    try:
        return parse_bonding_curve_state(base64.b64decode(data_field[0]))
    except (ValueError, TypeError):
        return None


async def resolve_bonding_curve(
    ctx: Context, mint: str, signature: str
) -> tuple[Optional[str], Optional[dict]]:
    """Определяет адрес bonding curve токена и её состояние.

    Основной путь — PDA-деривация из минта (без гаданий по транзакции)
    с проверкой аккаунта по дискриминатору. Запасной — разбор инструкции
    buy из транзакции (на случай нестандартных вариантов программы).
    Состояние кривой отдаётся дальше: в нём supply для фильтра концентрации.
    """
    s = ctx.settings
    derived = derive_bonding_curve(mint, s.pump_program)
    if derived:
        state = await _fetch_curve_state(ctx, derived)
        if state:
            return derived, state
        log.info("[%s] PDA кривой не подтвердился — пробую через транзакцию", mint)

    tx = await ctx.helius.get_transaction(signature)
    if not tx:
        # Транзакция могла ещё не доехать до ноды — одна повторная попытка
        await asyncio.sleep(3)
        tx = await ctx.helius.get_transaction(signature)
    if not tx or (tx.get("meta") or {}).get("err"):
        log.warning("[%s] ⚠️ не удалось получить транзакцию %s…", mint, signature[:16])
        return None, None
    ctx.stats.bump("tx_checked")
    parsed = find_buy_accounts(
        tx, s.pump_program, s.buy_mint_index, s.buy_bonding_curve_index,
        target_mint=mint,
    )
    if not parsed:
        log.warning("[%s] ⚠️ buy-инструкция не найдена в транзакции", mint)
        return None, None
    bonding_curve = parsed[1]
    return bonding_curve, await _fetch_curve_state(ctx, bonding_curve)


async def candidate_worker(ctx: Context) -> None:
    """Воркер полного анализа: берёт кандидатов из ограниченной очереди."""
    s = ctx.settings
    while True:
        signature, mint, event_mc = await ctx.candidate_queue.get()
        try:
            log.info(
                "[%s] 💵 MC $%.0f (из события) в диапазоне — токен идёт на анализ",
                mint, event_mc,
            )
            bonding_curve, curve_state = await resolve_bonding_curve(ctx, mint, signature)
            if not bonding_curve:
                # Короткий бан вместо мгновенного повтора, иначе токен
                # зацикливается: каждая новая покупка возвращает его в очередь
                log.info(
                    "[%s] 🔁 bonding curve не определена — повтор через ~%d сек",
                    mint, s.analysis_retry_ttl,
                )
                await ctx.redis.set(f"seen:{mint}", "1", ex=s.analysis_retry_ttl)
                continue
            raw_supply = (curve_state or {}).get("token_total_supply", 0)
            await analyze_token(ctx, mint, bonding_curve, raw_supply)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[%s] ошибка анализа", mint)
        finally:
            ctx.candidate_queue.task_done()


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
                s.ws_rpc_url,
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

                    candidates = screen_buy_events(ctx, value.get("logs") or [])
                    for mint, mc in candidates:
                        # Дедуп до постановки в очередь: один mint — один анализ
                        seen_key = f"seen:{mint}"
                        if not await ctx.redis.set(
                            seen_key, "1", ex=s.seen_mint_ttl, nx=True
                        ):
                            continue
                        try:
                            ctx.candidate_queue.put_nowait(
                                (value["signature"], mint, mc)
                            )
                        except asyncio.QueueFull:
                            # Очередь ограничена — излишек сбрасываем, метку
                            # снимаем, чтобы следующая покупка вернула токен
                            ctx.stats.bump("queue_dropped")
                            await ctx.redis.delete(seen_key)
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
                "💓 SOL $%s | за минуту: событий %d, покупок %d, капа из событий %d, "
                "tx %d | отсеяно: conc %d, score %d | прошло %d | "
                "очередь %d (сброшено %d) | алертов всего %d",
                f"{sol_price:.2f}" if sol_price else "?",
                delta("events_received"),
                delta("buys_seen"),
                delta("mc_calcs"),
                delta("tx_checked"),
                delta("filtered:concentration"),
                delta("filtered:score"),
                delta("filtered:passed"),
                ctx.candidate_queue.qsize(),
                delta("queue_dropped"),
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
            candidate_queue=asyncio.Queue(maxsize=settings.candidate_queue_size),
        )

        await tg_bot.set_my_commands(BOT_COMMANDS)
        await send_startup_message(tg_bot, settings.telegram_chat_id)
        await resume_pending_alerts(ctx)

        dispatcher = Dispatcher()
        dispatcher.include_router(commands_router)
        dispatcher["ctx"] = ctx

        tasks = [
            asyncio.create_task(
                sol_price_loop(
                    http, redis_client, settings.sol_price_interval, price_holder=ctx
                )
            ),
            asyncio.create_task(pump_logs_loop(ctx)),
            asyncio.create_task(outcomes_loop(ctx)),
            asyncio.create_task(heartbeat_loop(ctx)),
            asyncio.create_task(
                dispatcher.start_polling(tg_bot, handle_signals=False)
            ),
        ]
        tasks += [
            asyncio.create_task(candidate_worker(ctx))
            for _ in range(settings.max_concurrent_analyses)
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
