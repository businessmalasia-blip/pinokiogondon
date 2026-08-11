"""Загрузка настроек бота из .env."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Переменная окружения {name} не задана (см. .env.example)")
    return value


@dataclass(frozen=True)
class Settings:
    # Ключи и адреса
    helius_api_key: str
    telegram_bot_token: str
    telegram_chat_id: str
    redis_url: str
    pump_program: str

    # Индексы аккаунтов в инструкции buy
    buy_mint_index: int
    buy_bonding_curve_index: int

    # Market cap
    mc_analyze_min: float
    mc_analyze_max: float
    alert_mc_min: float
    alert_mc_max: float
    send_guard_mc_min: float
    send_guard_mc_max: float
    mc_poll_interval: float
    mc_wait_timeout: float
    mc_wait_abort_below: float
    mc_comeback_wait: float
    mc_stale_timeout: float

    # Предварительные проверки
    max_token_age_hours: float
    max_inactive_seconds: float

    # Цена SOL
    sol_price_interval: float

    # Rate limiting Helius
    helius_rate_limit: float

    # Фильтр 1: концентрация
    bonding_curve_exclude_percent: float
    holder_max_percent: float
    top10_max_percent: float

    # Фильтр 2: HUMAN
    human_min_percent: float
    unknown_max_percent: float
    # Ужесточённые пороги для Unknown-девов (защита от рагпулов)
    human_min_percent_unknown: float
    unknown_max_percent_unknown: float
    human_cache_ttl: int
    unknown_cache_ttl: int
    human_check_top: int
    human_min_holder_share: float
    human_min_candidates: int

    # Бандлы
    bundle_slot_window: int
    min_buy_count_last_2min: int
    max_bundle_percent: float

    # Скоринг
    min_score: float
    score_weight_human: float
    score_weight_msr: float
    score_weight_concentration: float
    score_weight_bundle: float

    # Фильтр 3: дев
    msr_min_percent: float
    dev_min_tokens: int
    dev_history_days: int
    dev_tx_limit: int
    dev_tokens_check_max: int
    survivor_min_volume_usd: float

    # WebSocket (отдельный URL — не Helius, чтобы не тратить streaming-кредиты)
    ws_rpc_url: str

    # Стабильный рост (Anti-Volatility)
    max_price_increase_percent: float
    stability_check_seconds: float
    # Мгновенный разворот: падение от пика на этот % за < STABILITY_CHECK_SECONDS сек
    sharp_reversal_drop_percent: float

    # Торговые сессии
    timezone: str
    eu_session_start: int
    eu_session_end: int
    us_session_start: int
    us_session_end: int

    # Фильтр: тренд (рост капы)
    min_trend_percent: float
    trend_wait_timeout: int
    trend_recheck_interval: int
    # Фильтр: USD-объём покупок за 5 мин (из WebSocket-событий)
    min_volume_usd_5min: float
    # Фильтр: уникальных покупателей за 5 мин
    min_unique_buyers_5min: int
    # Фильтр: дев купил токен в первые N сек после создания
    dev_early_buy_required: bool
    dev_early_buy_window: int

    # Прочее
    seen_mint_ttl: int
    max_concurrent_analyses: int
    candidate_queue_size: int
    das_min_accounts: int
    das_retry_delay: float
    das_retries: int
    analysis_retry_ttl: int
    alert_dedup_ttl: int
    log_level: str

    @property
    def rpc_http_url(self) -> str:
        return f"https://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"



def load_settings() -> Settings:
    return Settings(
        helius_api_key=_env("HELIUS_API_KEY"),
        telegram_bot_token=_env("TELEGRAM_BOT_TOKEN"),
        telegram_chat_id=_env("TELEGRAM_CHAT_ID"),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        pump_program=os.getenv(
            "PUMP_PROGRAM", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
        ),
        buy_mint_index=int(os.getenv("BUY_MINT_INDEX", "2")),
        buy_bonding_curve_index=int(os.getenv("BUY_BONDING_CURVE_INDEX", "3")),
        mc_analyze_min=float(os.getenv("MC_ANALYZE_MIN", "8000")),
        mc_analyze_max=float(os.getenv("MC_ANALYZE_MAX", "13000")),
        alert_mc_min=float(os.getenv("MIN_CAP_ALERT", "9000")),
        alert_mc_max=float(os.getenv("MAX_CAP_ALERT", "12000")),
        send_guard_mc_min=float(os.getenv("GUARD_MIN_CAP", "9000")),
        send_guard_mc_max=float(os.getenv("GUARD_MAX_CAP", "13000")),
        mc_poll_interval=float(os.getenv("MC_POLL_INTERVAL", "2")),
        mc_wait_timeout=float(os.getenv("MC_WAIT_TIMEOUT", "1200")),
        mc_wait_abort_below=float(os.getenv("MC_WAIT_ABORT_BELOW", "6000")),
        mc_comeback_wait=float(os.getenv("MC_COMEBACK_WAIT", "300")),
        mc_stale_timeout=float(os.getenv("MC_STALE_TIMEOUT", "30")),
        max_token_age_hours=float(os.getenv("MAX_TOKEN_AGE_HOURS", "2")),
        max_inactive_seconds=float(os.getenv("MAX_INACTIVE_SECONDS", "120")),
        sol_price_interval=float(os.getenv("SOL_PRICE_INTERVAL", "2")),
        helius_rate_limit=float(os.getenv("HELIUS_RATE_LIMIT", "0.3")),
        bonding_curve_exclude_percent=float(
            os.getenv("BONDING_CURVE_EXCLUDE_PERCENT", "50")
        ),
        holder_max_percent=float(os.getenv("MAX_SINGLE_HOLDER_PERCENT", "6.0")),
        top10_max_percent=float(os.getenv("MAX_TOP10_HOLDERS_PERCENT", "18.0")),
        human_min_percent=float(os.getenv("HUMAN_MIN_PERCENT", "60")),
        unknown_max_percent=float(os.getenv("UNKNOWN_MAX_PERCENT", "15")),
        human_min_percent_unknown=float(os.getenv("HUMAN_MIN_PERCENT_UNKNOWN", "55")),
        unknown_max_percent_unknown=float(os.getenv("UNKNOWN_MAX_PERCENT_UNKNOWN", "20")),
        human_cache_ttl=int(os.getenv("HUMAN_CACHE_TTL", "3600")),
        unknown_cache_ttl=int(os.getenv("UNKNOWN_CACHE_TTL", "3600")),
        human_check_top=int(os.getenv("HUMAN_CHECK_TOP", "30")),
        human_min_holder_share=float(os.getenv("HUMAN_MIN_HOLDER_SHARE", "0.5")),
        human_min_candidates=int(os.getenv("HUMAN_MIN_CANDIDATES", "10")),
        bundle_slot_window=int(os.getenv("BUNDLE_SLOT_WINDOW", "2")),
        min_buy_count_last_2min=int(os.getenv("MIN_BUY_COUNT_LAST_2MIN", "2")),
        max_bundle_percent=float(os.getenv("MAX_BUNDLE_PERCENT", "20.0")),
        min_score=float(os.getenv("MIN_SCORE", "8.0")),
        score_weight_human=float(os.getenv("SCORE_WEIGHT_HUMAN", "0.30")),
        score_weight_msr=float(os.getenv("SCORE_WEIGHT_MSR", "0.25")),
        score_weight_concentration=float(os.getenv("SCORE_WEIGHT_CONCENTRATION", "0.25")),
        score_weight_bundle=float(os.getenv("SCORE_WEIGHT_BUNDLE", "0.20")),
        msr_min_percent=float(os.getenv("MSR_MIN_PERCENT", "80")),
        dev_min_tokens=int(os.getenv("DEV_MIN_TOKENS", "3")),
        dev_history_days=int(os.getenv("DEV_HISTORY_DAYS", "30")),
        dev_tx_limit=int(os.getenv("DEV_TX_LIMIT", "50")),
        dev_tokens_check_max=int(os.getenv("DEV_TOKENS_CHECK_MAX", "20")),
        survivor_min_volume_usd=float(os.getenv("SURVIVOR_MIN_VOLUME_USD", "1000")),
        seen_mint_ttl=int(os.getenv("SEEN_MINT_TTL", "3600")),
        max_concurrent_analyses=int(os.getenv("MAX_CONCURRENT_ANALYSES", "3")),
        candidate_queue_size=int(os.getenv("CANDIDATE_QUEUE_SIZE", "100")),
        das_min_accounts=int(os.getenv("DAS_MIN_ACCOUNTS", "15")),
        das_retry_delay=float(os.getenv("DAS_RETRY_DELAY", "12")),
        das_retries=int(os.getenv("DAS_RETRIES", "2")),
        ws_rpc_url=os.getenv(
            "WS_RPC_URL",
            "wss://api.mainnet-beta.solana.com",
        ),
        max_price_increase_percent=float(os.getenv("MAX_PRICE_INCREASE_PERCENT", "30.0")),
        stability_check_seconds=float(os.getenv("STABILITY_CHECK_SECONDS", "60.0")),
        sharp_reversal_drop_percent=float(os.getenv("SHARP_REVERSAL_DROP_PERCENT", "10.0")),
        timezone=os.getenv("TIMEZONE", "Europe/Moscow"),
        eu_session_start=int(os.getenv("EU_SESSION_START", "10")),
        eu_session_end=int(os.getenv("EU_SESSION_END", "16")),
        us_session_start=int(os.getenv("US_SESSION_START", "16")),
        us_session_end=int(os.getenv("US_SESSION_END", "23")),
        min_trend_percent=float(os.getenv("MIN_TREND_PERCENT", "0.5")),
        trend_wait_timeout=int(os.getenv("TREND_WAIT_TIMEOUT", "300")),
        trend_recheck_interval=int(os.getenv("TREND_RECHECK_INTERVAL", "60")),
        min_volume_usd_5min=float(os.getenv("MIN_VOLUME_USD_5MIN", "150.0")),
        min_unique_buyers_5min=int(os.getenv("MIN_UNIQUE_BUYERS_5MIN", "2")),
        dev_early_buy_required=os.getenv("DEV_EARLY_BUY_REQUIRED", "true").lower()
        in ("1", "true", "yes"),
        dev_early_buy_window=int(os.getenv("DEV_EARLY_BUY_WINDOW", "300")),
        analysis_retry_ttl=int(os.getenv("ANALYSIS_RETRY_TTL", "180")),
        alert_dedup_ttl=int(os.getenv("ALERT_DEDUP_TTL", "86400")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
