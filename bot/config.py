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
    human_cache_ttl: int
    unknown_cache_ttl: int
    human_check_top: int
    human_min_holder_share: float
    human_min_candidates: int

    # Бандлы
    bundle_slot_window: int

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

    # Прочее
    seen_mint_ttl: int
    max_concurrent_analyses: int
    candidate_queue_size: int
    das_min_accounts: int
    das_retry_delay: float
    das_retries: int
    log_level: str

    @property
    def rpc_http_url(self) -> str:
        return f"https://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"

    @property
    def rpc_ws_url(self) -> str:
        return f"wss://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"


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
        alert_mc_min=float(os.getenv("ALERT_MC_MIN", "10000")),
        alert_mc_max=float(os.getenv("ALERT_MC_MAX", "12000")),
        send_guard_mc_min=float(os.getenv("SEND_GUARD_MC_MIN", "9000")),
        send_guard_mc_max=float(os.getenv("SEND_GUARD_MC_MAX", "13000")),
        mc_poll_interval=float(os.getenv("MC_POLL_INTERVAL", "2")),
        mc_wait_timeout=float(os.getenv("MC_WAIT_TIMEOUT", "1200")),
        mc_wait_abort_below=float(os.getenv("MC_WAIT_ABORT_BELOW", "6000")),
        sol_price_interval=float(os.getenv("SOL_PRICE_INTERVAL", "2")),
        helius_rate_limit=float(os.getenv("HELIUS_RATE_LIMIT", "0.25")),
        bonding_curve_exclude_percent=float(
            os.getenv("BONDING_CURVE_EXCLUDE_PERCENT", "50")
        ),
        holder_max_percent=float(os.getenv("HOLDER_MAX_PERCENT", "2.5")),
        top10_max_percent=float(os.getenv("TOP10_MAX_PERCENT", "15")),
        human_min_percent=float(os.getenv("HUMAN_MIN_PERCENT", "45")),
        unknown_max_percent=float(os.getenv("UNKNOWN_MAX_PERCENT", "30")),
        human_cache_ttl=int(os.getenv("HUMAN_CACHE_TTL", "3600")),
        unknown_cache_ttl=int(os.getenv("UNKNOWN_CACHE_TTL", "3600")),
        human_check_top=int(os.getenv("HUMAN_CHECK_TOP", "30")),
        human_min_holder_share=float(os.getenv("HUMAN_MIN_HOLDER_SHARE", "0.5")),
        human_min_candidates=int(os.getenv("HUMAN_MIN_CANDIDATES", "10")),
        bundle_slot_window=int(os.getenv("BUNDLE_SLOT_WINDOW", "2")),
        min_score=float(os.getenv("MIN_SCORE", "7.5")),
        score_weight_human=float(os.getenv("SCORE_WEIGHT_HUMAN", "0.30")),
        score_weight_msr=float(os.getenv("SCORE_WEIGHT_MSR", "0.25")),
        score_weight_concentration=float(os.getenv("SCORE_WEIGHT_CONCENTRATION", "0.25")),
        score_weight_bundle=float(os.getenv("SCORE_WEIGHT_BUNDLE", "0.20")),
        msr_min_percent=float(os.getenv("MSR_MIN_PERCENT", "70")),
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
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
