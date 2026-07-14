"""Скоринг монеты по уже посчитанным метрикам. Никаких запросов к API."""

from typing import Optional

from .config import Settings


def _human_score(human_percent: float) -> int:
    if human_percent >= 70:
        return 10
    if human_percent >= 60:
        return 7
    if human_percent >= 45:
        return 4
    return 0


def _msr_score(msr: Optional[float]) -> int:
    # <50 или Unknown (msr отсутствует) -> 0
    if msr is None:
        return 0
    if msr >= 85:
        return 10
    if msr >= 70:
        return 7
    if msr >= 50:
        return 4
    return 0


def _concentration_score(top10_percent: float) -> int:
    if top10_percent < 10:
        return 10
    if top10_percent <= 15:
        return 7
    return 0


def _bundle_score(bundle_percent: float) -> int:
    if bundle_percent <= 10:
        return 10
    if bundle_percent <= 25:
        return 7
    return 3


def calculate_score(token_data: dict, settings: Settings) -> dict:
    """Итоговый скор 0–10 по метрикам токена.

    token_data: {
        "human_percent": float,
        "msr": float | None,          # None для Unknown-дева
        "top10_percent": float,
        "bundle_percent": float,
    }
    Возвращает суб-скоры и взвешенный total (веса из .env).
    """
    human = _human_score(token_data["human_percent"])
    msr = _msr_score(token_data.get("msr"))
    concentration = _concentration_score(token_data["top10_percent"])
    bundle = _bundle_score(token_data["bundle_percent"])

    total = (
        human * settings.score_weight_human
        + msr * settings.score_weight_msr
        + concentration * settings.score_weight_concentration
        + bundle * settings.score_weight_bundle
    )
    return {
        "human": human,
        "msr": msr,
        "concentration": concentration,
        "bundle": bundle,
        "total": round(total, 1),
    }
