"""Отправка алертов в Telegram через aiogram."""

import html
import logging
from typing import Optional

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError

log = logging.getLogger(__name__)


def extract_token_meta(asset: Optional[dict]) -> tuple[str, str, Optional[str]]:
    """(name, symbol, image_url) из ответа Helius DAS getAsset."""
    name, symbol, image = "Unknown", "?", None
    if asset:
        content = asset.get("content") or {}
        metadata = content.get("metadata") or {}
        links = content.get("links") or {}
        name = metadata.get("name") or name
        symbol = metadata.get("symbol") or symbol
        image = links.get("image")
    return name.strip(), symbol.strip(), image


def build_alert_text(
    name: str,
    symbol: str,
    mint: str,
    human_percent: float,
    dev_status: str,
    msr: Optional[float],
    top10_percent: float,
    bundle_percent: float,
    score: dict,
    market_cap: float,
) -> str:
    msr_text = f"{msr:.0f}%" if msr is not None else "—"
    return (
        "🔥 <b>QUALITY TOKEN</b>\n"
        f"🪙 <b>{html.escape(name)}</b> ({html.escape(symbol)})\n"
        f"📋 CA: <code>{mint}</code>\n"
        f'🔗 <a href="https://photon-sol.tinyastro.io/token/{mint}">Photon</a> · '
        f'<a href="https://gmgn.ai/sol/token/{mint}">GMGN</a>\n'
        f"⭐ Score: {score['total']}/10\n"
        f"👥 Humans: {human_percent:.0f}% ({score['human']}/10)\n"
        f"👨‍💻 Dev: {dev_status}, MSR {msr_text} ({score['msr']}/10)\n"
        f"📊 Concentration: Top-10 {top10_percent:.1f}% ({score['concentration']}/10)\n"
        f"📦 Bundle: {bundle_percent:.1f}% ({score['bundle']}/10)\n"
        f"💰 MC: ${market_cap:,.0f}"
    )


async def send_alert(
    bot: Bot,
    chat_id: str,
    text: str,
    image_url: Optional[str],
) -> None:
    """Алерт с картинкой токена; при проблемах с картинкой — просто текст."""
    if image_url:
        try:
            await bot.send_photo(chat_id=chat_id, photo=image_url, caption=text)
            return
        except TelegramAPIError as exc:
            log.warning("Не удалось отправить фото (%s), шлю текстом", exc)
    await bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=False)


async def send_startup_message(bot: Bot, chat_id: str) -> None:
    try:
        await bot.send_message(chat_id=chat_id, text="🚀 Бот запущен")
    except TelegramAPIError as exc:
        log.error("Не удалось отправить стартовое сообщение: %s", exc)
