"""Отправка алертов в Telegram через aiogram. Карточный формат с рамкой."""

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


def extract_socials(asset: Optional[dict]) -> dict[str, str]:
    """Ссылки на соцсети токена из getAsset (что удалось найти)."""
    socials: dict[str, str] = {}
    if not asset:
        return socials
    content = asset.get("content") or {}
    links = content.get("links") or {}
    metadata = content.get("metadata") or {}
    # DAS кладёт сайт в external_url; twitter/telegram — если есть в метаданных
    website = links.get("external_url") or metadata.get("website")
    if website:
        socials["🌐 Web"] = website
    for key, label in (("twitter", "🐦 Twitter"), ("telegram", "✈️ TG")):
        url = metadata.get(key) or links.get(key)
        if url:
            socials[label] = url
    return socials


def _fmt_usd(value: Optional[float]) -> str:
    """Компактный формат: $9.8k, $1.2M."""
    if value is None:
        return "—"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}k"
    return f"${value:,.0f}"


def _fmt_age(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    seconds = max(seconds, 0)
    if seconds < 3600:
        return f"{int(seconds // 60)}м"
    if seconds < 86400:
        return f"{int(seconds // 3600)}ч {int((seconds % 3600) // 60)}м"
    return f"{int(seconds // 86400)}д"


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
    liquidity: Optional[float] = None,
    holders: Optional[int] = None,
    volume: Optional[float] = None,
    trades: Optional[tuple[int, int]] = None,
    age_seconds: Optional[float] = None,
    socials: Optional[dict[str, str]] = None,
) -> str:
    name_e = html.escape(name)
    sym_e = html.escape(symbol.lstrip("$"))

    if msr is not None:
        dev_line = f"{dev_status} (MSR {msr:.0f}%)"
    else:
        dev_line = dev_status

    # Дерево-карточка: собираем только доступные строки, углы проставляем в конце
    rows: list[str] = []
    rows.append(f"💰 Market Cap: <b>${market_cap:,.0f}</b>")
    if liquidity is not None:
        rows.append(f"💧 Liquidity: {_fmt_usd(liquidity)}")
    holders_part = f"👥 Holders: {holders}" if holders is not None else "👥 Holders: —"
    rows.append(f"{holders_part} │ 📊 Top-10: {top10_percent:.1f}%")
    rows.append(f"🧑 Humans: {human_percent:.0f}% │ 📦 Bundle: {bundle_percent:.1f}%")
    rows.append(f"👨‍💻 Dev: {dev_line}")
    if trades is not None:
        rows.append(f"🔁 Trades: B {trades[0]} │ S {trades[1]}")
    if volume is not None:
        rows.append(f"📈 Volume: {_fmt_usd(volume)}")
    if age_seconds is not None:
        rows.append(f"🕐 Age: {_fmt_age(age_seconds)}")

    tree = []
    for i, row in enumerate(rows):
        if i == 0:
            prefix = "┌"
        elif i == len(rows) - 1:
            prefix = "└"
        else:
            prefix = "├"
        tree.append(f"{prefix} {row}")
    tree_block = "\n".join(tree)

    subscores = (
        f"👥 {score['human']} · 👨‍💻 {score['msr']} · "
        f"📊 {score['concentration']} · 📦 {score['bundle']}"
    )

    links = [
        f'<a href="https://photon-sol.tinyastro.io/token/{mint}">Photon</a>',
        f'<a href="https://gmgn.ai/sol/token/{mint}">GMGN</a>',
        f'<a href="https://axiom.trade/t/{mint}">Axiom</a>',
        f'<a href="https://pump.fun/{mint}">Pump</a>',
    ]
    links_block = "🔱 " + " · ".join(links)

    parts = [
        f"🟢 <b>{name_e}</b> │ ${sym_e}   ⭐ {score['total']}/10",
        "",
        f"<code>{mint}</code>",
        "",
        tree_block,
        "",
        f"⭐ <b>Score {score['total']}/10</b>  ({subscores})",
        "",
        links_block,
    ]

    if socials:
        social_links = " │ ".join(
            f'<a href="{html.escape(url, quote=True)}">{label}</a>'
            for label, url in socials.items()
        )
        parts.append(f"🌐 {social_links}")

    return "\n".join(parts)


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
    await bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)


async def send_startup_message(bot: Bot, chat_id: str) -> None:
    try:
        await bot.send_message(chat_id=chat_id, text="🚀 Бот запущен")
    except TelegramAPIError as exc:
        log.error("Не удалось отправить стартовое сообщение: %s", exc)
