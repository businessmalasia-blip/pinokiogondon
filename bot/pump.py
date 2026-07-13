"""Разбор транзакций Pump.fun: поиск инструкций buy/create по дискриминаторам,
декодирование состояния bonding curve и событий TradeEvent из логов."""

import base64
import binascii
import hashlib
import struct
from typing import Iterator, Optional

import base58

# Anchor-дискриминаторы инструкций программы Pump.fun
BUY_DISCRIMINATOR = bytes.fromhex("66063d1201daebea")
CREATE_DISCRIMINATOR = bytes.fromhex("181ec828051c0777")
# Дискриминатор аккаунта BondingCurve (sha256("account:BondingCurve")[:8])
BONDING_CURVE_ACCOUNT_DISCRIMINATOR = bytes.fromhex("17b7f83760d8ac60")
# Дискриминатор события TradeEvent (sha256("event:TradeEvent")[:8])
TRADE_EVENT_DISCRIMINATOR = hashlib.sha256(b"event:TradeEvent").digest()[:8]

PROGRAM_DATA_PREFIX = "Program data: "

# Полный supply токена Pump.fun в сырых единицах (1 млрд с 6 decimals) —
# совпадает с token_total_supply в аккаунте кривой у всех токенов
TOKEN_TOTAL_SUPPLY_RAW = 1_000_000_000 * 10**6


def _discriminator(data_b58: str) -> bytes:
    try:
        return base58.b58decode(data_b58)[:8]
    except (ValueError, TypeError):
        return b""


def iter_program_instructions(tx: dict, program_id: str) -> Iterator[dict]:
    """Все инструкции указанной программы: верхнеуровневые и внутренние."""
    message = tx.get("transaction", {}).get("message", {})
    meta = tx.get("meta") or {}

    for ins in message.get("instructions", []):
        if ins.get("programId") == program_id and "data" in ins:
            yield ins
    for inner in meta.get("innerInstructions") or []:
        for ins in inner.get("instructions", []):
            if ins.get("programId") == program_id and "data" in ins:
                yield ins


def find_buy_accounts(
    tx: dict,
    program_id: str,
    mint_index: int,
    bonding_curve_index: int,
    target_mint: Optional[str] = None,
) -> Optional[tuple[str, str]]:
    """Извлекает (mint, bonding_curve) из аккаунтов инструкции buy.

    Адреса берутся строго из инструкции buy (включая внутренние),
    а не по приросту SOL на аккаунтах. Если задан target_mint,
    ищется инструкция именно этого минта (в транзакции-бандле
    может быть несколько покупок разных токенов).
    """
    for ins in iter_program_instructions(tx, program_id):
        if _discriminator(ins["data"]) != BUY_DISCRIMINATOR:
            continue
        accounts = ins.get("accounts", [])
        if len(accounts) <= max(mint_index, bonding_curve_index):
            continue
        if target_mint and accounts[mint_index] != target_mint:
            continue
        return accounts[mint_index], accounts[bonding_curve_index]
    return None


def find_created_mint(tx: dict, program_id: str) -> Optional[str]:
    """Mint созданного токена из инструкции create (accounts[0])."""
    for ins in iter_program_instructions(tx, program_id):
        if _discriminator(ins["data"]) != CREATE_DISCRIMINATOR:
            continue
        accounts = ins.get("accounts", [])
        if accounts:
            return accounts[0]
    return None


def parse_bonding_curve_state(data: bytes) -> Optional[dict]:
    """Состояние bonding curve Pump.fun.

    Раскладка аккаунта: 8 байт дискриминатора, затем u64 LE:
    virtual_token_reserves, virtual_sol_reserves, real_token_reserves,
    real_sol_reserves, token_total_supply, затем bool complete.
    Раскладка сверена с живыми аккаунтами mainnet (151 байт, поля на месте).
    """
    if len(data) < 49 or data[:8] != BONDING_CURVE_ACCOUNT_DISCRIMINATOR:
        return None
    vtok, vsol, rtok, rsol, supply = struct.unpack_from("<QQQQQ", data, 8)
    return {
        "virtual_token_reserves": vtok,
        "virtual_sol_reserves": vsol,
        "real_token_reserves": rtok,
        "real_sol_reserves": rsol,
        "token_total_supply": supply,
        "complete": bool(data[48]),
    }


def parse_trade_event(log_line: str) -> Optional[dict]:
    """TradeEvent из строки лога "Program data: <base64>".

    Раскладка: 8 байт дискриминатора, mint (32), sol_amount u64,
    token_amount u64, is_buy u8, user (32), timestamp i64,
    virtual_sol_reserves u64, virtual_token_reserves u64 (дальше идут
    поля новых версий программы — они для расчёта капы не нужны).
    """
    idx = log_line.find(PROGRAM_DATA_PREFIX)
    if idx < 0:
        return None
    try:
        raw = base64.b64decode(log_line[idx + len(PROGRAM_DATA_PREFIX):])
    except (binascii.Error, ValueError):
        return None
    if len(raw) < 113 or raw[:8] != TRADE_EVENT_DISCRIMINATOR:
        return None
    mint = base58.b58encode(raw[8:40]).decode()
    sol_amount, token_amount = struct.unpack_from("<QQ", raw, 40)
    is_buy = bool(raw[56])
    user = base58.b58encode(raw[57:89]).decode()
    timestamp, virtual_sol, virtual_token = struct.unpack_from("<qQQ", raw, 89)
    return {
        "mint": mint,
        "sol_amount": sol_amount,
        "token_amount": token_amount,
        "is_buy": is_buy,
        "user": user,
        "timestamp": timestamp,
        "virtual_sol_reserves": virtual_sol,
        "virtual_token_reserves": virtual_token,
    }


def iter_trade_events(logs: list[str]) -> Iterator[dict]:
    for line in logs:
        event = parse_trade_event(line)
        if event:
            yield event


def market_cap_from_reserves(
    virtual_sol_reserves: int, virtual_token_reserves: int, sol_price_usd: float
) -> Optional[float]:
    """Market cap токена прямо из резервов кривой, без RPC-запросов."""
    if virtual_token_reserves <= 0 or sol_price_usd <= 0:
        return None
    price_sol = (virtual_sol_reserves / 1e9) / (virtual_token_reserves / 1e6)
    return price_sol * (TOKEN_TOTAL_SUPPLY_RAW / 1e6) * sol_price_usd


def fee_payer(tx: dict) -> Optional[str]:
    keys = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
    if not keys:
        return None
    first = keys[0]
    if isinstance(first, dict):
        return first.get("pubkey")
    return first
