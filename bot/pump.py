"""Разбор транзакций Pump.fun: поиск инструкций buy/create по дискриминаторам
и декодирование состояния bonding curve."""

import struct
from typing import Iterator, Optional

import base58

# Anchor-дискриминаторы инструкций программы Pump.fun
BUY_DISCRIMINATOR = bytes.fromhex("66063d1201daebea")
CREATE_DISCRIMINATOR = bytes.fromhex("181ec828051c0777")
# Дискриминатор аккаунта BondingCurve (sha256("account:BondingCurve")[:8])
BONDING_CURVE_ACCOUNT_DISCRIMINATOR = bytes.fromhex("17b7f83760d8ac60")


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
    tx: dict, program_id: str, mint_index: int, bonding_curve_index: int
) -> Optional[tuple[str, str]]:
    """Извлекает (mint, bonding_curve) из аккаунтов инструкции buy.

    Адреса берутся строго из инструкции buy (включая внутренние),
    а не по приросту SOL на аккаунтах.
    """
    for ins in iter_program_instructions(tx, program_id):
        if _discriminator(ins["data"]) != BUY_DISCRIMINATOR:
            continue
        accounts = ins.get("accounts", [])
        if len(accounts) <= max(mint_index, bonding_curve_index):
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


def has_buy_log(logs: list[str]) -> bool:
    return any("Instruction: Buy" in line for line in logs)


def fee_payer(tx: dict) -> Optional[str]:
    keys = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
    if not keys:
        return None
    first = keys[0]
    if isinstance(first, dict):
        return first.get("pubkey")
    return first
