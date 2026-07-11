"""Разбор транзакций Pump.fun: поиск инструкций buy/create по дискриминаторам."""

from typing import Iterator, Optional

import base58

# Anchor-дискриминаторы инструкций программы Pump.fun
BUY_DISCRIMINATOR = bytes.fromhex("66063d1201daebea")
CREATE_DISCRIMINATOR = bytes.fromhex("181ec828051c0777")


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
