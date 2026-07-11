"""Клиент Helius (RPC + DAS) с общим rate limiting для всех запросов."""

import asyncio
import itertools
import logging
import time
from typing import Any, Optional

import aiohttp

log = logging.getLogger(__name__)


class HeliusClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        rpc_url: str,
        rate_limit: float,
    ) -> None:
        self._session = session
        self._rpc_url = rpc_url
        self._rate_limit = rate_limit
        self._lock = asyncio.Lock()
        self._last_request_at = 0.0
        self._id_counter = itertools.count(1)

    async def _throttle(self) -> None:
        """Гарантирует паузу между любыми запросами к Helius."""
        async with self._lock:
            now = time.monotonic()
            wait = self._last_request_at + self._rate_limit - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_at = time.monotonic()

    async def request(self, method: str, params: Any) -> Any:
        await self._throttle()
        payload = {
            "jsonrpc": "2.0",
            "id": next(self._id_counter),
            "method": method,
            "params": params,
        }
        try:
            async with self._session.post(
                self._rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.warning("Helius %s: сетевая ошибка: %s", method, exc)
            return None
        if "error" in data:
            log.warning("Helius %s: ошибка RPC: %s", method, data["error"])
            return None
        return data.get("result")

    # ----- Стандартные RPC-методы -----

    async def get_account_info(self, address: str) -> Optional[dict]:
        """Свежие данные аккаунта: lamports и data (base64)."""
        result = await self.request(
            "getAccountInfo", [address, {"encoding": "base64", "commitment": "confirmed"}]
        )
        if not result:
            return None
        return result.get("value")

    async def get_signatures(self, address: str, limit: int = 1) -> list[dict]:
        result = await self.request(
            "getSignaturesForAddress",
            [address, {"limit": limit, "commitment": "confirmed"}],
        )
        return result or []

    async def get_transaction(self, signature: str) -> Optional[dict]:
        return await self.request(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0,
                },
            ],
        )

    async def get_token_supply(self, mint: str) -> Optional[tuple[int, int]]:
        """Полный supply токена: (amount в сырых единицах, decimals)."""
        result = await self.request("getTokenSupply", [mint])
        if not result or not result.get("value"):
            return None
        value = result["value"]
        try:
            return int(value["amount"]), int(value["decimals"])
        except (KeyError, ValueError):
            return None

    # ----- DAS-методы -----

    async def get_token_accounts(self, mint: str, limit: int = 100) -> list[dict]:
        """Топ токен-аккаунтов через Helius DAS getTokenAccounts (POST)."""
        result = await self.request(
            "getTokenAccounts",
            {"mint": mint, "limit": limit, "page": 1, "options": {"showZeroBalance": False}},
        )
        if not result:
            return []
        return result.get("token_accounts", [])

    async def get_asset(self, mint: str) -> Optional[dict]:
        """Метаданные токена (название, тикер, картинка) через DAS getAsset."""
        return await self.request("getAsset", {"id": mint})
