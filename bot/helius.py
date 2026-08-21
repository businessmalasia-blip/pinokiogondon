"""Клиент Helius (DAS) + публичный Solana RPC для стандартных методов.

Helius тратит кредиты на каждый запрос. Стандартные RPC-методы
(getAccountInfo, getSignaturesForAddress, getTransaction и т.п.) работают
на любом Solana-узле, поэтому направляем их на бесплатный публичный RPC.
Helius используется только для DAS (getAsset) — это единственный метод,
требующий именно Helius-узла.
"""

import asyncio
import itertools
import logging
import time
from collections import deque
from typing import Any, Optional

import aiohttp

log = logging.getLogger(__name__)


class HeliusClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        rpc_url: str,           # Helius — только для DAS (getAsset)
        public_rpc_url: str,    # Публичный Solana RPC — для стандартных методов
        rate_limit: float,
    ) -> None:
        self._session = session
        self._rpc_url = rpc_url
        self._public_url = public_rpc_url
        self._rate_limit = rate_limit
        self._lock = asyncio.Lock()
        self._last_request_at = 0.0
        self._id_counter = itertools.count(1)
        # Скользящее окно 24ч для подсчёта кредитов Helius (только DAS-вызовы)
        self._call_timestamps: deque = deque()

    def _record_calls(self, count: int = 1) -> None:
        """Фиксирует count Helius DAS-вызовов для статистики расхода кредитов."""
        now = time.time()
        for _ in range(count):
            self._call_timestamps.append(now)
        cutoff = now - 86400
        while self._call_timestamps and self._call_timestamps[0] < cutoff:
            self._call_timestamps.popleft()

    def calls_last_24h(self) -> int:
        """Количество Helius DAS-вызовов за последние 24 часа."""
        cutoff = time.time() - 86400
        count = 0
        for t in reversed(self._call_timestamps):
            if t < cutoff:
                break
            count += 1
        return count

    async def _throttle(self) -> None:
        """Гарантирует паузу между запросами к Helius."""
        async with self._lock:
            now = time.monotonic()
            wait = self._last_request_at + self._rate_limit - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_at = time.monotonic()

    # ----- Helius DAS (rate-limited, credit-tracked) -----

    async def request(self, method: str, params: Any) -> Any:
        """Запрос к Helius — только для DAS-методов (getAsset и т.п.)."""
        for attempt in range(2):
            await self._throttle()
            self._record_calls(1)
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
                    if resp.status == 429:
                        if attempt == 0:
                            log.warning("Helius %s: 429 — повтор", method)
                            continue
                        log.warning("Helius %s: SKIPPED due to 429", method)
                        return None
                    data = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.warning("Helius %s: сетевая ошибка: %s", method, exc)
                return None
            if not isinstance(data, dict):
                return None
            if "error" in data:
                log.warning("Helius %s: ошибка RPC: %s", method, data["error"])
                return None
            return data.get("result")
        return None

    # ----- Публичный Solana RPC (без кредитов Helius, без rate limiting) -----

    async def _pub_request(self, method: str, params: Any) -> Any:
        """Запрос к публичному Solana RPC — не расходует кредиты Helius."""
        for attempt in range(2):
            payload = {
                "jsonrpc": "2.0",
                "id": next(self._id_counter),
                "method": method,
                "params": params,
            }
            try:
                async with self._session.post(
                    self._public_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status == 429:
                        if attempt == 0:
                            await asyncio.sleep(1)
                            continue
                        log.warning("PublicRPC %s: SKIPPED due to 429", method)
                        return None
                    data = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.warning("PublicRPC %s: сетевая ошибка: %s", method, exc)
                return None
            if not isinstance(data, dict):
                return None
            if "error" in data:
                log.warning("PublicRPC %s: ошибка RPC: %s", method, data["error"])
                return None
            return data.get("result")
        return None

    async def _pub_batch_request(self, requests: list[tuple[str, Any]]) -> list[Any]:
        """JSON-RPC batch к публичному Solana RPC."""
        if not requests:
            return []
        payload = [
            {"jsonrpc": "2.0", "id": i, "method": method, "params": params}
            for i, (method, params) in enumerate(requests)
        ]
        for attempt in range(2):
            try:
                async with self._session.post(
                    self._public_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 429:
                        if attempt == 0:
                            await asyncio.sleep(1)
                            continue
                        return [None] * len(requests)
                    data = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.warning("PublicRPC batch (%d): сетевая ошибка: %s", len(requests), exc)
                return [None] * len(requests)

            if isinstance(data, dict):
                # Публичный RPC не поддерживает батч — переходим на одиночные
                log.info("PublicRPC batch отклонён — переключаюсь на одиночные запросы")
                return [await self._pub_request(method, params) for method, params in requests]
            if not isinstance(data, list):
                log.warning("PublicRPC batch: неожиданный ответ: %s", str(data)[:200])
                return [None] * len(requests)

            by_id = {item.get("id"): item for item in data if isinstance(item, dict)}
            results: list[Any] = []
            for i in range(len(requests)):
                item = by_id.get(i, {})
                if "error" in item:
                    log.warning("PublicRPC batch #%d: ошибка RPC: %s", i, item["error"])
                results.append(item.get("result"))
            return results
        return [None] * len(requests)

    # ----- Стандартные RPC-методы (через публичный RPC, без кредитов) -----

    async def get_account_info(self, address: str) -> Optional[dict]:
        """Свежие данные аккаунта: lamports и data (base64)."""
        result = await self._pub_request(
            "getAccountInfo", [address, {"encoding": "base64", "commitment": "confirmed"}]
        )
        if not result:
            return None
        return result.get("value")

    async def get_signatures(self, address: str, limit: int = 1) -> list[dict]:
        result = await self._pub_request(
            "getSignaturesForAddress",
            [address, {"limit": limit, "commitment": "confirmed"}],
        )
        return result or []

    async def get_signatures_batch(
        self, addresses: list[str], limit: int = 1
    ) -> dict[str, list]:
        """getSignaturesForAddress для нескольких адресов одним batch-запросом."""
        requests = [
            ("getSignaturesForAddress", [address, {"limit": limit, "commitment": "confirmed"}])
            for address in addresses
        ]
        results = await self._pub_batch_request(requests)
        return {address: (result or []) for address, result in zip(addresses, results)}

    async def get_transaction(self, signature: str) -> Optional[dict]:
        return await self._pub_request(
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

    async def get_token_largest_accounts(self, mint: str) -> list[dict]:
        """Топ-20 крупнейших токен-аккаунтов минта."""
        result = await self._pub_request(
            "getTokenLargestAccounts", [mint, {"commitment": "confirmed"}]
        )
        if not result:
            return []
        return result.get("value") or []

    async def get_accounts_owners(self, addresses: list[str]) -> dict[str, str]:
        """Владельцы токен-аккаунтов одним запросом getMultipleAccounts."""
        if not addresses:
            return {}
        result = await self._pub_request(
            "getMultipleAccounts",
            [addresses, {"encoding": "jsonParsed", "commitment": "confirmed"}],
        )
        owners: dict[str, str] = {}
        for address, value in zip(addresses, (result or {}).get("value") or []):
            try:
                owners[address] = value["data"]["parsed"]["info"]["owner"]
            except (TypeError, KeyError):
                continue
        return owners

    async def get_transactions_batch(
        self, signatures: list[str]
    ) -> list[Optional[dict]]:
        """getTransaction для нескольких сигнатур одним batch-запросом."""
        if not signatures:
            return []
        params_template = {
            "encoding": "jsonParsed",
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 0,
        }
        requests = [
            ("getTransaction", [sig, params_template]) for sig in signatures
        ]
        return await self._pub_batch_request(requests)

    # ----- DAS-методы (только через Helius, тратят кредиты) -----

    async def get_asset(self, mint: str) -> Optional[dict]:
        """Метаданные токена (название, тикер, картинка) через DAS getAsset."""
        return await self.request("getAsset", {"id": mint})
