"""Клиент Helius: стандартные Solana RPC-вызовы идут на публичный эндпоинт
(не тратят Helius-кредиты), DAS-вызовы (getAsset) — через Helius (тарифицируются)."""

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
        rpc_url: str,   # публичный Solana RPC — кредиты не тратятся
        das_url: str,   # Helius endpoint — только для DAS (getAsset)
        rate_limit: float,
    ) -> None:
        self._session = session
        self._rpc_url = rpc_url
        self._das_url = das_url
        self._rate_limit = rate_limit
        self._lock = asyncio.Lock()
        self._last_das_at = 0.0
        self._id_counter = itertools.count(1)
        # None — ещё не знаем; False — публичный RPC не поддерживает batch
        self._batch_supported: Optional[bool] = None
        # Скользящее окно 24ч: считаем только DAS-вызовы (тарифицируемые Helius)
        self._das_timestamps: deque = deque()

    def _record_das_call(self, count: int = 1) -> None:
        """Фиксирует DAS-вызовы к Helius для статистики расхода кредитов."""
        now = time.time()
        for _ in range(count):
            self._das_timestamps.append(now)
        cutoff = now - 86400
        while self._das_timestamps and self._das_timestamps[0] < cutoff:
            self._das_timestamps.popleft()

    def calls_last_24h(self) -> int:
        """Количество DAS-вызовов к Helius за последние 24 часа (= расход кредитов)."""
        cutoff = time.time() - 86400
        count = 0
        for t in reversed(self._das_timestamps):
            if t < cutoff:
                break
            count += 1
        return count

    async def _throttle_das(self) -> None:
        """Пауза между DAS-запросами к Helius (rate limit)."""
        async with self._lock:
            now = time.monotonic()
            wait = self._last_das_at + self._rate_limit - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_das_at = time.monotonic()

    async def _post(self, url: str, payload: Any, timeout: float = 20) -> Any:
        """Низкоуровневый HTTP POST к любому RPC-эндпоинту."""
        try:
            async with self._session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.warning("RPC (%s) сетевая ошибка: %s", url.split("?")[0], exc)
            return None

    # ----- Стандартные RPC-методы → публичный Solana RPC (кредиты не тратятся) -----

    async def request(self, method: str, params: Any) -> Any:
        """Стандартный Solana RPC-вызов через публичный эндпоинт."""
        payload = {
            "jsonrpc": "2.0",
            "id": next(self._id_counter),
            "method": method,
            "params": params,
        }
        data = await self._post(self._rpc_url, payload)
        if data is None:
            return None
        if not isinstance(data, dict):
            log.warning("RPC %s: неожиданный ответ", method)
            return None
        if "error" in data:
            log.warning("RPC %s: ошибка: %s", method, data["error"])
            return None
        return data.get("result")

    async def _sequential_fallback(self, requests: list[tuple[str, Any]]) -> list[Any]:
        """Одиночные вызовы через публичный RPC (запасной путь при отсутствии batch)."""
        return [await self.request(method, params) for method, params in requests]

    async def batch_request(self, requests: list[tuple[str, Any]]) -> list[Any]:
        """JSON-RPC batch через публичный Solana RPC.

        Несколько вызовов в одном HTTP-запросе — экономит latency.
        Если публичный нод не поддерживает batch, переключается на одиночные.
        Кредиты Helius не расходуются.
        """
        if not requests:
            return []
        if self._batch_supported is False:
            return await self._sequential_fallback(requests)

        payload = [
            {"jsonrpc": "2.0", "id": i, "method": method, "params": params}
            for i, (method, params) in enumerate(requests)
        ]
        data = await self._post(self._rpc_url, payload, timeout=30)
        if data is None:
            return [None] * len(requests)

        if isinstance(data, dict):
            log.info(
                "Публичный RPC не поддерживает batch (%s) — одиночные запросы",
                (data.get("error") or {}).get("message", "?"),
            )
            self._batch_supported = False
            return await self._sequential_fallback(requests)
        if not isinstance(data, list):
            log.warning("Batch: неожиданный ответ: %s", str(data)[:200])
            return [None] * len(requests)

        self._batch_supported = True
        by_id = {item.get("id"): item for item in data if isinstance(item, dict)}
        results: list[Any] = []
        for i in range(len(requests)):
            item = by_id.get(i, {})
            if "error" in item:
                log.warning("Batch #%d: ошибка RPC: %s", i, item["error"])
            results.append(item.get("result"))
        return results

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

    async def get_signatures_batch(
        self, addresses: list[str], limit: int = 1
    ) -> dict[str, list]:
        """getSignaturesForAddress для нескольких адресов одним batch-запросом."""
        requests = [
            ("getSignaturesForAddress", [address, {"limit": limit, "commitment": "confirmed"}])
            for address in addresses
        ]
        results = await self.batch_request(requests)
        return {address: (result or []) for address, result in zip(addresses, results)}

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

    async def get_token_largest_accounts(self, mint: str) -> list[dict]:
        """Топ-20 крупнейших токен-аккаунтов минта."""
        result = await self.request(
            "getTokenLargestAccounts", [mint, {"commitment": "confirmed"}]
        )
        if not result:
            return []
        return result.get("value") or []

    async def get_accounts_owners(self, addresses: list[str]) -> dict[str, str]:
        """Владельцы токен-аккаунтов одним запросом getMultipleAccounts."""
        if not addresses:
            return {}
        result = await self.request(
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
        return await self.batch_request(requests)

    # ----- DAS-методы → Helius (тарифицируются, throttled) -----

    async def get_asset(self, mint: str) -> Optional[dict]:
        """Метаданные токена через DAS getAsset. Идёт через Helius — тратит кредиты."""
        await self._throttle_das()
        self._record_das_call(1)
        payload = {
            "jsonrpc": "2.0",
            "id": next(self._id_counter),
            "method": "getAsset",
            "params": {"id": mint},
        }
        data = await self._post(self._das_url, payload)
        if data is None:
            return None
        if not isinstance(data, dict):
            return None
        if "error" in data:
            log.warning("Helius DAS getAsset: ошибка: %s", data["error"])
            return None
        return data.get("result")
