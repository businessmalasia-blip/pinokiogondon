"""Клиент Helius (RPC + DAS) с общим rate limiting для всех запросов."""

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
        rpc_url: str,
        rate_limit: float,
    ) -> None:
        self._session = session
        self._rpc_url = rpc_url
        self._rate_limit = rate_limit
        self._lock = asyncio.Lock()
        self._last_request_at = 0.0
        self._id_counter = itertools.count(1)
        # None — ещё не знаем; False — тариф Helius отверг batch, ходим одиночными
        self._batch_supported: Optional[bool] = None
        # Скользящее окно 24ч для подсчёта кредитов Helius
        self._call_timestamps: deque = deque()

    def _record_calls(self, count: int = 1) -> None:
        """Фиксирует count RPC-вызовов для статистики расхода кредитов."""
        now = time.time()
        for _ in range(count):
            self._call_timestamps.append(now)
        cutoff = now - 86400
        while self._call_timestamps and self._call_timestamps[0] < cutoff:
            self._call_timestamps.popleft()

    def calls_last_24h(self) -> int:
        """Количество RPC-вызовов к Helius за последние 24 часа."""
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

    async def request(self, method: str, params: Any) -> Any:
        for attempt in range(2):  # максимум 1 повтор при 429
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

    async def _sequential_fallback(self, requests: list[tuple[str, Any]]) -> list[Any]:
        """Одиночные вызовы через общий rate limiter (пауза перед каждым)."""
        return [await self.request(method, params) for method, params in requests]

    async def batch_request(self, requests: list[tuple[str, Any]]) -> list[Any]:
        """JSON-RPC batch: несколько вызовов в одном HTTP-запросе.

        Проходит через тот же rate limiter одной паузой на весь батч.
        Бесплатный тариф Helius отвергает батчи ("max usage reached") —
        в этом случае клиент запоминает это и дальше ходит одиночными
        запросами с обычным rate limiting.
        Возвращает результаты в порядке запросов (None для ошибочных).
        """
        if not requests:
            return []
        if self._batch_supported is False:
            return await self._sequential_fallback(requests)

        payload = [
            {"jsonrpc": "2.0", "id": i, "method": method, "params": params}
            for i, (method, params) in enumerate(requests)
        ]
        for attempt in range(2):  # максимум 1 повтор при 429
            self._record_calls(len(requests))
            await self._throttle()
            try:
                async with self._session.post(
                    self._rpc_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 429:
                        if attempt == 0:
                            log.warning(
                                "Helius batch (%d вызовов): 429 — повтор",
                                len(requests),
                            )
                            continue
                        log.warning(
                            "Helius batch (%d вызовов): SKIPPED due to 429",
                            len(requests),
                        )
                        return [None] * len(requests)
                    data = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.warning("Helius batch (%d вызовов): сетевая ошибка: %s", len(requests), exc)
                return [None] * len(requests)

            if isinstance(data, dict):
                # Сервер ответил одним объектом-ошибкой — батчи на тарифе запрещены
                log.info(
                    "Helius batch отклонён тарифом (%s) — переключаюсь на одиночные запросы",
                    (data.get("error") or {}).get("message", "?"),
                )
                self._batch_supported = False
                return await self._sequential_fallback(requests)
            if not isinstance(data, list):
                log.warning("Helius batch: неожиданный ответ: %s", str(data)[:200])
                return [None] * len(requests)

            self._batch_supported = True
            by_id = {item.get("id"): item for item in data if isinstance(item, dict)}
            results: list[Any] = []
            for i in range(len(requests)):
                item = by_id.get(i, {})
                if "error" in item:
                    log.warning("Helius batch #%d: ошибка RPC: %s", i, item["error"])
                results.append(item.get("result"))
            return results
        return [None] * len(requests)

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

    # ----- DAS-методы -----

    async def get_asset(self, mint: str) -> Optional[dict]:
        """Метаданные токена (название, тикер, картинка) через DAS getAsset."""
        return await self.request("getAsset", {"id": mint})
