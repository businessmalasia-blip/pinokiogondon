"""Счётчики работы бота и учёт исходов алертов/отсевов (хранение в Redis)."""

import json
import time
from typing import Optional

from redis.asyncio import Redis

COUNTERS_KEY = "stats:counters"
ALERTS_LOG_KEY = "alerts:log"
OUTCOME_PENDING_KEY = "outcome:pending"
GRADCHECK_PENDING_KEY = "gradcheck:pending"

ALERTS_LOG_MAX = 200
GRADCHECK_PENDING_MAX = 5000

# Окна проверки исходов алертов: (метка, задержка в секундах)
OUTCOME_WINDOWS = (("1h", 3600), ("6h", 21600))
# Задержка проверки градуации для отсеянных/прошедших токенов
GRADCHECK_DELAY = 86400

FILTER_NAMES = ("concentration", "score", "passed")


class Stats:
    """Копит счётчики в памяти и периодически сбрасывает их в Redis,
    чтобы не писать в Redis на каждое событие WebSocket."""

    def __init__(self, redis_client: Redis) -> None:
        self.redis = redis_client
        self.started_at = time.time()
        self.last_event_at: Optional[float] = None
        self._pending: dict[str, int] = {}

    # ----- Счётчики -----

    def bump(self, field: str, n: int = 1) -> None:
        self._pending[field] = self._pending.get(field, 0) + n

    def mark_event(self) -> None:
        self.last_event_at = time.time()

    async def flush(self) -> None:
        if not self._pending:
            return
        pending, self._pending = self._pending, {}
        pipe = self.redis.pipeline()
        for field, n in pending.items():
            pipe.hincrby(COUNTERS_KEY, field, n)
        await pipe.execute()

    async def counters(self) -> dict[str, int]:
        data = await self.redis.hgetall(COUNTERS_KEY)
        result = {key: int(value) for key, value in data.items()}
        for field, n in self._pending.items():
            result[field] = result.get(field, 0) + n
        return result

    # ----- Алерты и их исходы -----

    async def record_alert(
        self, mint: str, name: str, symbol: str, market_cap: float, price: float
    ) -> None:
        now = time.time()
        record = json.dumps(
            {"mint": mint, "name": name, "symbol": symbol, "ts": now,
             "mc": market_cap, "price": price},
            ensure_ascii=False,
        )
        pipe = self.redis.pipeline()
        pipe.zadd(ALERTS_LOG_KEY, {record: now})
        pipe.zremrangebyrank(ALERTS_LOG_KEY, 0, -(ALERTS_LOG_MAX + 1))
        for label, delay in OUTCOME_WINDOWS:
            member = f"{label}|{now}|{mint}|{price}"
            pipe.zadd(OUTCOME_PENDING_KEY, {member: now + delay})
        await pipe.execute()
        self.bump("alerts_sent")

    async def last_alerts(self, count: int = 5) -> list[dict]:
        raw = await self.redis.zrevrange(ALERTS_LOG_KEY, 0, count - 1)
        alerts = []
        for item in raw:
            try:
                alerts.append(json.loads(item))
            except json.JSONDecodeError:
                continue
        return alerts

    # ----- Работа фильтров -----

    async def record_filter_result(self, mint: str, filter_name: str) -> None:
        """Фиксирует вердикт фильтра ("passed" — прошёл все) и ставит токен
        в очередь на проверку градуации через 24 часа."""
        self.bump(f"filtered:{filter_name}")
        if await self.redis.zcard(GRADCHECK_PENDING_KEY) < GRADCHECK_PENDING_MAX:
            await self.redis.zadd(
                GRADCHECK_PENDING_KEY,
                {f"{filter_name}|{mint}": time.time() + GRADCHECK_DELAY},
            )
