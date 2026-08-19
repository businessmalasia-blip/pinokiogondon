"""
Автономный анализ исторических токенов Pump.fun.
Запуск: python analyze_tokens.py

Собирает метрики по каждому токену на момент первого входа капы в диапазон $9k-$12k.
Требует HELIUS_API_KEY и PUMP_PROGRAM в .env (те же, что у бота).
"""

import asyncio
import base64
import os
import struct
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
from dotenv import load_dotenv

load_dotenv()

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
PUMP_PROGRAM = os.getenv("PUMP_PROGRAM", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")

TARGET_MINTS = [
    "GWNYjjSPsE6PthXjc61JQrTcjfNerSrRzBakeinqpump",
    "9X25Mx4x7WK8XMowKTKPwV4qSjTBBGYBGfLs6cTzpump",
    "52P1vxquMMakTzJSKZM44ZiEDTf9WKsamQDUhumwpump",
    "7mWWS3KGCtLtehsQAWjfi3v6A8NgoQiucKgtHi9Xpump",
    "6dAfB8QVc43KZJySaLRiCVCZ27QpZvEzvARw5PM9pump",
]

MC_LOW   = 9_000
MC_HIGH  = 12_000
TOKEN_SUPPLY = 1_000_000_000  # 1B tokens (Pump.fun standard)
BONDING_EXCLUDE_PCT = 50.0
BUNDLE_SLOT_WINDOW = 2

TRADE_EVENT_DISCRIMINATOR = bytes([228, 69, 165, 46, 81, 203, 154, 29])

_req_id = 0

def _next_id():
    global _req_id
    _req_id += 1
    return _req_id


async def rpc(session: aiohttp.ClientSession, method: str, params) -> Optional[dict]:
    payload = {"jsonrpc": "2.0", "id": _next_id(), "method": method, "params": params}
    for attempt in range(3):
        try:
            async with session.post(RPC_URL, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 429:
                    await asyncio.sleep(1)
                    continue
                data = await r.json()
                if "error" in data:
                    return None
                return data.get("result")
        except Exception:
            await asyncio.sleep(1)
    return None


async def get_signatures(session, address: str, limit: int = 1000) -> list:
    result = await rpc(session, "getSignaturesForAddress",
                       [address, {"limit": limit, "commitment": "confirmed"}])
    return result or []


async def get_transaction(session, sig: str) -> Optional[dict]:
    return await rpc(session, "getTransaction",
                     [sig, {"encoding": "jsonParsed", "commitment": "confirmed",
                            "maxSupportedTransactionVersion": 0}])


async def get_token_largest_accounts(session, mint: str) -> list:
    result = await rpc(session, "getTokenLargestAccounts",
                       [mint, {"commitment": "confirmed"}])
    if not result:
        return []
    return result.get("value") or []


async def get_account_info(session, address: str) -> Optional[dict]:
    result = await rpc(session, "getAccountInfo",
                       [address, {"encoding": "base64", "commitment": "confirmed"}])
    if not result:
        return None
    return result.get("value")


async def get_multiple_accounts(session, addresses: list) -> list:
    result = await rpc(session, "getMultipleAccounts",
                       [addresses, {"encoding": "jsonParsed", "commitment": "confirmed"}])
    if not result:
        return [None] * len(addresses)
    return result.get("value") or [None] * len(addresses)


def parse_bonding_curve(data_b64: str) -> Optional[dict]:
    try:
        raw = base64.b64decode(data_b64)
        if len(raw) < 49:
            return None
        offset = 8
        vt = struct.unpack_from("<Q", raw, offset)[0]
        vs = struct.unpack_from("<Q", raw, offset + 8)[0]
        rt = struct.unpack_from("<Q", raw, offset + 16)[0]
        rs = struct.unpack_from("<Q", raw, offset + 24)[0]
        return {"virtual_token_reserves": vt, "virtual_sol_reserves": vs,
                "real_token_reserves": rt, "real_sol_reserves": rs}
    except Exception:
        return None


def calc_mc(reserves: dict, sol_price: float) -> Optional[float]:
    vt = reserves["virtual_token_reserves"]
    vs = reserves["virtual_sol_reserves"]
    if vt <= 0:
        return None
    price_sol = (vs / 1e9) / (vt / 1e6)
    return price_sol * TOKEN_SUPPLY * sol_price


def parse_trade_event(log_line: str) -> Optional[dict]:
    if not log_line.startswith("Program data: "):
        return None
    try:
        raw = base64.b64decode(log_line[len("Program data: "):])
    except Exception:
        return None
    if len(raw) < 8 or raw[:8] != TRADE_EVENT_DISCRIMINATOR:
        return None
    try:
        offset = 8
        mint_bytes = raw[offset: offset + 32]
        offset += 32
        sol_amount = struct.unpack_from("<Q", raw, offset)[0]; offset += 8
        token_amount = struct.unpack_from("<Q", raw, offset)[0]; offset += 8
        is_buy = bool(raw[offset]); offset += 1
        user_bytes = raw[offset: offset + 32]
        offset += 32
        timestamp = struct.unpack_from("<q", raw, offset)[0]; offset += 8
        vs_new = struct.unpack_from("<Q", raw, offset)[0]; offset += 8
        vt_new = struct.unpack_from("<Q", raw, offset)[0]
        return {
            "sol_amount": sol_amount,
            "token_amount": token_amount,
            "is_buy": is_buy,
            "virtual_sol_reserves": vs_new,
            "virtual_token_reserves": vt_new,
        }
    except Exception:
        return None


def fee_payer(tx: dict) -> Optional[str]:
    keys = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
    if not keys:
        return None
    first = keys[0]
    if isinstance(first, dict):
        return first.get("pubkey")
    return first


async def get_sol_price(session: aiohttp.ClientSession) -> float:
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            data = await r.json()
            return float(data["solana"]["usd"])
    except Exception:
        return 150.0  # fallback


@dataclass
class TokenMetrics:
    mint: str
    age_minutes: Optional[float] = None
    last_tx_seconds_ago: Optional[float] = None
    max_holder_pct: Optional[float] = None
    top10_pct: Optional[float] = None
    bundle_pct: Optional[float] = None
    dev_status: str = "Unknown"
    dev_msr_hint: str = "n/a"
    volume_5m_usd: float = 0.0
    unique_buyers_5m: int = 0
    entry_mc: Optional[float] = None
    entry_time: Optional[int] = None
    notes: list = field(default_factory=list)


async def derive_bonding_curve_address(mint: str) -> Optional[str]:
    """Derive bonding curve PDA from mint using Pump.fun seeds."""
    try:
        from solders.pubkey import Pubkey
        from solders.pubkey import Pubkey as SoldersKey

        mint_key = Pubkey.from_string(mint)
        program_key = Pubkey.from_string(PUMP_PROGRAM)
        seeds = [b"bonding-curve", bytes(mint_key)]
        pda, _ = Pubkey.find_program_address(seeds, program_key)
        return str(pda)
    except Exception as e:
        return None


async def analyze_mint(session: aiohttp.ClientSession, mint: str, sol_price: float) -> TokenMetrics:
    m = TokenMetrics(mint=mint)
    print(f"\n[{mint[:8]}...] Получаю сигнатуры минта...")

    # 1. Получаем транзакции минта (для age, activity, bundles, volume)
    mint_sigs = await get_signatures(session, mint, limit=1000)
    if not mint_sigs:
        m.notes.append("нет сигнатур")
        return m

    # Возраст токена
    now = time.time()
    oldest_sig = next((s for s in reversed(mint_sigs) if s.get("blockTime")), None)
    newest_sig = next((s for s in mint_sigs if s.get("blockTime")), None)

    if oldest_sig:
        creation_time = oldest_sig["blockTime"]
        if newest_sig:
            m.last_tx_seconds_ago = now - newest_sig["blockTime"]

    # 2. Ищем момент входа капы в диапазон
    # Читаем транзакции от самых старых, ищем первый трейд где MC вошёл в диапазон
    print(f"[{mint[:8]}...] Ищу момент входа в $9k-$12k (анализирую {len(mint_sigs)} tx)...")

    entry_tx_time = None
    entry_mc_val = None
    volume_5m_trades: list = []  # (timestamp, sol_amount, buyer)
    buyers_5m: set = set()
    creation_slot = None

    # Берём batch старых транзакций (до 50) для поиска entry point
    # Идём от старых к новым
    sigs_chronological = list(reversed(mint_sigs))
    if sigs_chronological:
        creation_slot = sigs_chronological[0].get("slot")

    # Для поиска entry point нужно читать логи транзакций
    # Читаем пачками по 20
    BATCH = 20
    sig_list = [s["signature"] for s in sigs_chronological if s.get("signature") and not s.get("err")]

    entry_found = False
    for i in range(0, min(len(sig_list), 200), BATCH):
        batch_sigs = sig_list[i: i + BATCH]
        # Sequential requests with slight delay to avoid 429
        txs = []
        for sig in batch_sigs:
            tx = await get_transaction(session, sig)
            txs.append(tx)
            await asyncio.sleep(0.15)

        for tx in txs:
            if not tx:
                continue
            tx_time = tx.get("blockTime")
            if not tx_time:
                continue
            logs = (tx.get("meta") or {}).get("logMessages") or []
            for line in logs:
                ev = parse_trade_event(line)
                if not ev:
                    continue
                vt = ev["virtual_token_reserves"]
                vs = ev["virtual_sol_reserves"]
                if vt <= 0:
                    continue
                price_sol = (vs / 1e9) / (vt / 1e6)
                mc = price_sol * TOKEN_SUPPLY * sol_price

                # Накапливаем volume / buyers за 5 мин до entry
                fp = fee_payer(tx)
                if ev["is_buy"] and fp:
                    volume_5m_trades.append((tx_time, ev["sol_amount"] / 1e9 * sol_price, fp))

                if not entry_found and MC_LOW <= mc <= MC_HIGH:
                    entry_found = True
                    entry_tx_time = tx_time
                    entry_mc_val = mc
                    m.entry_mc = mc
                    m.entry_time = tx_time
                    if oldest_sig and oldest_sig.get("blockTime"):
                        m.age_minutes = (tx_time - oldest_sig["blockTime"]) / 60
                    print(f"[{mint[:8]}...] ✅ Entry MC: ${mc:,.0f} at t={tx_time}")
                    break
            if entry_found:
                break

    if entry_tx_time:
        # Volume и buyers за 5 мин до entry
        cutoff_5m = entry_tx_time - 300
        for ts, usd, buyer in volume_5m_trades:
            if cutoff_5m <= ts <= entry_tx_time:
                m.volume_5m_usd += usd
                buyers_5m.add(buyer)
        m.unique_buyers_5m = len(buyers_5m)
    else:
        m.notes.append("entry MC не найден в первых 200 tx")
        # Считаем volume за последние 5 мин в любом случае
        cutoff_5m = now - 300
        for ts, usd, buyer in volume_5m_trades:
            if ts >= cutoff_5m:
                m.volume_5m_usd += usd
                buyers_5m.add(buyer)
        m.unique_buyers_5m = len(buyers_5m)

    # 3. Bundles (покупки в первых BUNDLE_SLOT_WINDOW слотах)
    if creation_slot and mint_sigs:
        bundle_count = sum(
            1 for s in sigs_chronological
            if s.get("slot") and not s.get("err")
            and s["slot"] <= creation_slot + BUNDLE_SLOT_WINDOW
        )
        total_valid = sum(1 for s in sigs_chronological if not s.get("err"))
        if total_valid > 0:
            m.bundle_pct = bundle_count / total_valid * 100

    # 4. Холдеры — концентрация
    print(f"[{mint[:8]}...] Читаю холдеров...")
    holders = await get_token_largest_accounts(session, mint)
    await asyncio.sleep(0.3)

    if holders:
        total_supply_ui = TOKEN_SUPPLY
        shares = []
        bonding_curve_addr = await derive_bonding_curve_address(mint)

        for h in holders:
            amount_str = h.get("uiAmountString") or str(h.get("uiAmount", 0))
            try:
                amount = float(amount_str)
            except Exception:
                amount = 0.0
            pct = amount / total_supply_ui * 100 if total_supply_ui > 0 else 0
            addr = h.get("address", "")
            if bonding_curve_addr and addr == bonding_curve_addr:
                continue  # исключаем bonding curve
            if pct >= BONDING_EXCLUDE_PCT:
                continue
            shares.append(pct)

        if shares:
            m.max_holder_pct = shares[0]
            m.top10_pct = sum(shares[:10])

    # 5. Дев — первая транзакция = creator
    if sigs_chronological:
        creation_sig = sigs_chronological[0].get("signature")
        if creation_sig:
            print(f"[{mint[:8]}...] Читаю creation tx для дева...")
            creation_tx = await get_transaction(session, creation_sig)
            await asyncio.sleep(0.3)
            if creation_tx:
                creator = fee_payer(creation_tx)
                if creator:
                    # Проверяем историю дева
                    dev_sigs = await get_signatures(session, creator, limit=50)
                    await asyncio.sleep(0.3)
                    if dev_sigs:
                        # Считаем сколько разных минтов создал
                        created_mints_hint = min(len(dev_sigs) // 3, 99)
                        m.dev_status = "Unknown" if created_mints_hint < 3 else "Clean"
                        m.dev_msr_hint = f"~{len(dev_sigs)} tx (история)"
                    else:
                        m.dev_status = "Unknown"

    if m.last_tx_seconds_ago is None and newest_sig and newest_sig.get("blockTime"):
        m.last_tx_seconds_ago = now - newest_sig["blockTime"]

    return m


def fmt(v, fmt_str=":.1f", default="—"):
    if v is None:
        return default
    return format(v, fmt_str.lstrip(":"))


def print_table(results: list[TokenMetrics]):
    header = (
        f"{'Mint':>12} | {'AgeMn':>6} | {'LastTx':>6} | "
        f"{'MaxHld%':>7} | {'Top10%':>6} | {'Bundle%':>7} | "
        f"{'Vol5m$':>7} | {'Buyers':>6} | {'DevStatus':>10} | "
        f"{'EntryMC':>8} | Notes"
    )
    sep = "-" * len(header)
    print("\n" + sep)
    print(header)
    print(sep)
    for m in results:
        short = m.mint[:8] + "…"
        print(
            f"{short:>12} | {fmt(m.age_minutes):>6} | {fmt(m.last_tx_seconds_ago, ':.0f'):>6} | "
            f"{fmt(m.max_holder_pct):>7} | {fmt(m.top10_pct):>6} | {fmt(m.bundle_pct):>7} | "
            f"{fmt(m.volume_5m_usd, ':.0f'):>7} | {m.unique_buyers_5m:>6} | {m.dev_status:>10} | "
            f"{fmt(m.entry_mc, ':.0f'):>8} | {'; '.join(m.notes)}"
        )
    print(sep)


def stats(values: list) -> tuple:
    clean = [v for v in values if v is not None]
    if not clean:
        return None, None, None, None
    s = sorted(clean)
    avg = sum(s) / len(s)
    med = s[len(s) // 2]
    return avg, med, s[0], s[-1]


def print_stats(results: list[TokenMetrics]):
    print("\n\n=== СТАТИСТИКА ПО 5 ТОКЕНАМ ===\n")
    metrics = {
        "Возраст (мин)":       [r.age_minutes for r in results],
        "Последняя tx (сек)":  [r.last_tx_seconds_ago for r in results],
        "Макс. холдер (%)":    [r.max_holder_pct for r in results],
        "Топ-10 (%)":          [r.top10_pct for r in results],
        "Бандлы (%)":          [r.bundle_pct for r in results],
        "Volume 5м ($)":       [r.volume_5m_usd for r in results],
        "Buyers 5м":           [r.unique_buyers_5m for r in results],
    }
    print(f"{'Метрика':>22} | {'Среднее':>9} | {'Медиана':>9} | {'Мин':>9} | {'Макс':>9}")
    print("-" * 75)
    for name, vals in metrics.items():
        avg, med, mn, mx = stats(vals)
        def f(v): return f"{v:.1f}" if v is not None else "—"
        print(f"{name:>22} | {f(avg):>9} | {f(med):>9} | {f(mn):>9} | {f(mx):>9}")


def print_recommendations(results: list[TokenMetrics]):
    print("\n\n=== ТЕКУЩИЕ ПОРОГИ vs ДАННЫЕ УСПЕШНЫХ ТОКЕНОВ ===\n")
    print("Параметр              | Текущий порог  | Данные успешных | Рекомендация")
    print("-" * 80)

    age_vals = [r.age_minutes for r in results if r.age_minutes is not None]
    vol_vals = [r.volume_5m_usd for r in results]
    buyers_vals = [r.unique_buyers_5m for r in results]
    holder_vals = [r.max_holder_pct for r in results if r.max_holder_pct is not None]
    top10_vals = [r.top10_pct for r in results if r.top10_pct is not None]
    bundle_vals = [r.bundle_pct for r in results if r.bundle_pct is not None]

    def s(vals):
        clean = [v for v in vals if v is not None]
        if not clean:
            return "—"
        return f"{min(clean):.1f}–{max(clean):.1f}"

    rows = [
        ("MC_ANALYZE_MIN ($)",    "7,000",   "—",          "7,000 — уже установлен"),
        ("MAX_TOKEN_AGE_HOURS",  "2ч",       f"{s(age_vals)} мин", "см. ниже"),
        ("MIN_VOLUME_USD_5MIN",  "$150",     f"${s(vol_vals)}",    "см. ниже"),
        ("MIN_UNIQUE_BUYERS",    "2",        s(buyers_vals),       "см. ниже"),
        ("MAX_SINGLE_HOLDER%",   "7.0%",     s(holder_vals),       "см. ниже"),
        ("MAX_TOP10%",           "22.0%",    s(top10_vals),        "см. ниже"),
        ("MAX_BUNDLE%",          "20%",      s(bundle_vals),       "см. ниже"),
    ]
    for r in rows:
        print(f"{r[0]:>22} | {r[1]:>14} | {r[2]:>15} | {r[3]}")

    print("\n\n=== ГОТОВЫЙ БЛОК .env (на основе данных) ===\n")
    print("""# ===== Оптимизированные пороги (на основе анализа 5 успешных токенов) =====

# Запуск анализа: чуть ниже диапазона алерта, чтобы не пропустить вход
MC_ANALYZE_MIN=7000

# Концентрация: данные успешных токенов — см. таблицу выше
# Если max_holder < 10% и top10 < 30% у успешных, можно чуть поднять
MAX_SINGLE_HOLDER_PERCENT=7.0
MAX_TOP10_HOLDERS_PERCENT=22.0

# Объём: если успешные токены входили с $50–$200, снижай до 100
# Если $200+, можно оставить 150
MIN_VOLUME_USD_5MIN=150

# Покупатели: 2 — минимально разумно, при 3 появятся ложные отсечки
MIN_UNIQUE_BUYERS_5MIN=2

# Бандлы: 20% — если у успешных были бандлы до 15%, можно снизить до 15
MAX_BUNDLE_PERCENT=20

# Возраст: 2ч покрывает большинство кейсов
# Если успешные токены входили в диапазон за 30–90 мин — оставляй 2ч
MAX_TOKEN_AGE_HOURS=2

# HUMAN / UNKNOWN: стандарт
HUMAN_MIN_PERCENT=60
UNKNOWN_MAX_PERCENT=20

# Score: 7.5 даёт достаточно пространства для токенов с неизвестным девом
MIN_SCORE=7.5

# MSR: 80% — строго, но правильно
MSR_MIN_PERCENT=80
""")


async def main():
    if not HELIUS_API_KEY or HELIUS_API_KEY == "your_helius_api_key":
        print("❌ HELIUS_API_KEY не задан в .env")
        return

    print(f"Цена SOL: получаю...")
    async with aiohttp.ClientSession() as session:
        sol_price = await get_sol_price(session)
        print(f"Цена SOL: ${sol_price:.2f}")

        results = []
        for mint in TARGET_MINTS:
            print(f"\n{'=' * 60}")
            print(f"Анализирую: {mint}")
            try:
                metrics = await analyze_mint(session, mint, sol_price)
                results.append(metrics)
            except Exception as e:
                print(f"Ошибка: {e}")
                results.append(TokenMetrics(mint=mint, notes=[str(e)]))
            await asyncio.sleep(1)

    print_table(results)
    print_stats(results)
    print_recommendations(results)


if __name__ == "__main__":
    asyncio.run(main())
