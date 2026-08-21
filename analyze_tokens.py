"""
Анализ исторических токенов Pump.fun.
Запуск: python analyze_tokens.py

Данные берутся по адресу bonding curve (там вся активность).
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
# Цепочка бесплатных архивных RPC. Если один 429 — переключаемся на следующий.
ARCHIVE_RPCS = [
    "https://rpc.ankr.com/solana",
    "https://solana-rpc.publicnode.com",
    "https://api.mainnet-beta.solana.com",
]
_rpc_idx = 0  # текущий активный RPC в цепочке
RPC_URL_HELIUS = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
PUMP_PROGRAM   = os.getenv("PUMP_PROGRAM", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
SOL_PRICE_FALLBACK = 150.0

TARGET_MINTS = [
    "GWNYjjSPsE6PthXjc61JQrTcjfNerSrRzBakeinqpump",
    "9X25Mx4x7WK8XMowKTKPwV4qSjTBBGYBGfLs6cTzpump",
    "52P1vxquMMakTzJSKZM44ZiEDTf9WKsamQDUhumwpump",
    "7mWWS3KGCtLtehsQAWjfi3v6A8NgoQiucKgtHi9Xpump",
    "6dAfB8QVc43KZJySaLRiCVCZ27QpZvEzvARw5PM9pump",
]

MC_LOW             = 9_000
MC_HIGH            = 12_000
TOKEN_SUPPLY       = 1_000_000_000
BONDING_EXCLUDE    = 50.0
BUNDLE_SLOT_WINDOW = 2
TRADE_DISC         = bytes([228, 69, 165, 46, 81, 203, 154, 29])

_req_id = 0
def _nid():
    global _req_id; _req_id += 1; return _req_id


# ── RPC ─────────────────────────────────────────────────────────────────────

async def rpc(session: aiohttp.ClientSession, method: str, params,
              label="", url: str = None) -> Optional[object]:
    global _rpc_idx
    # Если url не задан — используем цепочку архивных RPC
    use_chain = url is None
    payload = {"jsonrpc": "2.0", "id": _nid(), "method": method, "params": params}
    for attempt in range(len(ARCHIVE_RPCS) * 2 + 2):
        target = ARCHIVE_RPCS[_rpc_idx % len(ARCHIVE_RPCS)] if use_chain else url
        try:
            async with session.post(target, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 429:
                    if use_chain:
                        old_idx = _rpc_idx % len(ARCHIVE_RPCS)
                        _rpc_idx += 1
                        new_idx = _rpc_idx % len(ARCHIVE_RPCS)
                        print(f"    [429→switch] {label or method}: "
                              f"{ARCHIVE_RPCS[old_idx].split('/')[2]} → "
                              f"{ARCHIVE_RPCS[new_idx].split('/')[2]}")
                        await asyncio.sleep(1)
                    else:
                        wait = min(2 ** attempt, 30)
                        print(f"    [429] {label or method} — жду {wait}с")
                        await asyncio.sleep(wait)
                    continue
                if r.status != 200:
                    print(f"    [HTTP {r.status}] {label or method} via {target.split('/')[2]}")
                    return None
                data = await r.json()
                if "error" in data:
                    print(f"    [RPC ERR] {label or method}: {data['error']}")
                    return None
                return data.get("result")
        except Exception as e:
            wait = min(2 ** (attempt // len(ARCHIVE_RPCS)), 10)
            print(f"    [EXC] {label or method}: {e} — жду {wait}с")
            await asyncio.sleep(wait)
            if use_chain:
                _rpc_idx += 1
    return None



async def get_sigs(session, address: str, limit: int = 1000) -> list:
    await asyncio.sleep(0.5)
    r = await rpc(session, "getSignaturesForAddress",
                  [address, {"limit": limit, "commitment": "confirmed"}],
                  label=f"getSigs({address[:8]})")  # цепочка RPC
    return r or []


async def get_tx(session, sig: str) -> Optional[dict]:
    return await rpc(session, "getTransaction",
                     [sig, {"encoding": "jsonParsed", "commitment": "confirmed",
                            "maxSupportedTransactionVersion": 0}],
                     label=f"getTx({sig[:8]})")  # цепочка RPC


async def get_largest(session, mint: str) -> list:
    r = await rpc(session, "getTokenLargestAccounts",
                  [mint, {"commitment": "confirmed"}], label="getLargest")  # цепочка
    return (r or {}).get("value") or []


async def get_asset(session, mint: str) -> Optional[dict]:
    # DAS — только Helius, публичных нод нет
    return await rpc(session, "getAsset", {"id": mint}, label="getAsset",
                     url=RPC_URL_HELIUS)


async def get_sol_price(session: aiohttp.ClientSession) -> float:
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
            timeout=aiohttp.ClientTimeout(total=10)) as r:
            d = await r.json()
            return float(d["solana"]["usd"])
    except Exception:
        print(f"  CoinGecko недоступен, используем fallback ${SOL_PRICE_FALLBACK}")
        return SOL_PRICE_FALLBACK


# ── Derive bonding curve PDA ─────────────────────────────────────────────────

def derive_bonding_curve(mint: str) -> Optional[str]:
    try:
        from solders.pubkey import Pubkey
        mint_key    = Pubkey.from_string(mint)
        program_key = Pubkey.from_string(PUMP_PROGRAM)
        seeds = [b"bonding-curve", bytes(mint_key)]
        pda, _ = Pubkey.find_program_address(seeds, program_key)
        return str(pda)
    except Exception as e:
        print(f"  PDA derive error: {e}")
        return None


# ── Parsers ──────────────────────────────────────────────────────────────────

def parse_trade(log_line: str) -> Optional[dict]:
    if not log_line.startswith("Program data: "):
        return None
    try:
        raw = base64.b64decode(log_line[14:])
    except Exception:
        return None
    if len(raw) < 8 or raw[:8] != TRADE_DISC:
        return None
    try:
        o = 8
        o += 32  # mint
        sol_amt   = struct.unpack_from("<Q", raw, o)[0]; o += 8
        tok_amt   = struct.unpack_from("<Q", raw, o)[0]; o += 8
        is_buy    = bool(raw[o]);                        o += 1
        o += 32  # user
        o += 8   # timestamp
        vs = struct.unpack_from("<Q", raw, o)[0]; o += 8
        vt = struct.unpack_from("<Q", raw, o)[0]
        return {"sol_amt": sol_amt, "tok_amt": tok_amt,
                "is_buy": is_buy, "vs": vs, "vt": vt}
    except Exception:
        return None


def mc_from_reserves(vs: int, vt: int, sol_price: float) -> Optional[float]:
    if vt <= 0:
        return None
    price_sol = (vs / 1e9) / (vt / 1e6)
    return price_sol * TOKEN_SUPPLY * sol_price


def fee_payer(tx: dict) -> Optional[str]:
    keys = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
    if not keys:
        return None
    f = keys[0]
    return f.get("pubkey") if isinstance(f, dict) else f


# ── Main analysis ─────────────────────────────────────────────────────────────

@dataclass
class Metrics:
    mint:               str
    bonding_curve:      Optional[str]  = None
    entry_mc:           Optional[float] = None
    age_minutes:        Optional[float] = None
    last_tx_sec:        Optional[float] = None
    max_holder_pct:     Optional[float] = None
    top10_pct:          Optional[float] = None
    bundle_pct:         Optional[float] = None
    vol_5m:             float           = 0.0
    buyers_5m:          int             = 0
    sell_ratio:         Optional[float] = None
    dev_addr:           Optional[str]   = None
    dev_status:         str             = "—"
    has_social:         bool            = False
    name:               str             = "—"
    symbol:             str             = "—"
    notes:              list            = field(default_factory=list)


async def analyze(session: aiohttp.ClientSession, mint: str, sol_price: float) -> Metrics:
    m = Metrics(mint=mint)
    short = mint[:8]
    print(f"\n  [{short}] Деривирую bonding curve...")

    bc = derive_bonding_curve(mint)
    m.bonding_curve = bc
    if not bc:
        m.notes.append("не удалось деривировать bonding curve")
        return m
    print(f"  [{short}] Bonding curve: {bc[:12]}...")

    # ── Metadata (DAS) ───────────────────────────────────────────────────────
    await asyncio.sleep(1.0)
    asset = await get_asset(session, mint)
    await asyncio.sleep(1.0)
    if asset:
        m.name   = asset.get("content", {}).get("metadata", {}).get("name",   "—")
        m.symbol = asset.get("content", {}).get("metadata", {}).get("symbol", "—")
        links    = asset.get("links") or {}
        m.has_social = bool(links.get("twitter") or links.get("website") or links.get("telegram"))
        print(f"  [{short}] Токен: {m.name} ({m.symbol}), соцсети: {m.has_social}")
    else:
        print(f"  [{short}] getAsset вернул None")

    # ── Сигнатуры с bonding curve (основная активность) ─────────────────────
    print(f"  [{short}] Запрашиваю сигнатуры bonding curve (limit=1000)...")
    bc_sigs = await get_sigs(session, bc, limit=1000)
    await asyncio.sleep(1.0)
    print(f"  [{short}] Сигнатур bonding curve: {len(bc_sigs)}")

    if not bc_sigs:
        # Пробуем mint напрямую (иногда там есть creation tx)
        print(f"  [{short}] Пробую mint напрямую...")
        mint_sigs = await get_sigs(session, mint, limit=100)
        await asyncio.sleep(1.0)
        print(f"  [{short}] Сигнатур mint: {len(mint_sigs)}")
        if not mint_sigs:
            m.notes.append("нет данных ни по bonding curve, ни по mint (токен слишком старый?)")
            return m
        bc_sigs = mint_sigs

    now = time.time()
    chron = list(reversed(bc_sigs))  # старые → новые
    creation_slot = chron[0].get("slot") if chron else None

    oldest_time = next((s["blockTime"] for s in chron if s.get("blockTime")), None)
    newest_time = next((s["blockTime"] for s in reversed(chron) if s.get("blockTime")), None)
    if newest_time:
        m.last_tx_sec = now - newest_time

    # ── Читаем транзакции (индивидуально — public RPC throttles batches) ───────
    MAX_TX   = 300  # max транзакций для поиска entry MC
    print(f"  [{short}] Читаю транзакции (max {MAX_TX}, по одной, 0.5с между)...")
    valid_sigs = [s["signature"] for s in chron if s.get("signature") and not s.get("err")]

    entry_time  = None
    all_trades  = []   # (block_time, sol_usd, is_buy, fee_payer_addr)
    buy_count   = 0
    sell_count  = 0

    for i, sig in enumerate(valid_sigs[:MAX_TX]):
        if i % 25 == 0:
            print(f"  [{short}] tx {i+1}/{min(len(valid_sigs), MAX_TX)}...")
        tx = await get_tx(session, sig)
        await asyncio.sleep(0.5)
        if not tx:
            continue
        tx_time = tx.get("blockTime")
        fp      = fee_payer(tx)
        logs    = (tx.get("meta") or {}).get("logMessages") or []
        for line in logs:
            ev = parse_trade(line)
            if not ev:
                continue
            mc  = mc_from_reserves(ev["vs"], ev["vt"], sol_price)
            usd = ev["sol_amt"] / 1e9 * sol_price
            if ev["is_buy"]:
                buy_count += 1
            else:
                sell_count += 1
            if tx_time and fp:
                all_trades.append((tx_time, usd, ev["is_buy"], fp))

            # Первый вход в диапазон
            if entry_time is None and mc and MC_LOW <= mc <= MC_HIGH:
                entry_time  = tx_time
                m.entry_mc  = mc
                if oldest_time and tx_time:
                    m.age_minutes = (tx_time - oldest_time) / 60
                age_str = f" (возраст {m.age_minutes:.1f} мин)" if m.age_minutes else ""
                print(f"  [{short}] ✅ Entry MC ${mc:,.0f}{age_str}")

        if entry_time and i >= 50:
            break  # entry найден и набрали данные для volume

    # ── Volume и buyers за 5 мин до entry ───────────────────────────────────
    ref_time = entry_time or newest_time or now
    cutoff   = ref_time - 300
    window_trades = [(ts, usd, buy, fp) for ts, usd, buy, fp in all_trades
                     if cutoff <= ts <= ref_time]
    m.vol_5m   = sum(usd for _, usd, buy, _ in window_trades if buy)
    m.buyers_5m = len({fp for _, _, buy, fp in window_trades if buy})

    # ── Buy/sell ratio (по всем транзакциям) ────────────────────────────────
    total_trades = buy_count + sell_count
    if total_trades > 0:
        m.sell_ratio = sell_count / total_trades * 100

    # ── Бандлы ──────────────────────────────────────────────────────────────
    if creation_slot:
        bundle_tx   = sum(1 for s in chron
                          if s.get("slot") and not s.get("err")
                          and s["slot"] <= creation_slot + BUNDLE_SLOT_WINDOW)
        total_valid = sum(1 for s in chron if not s.get("err"))
        if total_valid > 0:
            m.bundle_pct = bundle_tx / total_valid * 100

    # ── Холдеры ──────────────────────────────────────────────────────────────
    print(f"  [{short}] Читаю холдеров...")
    await asyncio.sleep(1.0)
    holders = await get_largest(session, mint)
    await asyncio.sleep(1.0)
    if holders:
        shares = []
        for h in holders:
            try:
                amt = float(h.get("uiAmountString") or h.get("uiAmount") or 0)
            except Exception:
                amt = 0.0
            pct = amt / TOKEN_SUPPLY * 100
            if pct >= BONDING_EXCLUDE:
                continue
            shares.append(pct)
        if shares:
            m.max_holder_pct = shares[0]
            m.top10_pct      = sum(shares[:10])

    # ── Дев (первый fee payer) ────────────────────────────────────────────────
    creation_sig = chron[0].get("signature") if chron else None
    if creation_sig:
        print(f"  [{short}] Читаю creation tx для дева...")
        creation_tx = await get_tx(session, creation_sig)
        await asyncio.sleep(1.0)
        if creation_tx:
            creator = fee_payer(creation_tx)
            m.dev_addr = creator
            if creator:
                dev_sigs = await get_sigs(session, creator, limit=50)
                await asyncio.sleep(1.0)
                if dev_sigs:
                    m.dev_status = f"Unknown (история: {len(dev_sigs)} tx)"
                else:
                    m.dev_status = "Unknown (нет истории)"

    if not entry_time:
        m.notes.append("entry MC не найден в первых 300 tx")

    return m


# ── Output ────────────────────────────────────────────────────────────────────

def fmt(v, spec=":.1f", default="—"):
    if v is None:
        return default
    try:
        return format(v, spec.lstrip(":"))
    except Exception:
        return str(v)


def print_table(results: list[Metrics]):
    print("\n" + "═" * 110)
    print(f"{'#':>2}  {'Токен (name/symbol)':>20} | {'AgeMn':>6} | {'LastTx':>7} | "
          f"{'MaxH%':>5} | {'Top10%':>6} | {'Bndl%':>5} | {'Vol5m$':>7} | "
          f"{'Buy5m':>5} | {'Sell%':>5} | {'Social':>6} | {'EntryMC':>8} | Notes")
    print("─" * 110)
    for i, m in enumerate(results, 1):
        label = f"{m.symbol}/{m.name[:8]}" if m.symbol != "—" else m.mint[:10]
        print(
            f"{i:>2}  {label:>20} | "
            f"{fmt(m.age_minutes):>6} | "
            f"{fmt(m.last_tx_sec, ':.0f'):>7} | "
            f"{fmt(m.max_holder_pct):>5} | "
            f"{fmt(m.top10_pct):>6} | "
            f"{fmt(m.bundle_pct):>5} | "
            f"{fmt(m.vol_5m, ':.0f'):>7} | "
            f"{m.buyers_5m:>5} | "
            f"{fmt(m.sell_ratio):>5} | "
            f"{'✅' if m.has_social else '❌':>6} | "
            f"{fmt(m.entry_mc, ':.0f'):>8} | "
            f"{'; '.join(m.notes)}"
        )
    print("═" * 110)


def stats(vals):
    c = [v for v in vals if v is not None]
    if not c:
        return None, None, None, None
    s = sorted(c)
    return sum(s)/len(s), s[len(s)//2], s[0], s[-1]


def print_stats(results: list[Metrics]):
    print("\n\n=== СТАТИСТИКА ===\n")
    metrics_map = {
        "Возраст (мин)":      [r.age_minutes      for r in results],
        "Последняя tx (сек)": [r.last_tx_sec      for r in results],
        "Макс. холдер (%)":   [r.max_holder_pct   for r in results],
        "Топ-10 (%)":         [r.top10_pct        for r in results],
        "Бандлы (%)":         [r.bundle_pct       for r in results],
        "Volume 5м ($)":      [r.vol_5m           for r in results],
        "Buyers 5м":          [float(r.buyers_5m) for r in results],
        "Sell ratio (%)":     [r.sell_ratio       for r in results],
    }
    print(f"{'Метрика':>22} | {'Среднее':>9} | {'Медиана':>9} | {'Мин':>9} | {'Макс':>9}")
    print("-" * 70)
    for name, vals in metrics_map.items():
        avg, med, mn, mx = stats(vals)
        f = lambda v: f"{v:.1f}" if v is not None else "—"
        print(f"{name:>22} | {f(avg):>9} | {f(med):>9} | {f(mn):>9} | {f(mx):>9}")

    soc = sum(1 for r in results if r.has_social)
    print(f"\nСоцсети (twitter/site): {soc}/{len(results)} токенов")


def print_recommendations(results: list[Metrics]):
    age_vals    = [r.age_minutes    for r in results if r.age_minutes    is not None]
    vol_vals    = [r.vol_5m         for r in results]
    buy_vals    = [r.buyers_5m      for r in results]
    h_vals      = [r.max_holder_pct for r in results if r.max_holder_pct is not None]
    t10_vals    = [r.top10_pct      for r in results if r.top10_pct      is not None]
    bnd_vals    = [r.bundle_pct     for r in results if r.bundle_pct     is not None]
    sell_vals   = [r.sell_ratio     for r in results if r.sell_ratio     is not None]

    def rng(vals):
        c = [v for v in vals if v is not None]
        return f"{min(c):.1f}–{max(c):.1f}" if c else "—"

    print("\n\n=== ТЕКУЩИЕ ПОРОГИ vs ДАННЫЕ ===\n")
    rows = [
        ("MAX_TOKEN_AGE_HOURS",   "2ч (120мин)",  f"{rng(age_vals)} мин"),
        ("MIN_VOLUME_USD_5MIN",   "$150",          f"${rng(vol_vals)}"),
        ("MIN_UNIQUE_BUYERS_5MIN","2",             rng(buy_vals)),
        ("MAX_SINGLE_HOLDER%",    "7.0%",          rng(h_vals)),
        ("MAX_TOP10%",            "22.0%",         rng(t10_vals)),
        ("MAX_BUNDLE%",           "20%",           rng(bnd_vals)),
        ("Sell ratio (инфо)",     "нет порога",    rng(sell_vals)),
    ]
    print(f"{'Параметр':>24} | {'Текущий':>14} | {'Успешные токены':>16}")
    print("-" * 65)
    for p, cur, data in rows:
        print(f"{p:>24} | {cur:>14} | {data:>16}")

    print("\n\n=== ГОТОВЫЙ БЛОК .env ===\n")
    # Рассчитываем рекомендуемые значения на основе данных
    def safe_max(vals, fallback):
        c = [v for v in vals if v is not None]
        return max(c) * 1.15 if c else fallback  # +15% запас сверху

    def safe_min(vals, fallback):
        c = [v for v in vals if v is not None]
        return min(c) * 0.85 if c else fallback  # -15% запас снизу

    rec_age    = max(safe_max(age_vals, 90) / 60, 1.0)
    rec_vol    = max(safe_min(vol_vals, 150), 50)
    rec_buyers = max(min(buy_vals) if buy_vals else 2, 2)
    rec_maxh   = min(safe_max(h_vals, 7.0), 15.0)
    rec_top10  = min(safe_max(t10_vals, 22.0), 35.0)
    rec_bundle = min(safe_max(bnd_vals, 20.0), 25.0)

    print(f"# Рекомендации на основе {len([r for r in results if r.entry_mc])} токенов с данными\n")
    print(f"MAX_TOKEN_AGE_HOURS={rec_age:.1f}    # успешные входили за {rng(age_vals)} мин")
    print(f"MIN_VOLUME_USD_5MIN={rec_vol:.0f}   # объём успешных: ${rng(vol_vals)}")
    print(f"MIN_UNIQUE_BUYERS_5MIN={rec_buyers}   # покупателей успешных: {rng(buy_vals)}")
    print(f"MAX_SINGLE_HOLDER_PERCENT={rec_maxh:.1f}  # макс. холдер успешных: {rng(h_vals)}%")
    print(f"MAX_TOP10_HOLDERS_PERCENT={rec_top10:.1f}  # топ-10 успешных: {rng(t10_vals)}%")
    print(f"MAX_BUNDLE_PERCENT={rec_bundle:.0f}       # бандлы успешных: {rng(bnd_vals)}%")
    if sell_vals:
        print(f"\n# Sell ratio успешных: {rng(sell_vals)}% — учти при добавлении фильтра")


async def main():
    if not HELIUS_API_KEY or "your_" in HELIUS_API_KEY:
        print("❌ HELIUS_API_KEY не задан в .env")
        return

    print("Получаю цену SOL...")
    async with aiohttp.ClientSession() as session:
        sol_price = await get_sol_price(session)
        print(f"SOL = ${sol_price:.2f}\n")

        results = []
        for mint in TARGET_MINTS:
            print(f"\n{'═'*60}\nАнализирую: {mint}")
            try:
                m = await analyze(session, mint, sol_price)
            except Exception as e:
                print(f"  ОШИБКА: {e}")
                m = Metrics(mint=mint, notes=[str(e)])
            results.append(m)
            await asyncio.sleep(3.0)

    print_table(results)
    print_stats(results)
    print_recommendations(results)


if __name__ == "__main__":
    asyncio.run(main())
