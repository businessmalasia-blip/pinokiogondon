# Pump.fun Screening Bot

Бот скрининга токенов Pump.fun: слушает покупки через WebSocket Helius,
фильтрует токены по трём проверкам и шлёт алерты в Telegram.

## Пайплайн

1. **WebSocket** — подписка `logsSubscribe` на программу Pump.fun
   (`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`) через
   `wss://mainnet.helius-rpc.com`. Из транзакций с инструкцией `buy`
   извлекаются mint и адрес bonding curve — строго из аккаунтов самой
   инструкции buy (включая внутренние), а не по приросту SOL.
2. **Market cap** — свежий `getAccountInfo` по bonding curve:
   `(lamports / 1e9) * цена SOL`. Цена SOL кэшируется в Redis и
   обновляется из Jupiter раз в 2 секунды. При капе ≥ $8 000 запускается анализ.
3. **Три фильтра** (`bot/filters.py`):
   - `check_concentration` — топ-100 холдеров через Helius DAS
     `getTokenAccounts`; bonding curve (доля > 50%) исключается; ни один
     холдер не держит > 2.5% supply, топ-10 в сумме < 15%.
   - `calculate_human_percent` — HUMAN-кошельки (есть история транзакций,
     кэш в Redis `human:{address}`); проходит при HUMAN ≥ 60% и UNKNOWN ≤ 20%.
   - `check_dev` — создатель токена (feePayer первой транзакции mint),
     кэш плохих девов `bad_dev:{address}`; MSR = доля «выживших» токенов
     дева за 30 дней (цена > 0 и объём > $1000 по Jupiter).
     ≥ 3 токенов и MSR ≥ 70% → Clean; MSR < 70% → Bad; < 3 токенов → Unknown.
4. **Ожидание диапазона** — после прохождения всех фильтров бот каждые
   2 секунды пересчитывает капу, пока она не войдёт в $10 000–$12 000.
5. **Алерт в Telegram** (aiogram, HTML) — с картинкой токена, названием и
   тикером из Helius DAS `getAsset`, ссылкой на Photon, HUMAN-процентом,
   статусом дева и капой. Если в момент отправки капа вне $9 000–$13 000,
   алерт не отправляется.

Все запросы к Helius идут через общий rate limiter (0.2 сек между запросами).
При старте бот шлёт в Telegram «🚀 Бот запущен».

## Структура

```
bot/
  config.py   — настройки из .env
  helius.py   — клиент Helius (RPC + DAS) с rate limiting
  pump.py     — разбор инструкций buy/create по дискриминаторам
  prices.py   — цена SOL из Jupiter (кэш в Redis) и расчёт market cap
  filters.py  — три фильтра анализа
  alerts.py   — Telegram-алерты
  main.py     — WebSocket-цикл и пайплайн
```

## Запуск

Требования: Python 3.10, Redis.

```bash
pip install -r requirements.txt
cp .env.example .env   # заполнить HELIUS_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
python -m bot.main
```

Все пороги (капа, HUMAN 60/20, MSR 70% и т.д.) настраиваются в `.env`,
см. `.env.example`.
