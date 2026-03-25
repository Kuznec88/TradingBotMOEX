# TradingBotMOEX

Рабочий контур: `fix_engine` — **рынок через T‑Invest API (sandbox)**, исполнение **paper** (без реальных заявок на биржу).

## Запуск

1. Установить зависимости: `pip install -r requirements.txt`
2. В `fix_engine/settings.cfg` задать `DataProvider=TINKOFF_SANDBOX` и при необходимости `TBankInstrumentId`, `TBankSymbol`.
3. Токен sandbox (предпочтительно через env):

```powershell
$env:TINKOFF_SANDBOX_TOKEN = "<токен>"
python main.py
```

Или из каталога `fix_engine`:

```powershell
cd fix_engine
python main.py
```

## Что делает раннер

- стримит котировки с **T‑Bank sandbox** (`tbank_sandbox_feed`) в `MarketDataEngine`;
- стратегия и (опционально) market making работают на тех же данных;
- исполнение — локальная симуляция / paper (`ExecutionGateway`), без маршрутизации на биржу.

## Команды в консоли

(если включён интерактив в раннере)

- `m SYMBOL SIDE QTY` — market order (paper)
- `l SYMBOL SIDE QTY PRICE` — limit order (paper)
- `c CL_ORD_ID` — отмена
- `sig LAST_PRICE SYMBOL QTY` — прогон сигнала по цене
- `md SYMBOL BID ASK LAST VOLUME [QTY]` — ручной market data
- `pos SYMBOL` — позиция
- `open` — открытые ордера
- `q` — выход

## Логи и артефакты

- структурированные логи приложения: каталог `fix_engine/log/` (создаётся при старте);
- `trade_economics.db` — локальная SQLite-экономика; в `.gitignore`;
- старые FIX session-файлы в `fix_engine/store/` не используются при `TINKOFF_SANDBOX` — каталог можно не коммитить.
