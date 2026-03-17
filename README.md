# TradingBotMOEX

Актуальный проектный контур: `fix_engine` (TRADE + DROP_COPY).

## Запуск

Из корня проекта:

```powershell
python main.py
```

Альтернатива (то же самое):

```powershell
cd "C:\Users\Admin\Python Projects\TradingBotMOEX\fix_engine"
python main.py
```

## Что делает раннер

- поднимает 2 FIX-сессии:
  - `TRADE` (`IFIX-EQ-UAT`)
  - `DROP_COPY` (`IFIX-DC-EQ-UAT`)
- маршрутизирует сообщения по типу сессии
- ведет трекинг ордеров через `OrderManager`
- принимает market data в `MarketDataEngine`

## Команды в консоли

- `m SYMBOL SIDE QTY` — Market order  
  Пример: `m SBER 1 10`
- `l SYMBOL SIDE QTY PRICE` — Limit order  
  Пример: `l SBER 2 5 317.25`
- `c CL_ORD_ID` — Cancel order
- `sig LAST_PRICE SYMBOL QTY` — прогнать сигнал стратегии по цене
- `md SYMBOL BID ASK LAST VOLUME [QTY]` — вручную подать market data
- `pos SYMBOL` — показать позицию
- `open` — показать открытые ордера
- `q` — выход

## Логи

- QuickFIX file logs: `fix_engine/log/`
- App logs: `fix_engine/log/fix_client.log`
