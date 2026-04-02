"""
Тестовая торговля в песочнице T-Invest:
- берёт токен только из settings.local.cfg, ключ TBankSandboxToken
- читает TBankInstrumentId
- получает accounts и выбирает первый (или TradingAccountEquities, если задан)
- берёт best ask из order_book и выставляет LIMIT BUY на 1 лот через sandbox.post_sandbox_order

Запуск из корня репозитория:
  python fix_engine/tools/place_tbank_sandbox_order.py
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

from fix_engine.tools.common_cfg_dir import (
    TBANK_INVEST_GRPC_HOST_PROD,
    read_cfg_value_from_dir,
    read_tinvest_token_from_dir,
)


def _q_to_float(q: object | None) -> float:
    if q is None:
        return 0.0
    u = float(getattr(q, "units", 0) or 0)
    n = float(getattr(q, "nano", 0) or 0)
    return u + n / 1_000_000_000.0


def main() -> None:
    base_dir = Path(__file__).resolve().parents[1]
    if str(base_dir.parent) not in sys.path:
        sys.path.insert(0, str(base_dir.parent))

    token = read_tinvest_token_from_dir(base_dir).strip()
    if not token:
        print(
            "Нет токена: задайте TBankSandboxToken в settings.local.cfg.",
            file=sys.stderr,
        )
        sys.exit(1)

    host = read_cfg_value_from_dir(base_dir, "TBankSandboxHost", TBANK_INVEST_GRPC_HOST_PROD).strip()
    instrument_id = read_cfg_value_from_dir(base_dir, "TBankInstrumentId", "").strip()
    if not instrument_id:
        print("TBankInstrumentId пуст", file=sys.stderr)
        sys.exit(1)

    preferred_acc = read_cfg_value_from_dir(base_dir, "TradingAccountEquities", "").strip()

    from t_tech.invest import Client
    from t_tech.invest.schemas import OrderDirection, OrderType
    from decimal import Decimal

    from t_tech.invest.utils import decimal_to_quotation

    with Client(token, target=host) as client:
        # Для sandbox-ордеров нужен sandbox account_id (не обычный broker id).
        # Предпочитаем явно заданный TradingAccountEquities, иначе берём первый sandbox-аккаунт,
        # иначе открываем новый.
        acc_id = preferred_acc
        if not acc_id:
            sb = client.sandbox.get_sandbox_accounts()
            sb_accs = list(getattr(sb, "accounts", []) or [])
            if sb_accs:
                a0 = sb_accs[0]
                acc_id = str(getattr(a0, "id", "") or getattr(a0, "account_id", "")).strip()
        if not acc_id:
            opened = client.sandbox.open_sandbox_account()
            acc_id = str(getattr(opened, "account_id", "") or getattr(opened, "id", "")).strip()
        if not acc_id:
            print("sandbox account_id не найден/не создан", file=sys.stderr)
            sys.exit(2)

        book = client.market_data.get_order_book(instrument_id=instrument_id, depth=1)
        asks = list(getattr(book, "asks", []) or [])
        if not asks:
            print("order_book: нет asks", file=sys.stderr)
            sys.exit(3)
        ask0 = asks[0]
        px = _q_to_float(getattr(ask0, "price", None))
        if px <= 0:
            print("order_book: ask price пуст/0", file=sys.stderr)
            sys.exit(3)

        quotation = decimal_to_quotation(Decimal(str(px)))
        resp = client.sandbox.post_sandbox_order(
            account_id=acc_id,
            instrument_id=instrument_id,
            quantity=1,
            price=quotation,
            direction=OrderDirection.ORDER_DIRECTION_BUY,
            order_type=OrderType.ORDER_TYPE_LIMIT,
            order_id=str(uuid.uuid4()),
        )

    print(f"OK host={host} account_id={acc_id} instrument_id={instrument_id} px={px:.4f}")
    print(f"order_id={getattr(resp, 'order_id', None)} status={getattr(resp, 'execution_report_status', None)}")


if __name__ == "__main__":
    main()

