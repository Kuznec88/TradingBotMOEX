"""
Unary-запросы котировок T-Invest (песочница): последняя цена и стакан.
Запуск из корня репозитория: python -m fix_engine.tools.fetch_tbank_quotes
или из fix_engine: python tools/fetch_tbank_quotes.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _read_cfg_value(cfg_dir: Path, key: str, default: str = "") -> str:
    for name in ("settings.local.cfg", "settings.runtime.cfg", "settings.cfg"):
        path = cfg_dir / name
        if not path.exists():
            continue
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() == key:
                return v.strip()
    return default


def _q_to_float(q: object | None) -> float:
    if q is None:
        return 0.0
    u = float(getattr(q, "units", 0) or 0)
    n = float(getattr(q, "nano", 0) or 0)
    return u + n / 1_000_000_000.0


def main() -> None:
    fix_engine_dir = Path(__file__).resolve().parents[1]
    if str(fix_engine_dir) not in sys.path:
        sys.path.insert(0, str(fix_engine_dir.parent))

    token = os.environ.get("TINKOFF_TOKEN", "").strip() or os.environ.get("TINKOFF_SANDBOX_TOKEN", "").strip()
    if not token:
        token = _read_cfg_value(fix_engine_dir, "TBankSandboxToken")
    if not token:
        print("Задайте TINKOFF_TOKEN (или TINKOFF_SANDBOX_TOKEN) или TBankSandboxToken в settings.local.cfg", file=sys.stderr)
        sys.exit(1)

    host = _read_cfg_value(fix_engine_dir, "TBankSandboxHost", "invest-public-api.tinkoff.ru:443")
    instrument_id = _read_cfg_value(fix_engine_dir, "TBankInstrumentId", "")
    symbol = _read_cfg_value(fix_engine_dir, "TBankSymbol", "")
    depth = int(_read_cfg_value(fix_engine_dir, "TBankOrderBookDepth", "5") or "5")

    if not instrument_id:
        print("TBankInstrumentId пуст в settings.cfg", file=sys.stderr)
        sys.exit(1)

    from t_tech.invest import Client

    with Client(token, target=host) as client:
        last = client.market_data.get_last_prices(instrument_id=[instrument_id])
        book = client.market_data.get_order_book(
            instrument_id=instrument_id,
            depth=max(1, depth),
        )

    print(f"instrument_id={instrument_id} symbol={symbol} host={host}")
    for p in last.last_prices:
        px = _q_to_float(getattr(p, "price", None))
        t = getattr(p, "time", None)
        print(f"  last_price: {px:.4f} time={t}")
    ob = book
    bids = list(getattr(ob, "bids", []) or [])
    asks = list(getattr(ob, "asks", []) or [])
    print(f"  order_book depth={depth} bids={len(bids)} asks={len(asks)}")
    for i, lvl in enumerate(bids[:5]):
        print(f"    bid[{i}] px={_q_to_float(getattr(lvl, 'price', None)):.4f} qty={getattr(lvl, 'quantity', 0)}")
    for i, lvl in enumerate(asks[:5]):
        print(f"    ask[{i}] px={_q_to_float(getattr(lvl, 'price', None)):.4f} qty={getattr(lvl, 'quantity', 0)}")


if __name__ == "__main__":
    main()
