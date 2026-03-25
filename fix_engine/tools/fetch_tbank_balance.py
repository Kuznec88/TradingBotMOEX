"""
Счета и портфель T-Invest (продовый gRPC). Нужен полный токен Invest API;
read-only токен только на котировки даёт UNAUTHENTICATED на GetAccounts.

Запуск из fix_engine: python tools/fetch_tbank_balance.py
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


def _money(x: object | None) -> float | None:
    if x is None:
        return None
    u = float(getattr(x, "units", 0) or 0)
    n = float(getattr(x, "nano", 0) or 0)
    return u + n / 1_000_000_000.0


def main() -> None:
    fix_engine_dir = Path(__file__).resolve().parents[1]
    if str(fix_engine_dir) not in sys.path:
        sys.path.insert(0, str(fix_engine_dir.parent))

    token = (
        os.environ.get("TINKOFF_TOKEN", "").strip()
        or os.environ.get("TINKOFF_SANDBOX_TOKEN", "").strip()
        or _read_cfg_value(fix_engine_dir, "TBankSandboxToken")
    )
    if not token:
        print("Задайте TINKOFF_TOKEN или TBankSandboxToken в settings.local.cfg", file=sys.stderr)
        sys.exit(1)

    host = _read_cfg_value(fix_engine_dir, "TBankSandboxHost", "invest-public-api.tinkoff.ru:443")

    from t_tech.invest import Client
    from t_tech.invest.exceptions import UnauthenticatedError

    try:
        with Client(token, target=host) as client:
            accs = client.users.get_accounts()
            print(f"host={host}")
            print(f"accounts={len(accs.accounts)}")
            for a in accs.accounts:
                aid = str(getattr(a, "id", "") or getattr(a, "account_id", ""))
                print("---")
                print(f"  id={aid} name={getattr(a, 'name', '')!r} type={getattr(a, 'type', '')!r}")
                port = client.operations.get_portfolio(account_id=aid)
                print(f"  total_amount_portfolio={_money(getattr(port, 'total_amount_portfolio', None))}")
                print(
                    f"  breakdown: currencies={_money(getattr(port, 'total_amount_currencies', None))} "
                    f"shares={_money(getattr(port, 'total_amount_shares', None))} "
                    f"bonds={_money(getattr(port, 'total_amount_bonds', None))} "
                    f"etf={_money(getattr(port, 'total_amount_etf', None))}"
                )
                w = client.operations.get_withdraw_limits(account_id=aid)
                for m in getattr(w, "money", []) or []:
                    print(f"  withdraw_limit {getattr(m, 'currency', '')}: {_money(getattr(m, 'amount', None))}")
    except UnauthenticatedError as exc:
        print(
            "UNAUTHENTICATED: для счетов/портфеля нужен полный токен T-Invest, "
            "не read-only только на рыночные данные.",
            file=sys.stderr,
        )
        print(exc, file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
