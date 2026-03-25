"""
Исторические свечи T-Invest за окно (например 14 дней) через get_candles.

Важно:
- Это OHLC/объёмы по интервалу, не каждый тик стакана.
- Лимиты API зависят от интервала (см. доку T-Bank): 1m — обычно до ~1 суток за запрос,
  5m — до ~недели, 15m — до ~3 недель одним запросом.
- Для 1m за 14 дней делается 14 посуточных запросов.

Запуск из fix_engine:
  python tools/fetch_tbank_candles_history.py
  python tools/fetch_tbank_candles_history.py --interval 15m --days 14
  python tools/fetch_tbank_candles_history.py --interval 1m --days 14 --out candles_sber.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _read_cfg(cfg_dir: Path, key: str, default: str = "") -> str:
    for name in ("settings.local.cfg", "settings.runtime.cfg", "settings.cfg"):
        p = cfg_dir / name
        if not p.exists():
            continue
        for raw in p.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() == key:
                return v.strip()
    return default


def _q_float(q: object | None) -> float:
    if q is None:
        return 0.0
    u = float(getattr(q, "units", 0) or 0)
    n = float(getattr(q, "nano", 0) or 0)
    return u + n / 1_000_000_000.0


def main() -> None:
    fix_engine_dir = Path(__file__).resolve().parents[1]
    if str(fix_engine_dir) not in sys.path:
        sys.path.insert(0, str(fix_engine_dir.parent))

    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=float, default=14.0, help="глубина назад от now (UTC)")
    ap.add_argument(
        "--interval",
        choices=("1m", "5m", "15m", "1h"),
        default="15m",
        help="интервал свечей (15m и 1h удобны для длинного окна одним запросом)",
    )
    ap.add_argument("--out", type=str, default="", help="CSV или пусто = только сводка в stdout")
    args = ap.parse_args()

    token = (
        os.environ.get("TINKOFF_TOKEN", "").strip()
        or os.environ.get("TINKOFF_SANDBOX_TOKEN", "").strip()
        or _read_cfg(fix_engine_dir, "TBankSandboxToken")
    )
    if not token:
        print("Нет токена (TINKOFF_TOKEN / settings.local.cfg)", file=sys.stderr)
        sys.exit(1)

    host = _read_cfg(fix_engine_dir, "TBankSandboxHost", "invest-public-api.tinkoff.ru:443")
    iid = _read_cfg(fix_engine_dir, "TBankInstrumentId", "")
    if not iid:
        print("TBankInstrumentId пуст в settings", file=sys.stderr)
        sys.exit(1)

    from t_tech.invest import Client
    from t_tech.invest.schemas import CandleInterval

    interval_map = {
        "1m": CandleInterval.CANDLE_INTERVAL_1_MIN,
        "5m": CandleInterval.CANDLE_INTERVAL_5_MIN,
        "15m": CandleInterval.CANDLE_INTERVAL_15_MIN,
        "1h": CandleInterval.CANDLE_INTERVAL_HOUR,
    }
    ci = interval_map[args.interval]

    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=args.days)

    rows: list[dict[str, object]] = []

    with Client(token, target=host) as client:
        if args.interval == "1m" and args.days > 1.5:
            # посуточно: у API ограничение ~1 день на 1m
            day = timedelta(days=1)
            cur = from_dt
            while cur < to_dt:
                nxt = min(cur + day, to_dt)
                r = client.market_data.get_candles(
                    instrument_id=iid,
                    from_=cur,
                    to=nxt,
                    interval=ci,
                    limit=2400,
                )
                for c in r.candles:
                    rows.append(_candle_row(c))
                cur = nxt
        else:
            r = client.market_data.get_candles(
                instrument_id=iid,
                from_=from_dt,
                to=to_dt,
                interval=ci,
                limit=2400,
            )
            for c in r.candles:
                rows.append(_candle_row(c))

    print(
        f"host={host} instrument_id={iid} interval={args.interval} "
        f"from={from_dt.isoformat()} to={to_dt.isoformat()} candles={len(rows)}"
    )
    if not rows:
        return

    if args.out:
        outp = Path(args.out)
        with outp.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"written {outp}")
    else:
        for i, row in enumerate(rows[:5]):
            print(" sample", i, row)
        if len(rows) > 5:
            print(f" ... +{len(rows) - 5} rows")


def _candle_row(c: object) -> dict[str, object]:
    t = getattr(c, "time", None)
    ts = t.isoformat() if t is not None else ""
    return {
        "time_utc": ts,
        "open": _q_float(getattr(c, "open", None)),
        "high": _q_float(getattr(c, "high", None)),
        "low": _q_float(getattr(c, "low", None)),
        "close": _q_float(getattr(c, "close", None)),
        "volume": int(getattr(c, "volume", 0) or 0),
        "is_complete": bool(getattr(c, "is_complete", False)),
    }


if __name__ == "__main__":
    main()
