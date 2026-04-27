"""
Исторические свечи T-Invest за окно через get_candles (длинные периоды — несколько запросов).

Важно:
- Это OHLC/объёмы по интервалу, не каждый тик стакана.
- Лимит за вызов — до 2400 свечей; длинные окна автоматически режутся на чанки.
- 1m за >1.5 суток — посуточно; прочие интервалы — шаг chunk_days (авто от ТФ).

Запуск из fix_engine:
  python tools/fetch_tbank_candles_history.py --interval 1h --days 183 --out history_1h.csv
  python tools/fetch_tbank_candles_history.py --interval 15m --days 14
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


from fix_engine.tools.common_cfg_dir import (
    TBANK_INVEST_GRPC_HOST_PROD,
    read_cfg_value_from_dir,
    read_tinvest_token_from_dir,
)


def _q_float(q: object | None) -> float:
    if q is None:
        return 0.0
    u = float(getattr(q, "units", 0) or 0)
    n = float(getattr(q, "nano", 0) or 0)
    return u + n / 1_000_000_000.0


def _bars_per_day(interval: str) -> float:
    return {"1m": 1440.0, "5m": 288.0, "15m": 96.0, "1h": 24.0, "4h": 6.0}[interval]


def _chunk_days_for_interval(interval: str, *, limit: int = 2400, safety: float = 0.92) -> float:
    """Макс. дней за один get_candles, чтобы уложиться в limit свечей."""
    bpd = _bars_per_day(interval)
    return max(1.0, (limit * safety) / bpd)


def _dedupe_sort_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    by_t: dict[str, dict[str, object]] = {}
    for r in rows:
        t = str(r.get("time_utc", ""))
        by_t[t] = r
    return [by_t[k] for k in sorted(by_t.keys())]


def main() -> None:
    fix_engine_dir = Path(__file__).resolve().parents[1]
    if str(fix_engine_dir) not in sys.path:
        sys.path.insert(0, str(fix_engine_dir.parent))

    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=float, default=14.0, help="глубина назад от now (UTC)")
    ap.add_argument(
        "--interval",
        choices=("1m", "5m", "15m", "1h", "4h"),
        default="15m",
        help="интервал свечей (длинное окно режется на чанки под лимит 2400 свечей)",
    )
    ap.add_argument("--out", type=str, default="", help="CSV или пусто = только сводка в stdout")
    ap.add_argument(
        "--chunk-days",
        type=float,
        default=0.0,
        help="дней за один запрос (0 = авто от interval и limit 2400)",
    )
    ap.add_argument(
        "--instrument-id",
        type=str,
        default="",
        help="UID инструмента T-Invest; пусто = TBankInstrumentId из settings",
    )
    args = ap.parse_args()

    token = read_tinvest_token_from_dir(fix_engine_dir)
    if not token:
        print("Нет токена: задайте TBankSandboxToken в settings.local.cfg", file=sys.stderr)
        sys.exit(1)

    host = read_cfg_value_from_dir(fix_engine_dir, "TBankSandboxHost", TBANK_INVEST_GRPC_HOST_PROD)
    iid = (args.instrument_id or "").strip() or read_cfg_value_from_dir(fix_engine_dir, "TBankInstrumentId", "")
    if not iid:
        print("TBankInstrumentId пуст (и не задан --instrument-id)", file=sys.stderr)
        sys.exit(1)

    from t_tech.invest import Client
    from t_tech.invest.schemas import CandleInterval

    interval_map = {
        "1m": CandleInterval.CANDLE_INTERVAL_1_MIN,
        "5m": CandleInterval.CANDLE_INTERVAL_5_MIN,
        "15m": CandleInterval.CANDLE_INTERVAL_15_MIN,
        "1h": CandleInterval.CANDLE_INTERVAL_HOUR,
        "4h": CandleInterval.CANDLE_INTERVAL_4_HOUR,
    }
    ci = interval_map[args.interval]

    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=args.days)

    rows: list[dict[str, object]] = []
    limit = 2400

    if args.chunk_days > 0:
        chunk = timedelta(days=args.chunk_days)
    else:
        chunk = timedelta(days=_chunk_days_for_interval(args.interval, limit=limit))

    with Client(token, target=host) as client:
        if args.interval == "1m" and args.days > 1.5:
            day = timedelta(days=1)
            cur = from_dt
            while cur < to_dt:
                nxt = min(cur + day, to_dt)
                r = client.market_data.get_candles(
                    instrument_id=iid,
                    from_=cur,
                    to=nxt,
                    interval=ci,
                    limit=limit,
                )
                for c in r.candles:
                    rows.append(_candle_row(c))
                cur = nxt
        else:
            cur = from_dt
            while cur < to_dt:
                nxt = min(cur + chunk, to_dt)
                r = client.market_data.get_candles(
                    instrument_id=iid,
                    from_=cur,
                    to=nxt,
                    interval=ci,
                    limit=limit,
                )
                for c in r.candles:
                    rows.append(_candle_row(c))
                cur = nxt

    rows = _dedupe_sort_rows(rows)

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
