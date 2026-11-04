"""
Склеенная история ближайших квартальных фьючерсов T-Invest для бэктестов.

Контракт используется до expiration - roll_days, затем следующий. Цены старых контрактов
сдвигаются на разрыв в момент перехода (back-adjust), чтобы переход не выглядел движением цены.
Только чтение: заявки не отправляются.

  python -m fix_engine.tools.fetch_continuous_futures --assets SR GZ LK Si MX --start 2023-06-01
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from fix_engine.tools.common_cfg_dir import read_tinvest_token_from_dir

SANDBOX_HOST = "sandbox-invest-public-api.tbank.ru:443"
PRICE_COLUMNS = ("open", "high", "low", "close")
CHUNK_DAYS = {"1h": 80, "4h": 80}


def stitch_contracts(parts: list[pd.DataFrame]) -> pd.DataFrame:
    """Склеить контракты по времени (старые -> новые), сдвинув старые цены на разрыв при переходе."""
    adjusted: list[pd.DataFrame] = []
    shift = 0.0
    for i in range(len(parts) - 1, -1, -1):
        part = parts[i].copy()
        for col in PRICE_COLUMNS:
            part[col] = part[col] + shift
        adjusted.append(part)
        if i > 0:
            shift += float(parts[i]["close"].iloc[0]) - float(parts[i - 1]["close"].iloc[-1])
    return pd.concat(adjusted[::-1]).sort_values("time_utc")


def _q_float(q: object) -> float:
    return float(getattr(q, "units", 0) or 0) + float(getattr(q, "nano", 0) or 0) / 1_000_000_000.0


def _fetch_contract(client: object, uid: str, ticker: str, interval: object, chunk_days: int, start: datetime, end: datetime) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=chunk_days), end)
        resp = client.market_data.get_candles(instrument_id=uid, from_=cur, to=nxt, interval=interval, limit=2400)
        rows.extend(
            {
                "time_utc": c.time,
                "open": _q_float(c.open),
                "high": _q_float(c.high),
                "low": _q_float(c.low),
                "close": _q_float(c.close),
                "volume": c.volume,
                "is_complete": c.is_complete,
                "contract": ticker,
            }
            for c in resp.candles
        )
        cur = nxt
        time.sleep(0.11)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).drop_duplicates("time_utc").sort_values("time_utc")


def main() -> int:
    fix_engine_dir = Path(__file__).resolve().parents[1]
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--assets", nargs="+", default=["SR", "GZ", "LK", "Si", "MX"], help="префиксы тикеров")
    ap.add_argument("--start", default="2023-06-01")
    ap.add_argument("--end", default="", help="по умолчанию до текущего момента")
    ap.add_argument("--roll-days", type=int, default=7)
    ap.add_argument("--intervals", nargs="+", choices=tuple(CHUNK_DAYS), default=list(CHUNK_DAYS))
    ap.add_argument("--host", default=SANDBOX_HOST)
    ap.add_argument("--out-dir", type=Path, default=fix_engine_dir.parent / "data" / "futures")
    args = ap.parse_args()

    token = read_tinvest_token_from_dir(fix_engine_dir)
    if not token:
        print("Нет токена: задайте TBankSandboxToken в settings.local.cfg", file=sys.stderr)
        return 1

    from fix_engine.data.preflight import prepare_invest_api_tls_trust_store

    prepare_invest_api_tls_trust_store(fix_engine_dir)

    from t_tech.invest import Client
    from t_tech.invest.schemas import CandleInterval, InstrumentStatus

    intervals = {
        "1h": CandleInterval.CANDLE_INTERVAL_HOUR,
        "4h": CandleInterval.CANDLE_INTERVAL_4_HOUR,
    }
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    now = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc) if args.end else datetime.now(timezone.utc)
    roll = timedelta(days=args.roll_days)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    meta_path = args.out_dir / "meta.json"
    meta: dict[str, dict[str, object]] = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}

    with Client(token, target=args.host) as client:
        futures = client.instruments.futures(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL).instruments
        for asset in args.assets:
            contracts = sorted(
                (
                    f
                    for f in futures
                    if f.ticker.startswith(asset)
                    and len(f.ticker) == 4
                    and f.ticker[2] in "HMUZ"
                    and f.expiration_date > start
                ),
                key=lambda f: f.expiration_date,
            )
            if not contracts:
                print(f"{asset}: контракты не найдены", file=sys.stderr)
                continue
            meta[asset] = {
                "contracts": [f.ticker for f in contracts],
                "tick": _q_float(contracts[-1].min_price_increment),
                "tick_rub": _q_float(contracts[-1].min_price_increment_amount),
            }
            for name in args.intervals:
                parts: list[pd.DataFrame] = []
                prev_end = start
                for f in contracts:
                    end = min(f.expiration_date - roll, now)
                    if end <= prev_end:
                        continue
                    part = _fetch_contract(client, f.uid, f.ticker, intervals[name], CHUNK_DAYS[name], prev_end, end)
                    if not part.empty:
                        parts.append(part)
                    prev_end = end
                if not parts:
                    print(f"{asset} {name}: свечей нет", file=sys.stderr)
                    continue
                candles = stitch_contracts(parts)
                candles = candles[candles["is_complete"]]
                candles.to_csv(args.out_dir / f"{asset}_{name}.csv", index=False)
                print(f"{asset} {name}: {len(candles)} свечей, контрактов {candles['contract'].nunique()}", flush=True)

    meta_path.write_text(json.dumps(meta, indent=1), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
