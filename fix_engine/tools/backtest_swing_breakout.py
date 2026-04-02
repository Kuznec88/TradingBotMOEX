"""
Бэктест свинг-стратегии swing_breakout на CSV свечей (как из fetch_tbank_candles_history.py).

Пример:
  cd fix_engine
  python tools/fetch_tbank_candles_history.py --interval 15m --days 30 --out ../_candles_15m.csv
  python tools/backtest_swing_breakout.py --csv ../_candles_15m.csv --rub-per-point 1 --tp-rub 200 --sl-rub 100
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    fix_engine_dir = Path(__file__).resolve().parents[1]
    if str(fix_engine_dir.parent) not in sys.path:
        sys.path.insert(0, str(fix_engine_dir.parent))

    from fix_engine.strategy.swing_breakout import (
        SwingBreakoutParams,
        load_ohlcv_csv,
        run_backtest,
    )

    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=str, required=True, help="CSV: time_utc,open,high,low,close,volume")
    ap.add_argument("--n-levels", type=int, default=20)
    ap.add_argument("--k-volume", type=float, default=1.5)
    ap.add_argument("--ma", type=int, default=50)
    ap.add_argument("--tp-rub", type=float, default=200.0)
    ap.add_argument("--sl-rub", type=float, default=100.0)
    ap.add_argument("--rub-per-point", type=float, default=1.0, help="руб/пункт для 1 лота (калибровка под инструмент)")
    ap.add_argument("--tick", type=float, default=1.0)
    ap.add_argument("--slip-ticks", type=int, default=1)
    ap.add_argument("--commission", type=float, default=0.0004, help="доля на сторону, 0.04%% = 0.0004")
    ap.add_argument("--notional-scale", type=float, default=1.0)
    ap.add_argument("--trail-act-rub", type=float, default=0.0, help="0 = без трейлинга")
    ap.add_argument("--trail-gap-rub", type=float, default=50.0)
    ap.add_argument("--trades-jsonl", type=str, default="", help="опционально записать сделки построчно JSON")
    args = ap.parse_args()

    df = load_ohlcv_csv(args.csv)
    params = SwingBreakoutParams(
        n_levels=args.n_levels,
        k_volume=args.k_volume,
        ma_period=args.ma,
        take_profit_rub=args.tp_rub,
        stop_loss_rub=args.sl_rub,
        rub_per_point=args.rub_per_point,
        tick_size=args.tick,
        slippage_ticks=args.slip_ticks,
        commission_rate=args.commission,
        notional_scale=args.notional_scale,
        trailing_activation_rub=args.trail_act_rub,
        trailing_gap_rub=args.trail_gap_rub,
    )
    res = run_backtest(df, params, log_trades=False)
    print(json.dumps(res.summary(), ensure_ascii=False, indent=2))

    if args.trades_jsonl:
        outp = Path(args.trades_jsonl)
        with outp.open("w", encoding="utf-8") as f:
            for t in res.trades:
                f.write(t.to_log_line() + "\n")
        print(f"trades written: {outp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
