"""
Бэктест swing_breakout по CSV свечей (формат как у fetch_tbank_candles_history).

Запуск: python -m fix_engine.backtest.swing_csv --csv path.csv
Для CSV с интервалом 1h обычно нужен --no-htf (иначе HTF совпадает с базой и сделок не будет).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _print_human_summary(s: dict[str, Any]) -> None:
    wr = s.get("winrate")
    wr_s = f"{100.0 * wr:.1f}%" if wr is not None else "—"
    def _f(x: Any) -> str:
        if x is None:
            return "—"
        if isinstance(x, float):
            return f"{x:.2f}"
        return str(x)

    print(
        "\n--- Итог ---\n"
        f"Сделок: {s.get('n_trades')}\n"
        f"Чистый PnL (руб): {_f(s.get('total_pnl_net_rub'))}\n"
        f"Винрейт: {wr_s}\n"
        f"Средний выигрыш (руб): {_f(s.get('avg_win_rub'))}\n"
        f"Средний проигрыш (руб): {_f(s.get('avg_loss_rub'))}\n"
        f"Ожидание на сделку (руб): {_f(s.get('expectancy_rub'))}\n"
        f"Profit factor: {_f(s.get('profit_factor'))}\n"
        f"Макс. просадка (руб): {_f(s.get('max_drawdown_rub'))}\n",
        end="",
    )


def main() -> int:
    """Парсит CLI, грузит OHLCV, вызывает run_backtest; печатает JSON-итог."""
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    fix_engine_dir = Path(__file__).resolve().parents[1]
    if str(fix_engine_dir.parent) not in sys.path:
        sys.path.insert(0, str(fix_engine_dir.parent))

    from fix_engine.strategy.swing_breakout import (
        SwingBreakoutParams,
        load_ohlcv_csv,
        run_backtest,
    )

    ap = argparse.ArgumentParser(description="Backtest swing_breakout on candle CSV")
    ap.add_argument("--csv", type=str, required=True, help="CSV: time_utc,open,high,low,close,volume")
    ap.add_argument("--n-levels", type=int, default=20)
    ap.add_argument("--k-volume", type=float, default=1.5)
    ap.add_argument("--ma", type=int, default=50)
    ap.add_argument("--tp-rub", type=float, default=200.0)
    ap.add_argument("--sl-rub", type=float, default=100.0)
    ap.add_argument("--rub-per-point", type=float, default=1.0, help="руб/пункт для 1 лота")
    ap.add_argument("--tick", type=float, default=1.0)
    ap.add_argument("--slip-ticks", type=int, default=1)
    ap.add_argument("--commission", type=float, default=0.0004, help="доля на сторону, 0.04%% = 0.0004")
    ap.add_argument("--notional-scale", type=float, default=1.0)
    ap.add_argument("--trail-act-rub", type=float, default=0.0, help="0 = без трейлинга")
    ap.add_argument("--trail-gap-rub", type=float, default=50.0)
    ap.add_argument("--trades-jsonl", type=str, default="", help="опционально: сделки построчно JSON")
    ap.add_argument(
        "--no-htf",
        action="store_true",
        help="отключить старший ТФ (для бэктеста на 1h CSV, когда htf_resample_rule=1h даёт 0 сделок)",
    )
    ap.add_argument(
        "--no-volume-spike",
        action="store_true",
        help="не требовать всплеск объёма на пробое (volume_spike_required=False)",
    )
    args = ap.parse_args()

    df = load_ohlcv_csv(args.csv)
    base = SwingBreakoutParams()
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
        htf_resample_rule="" if args.no_htf else base.htf_resample_rule,
        volume_spike_required=False if args.no_volume_spike else base.volume_spike_required,
    )
    res = run_backtest(df, params, log_trades=False)
    summ = res.summary()
    print(json.dumps(summ, ensure_ascii=False, indent=2))
    _print_human_summary(summ)

    if args.trades_jsonl:
        outp = Path(args.trades_jsonl)
        with outp.open("w", encoding="utf-8") as f:
            for t in res.trades:
                f.write(t.to_log_line() + "\n")
        print(f"trades written: {outp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
