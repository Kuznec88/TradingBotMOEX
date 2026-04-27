"""Сравнение baseline v2 vs signal_score (threshold) vs top-N/день на одном CSV."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fix_engine.backtest.experiment_metrics import extended_metrics
from fix_engine.strategy.signal_scoring import SignalScorePolicy
from fix_engine.strategy.swing_breakout import SwingBreakoutParams, load_ohlcv_csv
from fix_engine.strategy.swing_v2 import SwingV2Config, run_backtest_v2


def _row(name: str, res, params: SwingBreakoutParams) -> dict:
    m = extended_metrics(res, params)
    return {"variant": name, **{k: m[k] for k in sorted(m.keys())}}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=str, default=str(Path(__file__).resolve().parent / "history_1h_12m.csv"))
    ap.add_argument("--out", type=str, default="")
    ap.add_argument("--min-score", type=float, default=0.6, help="Порог signal_score для режима score_thr")
    ap.add_argument("--top-n", type=int, default=3, help="Top-N сигналов за день (режим topn)")
    ap.add_argument("--continuation-vol-mult", type=float, default=1.35)
    args = ap.parse_args()

    df = load_ohlcv_csv(args.csv)
    p_soft = SwingBreakoutParams(volume_spike_required=False, htf_resample_rule="", htf_gate_mode="soft")

    rows: list[dict] = []

    # Baseline v2: ensemble on, без фильтра по score
    cfg_base = SwingV2Config(
        enable_continuation=True,
        enable_ema_pullback=True,
        continuation_vol_mult=args.continuation_vol_mult,
        score_policy=None,
    )
    r0 = run_backtest_v2(df, p_soft, cfg=cfg_base, log_trades=False)
    rows.append(_row("v2_baseline_no_score_filter", r0, p_soft))

    pol_thr = SignalScorePolicy(
        min_score=float(args.min_score),
        use_top_n_per_day=False,
    )
    cfg_thr = SwingV2Config(
        enable_continuation=True,
        enable_ema_pullback=True,
        continuation_vol_mult=args.continuation_vol_mult,
        score_policy=pol_thr,
    )
    r1 = run_backtest_v2(df, p_soft, cfg=cfg_thr, log_trades=False)
    rows.append(_row(f"v2_score_min_{args.min_score:g}", r1, p_soft))

    pol_topn = SignalScorePolicy(
        min_score=max(0.45, float(args.min_score) - 0.1),
        use_top_n_per_day=True,
        top_n_per_day=max(1, int(args.top_n)),
    )
    cfg_topn = SwingV2Config(
        enable_continuation=True,
        enable_ema_pullback=True,
        continuation_vol_mult=args.continuation_vol_mult,
        score_policy=pol_topn,
    )
    r2 = run_backtest_v2(df, p_soft, cfg=cfg_topn, log_trades=False)
    rows.append(_row(f"v2_top{args.top_n}_per_day_min{pol_topn.min_score:g}", r2, p_soft))

    keys_show = (
        "n_trades",
        "total_pnl_net_rub",
        "profit_factor",
        "pnl_without_top_1_trade",
        "sharpe_like_pnl",
        "max_drawdown_rub",
        "avg_signal_score",
        "trades_by_setup_tag_json",
    )
    for r in rows:
        parts = [f"{r['variant']}"]
        for k in keys_show:
            v = r.get(k)
            if v is None:
                parts.append(f"{k}=—")
            elif isinstance(v, float):
                parts.append(f"{k}={v:.4f}")
            else:
                parts.append(f"{k}={v}")
        print(" | ".join(parts))

    if args.out:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        keys = list(rows[0].keys())
        with outp.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(rows)
        print("wrote", outp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
