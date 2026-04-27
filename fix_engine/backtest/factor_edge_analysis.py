"""
Factor-based edge validation: биннинг компонент signal_score, метрики по бинам,
ablation, interaction effects, предложение весов по эффекту (на малых выборках — осторожно).

Запуск (из корня репозитория):
  python -m fix_engine.backtest.factor_edge_analysis --csv fix_engine/backtest/history_1h_12m.csv
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fix_engine.backtest.experiment_metrics import extended_metrics
from fix_engine.strategy.signal_scoring import (
    SignalScorePolicy,
    SignalScoreWeights,
    renormalize_weights_drop,
)
from fix_engine.strategy.swing_breakout import SwingBreakoutParams, TradeRecord, load_ohlcv_csv
from fix_engine.strategy.swing_v2 import SwingV2Config, run_backtest_v2

SCORE_KEYS = (
    "score_impulse_strength",
    "score_atr_expansion",
    "score_retest_speed",
    "score_htf_score",
    "score_ema_distance",
    "score_candle_structure",
)

FACTOR_SHORT = {
    "score_impulse_strength": "impulse_strength",
    "score_atr_expansion": "atr_expansion",
    "score_retest_speed": "retest_speed",
    "score_htf_score": "htf_score",
    "score_ema_distance": "ema_distance",
    "score_candle_structure": "candle_structure",
}


def _profit_factor(pnls: list[float]) -> float | None:
    gp = sum(x for x in pnls if x > 0)
    gl = sum(-x for x in pnls if x < 0)
    if gl <= 1e-12:
        return None
    return gp / gl


def _pnl_wo_top1(pnls: list[float]) -> float:
    if not pnls:
        return 0.0
    s = sorted(pnls, reverse=True)
    return sum(s) - s[0]


def _winrate(pnls: list[float]) -> float | None:
    if not pnls:
        return None
    return sum(1 for x in pnls if x > 0) / len(pnls)


def trades_to_dataframe(trades: list[TradeRecord]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for t in trades:
        m = t.meta or {}
        row = {
            "pnl_net_rub": float(t.pnl_net_rub),
            "setup_tag": str(m.get("setup_tag", "") or ""),
            "signal_score": m.get("signal_score"),
        }
        for k in SCORE_KEYS:
            v = m.get(k)
            row[k] = float(v) if v is not None and v != "" and math.isfinite(float(v)) else np.nan
        for k in ("htf_score_raw", "ema_dist_atr_signed", "candle_body_frac"):
            v = m.get(k)
            row[k] = float(v) if v is not None and v != "" and math.isfinite(float(v)) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def bin_column_metrics(
    df: pd.DataFrame,
    col: str,
    *,
    n_bins: int = 5,
    label: str = "",
) -> pd.DataFrame:
    """Квантильные бины; при малых n — меньше бинов."""
    s = df[[col, "pnl_net_rub"]].dropna(subset=[col])
    if s.empty:
        return pd.DataFrame()
    n = len(s)
    q = min(n_bins, max(2, n // 3))
    try:
        s = s.copy()
        s["bin"] = pd.qcut(s[col], q=q, labels=False, duplicates="drop")
    except (ValueError, TypeError):
        return pd.DataFrame()
    out_rows: list[dict[str, Any]] = []
    for b in sorted(s["bin"].dropna().unique()):
        sub = s[s["bin"] == b]["pnl_net_rub"].tolist()
        pf = _profit_factor(sub)
        out_rows.append(
            {
                "factor": label or col,
                "bin": int(b),
                "n": len(sub),
                "avg_pnl": float(np.mean(sub)) if sub else 0.0,
                "winrate": _winrate(sub),
                "profit_factor": pf,
                "pnl_wo_top1": _pnl_wo_top1(sub),
            }
        )
    return pd.DataFrame(out_rows)


def quintile_effect(df: pd.DataFrame, col: str) -> float:
    """Средний PnL верхнего квинтиля минус нижнего по col (по score 0..1)."""
    s = df[[col, "pnl_net_rub"]].dropna(subset=[col])
    if len(s) < 10:
        return 0.0
    try:
        s = s.copy()
        s["q"] = pd.qcut(s[col], q=5, labels=False, duplicates="drop")
    except (ValueError, TypeError):
        return 0.0
    if s["q"].nunique() < 2:
        return 0.0
    means = s.groupby("q", observed=False)["pnl_net_rub"].mean()
    hi = float(means.max())
    lo = float(means.min())
    return hi - lo


def median_split_effect(df: pd.DataFrame, col: str) -> float:
    """Средний PnL выше медианы минус ниже (устойчивее на n < 30, чем 5 квинтилей)."""
    s = df[[col, "pnl_net_rub"]].dropna(subset=[col])
    if len(s) < 4:
        return 0.0
    med = float(s[col].median())
    hi = s.loc[s[col] >= med, "pnl_net_rub"]
    lo = s.loc[s[col] < med, "pnl_net_rub"]
    if hi.empty or lo.empty:
        return 0.0
    return float(hi.mean() - lo.mean())


def suggest_weights_from_effects(effects: dict[str, float]) -> SignalScoreWeights:
    """Веса пропорционально max(0, effect). Если все <= 0 — равные веса."""
    keys = [FACTOR_SHORT[k] for k in SCORE_KEYS]
    pos = {k: max(0.0, effects.get(k, 0.0)) for k in keys}
    tot = sum(pos.values())
    if tot <= 1e-12:
        v = 1.0 / len(keys)
        return SignalScoreWeights(
            w_impulse_strength=v,
            w_atr_expansion=v,
            w_retest=v,
            w_htf=v,
            w_heat=v,
            w_body=v,
        )
    return SignalScoreWeights(
        w_impulse_strength=pos["impulse_strength"] / tot,
        w_atr_expansion=pos["atr_expansion"] / tot,
        w_retest=pos["retest_speed"] / tot,
        w_htf=pos["htf_score"] / tot,
        w_heat=pos["ema_distance"] / tot,
        w_body=pos["candle_structure"] / tot,
    )


def run_baseline_and_collect(
    csv_path: Path,
    *,
    continuation_vol_mult: float,
) -> tuple[pd.DataFrame, SwingBreakoutParams]:
    df = load_ohlcv_csv(str(csv_path))
    p_soft = SwingBreakoutParams(volume_spike_required=False, htf_resample_rule="", htf_gate_mode="soft")
    cfg = SwingV2Config(
        enable_continuation=True,
        enable_ema_pullback=True,
        continuation_vol_mult=continuation_vol_mult,
        score_policy=None,
    )
    res = run_backtest_v2(df, p_soft, cfg=cfg, log_trades=False)
    tdf = trades_to_dataframe(res.trades)
    if tdf.empty:
        return tdf, p_soft
    # sanity: компоненты должны быть в meta
    miss = sum(1 for k in SCORE_KEYS if tdf[k].isna().all())
    if miss == len(SCORE_KEYS):
        raise RuntimeError(
            "В meta сделок нет score_* полей. Нужен run_backtest с intent_to_entry_meta(row=...)."
        )
    return tdf, p_soft


def ablation_backtests(
    csv_path: Path,
    *,
    continuation_vol_mult: float,
    min_score: float,
) -> pd.DataFrame:
    df = load_ohlcv_csv(str(csv_path))
    p_soft = SwingBreakoutParams(volume_spike_required=False, htf_resample_rule="", htf_gate_mode="soft")
    rows: list[dict[str, Any]] = []

    def one(name: str, pol: SignalScorePolicy | None) -> None:
        cfg = SwingV2Config(
            enable_continuation=True,
            enable_ema_pullback=True,
            continuation_vol_mult=continuation_vol_mult,
            score_policy=pol,
        )
        res = run_backtest_v2(df, p_soft, cfg=cfg, log_trades=False)
        m = extended_metrics(res, p_soft)
        rows.append(
            {
                "variant": name,
                "n_trades": m.get("n_trades"),
                "total_pnl_net_rub": m.get("total_pnl_net_rub"),
                "profit_factor": m.get("profit_factor"),
                "pnl_without_top_1_trade": m.get("pnl_without_top_1_trade"),
                "winrate": m.get("winrate"),
            }
        )

    one("score_filter_off", None)
    one(
        "legacy_impulse_combo_thr",
        SignalScorePolicy(
            min_score=min_score,
            weights=SignalScoreWeights(combine_impulse=True),
        ),
    )
    one(
        "split_factors_default_thr",
        SignalScorePolicy(min_score=min_score, weights=SignalScoreWeights(combine_impulse=False)),
    )
    for fk in FACTOR_SHORT.values():
        w = renormalize_weights_drop(SignalScoreWeights(), frozenset({fk}))
        one(f"ablation_drop_{fk}", SignalScorePolicy(min_score=min_score, weights=w))
    return pd.DataFrame(rows)


def interaction_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 8:
        return pd.DataFrame()
    out: list[dict[str, Any]] = []
    a = df["score_atr_expansion"]
    r = df["score_retest_speed"]
    med_a = a.median()
    med_r = r.median()
    hi_a = a >= med_a
    fast_r = r >= med_r
    # HTF + pullback tag
    tag = df["setup_tag"].astype(str)
    htf = df["score_htf_score"]
    med_h = htf.median()

    masks = [
        ("high_atr_expansion_and_fast_retest", hi_a & fast_r),
        ("high_atr_expansion_slow_retest", hi_a & ~fast_r),
        ("low_atr_expansion_fast_retest", ~hi_a & fast_r),
        ("htf_high_and_pullback", (htf >= med_h) & tag.str.contains("pullback", case=False)),
        ("htf_low_and_pullback", (htf < med_h) & tag.str.contains("pullback", case=False)),
    ]
    for name, msk in masks:
        sub = df.loc[msk, "pnl_net_rub"].tolist()
        if not sub:
            continue
        out.append(
            {
                "interaction": name,
                "n": len(sub),
                "avg_pnl": float(np.mean(sub)),
                "winrate": _winrate(sub),
                "profit_factor": _profit_factor(sub),
                "pnl_wo_top1": _pnl_wo_top1(sub),
            }
        )
    return pd.DataFrame(out)


def main() -> int:
    ap = argparse.ArgumentParser(description="Factor edge validation for signal_score components")
    ap.add_argument("--csv", type=str, default=str(Path(__file__).resolve().parent / "history_1h_12m.csv"))
    ap.add_argument("--out-dir", type=str, default="")
    ap.add_argument("--min-score", type=float, default=0.6)
    ap.add_argument("--continuation-vol-mult", type=float, default=1.35)
    ap.add_argument("--n-bins", type=int, default=5)
    args = ap.parse_args()
    csv_path = Path(args.csv)
    out_dir = Path(args.out_dir) if args.out_dir else csv_path.parent

    tdf, _ = run_baseline_and_collect(
        csv_path,
        continuation_vol_mult=args.continuation_vol_mult,
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=== Sample ===")
    print("n_trades:", len(tdf))
    if tdf.empty:
        print("Нет сделок — расширьте CSV или ослабьте параметры.")
        return 1

    print("\n=== Quintile spread (needs n>=10 for 5 bins) ===")
    effects_q: dict[str, float] = {}
    for sk in SCORE_KEYS:
        fk = FACTOR_SHORT[sk]
        e = quintile_effect(tdf, sk)
        effects_q[fk] = e
        print(f"  {fk}: {e:.4f}")

    print("\n=== Median split (high vs low factor score), spread of mean PnL — основной эвристик при малых n ===")
    effects_med: dict[str, float] = {}
    for sk in SCORE_KEYS:
        fk = FACTOR_SHORT[sk]
        e = median_split_effect(tdf, sk)
        effects_med[fk] = e
        print(f"  {fk}: {e:.4f}")

    print("\n=== Pearson corr(factor score, PnL) ===")
    for sk in SCORE_KEYS:
        fk = FACTOR_SHORT[sk]
        c = tdf[[sk, "pnl_net_rub"]].dropna()
        r = float(c[sk].corr(c["pnl_net_rub"])) if len(c) >= 3 else float("nan")
        print(f"  {fk}: {r:.4f}" if math.isfinite(r) else f"  {fk}: n/a")

    effects = effects_med
    w_suggested = suggest_weights_from_effects(effects)
    print("\n=== Suggested weights (from positive median-split effects; equal if none) ===")
    print(w_suggested)

    print("\n=== Binned metrics (quantile bins) ===")
    all_bin = []
    for sk in SCORE_KEYS:
        fk = FACTOR_SHORT[sk]
        if tdf[sk].dropna().nunique() <= 1:
            print(f"\n-- {fk} -- (skipped: no variance)")
            continue
        b = bin_column_metrics(tdf, sk, n_bins=args.n_bins, label=fk)
        if not b.empty:
            all_bin.append(b)
            print(f"\n-- {fk} --")
            print(b.to_string(index=False))
        else:
            print(f"\n-- {fk} -- (could not form bins)")
    if all_bin:
        pd.concat(all_bin, ignore_index=True).to_csv(out_dir / "factor_bins_metrics.csv", index=False)

    print("\n=== Interaction effects (medians split) ===")
    inter = interaction_summary(tdf)
    if not inter.empty:
        print(inter.to_string(index=False))
        inter.to_csv(out_dir / "factor_interactions.csv", index=False)

    print("\n=== Ablation (score threshold + drop one factor, renormalize) ===")
    abl = ablation_backtests(
        csv_path,
        continuation_vol_mult=args.continuation_vol_mult,
        min_score=args.min_score,
    )
    print(abl.to_string(index=False))
    abl.to_csv(out_dir / "factor_ablation_backtest.csv", index=False)

    pos_effects = {k: v for k, v in effects.items() if v > 0}
    neg_effects = {k: v for k, v in effects.items() if v < 0}
    print("\n=== Conclusion (heuristic; proof needs more n / OOS / bootstrap) ===")
    print(f"Positive median-split factors: {list(pos_effects.keys()) or 'none'}")
    print(f"Negative median-split: {list(neg_effects.keys()) or 'none'}")
    if not pos_effects and len(tdf) < 40:
        print(
            "No positive median-split edge on this sample. Likely: noise, small n, or no predictive power here.\n"
            "Next: longer history, OOS, new features (vol regime shift, failed breakout rate, liquidity sweep, time-of-day)."
        )

    summary = {
        "n_trades": len(tdf),
        "quintile_effects": effects_q,
        "median_split_effects": effects_med,
        "suggested_weights": w_suggested.__dict__,
        "min_score_used": args.min_score,
    }
    (out_dir / "factor_edge_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\nWrote {out_dir / 'factor_bins_metrics.csv'}, factor_ablation_backtest.csv, factor_edge_summary.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
