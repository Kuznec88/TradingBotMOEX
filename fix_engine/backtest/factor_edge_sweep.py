"""
Sweep: baseline (без score) vs legacy impulse-combo vs split factors vs optional JSON-веса из factor_edge_analysis.

  python -m fix_engine.backtest.factor_edge_sweep --csv fix_engine/backtest/history_1h_12m.csv
  python -m fix_engine.backtest.factor_edge_sweep --weights-json fix_engine/backtest/factor_edge_summary.json
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fix_engine.backtest.experiment_metrics import extended_metrics
from fix_engine.strategy.signal_scoring import SignalScorePolicy, SignalScoreWeights
from fix_engine.strategy.swing_breakout import SwingBreakoutParams, load_ohlcv_csv
from fix_engine.strategy.swing_v2 import SwingV2Config, run_backtest_v2


def _row(name: str, res, params: SwingBreakoutParams) -> dict:
    m = extended_metrics(res, params)
    return {"variant": name, **{k: m[k] for k in sorted(m.keys())}}


def discover_ohlcv_csvs(directory: Path) -> list[Path]:
    """Только `history_*.csv` (полноценная история). `_hist*` — короткие сэмплы, в sweep не включаем."""
    skip = ("research_", "experiment_", "factor_")
    out: list[Path] = []
    for p in sorted(directory.iterdir()):
        if not p.is_file() or p.suffix.lower() != ".csv":
            continue
        n = p.name.lower()
        if any(n.startswith(s) for s in skip):
            continue
        if n.startswith("history_"):
            out.append(p)
    return out


def _weights_from_summary_json(path: Path) -> SignalScoreWeights | None:
    if not path.is_file():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    sw = data.get("suggested_weights")
    if not isinstance(sw, dict):
        return None
    return SignalScoreWeights(
        combine_impulse=False,
        w_impulse_strength=float(sw.get("w_impulse_strength", 1 / 6)),
        w_atr_expansion=float(sw.get("w_atr_expansion", 1 / 6)),
        w_retest=float(sw.get("w_retest", 1 / 6)),
        w_htf=float(sw.get("w_htf", 1 / 6)),
        w_heat=float(sw.get("w_heat", 1 / 6)),
        w_body=float(sw.get("w_body", 1 / 6)),
    )


def run_factor_sweep(
    csv_path: str | Path,
    *,
    min_score: float = 0.6,
    top_n: int = 3,
    continuation_vol_mult: float = 1.35,
    weights_json: str | Path | None = None,
    frame: str | None = None,
) -> list[dict]:
    """Один CSV: baseline, legacy score, split factors, factor JSON (если есть), top-N/день."""
    csv_path = Path(csv_path)
    label = frame or csv_path.name
    df = load_ohlcv_csv(str(csv_path))
    p_soft = SwingBreakoutParams(volume_spike_required=False, htf_resample_rule="", htf_gate_mode="soft")
    rows: list[dict] = []

    cfg_base = SwingV2Config(
        enable_continuation=True,
        enable_ema_pullback=True,
        continuation_vol_mult=continuation_vol_mult,
        score_policy=None,
    )
    r0 = run_backtest_v2(df, p_soft, cfg=cfg_base, log_trades=False)
    rows.append(_row("baseline_no_score_filter", r0, p_soft))

    pol_legacy = SignalScorePolicy(
        min_score=float(min_score),
        weights=SignalScoreWeights(combine_impulse=True),
    )
    cfg_l = SwingV2Config(
        enable_continuation=True,
        enable_ema_pullback=True,
        continuation_vol_mult=continuation_vol_mult,
        score_policy=pol_legacy,
    )
    r1 = run_backtest_v2(df, p_soft, cfg=cfg_l, log_trades=False)
    rows.append(_row(f"old_score_legacy_combo_min_{min_score:g}", r1, p_soft))

    pol_split = SignalScorePolicy(
        min_score=float(min_score),
        weights=SignalScoreWeights(combine_impulse=False),
    )
    cfg_s = SwingV2Config(
        enable_continuation=True,
        enable_ema_pullback=True,
        continuation_vol_mult=continuation_vol_mult,
        score_policy=pol_split,
    )
    r2 = run_backtest_v2(df, p_soft, cfg=cfg_s, log_trades=False)
    rows.append(_row(f"split_factors_default_min_{min_score:g}", r2, p_soft))

    wj = Path(weights_json) if weights_json else Path(__file__).resolve().parent / "factor_edge_summary.json"
    w_custom = _weights_from_summary_json(wj)
    if w_custom is not None:
        pol_f = SignalScorePolicy(min_score=float(min_score), weights=w_custom)
        cfg_f = SwingV2Config(
            enable_continuation=True,
            enable_ema_pullback=True,
            continuation_vol_mult=continuation_vol_mult,
            score_policy=pol_f,
        )
        r3 = run_backtest_v2(df, p_soft, cfg=cfg_f, log_trades=False)
        rows.append(_row(f"factor_weights_from_{wj.name}_min_{min_score:g}", r3, p_soft))

    pol_topn = SignalScorePolicy(
        min_score=max(0.45, float(min_score) - 0.1),
        use_top_n_per_day=True,
        top_n_per_day=max(1, int(top_n)),
        weights=SignalScoreWeights(combine_impulse=False),
    )
    cfg_t = SwingV2Config(
        enable_continuation=True,
        enable_ema_pullback=True,
        continuation_vol_mult=continuation_vol_mult,
        score_policy=pol_topn,
    )
    r4 = run_backtest_v2(df, p_soft, cfg=cfg_t, log_trades=False)
    rows.append(_row(f"split_top{top_n}_per_day_min{pol_topn.min_score:g}", r4, p_soft))

    for r in rows:
        r["frame"] = label
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=str, default=str(Path(__file__).resolve().parent / "history_1h_12m.csv"))
    ap.add_argument("--out", type=str, default="")
    ap.add_argument("--min-score", type=float, default=0.6)
    ap.add_argument("--top-n", type=int, default=3)
    ap.add_argument("--continuation-vol-mult", type=float, default=1.35)
    ap.add_argument(
        "--weights-json",
        type=str,
        default="",
        help="factor_edge_summary.json с полем suggested_weights",
    )
    ap.add_argument(
        "--batch-dir",
        type=str,
        default="",
        help="Если задан — прогон по всем history_*.csv / _hist*.csv в каталоге (игнор --csv).",
    )
    args = ap.parse_args()

    keys_show = (
        "n_trades",
        "total_pnl_net_rub",
        "profit_factor",
        "pnl_without_top_1_trade",
        "sharpe_like_pnl",
        "max_drawdown_rub",
        "avg_signal_score",
        "interesting_candidate",
        "trades_by_setup_tag_json",
    )

    if args.batch_dir:
        bdir = Path(args.batch_dir)
        paths = discover_ohlcv_csvs(bdir)
        if not paths:
            print("No OHLCV csv files found in", bdir)
            return 1
        all_rows: list[dict] = []
        wj_arg = args.weights_json if args.weights_json else None
        for p in paths:
            try:
                part = run_factor_sweep(
                    p,
                    min_score=args.min_score,
                    top_n=args.top_n,
                    continuation_vol_mult=args.continuation_vol_mult,
                    weights_json=wj_arg,
                    frame=p.name,
                )
                all_rows.extend(part)
            except Exception as exc:
                all_rows.append(
                    {
                        "frame": p.name,
                        "variant": f"ERROR: {exc}",
                        "n_trades": None,
                        "total_pnl_net_rub": None,
                        "profit_factor": None,
                    }
                )
        rows = all_rows
        # Печать: сначала по фрейму блоки с ключевыми метриками
        by_frame: dict[str, list[dict]] = {}
        for r in rows:
            fr = str(r.get("frame", "?"))
            by_frame.setdefault(fr, []).append(r)
        for fr in sorted(by_frame.keys()):
            print(f"\n=== {fr} ===")
            for r in by_frame[fr]:
                if "variant" not in r:
                    continue
                parts = [r["variant"]]
                for k in keys_show:
                    v = r.get(k)
                    if v is None:
                        parts.append(f"{k}=—")
                    elif isinstance(v, float):
                        parts.append(f"{k}={v:.4f}")
                    else:
                        parts.append(f"{k}={v}")
                print(" | ".join(parts))
    else:
        wj = args.weights_json if args.weights_json else None
        rows = run_factor_sweep(
            args.csv,
            min_score=args.min_score,
            top_n=args.top_n,
            continuation_vol_mult=args.continuation_vol_mult,
            weights_json=wj,
            frame=Path(args.csv).name,
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

    if args.out and rows:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        fn = ["frame", "variant"]
        rest = sorted({k for r in rows for k in r if k not in fn})
        fieldnames = fn + rest
        with outp.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in fieldnames})
        print("wrote", outp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
