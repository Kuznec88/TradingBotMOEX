"""
Мульти-датасетный бэктест: где стратегия даёт больше сделок / устойчивость.

  python -m fix_engine.backtest.run_research_sweep \\
    --manifest fix_engine/backtest/research_manifest.yaml \\
    --out fix_engine/backtest/research_sweep_results.csv

Перед запуском подготовьте CSV (fetch_tbank_candles_history) по путям из манифеста.

Опционально у строки datasets задайте params: { ... } — поля SwingBreakoutParams (поверх baseline_params).
htf_resample_rule в строке всегда перезаписывает одноимённый ключ из params.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fix_engine.backtest.experiment_metrics import extended_metrics
from fix_engine.strategy.swing_breakout import SwingBreakoutParams, load_ohlcv_csv, run_backtest


def _load_manifest(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in (".yaml", ".yml"):
        import yaml  # type: ignore[import-untyped]

        return yaml.safe_load(text)
    return json.loads(text)


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", type=str, default="fix_engine/backtest/research_manifest.yaml")
    ap.add_argument("--out", type=str, default="fix_engine/backtest/research_sweep_results.csv")
    args = ap.parse_args()

    man_path = Path(args.manifest)
    if not man_path.is_file():
        print(f"manifest not found: {man_path}", file=sys.stderr)
        return 1

    data = _load_manifest(man_path)
    baseline = data.get("baseline_params") or {}
    datasets = data.get("datasets") or []
    if not isinstance(datasets, list) or not datasets:
        print("datasets must be non-empty list", file=sys.stderr)
        return 1

    base = SwingBreakoutParams()
    rows: list[dict[str, Any]] = []

    for ds in datasets:
        if not isinstance(ds, dict):
            continue
        run_id = str(ds.get("run_id", "run"))
        csv_rel = str(ds.get("csv", "")).strip()
        htf = str(ds.get("htf_resample_rule", "")).strip()
        label = str(ds.get("instrument_label", "")).strip()
        row_params = ds.get("params") or {}
        if not isinstance(row_params, dict):
            print(f"skip {run_id}: params must be a dict", file=sys.stderr)
            continue

        p_csv = (_ROOT / csv_rel).resolve() if csv_rel else None
        if p_csv is None or not p_csv.is_file():
            print(f"skip {run_id}: missing csv {csv_rel}")
            continue

        df = load_ohlcv_csv(str(p_csv))
        try:
            t0 = str(df.index.min()) if len(df) else ""
            t1 = str(df.index.max()) if len(df) else ""
        except Exception:
            t0, t1 = "", ""

        overrides = {**baseline, **row_params, "htf_resample_rule": htf}
        try:
            params = replace(base, **overrides)
        except TypeError as e:
            print(f"{run_id}: bad baseline_params key: {e}", file=sys.stderr)
            return 1

        res = run_backtest(df, params, log_trades=False)
        m = extended_metrics(res, params)

        row: dict[str, Any] = {
            "run_id": run_id,
            "instrument_label": label,
            "csv": csv_rel,
            "htf_resample_rule": htf,
            "bars": len(df),
            "time_first": t0,
            "time_last": t1,
            "params_row_json": json.dumps(row_params, ensure_ascii=False, sort_keys=True) if row_params else "",
        }
        row.update(m)
        rows.append(row)

    if not rows:
        print("no rows (all CSV missing?)", file=sys.stderr)
        return 1

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with outp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {outp} ({len(rows)} runs)\n")
    # Краткая сводка: сортировка по числу сделок, затем pnl_wo_top1
    def sk(r: dict[str, Any]) -> tuple:
        n = int(r.get("n_trades") or 0)
        p1 = r.get("pnl_without_top_1_trade")
        p1v = float(p1) if p1 is not None else float("-inf")
        return (n, p1v)

    ranked = sorted(rows, key=sk, reverse=True)
    print("--- Ranked by trade count (then pnl w/o top-1) ---")
    for r in ranked[:8]:
        print(
            f"{r['run_id']}: n={r.get('n_trades')} pnl={r.get('total_pnl_net_rub')} "
            f"PF={r.get('profit_factor')} pnl_wo1={r.get('pnl_without_top_1_trade')} "
            f"interesting={r.get('interesting_candidate')} "
            f"fast_retest%={r.get('pct_fast_retest_le2_bars')} "
            f"atr_ge_min%={r.get('pct_trades_atr_slope_ge_min_expansion')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
