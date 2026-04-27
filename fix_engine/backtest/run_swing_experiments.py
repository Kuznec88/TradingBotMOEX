"""
Пакетный бэктест swing_breakout по матрице конфигов (YAML/JSON).

Пример:
  python -m fix_engine.backtest.run_swing_experiments \\
    --csv fix_engine/backtest/history_1h_6m.csv \\
    --experiments fix_engine/backtest/experiments.yaml \\
    --out fix_engine/backtest/experiment_results.csv
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


def _load_yaml_or_json(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    suf = path.suffix.lower()
    if suf in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError as e:
            raise SystemExit(
                "Нужен PyYAML: pip install pyyaml (или укажите .json с тем же форматом)"
            ) from e
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("root must be object with 'configs' list")
    return data


def _normalize_overrides(d: dict[str, Any]) -> dict[str, Any]:
    """Импульс выкл.: оба порога в ноль (как в логике стратегии)."""
    out = dict(d)
    if out.get("impulse_strength") == 0:
        out["impulse_k"] = 0.0
    return out


def _row_from_metrics(
    config_id: str,
    overrides: dict[str, Any],
    m: dict[str, Any],
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "config_id": config_id,
        "trades_count": m.get("n_trades"),
        "pnl_net_rub": m.get("total_pnl_net_rub"),
        "winrate": m.get("winrate"),
        "profit_factor": m.get("profit_factor"),
        "max_drawdown_rub": m.get("max_drawdown_rub"),
        "avg_win_rub": m.get("avg_win_rub"),
        "avg_loss_rub": m.get("avg_loss_rub"),
        "expectancy_rub": m.get("expectancy_rub"),
        "avg_retest_bars": m.get("avg_retest_bars"),
        "avg_atr_slope": m.get("avg_atr_slope"),
        "avg_impulse_strength_ratio": m.get("avg_impulse_strength_ratio"),
        "pct_market_entries": m.get("pct_market_entries"),
        "n_market_entries": m.get("n_market_entries"),
        "n_limit_entries": m.get("n_limit_entries"),
        "pnl_without_top_1_trade": m.get("pnl_without_top_1_trade"),
        "pnl_without_top_2_trades": m.get("pnl_without_top_2_trades"),
        "interesting_candidate": m.get("interesting_candidate"),
        "params_json": json.dumps(overrides, ensure_ascii=False, sort_keys=True),
    }
    return row


def _print_top5(rows: list[dict[str, Any]]) -> None:
    def sort_key(r: dict[str, Any]) -> tuple:
        ic = bool(r.get("interesting_candidate"))
        n = int(r.get("trades_count") or 0)
        exp = r.get("expectancy_rub")
        exp_v = float(exp) if exp is not None else float("-inf")
        pnl1 = r.get("pnl_without_top_1_trade")
        pnl1_v = float(pnl1) if pnl1 is not None else float("-inf")
        return (ic, n, pnl1_v, exp_v)

    ranked = sorted(rows, key=sort_key, reverse=True)
    print("\n--- Top 5 (interesting first, then pnl w/o top1, expectancy, trades) ---")
    for i, r in enumerate(ranked[:5], 1):
        print(
            f"{i}. {r['config_id']}  trades={r['trades_count']}  "
            f"PF={r['profit_factor']}  exp={r['expectancy_rub']}  "
            f"pnl={r['pnl_net_rub']}  pnl_wo1={r['pnl_without_top_1_trade']}  "
            f"interesting={r['interesting_candidate']}"
        )


def _verdict(rows: list[dict[str, Any]]) -> None:
    any_interesting = any(r.get("interesting_candidate") for r in rows)
    best_n = max((int(r.get("trades_count") or 0) for r in rows), default=0)
    print("\n--- Edge verdict (heuristic, not statistical proof) ---")
    if any_interesting:
        print("ROBUST EDGE: есть конфиг(и), прошедшие жёсткие критерии (N≥30, PF>1.3, pnl_wo_top1>0).")
    elif best_n >= 15:
        print("POSSIBLE EDGE: сделок больше, чем в baseline, но ни один конфиг не прошёл все критерии «интересного».")
    else:
        print("NO EDGE (в рамках эксперимента): мало сделок или слабая устойчивость к выбросам / PF.")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    ap = argparse.ArgumentParser(description="Batch swing_breakout experiments on one CSV")
    ap.add_argument("--csv", type=str, required=True)
    ap.add_argument(
        "--experiments",
        type=str,
        default="",
        help="YAML или JSON: { defaults?: {}, configs: [{id, ...param overrides}] }",
    )
    ap.add_argument("--out", type=str, default="fix_engine/backtest/experiment_results.csv")
    args = ap.parse_args()

    fix_engine_dir = Path(__file__).resolve().parents[1]

    csv_path = Path(args.csv)
    if not csv_path.is_file():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    exp_path = Path(args.experiments) if args.experiments else fix_engine_dir / "backtest" / "experiments.yaml"
    if not exp_path.is_file():
        print(f"Experiments file not found: {exp_path}", file=sys.stderr)
        return 1

    data = _load_yaml_or_json(exp_path)
    defaults = data.get("defaults") or {}
    configs = data.get("configs")
    if not isinstance(configs, list) or not configs:
        print("configs must be non-empty list", file=sys.stderr)
        return 1

    df = load_ohlcv_csv(str(csv_path))
    base = SwingBreakoutParams()
    rows_out: list[dict[str, Any]] = []

    for raw in configs:
        if not isinstance(raw, dict):
            continue
        cfg = dict(raw)
        cid = str(cfg.pop("id", "") or cfg.pop("config_id", "") or "unnamed")
        desc = cfg.pop("description", None)
        merged = {**defaults, **cfg}
        if desc is not None:
            merged.pop("description", None)
        overrides = _normalize_overrides(merged)
        try:
            params = replace(base, **overrides)
        except TypeError as e:
            print(f"config {cid}: bad param keys: {e}", file=sys.stderr)
            return 1

        res = run_backtest(df, params, log_trades=False)
        m = extended_metrics(res, params)
        rows_out.append(_row_from_metrics(cid, overrides, m))

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    if not rows_out:
        print("no result rows", file=sys.stderr)
        return 1
    fieldnames = list(rows_out[0].keys())
    with outp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_out)

    print(f"Wrote {outp}  ({len(rows_out)} configs)")
    _print_top5(rows_out)
    _verdict(rows_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
