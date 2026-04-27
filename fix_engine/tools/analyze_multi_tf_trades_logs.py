"""
Сравнение нескольких trades_log.csv (разные таймфреймы бэктеста).

  python -m fix_engine.tools.analyze_multi_tf_trades_logs --manifest path/to/manifest.json
  python -m fix_engine.tools.analyze_multi_tf_trades_logs --manifest m.json --json-out out.json

Манифест: см. multi_tf_trades_manifest.example.json

Gate по умолчанию: n_trades >= 100, span_bars >= 300 (по entry_bar/exit_bar).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _estimate_span_bars(df) -> int | None:
    if "entry_bar" in df.columns and "exit_bar" in df.columns:
        try:
            lo = int(df["entry_bar"].min())
            hi = int(df["exit_bar"].max())
            return max(0, hi - lo + 1)
        except Exception:
            return None
    return None


def _load_manifest(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if "timeframes" not in data or not isinstance(data["timeframes"], list):
        raise SystemExit("manifest must contain 'timeframes' array")
    return data


def analyze_one(
    label: str,
    csv_path: Path,
    bar_minutes: float,
    min_segment_n: int,
) -> dict[str, Any]:
    from fix_engine.tools.analyze_swing_trades_log import build_report, load_trades_csv

    df = load_trades_csv(csv_path)
    rep = build_report(df, str(csv_path.resolve()), min_segment_n)
    span = _estimate_span_bars(df)
    rep["timeframe_label"] = label
    rep["bar_minutes"] = bar_minutes
    rep["span_bars_est"] = span
    if span is not None:
        rep["span_hours_est"] = span * float(bar_minutes) / 60.0
    else:
        rep["span_hours_est"] = None
    return rep


def _passes_tf_gate(rep: dict, min_trades: int, min_span: int) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    n = int(rep.get("summary", {}).get("n_trades", 0))
    if n < min_trades:
        reasons.append(f"n_trades={n} < {min_trades}")
    span = rep.get("span_bars_est")
    if span is None:
        reasons.append("span_bars unknown (CSV needs entry_bar, exit_bar)")
    elif span < min_span:
        reasons.append(f"span_bars={span} < {min_span}")
    return (len(reasons) == 0), reasons


def _rank_valid(
    reports: list[dict], valid_labels: set[str], key: str, reverse: bool
) -> list[tuple[str, float]]:
    rows: list[tuple[str, float]] = []
    for r in reports:
        lab = r.get("timeframe_label", "")
        if lab not in valid_labels:
            continue
        v = r.get("summary", {}).get(key)
        if v is None:
            continue
        rows.append((lab, float(v)))
    rows.sort(key=lambda x: x[1], reverse=reverse)
    return rows


def print_report(manifest: dict[str, Any], reports: list[dict]) -> None:
    min_tr = int(manifest.get("min_trades_per_timeframe", 100))
    min_span = int(manifest.get("min_span_bars", 300))
    min_seg = int(manifest.get("min_segment_n", 20))
    by_lab = {x["timeframe_label"]: x for x in reports}

    print("=" * 72)
    print("1. DATA QUALITY & METRICS PER TIMEFRAME")
    print("=" * 72)

    valid_labels: set[str] = set()
    for r in reports:
        lab = r["timeframe_label"]
        ok, why = _passes_tf_gate(r, min_tr, min_span)
        if ok:
            valid_labels.add(lab)
        s = r["summary"]
        gate = "PASS" if ok else "REJECT"
        print(f"\n[{lab}] gate={gate}")
        if not ok:
            print(f"  reasons: {', '.join(why)}")
        print(
            f"  n_trades={s.get('n_trades')}  span_bars~={r.get('span_bars_est')}  "
            f"span_h~={r.get('span_hours_est')}"
        )
        print(
            f"  winrate={s.get('winrate')}  avg_win={s.get('avg_win_rub')}  "
            f"avg_loss={s.get('avg_loss_rub')}"
        )
        print(
            f"  expectancy_rub={s.get('expectancy_rub')}  PF={s.get('profit_factor')}  "
            f"max_dd_rub={s.get('max_drawdown_rub')}"
        )

    print("\n" + "=" * 72)
    print("2. NON-LOSING ZONES (composite, n>=%d, exp>=0, PF>=1)" % min_seg)
    print("=" * 72)
    for r in reports:
        print(f"\n[{r['timeframe_label']}] count={len(r.get('non_losing_zones', []))}")
        for z in r.get("non_losing_zones", [])[:12]:
            print(
                f"  {z.get('segment')}  n={z.get('n_trades')}  "
                f"exp={z.get('expectancy_rub')}  PF={z.get('profit_factor')}"
            )
        if len(r.get("non_losing_zones", [])) > 12:
            print(f"  ... +{len(r['non_losing_zones']) - 12} more")

    print("\n" + "=" * 72)
    print("3. EDGE ZONES (exp>0, PF>1.2)")
    print("=" * 72)
    for r in reports:
        print(f"\n[{r['timeframe_label']}]")
        if not r.get("edge_zones"):
            print("  (none)")
        for z in r.get("edge_zones", []):
            print(
                f"  {z.get('segment')}  n={z.get('n_trades')}  "
                f"exp={z.get('expectancy_rub')}  PF={z.get('profit_factor')}"
            )

    print("\n" + "=" * 72)
    print("4. COMPARE (GATE=PASS only)")
    print("=" * 72)
    if not valid_labels:
        print("  No timeframe passed the gate.")
    else:
        print(f"  highest expectancy: {_rank_valid(reports, valid_labels, 'expectancy_rub', True)[:3]}")
        print(f"  highest PF: {_rank_valid(reports, valid_labels, 'profit_factor', True)[:3]}")
        print(f"  lowest max DD: {_rank_valid(reports, valid_labels, 'max_drawdown_rub', False)[:3]}")

    print("\n" + "=" * 72)
    print("5. TRADE FREQUENCY vs QUALITY")
    print("=" * 72)
    for r in reports:
        s = r["summary"]
        print(f"  [{r['timeframe_label']}] n={s.get('n_trades')}  expectancy={s.get('expectancy_rub')}")
    print("  Prefer higher expectancy per trade OOS; high n + negative exp = churn.")

    print("\n" + "=" * 72)
    print("6. TIMEFRAME VERDICT")
    print("=" * 72)
    be = _rank_valid(reports, valid_labels, "expectancy_rub", True)
    we = _rank_valid(reports, valid_labels, "expectancy_rub", False)
    if be:
        print(f"  BEST (expectancy): {be[0][0]}")
        print(f"  WORST (expectancy): {we[0][0]}")
    else:
        print("  BEST/WORST: undetermined.")

    print("\n" + "=" * 72)
    print("7. FINAL VERDICT")
    print("=" * 72)

    any_edge = False
    best_label = None
    best_exp_val = -1e18
    best_keep = ""

    for lab in valid_labels:
        r = by_lab[lab]
        s = r["summary"]
        exp = s.get("expectancy_rub")
        pf = s.get("profit_factor")
        if exp is not None and exp > 0 and pf is not None and pf > 1.0:
            any_edge = True
            if exp > best_exp_val:
                best_exp_val = exp
                best_label = lab
                best_keep = r.get("keep_rule", "")

    if not valid_labels:
        verdict = "DO NOT TRADE"
        why = "No TF passes gate (n>=%d, span>=%d bars, span known)." % (min_tr, min_span)
    elif not any_edge:
        verdict = "DO NOT TRADE"
        why = "No passing TF has expectancy>0 and PF>1 on full sample (in-sample)."
    else:
        verdict = "TRADE (conditional — verify OOS)"
        why = f"Passing TF: {best_label} (best positive exp among valid)."

    print(f"  {verdict}")
    print(f"  {why}")
    if best_label:
        print(f"  Focus timeframe: {best_label}")
    if best_keep:
        print(f"  Conditions (in-sample keep_rule): {best_keep}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Multi-timeframe trades_log analysis")
    ap.add_argument("--manifest", type=str, required=True, help="JSON manifest path")
    ap.add_argument("--json-out", type=str, default="", help="Optional full JSON dump")
    args = ap.parse_args()

    mp = Path(args.manifest)
    if not mp.is_file():
        print(f"Manifest not found: {mp}", file=sys.stderr)
        return 2

    manifest = _load_manifest(mp)
    min_seg = int(manifest.get("min_segment_n", 20))

    reports: list[dict] = []
    for tf in manifest["timeframes"]:
        label = str(tf.get("label", "")).strip() or "unnamed"
        csv_p = Path(str(tf["csv"]))
        if not csv_p.is_file():
            print(f"SKIP missing csv: {label} -> {csv_p}", file=sys.stderr)
            continue
        bar_m = float(tf.get("bar_minutes", 15))
        reports.append(analyze_one(label, csv_p, bar_m, min_seg))

    if not reports:
        print("No CSV files loaded.")
        return 1

    print_report(manifest, reports)

    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps({"manifest": manifest, "reports": reports}, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"\nWrote JSON: {args.json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
