"""
Количественный разбор trades_log.csv (breakout-retest futures).

Запуск:
  python -m fix_engine.tools.analyze_swing_trades_log --csv path/to/trades_log.csv
  python -m fix_engine.tools.analyze_swing_trades_log --demo
  python -m fix_engine.tools.analyze_swing_trades_log --csv x.csv --json

Ограничения: один лог = in-sample; edge подтверждайте OOS/walk-forward.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path


def load_trades_csv(path: Path):
    import pandas as pd

    df = pd.read_csv(path)
    col_pnl = next((c for c in ("pnl_net_rub", "pnl", "PnL") if c in df.columns), None)
    if col_pnl is None:
        raise SystemExit("CSV must contain pnl_net_rub or pnl")
    df = df.rename(columns={col_pnl: "pnl_net_rub"})
    if "impulse_strength_ratio" not in df.columns and "impulse_strength" in df.columns:
        df = df.rename(columns={"impulse_strength": "impulse_strength_ratio"})
    if "retest_bars_to_touch" not in df.columns and "retest_bars" in df.columns:
        df = df.rename(columns={"retest_bars": "retest_bars_to_touch"})
    if "rr_multiple" not in df.columns and "RR" in df.columns:
        df = df.rename(columns={"RR": "rr_multiple"})
    df["pnl_net_rub"] = pd.to_numeric(df["pnl_net_rub"], errors="coerce")
    for c in ("impulse_strength_ratio", "atr_slope", "retest_bars_to_touch", "rr_multiple", "commission_to_gross"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "entry_reason" in df.columns:
        er = df["entry_reason"].astype(str).str.lower()
        df["entry_type"] = er.apply(
            lambda s: "market" if "market" in s or "strong_breakout" in s else "limit"
        )
    elif "use_market_entry" in df.columns:
        df["entry_type"] = df["use_market_entry"].apply(
            lambda x: "market" if str(x).lower() in {"1", "true", "y", "yes"} else "limit"
        )
    else:
        df["entry_type"] = "unknown"
    df = df.dropna(subset=["pnl_net_rub"])
    df = df.reset_index(drop=True)
    return df


def core_metrics(pnl) -> dict:
    import pandas as pd

    pnl = pnl.astype(float)
    n = int(len(pnl))
    if n == 0:
        return {
            "n_trades": 0,
            "winrate": None,
            "avg_win_rub": None,
            "avg_loss_rub": None,
            "expectancy_rub": None,
            "profit_factor": None,
            "max_drawdown_rub": 0.0,
        }
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    wr = float((pnl > 0).mean())
    aw = float(wins.mean()) if len(wins) else None
    al = float(losses.mean()) if len(losses) else None
    exp = float(pnl.mean())
    gp = float(wins.sum()) if len(wins) else 0.0
    gl = abs(float(losses.sum())) if len(losses) else 0.0
    pf = (gp / gl) if gl > 1e-9 else None
    eq = pnl.cumsum()
    peak = eq.cummax()
    dd = float((peak - eq).max())
    return {
        "n_trades": n,
        "winrate": wr,
        "avg_win_rub": aw,
        "avg_loss_rub": al,
        "expectancy_rub": exp,
        "profit_factor": pf,
        "max_drawdown_rub": dd,
    }


def atr_slope_bucket(x: float | None) -> str | None:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    if x < 1.0:
        return "<1.0"
    if x <= 1.1:
        return "1.0-1.1"
    return ">1.1"


def retest_bucket(x: float | None) -> str | None:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    xi = int(x)
    if xi <= 2:
        return "fast_1_2"
    if xi <= 5:
        return "medium_3_5"
    return "slow_gt5"


def impulse_tertile_labels(df) -> tuple[list[str] | None, float | None, float | None]:
    """Низкий/средний/высокий импульс = 1/3 и 2/3 квантили по выборке."""
    import pandas as pd

    if "impulse_strength_ratio" not in df.columns:
        return None, None, None
    s = df["impulse_strength_ratio"].dropna()
    if len(s) < 9:
        return None, None, None
    q1, q2 = float(s.quantile(1.0 / 3.0)), float(s.quantile(2.0 / 3.0))

    def lab(x) -> str | None:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        if float(x) <= q1:
            return "impulse_low_q"
        if float(x) <= q2:
            return "impulse_mid_q"
        return "impulse_high_q"

    return [lab(v) for v in df["impulse_strength_ratio"]], q1, q2


def attach_segment_columns(df):
    import pandas as pd

    d = df.copy()
    if "atr_slope" in d.columns:
        d["_atr_b"] = d["atr_slope"].apply(atr_slope_bucket)
    else:
        d["_atr_b"] = None
    labs, q1, q2 = impulse_tertile_labels(d)
    d["_imp_q"] = labs if labs is not None else None
    if "retest_bars_to_touch" in d.columns:
        d["_ret_b"] = d["retest_bars_to_touch"].apply(retest_bucket)
    else:
        d["_ret_b"] = None
    d["_ent"] = d["entry_type"].astype(str)

    def _sk(col: str) -> "pd.Series":
        import pandas as pd

        if col not in d.columns:
            return pd.Series(["NA"] * len(d), index=d.index)
        s = d[col].astype(object)
        return s.where(s.notna(), "NA").astype(str)

    d["_seg_key"] = _sk("_atr_b") + "|" + _sk("_imp_q") + "|" + _sk("_ret_b") + "|" + d["_ent"].astype(str)
    return d, (q1, q2)


def segment_row(name: str, pnl) -> dict:
    m = core_metrics(pnl)
    m["segment"] = name
    return m


def one_dimensional_segments(df, min_n: int) -> list[dict]:
    """Бакеты по одной оси (для отчёта и remove-списка)."""
    import pandas as pd

    out: list[dict] = []
    d, _ = attach_segment_columns(df)
    if d["_imp_q"] is not None:
        for b in ["impulse_low_q", "impulse_mid_q", "impulse_high_q"]:
            sub = d.loc[d["_imp_q"] == b, "pnl_net_rub"]
            if len(sub) >= min_n:
                out.append(segment_row(b, sub))
            elif len(sub) > 0:
                out.append({"segment": b, "n_trades": len(sub), "note": f"below_min_n={min_n}"})
    if "_atr_b" in d.columns and d["_atr_b"].notna().any():
        for b in ["<1.0", "1.0-1.1", ">1.1"]:
            sub = d.loc[d["_atr_b"] == b, "pnl_net_rub"]
            if len(sub) >= min_n:
                out.append(segment_row(f"atr_{b}", sub))
            elif len(sub) > 0:
                out.append({"segment": f"atr_{b}", "n_trades": len(sub), "note": f"below_min_n={min_n}"})
    if "_ret_b" in d.columns and d["_ret_b"].notna().any():
        for b in ["fast_1_2", "medium_3_5", "slow_gt5"]:
            sub = d.loc[d["_ret_b"] == b, "pnl_net_rub"]
            if len(sub) >= min_n:
                out.append(segment_row(f"retest_{b}", sub))
            elif len(sub) > 0:
                out.append({"segment": f"retest_{b}", "n_trades": len(sub), "note": f"below_min_n={min_n}"})
    for et in d["_ent"].dropna().unique():
        if str(et) == "unknown":
            continue
        sub = d.loc[d["_ent"] == et, "pnl_net_rub"]
        if len(sub) >= min_n:
            out.append(segment_row(f"entry_{et}", sub))
        elif len(sub) > 0:
            out.append({"segment": f"entry_{et}", "n_trades": len(sub), "note": f"below_min_n={min_n}"})
    return out


def composite_segment_stats(df, min_n: int) -> list[dict]:
    """4D ячейки atr|impulse|retest|entry."""
    import pandas as pd

    d, _ = attach_segment_columns(df)
    rows: list[dict] = []
    for key, grp in d.groupby("_seg_key", dropna=False):
        pnl = grp["pnl_net_rub"]
        if len(pnl) < min_n:
            continue
        m = core_metrics(pnl)
        m["segment"] = str(key)
        m["seg_key"] = str(key)
        rows.append(m)
    rows.sort(key=lambda x: (x.get("expectancy_rub") is None, x.get("expectancy_rub", 0.0)))
    return rows


def filter_segments(
    segments: list[dict],
    *,
    min_n: int,
    exp_min: float | None = None,
    exp_max: float | None = None,
    pf_min: float | None = None,
    pf_max: float | None = None,
) -> list[dict]:
    out = []
    for s in segments:
        n = int(s.get("n_trades", 0))
        if n < min_n or "expectancy_rub" not in s:
            continue
        exp = s["expectancy_rub"]
        pf = s.get("profit_factor")
        if exp_min is not None and exp < exp_min:
            continue
        if exp_max is not None and exp > exp_max:
            continue
        if pf_min is not None:
            if pf is None:
                pass  # все прибыли; считаем PF >= любого порога
            elif pf < pf_min:
                continue
        if pf_max is not None and (pf is not None and pf > pf_max):
            continue
        out.append(s)
    return out


def simulate_remove_worst_composite_fraction(df, min_n: int, frac: float) -> tuple[object, dict]:
    """Удалить сделки из худших ceil(frac * K) композитных сегментов (K = число сегментов с n>=min_n)."""
    import pandas as pd

    d, _ = attach_segment_columns(df)
    comp = composite_segment_stats(df, min_n=min_n)
    if not comp:
        return df, core_metrics(df["pnl_net_rub"])
    k_remove = max(1, int(math.ceil(len(comp) * frac)))
    worst_keys = {c["seg_key"] for c in comp[:k_remove]}
    keep = ~d["_seg_key"].isin(worst_keys)
    # сделки в «разреженных» ячейках (не в comp) остаются
    sparse = ~d["_seg_key"].isin({c["seg_key"] for c in comp})
    mask = keep | sparse
    cols = [c for c in df.columns if c in d.columns]
    filt = d.loc[mask, cols]
    return filt, core_metrics(filt["pnl_net_rub"])


def ranges_from_edge_trades(df, edge_seg_keys: set[str]) -> dict:
    """Min/max по сделкам, попавшим в EDGE-композитные сегменты."""
    import pandas as pd

    d, _ = attach_segment_columns(df)
    sub = d[d["_seg_key"].isin(edge_seg_keys)]
    if sub.empty:
        return {}
    out: dict[str, str] = {}
    if "atr_slope" in sub.columns:
        a = sub["atr_slope"].dropna()
        if len(a):
            out["atr_slope_min_max"] = f"{float(a.min()):.4f}-{float(a.max()):.4f}"
    if "impulse_strength_ratio" in sub.columns:
        i = sub["impulse_strength_ratio"].dropna()
        if len(i):
            out["impulse_strength_min_max"] = f"{float(i.min()):.4f}-{float(i.max()):.4f}"
    if "retest_bars_to_touch" in sub.columns:
        r = sub["retest_bars_to_touch"].dropna()
        if len(r):
            out["retest_bars_interval"] = f"{int(r.min())}-{int(r.max())}"
    return out


def loss_streak_features(df) -> dict:
    pnl = df["pnl_net_rub"].tolist()
    after_0: list[float] = []
    after_1: list[float] = []
    after_2p: list[float] = []
    streak = 0
    for x in pnl:
        if streak == 0:
            after_0.append(x)
        elif streak == 1:
            after_1.append(x)
        else:
            after_2p.append(x)
        if x < 0:
            streak += 1
        else:
            streak = 0

    def _m(xs: list[float]) -> dict:
        import pandas as pd

        if not xs:
            return {"n": 0}
        s = pd.Series(xs)
        return {**core_metrics(s), "n": len(xs)}

    return {"after_0_losses": _m(after_0), "after_1_loss": _m(after_1), "after_2plus_losses": _m(after_2p)}


def commission_analysis(df) -> dict:
    if "commission_to_gross" not in df.columns:
        return {"note": "commission_to_gross column missing"}
    cg = df["commission_to_gross"].astype(float)
    gross_pos = df["pnl_gross_rub"].astype(float) > 0 if "pnl_gross_rub" in df.columns else df["pnl_net_rub"] > 0
    winish = df["pnl_net_rub"] > 0
    mask = gross_pos & winish & cg.notna()
    sub = df.loc[mask]
    high = sub.loc[sub["commission_to_gross"].astype(float) > 0.3]
    n_all = max(len(df), 1)
    out = {
        "winning_trades_commission_to_gross_gt_30pct": int(len(high)),
        "pct_of_all_trades": float(len(high) / n_all * 100),
        "winners_with_gross_positive_n": int(len(sub)),
    }
    rr = df["rr_multiple"].dropna().astype(float) if "rr_multiple" in df.columns else None
    if rr is not None and len(rr):
        out["median_rr_all"] = float(rr.median())
    out["suggest_min_rr"] = (
        "If >30% of winners have commission_to_gross>0.30, raise target RR or commission_min_profit_mult; "
        "aim median commission_to_gross on winners <0.20-0.25 when gross_win is small."
    )
    return out


def market_vs_limit(df, min_n: int) -> dict:
    out = {}
    for et in ("market", "limit"):
        sub = df.loc[df["entry_type"] == et, "pnl_net_rub"]
        if len(sub) >= min_n:
            out[et] = core_metrics(sub)
        else:
            out[et] = {"n_trades": len(sub), "note": f"below_min_n={min_n}"}
    return out


def market_limit_verdict_text(mv: dict, min_n: int) -> str:
    m = mv.get("market", {})
    l = mv.get("limit", {})
    me = m.get("expectancy_rub")
    le = l.get("expectancy_rub")
    mn = m.get("n_trades", 0)
    ln = l.get("n_trades", 0)
    if isinstance(mn, int) and mn < min_n and isinstance(ln, int) and ln < min_n:
        return "Insufficient n for market and limit; no verdict."
    if me is not None and le is not None and mn >= min_n and ln >= min_n:
        if me > le and (m.get("profit_factor") or 0) >= (l.get("profit_factor") or 0):
            return (
                "Market shows higher expectancy with comparable or better PF; prefer market on strong-breakout "
                "conditions (if OOS confirms). Limit retains more selective fills — compare winrate vs expectancy."
            )
        if le > me:
            return (
                "Limit entries show higher expectancy in this log; market may pay spread/slippage. "
                "Use market only when impulse/ATR zones justify it OOS."
            )
    if me is not None and le is not None and mn >= min_n and ln >= min_n:
        return "Mixed; compare PF and drawdown, not only expectancy."
    return "One side below min_segment_n; interpret the side with sufficient n only."


def final_verdict(summary: dict, non_losing: list, edge: list, n_original: int) -> dict:
    exp = summary.get("expectancy_rub")
    pf = summary.get("profit_factor")
    n = summary.get("n_trades", 0)
    edge_n = sum(int(s.get("n_trades", 0)) for s in edge)
    text_parts = []

    if n < 30:
        verdict = "NO"
        text_parts.append(f"Sample too small (n={n}); cannot support a REAL edge claim.")
    elif exp is not None and exp > 0 and pf is not None and pf > 1.0:
        verdict = "YES"
        text_parts.append(f"Full sample: expectancy={exp:.2f}, PF={pf:.2f}, n={n}.")
    elif exp is not None and exp <= 0 and (pf is None or pf <= 1.0):
        verdict = "NO"
        text_parts.append(f"Full sample not profitable (expectancy={exp}, PF={pf}).")
    else:
        verdict = "CONDITIONAL"
        text_parts.append("Full sample mixed; see EDGE zones only.")

    if edge and edge_n >= 20:
        text_parts.append(
            f"EDGE composite cells cover {edge_n} trades (overlapping count if same trade in multiple keys — here per-cell); validate overlap manually."
        )
    if not edge and verdict == "YES":
        text_parts.append("No composite EDGE cells met strict thresholds; edge is whole-sample only (fragile).")

    return {"verdict": verdict, "explanation": " ".join(text_parts)}


def keep_rule_sentence(non_losing: list, edge: list, ranges: dict, min_n: int) -> str:
    if not non_losing and not edge:
        return "Cannot define a keep rule: no segment meets min_n and non-losing thresholds."
    parts = []
    if edge:
        parts.append(
            f"Prioritize trades in composite EDGE cells (expectancy>0, PF>1.2, n>={min_n}); see EDGE ZONES list."
        )
    if ranges:
        parts.append(
            "Rough feature window from EDGE trades: "
            + ", ".join(f"{k}={v}" for k, v in ranges.items())
            + " (in-sample hull, not causal thresholds)."
        )
    if not parts:
        parts.append("Use non-losing composite segments from report; confirm OOS.")
    return " ".join(parts)


def print_text_report(rep: dict) -> None:
    print("=" * 72)
    print("1. SUMMARY METRICS")
    print("=" * 72)
    s = rep["summary"]
    print(
        f"  n_trades={s.get('n_trades')}  winrate={s.get('winrate')}  "
        f"avg_win={s.get('avg_win_rub')}  avg_loss={s.get('avg_loss_rub')}"
    )
    print(
        f"  expectancy_rub={s.get('expectancy_rub')}  profit_factor={s.get('profit_factor')}  "
        f"max_drawdown_rub={s.get('max_drawdown_rub')}"
    )

    print("\n" + "=" * 72)
    print("2. NON-LOSING ZONES (expectancy>=0, PF>=1.0, composite n>=min_segment_n)")
    print("=" * 72)
    for z in rep.get("non_losing_zones", []):
        ex = z.get("expectancy_rub")
        exs = f"{ex:.4f}" if isinstance(ex, (int, float)) else str(ex)
        print(f"  {z.get('segment')}  n={z.get('n_trades')}  exp={exs}  PF={z.get('profit_factor')}")
    if not rep.get("non_losing_zones"):
        print("  (none meeting criteria)")

    print("\n" + "=" * 72)
    print("3. EDGE ZONES (expectancy>0, PF>1.2)")
    print("=" * 72)
    for z in rep.get("edge_zones", []):
        ex = z.get("expectancy_rub")
        exs = f"{ex:.4f}" if isinstance(ex, (int, float)) else str(ex)
        print(f"  {z.get('segment')}  n={z.get('n_trades')}  exp={exs}  PF={z.get('profit_factor')}")
    if not rep.get("edge_zones"):
        print("  (none)")

    print("\n" + "=" * 72)
    print("4. CONDITIONS TO REMOVE (expectancy<0, PF<1, n>=min)")
    print("=" * 72)
    for z in rep.get("remove_zones", []):
        ex = z.get("expectancy_rub")
        exs = f"{ex:.4f}" if isinstance(ex, (int, float)) else str(ex)
        print(f"  {z.get('segment')}  n={z.get('n_trades')}  exp={exs}  PF={z.get('profit_factor')}")
    if not rep.get("remove_zones"):
        print("  (none meeting criteria)")

    print("\n" + "=" * 72)
    print("5. TRADE REDUCTION (remove worst fraction of composite segments by expectancy)")
    print("=" * 72)
    for lab, m in rep.get("reduction_simulation", {}).items():
        print(f"  {lab}: n={m.get('n_trades')} exp={m.get('expectancy_rub')} PF={m.get('profit_factor')} MDD={m.get('max_drawdown_rub')}")

    print("\n" + "=" * 72)
    print("6. PARAMETER RANGES (from EDGE trades, min-max hull)")
    print("=" * 72)
    for k, v in rep.get("parameter_ranges_edge", {}).items():
        print(f"  {k}: {v}")
    if not rep.get("parameter_ranges_edge"):
        print("  (insufficient EDGE trades with features)")

    print("\n" + "=" * 72)
    print("7. MARKET vs LIMIT")
    print("=" * 72)
    print(json.dumps(rep.get("market_vs_limit", {}), ensure_ascii=False, indent=2))
    print(f"\n  Verdict: {rep.get('market_limit_verdict', '')}")

    print("\n" + "=" * 72)
    print("8. COMMISSION")
    print("=" * 72)
    print(json.dumps(rep.get("commission", {}), ensure_ascii=False, indent=2))

    print("\n" + "=" * 72)
    print("9. LOSS STREAK (order of rows = sequence)")
    print("=" * 72)
    print(json.dumps(rep.get("loss_streak", {}), ensure_ascii=False, indent=2))

    print("\n" + "=" * 72)
    print("10. FINAL VERDICT")
    print("=" * 72)
    fv = rep.get("final_verdict", {})
    print(f"  {fv.get('verdict')}: {fv.get('explanation')}")
    print(f"\n  KEEP TRADES ONLY WHEN:\n  {rep.get('keep_rule', '')}\n")


def demo_frame():
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(42)
    n = 280
    impulse = rng.uniform(1.0, 1.6, n)
    atr = rng.uniform(0.92, 1.25, n)
    retest = rng.integers(1, 8, n)
    market = rng.random(n) < 0.28
    edge = (impulse - 1.2) * 80 + (atr - 1.05) * 120 - (retest - 3) * 15 + np.where(market, 25.0, 0.0)
    pnl = edge + rng.normal(0, 160, n)
    gross = np.where(pnl > 0, np.abs(pnl) + 40, np.abs(pnl))
    fees = gross * 0.0008 * rng.uniform(0.8, 1.4, n)
    entry_reason = np.where(market, "strong_breakout_market", "retest_limit_confirm")
    return pd.DataFrame(
        {
            "pnl_net_rub": pnl,
            "pnl_gross_rub": np.sign(pnl) * gross,
            "fees_rub": fees,
            "commission_to_gross": fees / np.maximum(np.abs(gross), 1e-9),
            "entry_reason": entry_reason,
            "entry_type": np.where(market, "market", "limit"),
            "impulse_strength_ratio": impulse,
            "atr_slope": atr,
            "retest_bars_to_touch": retest,
            "rr_multiple": rng.uniform(1.5, 2.5, n),
            "exit_reason": np.where(rng.random(n) < 0.42, "take_profit", "stop_loss"),
        }
    )


def build_report(df, src: str, min_segment_n: int) -> dict:
    summary = core_metrics(df["pnl_net_rub"])
    comp = composite_segment_stats(df, min_n=min_segment_n)

    non_losing = filter_segments(comp, min_n=min_segment_n, exp_min=0.0, pf_min=1.0)
    edge = filter_segments(comp, min_n=min_segment_n, exp_min=0.0, pf_min=1.2)
    # strict EDGE: exp > 0
    edge = [s for s in edge if s.get("expectancy_rub", 0) > 1e-9]

    remove_zones = []
    for s in comp:
        exp = s.get("expectancy_rub")
        pf = s.get("profit_factor")
        if exp is None or pf is None:
            continue
        if exp < 0 and pf < 1.0:
            remove_zones.append(s)

    edge_keys = {s["seg_key"] for s in edge}
    param_ranges = ranges_from_edge_trades(df, edge_keys)

    reduction = {"baseline": summary}
    for pct in (20, 30, 50):
        frac = pct / 100.0
        _, m = simulate_remove_worst_composite_fraction(df, min_segment_n, frac)
        reduction[f"after_drop_worst_{pct}pct_composite_segments"] = m

    one_d = one_dimensional_segments(df, min_segment_n)
    fv = final_verdict(summary, non_losing, edge, len(df))
    keep = keep_rule_sentence(non_losing, edge, param_ranges, min_segment_n)
    mvl = market_vs_limit(df, min_segment_n)
    mlv_txt = market_limit_verdict_text(mvl, min_segment_n)

    return {
        "source": src,
        "n_rows": len(df),
        "min_segment_n": min_segment_n,
        "impulse_tertile_note": "impulse buckets = 1/3, 2/3 sample quantiles on impulse_strength_ratio",
        "summary": summary,
        "one_dimensional_segments": one_d,
        "composite_segments_ranked_worst_first": comp,
        "non_losing_zones": non_losing,
        "edge_zones": edge,
        "remove_zones": remove_zones,
        "reduction_simulation": reduction,
        "parameter_ranges_edge": param_ranges,
        "market_vs_limit": mvl,
        "market_limit_verdict": mlv_txt,
        "commission": commission_analysis(df),
        "loss_streak": loss_streak_features(df),
        "final_verdict": fv,
        "keep_rule": keep,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Analyze swing trades_log CSV (quant report)")
    ap.add_argument("--csv", type=str, default="", help="Path to trades_log.csv")
    ap.add_argument("--demo", action="store_true", help="Synthetic data smoke test")
    ap.add_argument("--min-segment-n", type=int, default=20, help="Min trades per segment (default 20)")
    ap.add_argument("--json", action="store_true", help="Print JSON only (no text report)")
    args = ap.parse_args()

    if args.demo:
        df = demo_frame()
        src = "DEMO_SYNTHETIC"
    else:
        p = Path(args.csv)
        if not p.is_file():
            print("Usage: --csv path/to/trades_log.csv  OR  --demo", file=sys.stderr)
            return 2
        df = load_trades_csv(p)
        src = str(p.resolve())

    rep = build_report(df, src, args.min_segment_n)

    if args.json:
        # composite list may be long
        print(json.dumps(rep, ensure_ascii=False, indent=2, default=str))
    else:
        print_text_report(rep)
        if args.demo:
            print("(Demo data: illustrative only, not real edge.)\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
