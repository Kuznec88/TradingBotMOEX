"""Per-feature bins, correlations, monotonicity, time-split stability (raw + alpha columns)."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from quant.core.constants import MIN_TRADES_RELIABLE


def _profit_factor(pnls: np.ndarray) -> float | None:
    pnls = np.asarray(pnls, dtype=float)
    gp = pnls[pnls > 0].sum()
    gl = -pnls[pnls < 0].sum()
    if gl <= 1e-12:
        return None
    return float(gp / gl)


def _pnl_wo_top1(pnls: np.ndarray) -> float:
    pnls = np.sort(np.asarray(pnls, dtype=float))[::-1]
    if len(pnls) == 0:
        return 0.0
    return float(pnls.sum() - pnls[0])


def factor_bin_table(df: pd.DataFrame, col: str, q: int = 5) -> pd.DataFrame:
    if col not in df.columns or df[col].dropna().nunique() <= 1:
        return pd.DataFrame()
    sub = df[[col, "pnl"]].dropna(subset=[col])
    if len(sub) < 4:
        return pd.DataFrame()
    q_eff = min(q, max(2, len(sub) // 3))
    try:
        sub = sub.copy()
        sub["bin"] = pd.qcut(sub[col], q=q_eff, labels=False, duplicates="drop")
    except (ValueError, TypeError):
        return pd.DataFrame()
    rows = []
    for b in sorted(sub["bin"].dropna().unique()):
        p = sub.loc[sub["bin"] == b, "pnl"].values
        rows.append(
            {
                "bin": int(b),
                "n": len(p),
                "avg_pnl": float(np.mean(p)),
                "median_pnl": float(np.median(p)),
                "winrate": float(np.mean(p > 0)),
                "profit_factor": _profit_factor(p),
                "pnl_wo_top1": _pnl_wo_top1(p),
            }
        )
    return pd.DataFrame(rows)


def spearman_corr_numpy(x: np.ndarray, y: np.ndarray) -> float | None:
    if len(x) < 3:
        return None
    from numpy import argsort

    rx = argsort(argsort(x))
    ry = argsort(argsort(y))
    rx = rx.astype(float)
    ry = ry.astype(float)
    cx = rx - rx.mean()
    cy = ry - ry.mean()
    den = np.sqrt((cx**2).sum() * (cy**2).sum())
    if den <= 1e-12:
        return None
    return float((cx * cy).sum() / den)


def monotonicity_score(bin_means: list[float]) -> tuple[bool, float]:
    if len(bin_means) < 3:
        return False, 0.0
    x = np.arange(len(bin_means), dtype=float)
    y = np.asarray(bin_means, dtype=float)
    r = spearman_corr_numpy(x, y)
    if r is None:
        return False, 0.0
    mono = abs(r) >= 0.85 and len(set(np.sign(np.diff(y)))) <= 1
    return bool(mono), float(abs(r))


def analyze_single_factor(df: pd.DataFrame, col: str, q: int) -> dict[str, Any]:
    y = df["pnl"]
    x = df[col]
    pear: float | None = None
    mask = x.notna() & y.notna()
    if mask.sum() >= 3 and float(x[mask].std(ddof=0)) > 1e-12:
        pear = float(x[mask].corr(y[mask]))
    spear = spearman_corr_numpy(x[mask].values, y[mask].values) if mask.sum() >= 3 else None

    bdf = factor_bin_table(df, col, q=q)
    bin_means: list[float] = []
    if not bdf.empty and "avg_pnl" in bdf.columns:
        bin_means = bdf["avg_pnl"].tolist()
    mono_flag, mono_rho = monotonicity_score(bin_means) if len(bin_means) >= 3 else (False, 0.0)

    return {
        "factor": col,
        "n_nonnull": int(x.notna().sum()),
        "pearson_pnl": pear,
        "spearman_pnl": spear,
        "monotonic_bins": mono_flag,
        "monotonicity_rho": mono_rho,
        "bin_table": bdf,
    }


def time_split(df: pd.DataFrame, train_frac: float = 0.7) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty or "timestamp" not in df.columns:
        return df, pd.DataFrame()
    d = df.copy()
    d["_ts"] = pd.to_datetime(d["timestamp"], errors="coerce")
    d = d.sort_values("_ts")
    n = len(d)
    cut = max(1, int(n * train_frac))
    train = d.iloc[:cut].drop(columns=["_ts"], errors="ignore")
    test = d.iloc[cut:].drop(columns=["_ts"], errors="ignore")
    return train, test


def stability_for_factor(df: pd.DataFrame, col: str) -> dict[str, Any]:
    tr, te = time_split(df, 0.7)
    out: dict[str, Any] = {"factor": col}
    for name, part in ("train", tr), ("test", te):
        if len(part) < 5:
            out[f"spearman_{name}"] = None
            continue
        x = part[col]
        y = part["pnl"]
        m = x.notna() & y.notna()
        out[f"spearman_{name}"] = spearman_corr_numpy(x[m].values, y[m].values) if m.sum() >= 3 else None
    st = out.get("spearman_train")
    se = out.get("spearman_test")
    if st is None or se is None or (st == 0 and se == 0):
        out["sign_consistent"] = None
        out["stability_score"] = 0.0
    else:
        out["sign_consistent"] = (st >= 0) == (se >= 0)
        out["stability_score"] = float(1.0 - min(1.0, abs(st - se)))
    return out


def default_feature_columns(df: pd.DataFrame) -> list[str]:
    return sorted(
        c
        for c in df.columns
        if (c.startswith("raw_") or c.startswith("alpha_")) and c not in ("raw_",)
    )


def recommend_drop(row: dict[str, Any]) -> bool:
    """Heuristic: drop if unstable sign or negligible correlation."""
    sp = row.get("spearman_pnl")
    sc = row.get("sign_consistent")
    if sp is None or not isinstance(sp, (int, float)):
        return True
    if abs(float(sp)) < 0.03:
        return True
    if sc is False:
        return True
    if row.get("monotonic_bins") is False and abs(float(sp)) < 0.08:
        return True
    return False


def build_factor_analysis_tables(
    df: pd.DataFrame,
    *,
    q_bins: int = 5,
    feature_cols: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, pd.DataFrame]]:
    cols = feature_cols if feature_cols is not None else default_feature_columns(df)
    rows: list[dict[str, Any]] = []
    bin_csvs: dict[str, pd.DataFrame] = {}
    n = len(df)
    unreliable = n < MIN_TRADES_RELIABLE

    for col in cols:
        if col not in df.columns or col == "pnl":
            continue
        a = analyze_single_factor(df, col, q=q_bins)
        st = stability_for_factor(df, col)
        base = {
            "factor": col,
            "n_trades": n,
            "n_nonnull": a["n_nonnull"],
            "unreliable_small_sample": unreliable,
            "pearson_pnl": a["pearson_pnl"],
            "spearman_pnl": a["spearman_pnl"],
            "monotonic_bins": a["monotonic_bins"],
            "monotonicity_rho": a["monotonicity_rho"],
            "spearman_train": st.get("spearman_train"),
            "spearman_test": st.get("spearman_test"),
            "sign_consistent": st.get("sign_consistent"),
            "stability_score": st.get("stability_score"),
        }
        base["recommended_drop"] = recommend_drop(
            {
                "spearman_pnl": a["spearman_pnl"],
                "sign_consistent": st.get("sign_consistent"),
                "monotonic_bins": a["monotonic_bins"],
            }
        )
        rows.append(base)
        if not a["bin_table"].empty:
            b = a["bin_table"].copy()
            b.insert(0, "factor", col)
            bin_csvs[col] = b

    summary = {
        "n_trades": n,
        "unreliable_n_lt_30": unreliable,
        "features": {r["factor"]: {k: v for k, v in r.items() if k != "factor"} for r in rows},
    }
    return pd.DataFrame(rows), summary, bin_csvs
