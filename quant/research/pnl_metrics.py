"""Aggregate trade PnL statistics: expectancy, PF, pnl without top trade, holdout splits."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def profit_factor(pnls: np.ndarray) -> float | None:
    pnls = np.asarray(pnls, dtype=float)
    gp = pnls[pnls > 0].sum()
    gl = -pnls[pnls < 0].sum()
    if gl <= 1e-12:
        return None
    return float(gp / gl)


def pnl_without_top_one(pnls: np.ndarray) -> float:
    pnls = np.sort(np.asarray(pnls, dtype=float))[::-1]
    if len(pnls) == 0:
        return 0.0
    return float(pnls.sum() - pnls[0])


def expectancy_decomposition(pnls: np.ndarray) -> dict[str, float | None]:
    """
    Classic decomposition: E = winrate * avg_win - (1-winrate) * abs(avg_loss_on_losers).
    ``mean(pnls)`` should match within tolerance when losses are negative.
    """
    pnls = np.asarray(pnls, dtype=float)
    n = len(pnls)
    if n == 0:
        return {
            "n": 0.0,
            "winrate": None,
            "avg_win": None,
            "avg_loss": None,
            "expectancy_from_components": None,
            "mean_pnl": None,
        }
    wins = pnls[pnls > 0]
    losses = pnls[pnls <= 0]
    wr = float(len(wins) / n)
    avg_win = float(wins.mean()) if len(wins) else 0.0
    avg_loss = float(losses.mean()) if len(losses) else 0.0  # negative or zero
    exp_comp = wr * avg_win + (1.0 - wr) * avg_loss
    return {
        "n": float(n),
        "winrate": wr,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "expectancy_from_components": float(exp_comp),
        "mean_pnl": float(np.mean(pnls)),
    }


def aggregate_sample_metrics(trades_df: pd.DataFrame) -> dict[str, Any]:
    """Full-sample metrics (in-sample; use holdout_metrics for OOS slice)."""
    if trades_df.empty or "pnl" not in trades_df.columns:
        return {
            "n": 0,
            "expectancy": None,
            "profit_factor": None,
            "pnl_wo_top1": None,
            "winrate": None,
            "sum_pnl": 0.0,
        }
    p = trades_df["pnl"].astype(float).values
    n = len(p)
    dec = expectancy_decomposition(p)
    return {
        "n": n,
        "expectancy": dec.get("mean_pnl"),
        "expectancy_decomposition": dec,
        "profit_factor": profit_factor(p),
        "pnl_wo_top1": pnl_without_top_one(p),
        "winrate": dec.get("winrate"),
        "sum_pnl": float(np.sum(p)),
    }


def holdout_metrics(trades_df: pd.DataFrame, *, train_frac: float = 0.7) -> dict[str, Any]:
    """Time-ordered holdout: metrics on last (1-train_frac) of trades by timestamp."""
    if trades_df.empty or "timestamp" not in trades_df.columns:
        return {"train": {}, "test": {}, "note": "empty_or_no_timestamp"}
    d = trades_df.copy()
    d["_ts"] = pd.to_datetime(d["timestamp"], errors="coerce")
    d = d.sort_values("_ts").dropna(subset=["_ts"])
    if len(d) < 8:
        return {"train": {}, "test": {}, "note": "too_few_rows"}
    cut = max(2, int(len(d) * train_frac))
    train = d.iloc[:cut]
    test = d.iloc[cut:]
    return {
        "train": aggregate_sample_metrics(train),
        "test": aggregate_sample_metrics(test),
        "train_frac": train_frac,
        "n_train": len(train),
        "n_test": len(test),
    }


def extrapolate_6m_pnl_rub(
    trades_df: pd.DataFrame,
    *,
    expectancy_rub: float | None = None,
) -> dict[str, Any]:
    """
    Scale observed expectancy × estimated trades per 6 months from sample span.
    All monetary fields are **RUB per 1 contract** (same units as ``trades_df["pnl"]`` at backtest qty=1).
    No optimization — descriptive extrapolation only.
    """
    if trades_df.empty or "pnl" not in trades_df.columns:
        return {"note": "empty", "projected_6m_pnl_rub": None}
    exp = expectancy_rub
    if exp is None:
        exp = float(trades_df["pnl"].mean())
    t = pd.to_datetime(trades_df["timestamp"], errors="coerce")
    valid = t.notna()
    if valid.sum() < 2:
        months = 1.0
        span_days = None
    else:
        span_days = float(max(1.0, (t[valid].max() - t[valid].min()).days))
        months = max(span_days / 30.0, 0.25)
    tpm = len(trades_df) / months
    proj = exp * tpm * 6.0
    return {
        "pnl_unit": "rub_per_contract",
        "expectancy_rub_per_trade": exp,
        "trades_per_month_est": tpm,
        "span_days_est": span_days if valid.sum() >= 2 else None,
        "projected_6m_pnl_rub": proj,
        "note": "extrapolation_not_forecast",
    }


def sizing_scale_for_6m_target_rub(
    trades_df: pd.DataFrame,
    *,
    target_6m_pnl_rub: float,
    projected_6m_pnl_rub: float | None = None,
    planned_contracts: float = 1.0,
) -> dict[str, Any]:
    """
    Business framing: research uses ``qty=1`` → **RUB per 1 contract**; total account PnL scales as
    per_contract × contracts. ``target_6m_pnl_rub`` is the **per-contract** 6M RUB goal; compare to
    ``projected_6m_pnl_rub`` (also per contract). ``implied_qty_multiplier`` scales vs that backtest.
    ``planned_contracts`` only fills **account** view fields (e.g. 50 → show projected account RUB).
    """
    pc = max(float(planned_contracts), 1e-9)
    out: dict[str, Any] = {
        "pnl_unit": "rub_per_contract",
        "planned_contracts": pc,
        "target_6m_per_contract_rub": float(target_6m_pnl_rub),
        "target_6m_account_rub": float(target_6m_pnl_rub) * pc,
        "projected_6m_pnl_rub": projected_6m_pnl_rub,
        "implied_qty_multiplier": None,
        "note": None,
    }
    if target_6m_pnl_rub <= 0:
        out["note"] = "target_disabled"
        return out
    if projected_6m_pnl_rub is None:
        ex = extrapolate_6m_pnl_rub(trades_df)
        projected_6m_pnl_rub = ex.get("projected_6m_pnl_rub")
        out["projected_6m_pnl_rub"] = projected_6m_pnl_rub
    proj = projected_6m_pnl_rub
    if proj is None:
        out["note"] = "no_projection"
        return out
    if abs(float(proj)) < 1e-9:
        out["note"] = "projected_6m_near_zero"
        return out
    if float(proj) <= 0:
        out["note"] = "negative_or_zero_edge_scaling_wont_fix"
        return out
    mult = float(target_6m_pnl_rub) / float(proj)
    out["implied_qty_multiplier"] = mult
    out["projected_6m_account_rub_at_planned_contracts"] = float(proj) * pc
    out["note"] = "linear_qty_scale_vs_research_backtest_assumes_same_edge"
    return out
