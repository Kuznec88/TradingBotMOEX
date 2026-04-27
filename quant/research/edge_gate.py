"""
PnL-aware edge gate: full-sample economics + factor / interaction evidence.

Falsification-first: failing the gate is a valid outcome (no tune to “pass”).
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from quant.core.constants import MIN_TRADES_RELIABLE
from quant.research.pnl_metrics import aggregate_sample_metrics


def _count_strong_factors(
    factor_analysis_df: pd.DataFrame,
    *,
    min_abs_spearman: float = 0.15,
    min_stability: float = 0.35,
) -> int:
    strong = 0
    for _, r in factor_analysis_df.iterrows():
        sp = r.get("spearman_pnl")
        st = r.get("stability_score")
        sc = r.get("sign_consistent")
        if sp is None or not isinstance(sp, (int, float)) or abs(float(sp)) < min_abs_spearman:
            continue
        if sc is not True:
            continue
        if not isinstance(st, (int, float)) or float(st) < min_stability:
            continue
        strong += 1
    return strong


def edge_hypothesis_accepted(
    trades_df: pd.DataFrame,
    factor_analysis_df: pd.DataFrame,
    *,
    interaction_hits: int = 0,
    min_trades: int = MIN_TRADES_RELIABLE,
    min_pf: float = 1.2,
    min_abs_spearman: float = 0.15,
    min_stability: float = 0.35,
    min_strong_factors: int = 2,
) -> tuple[bool, dict[str, Any]]:
    """
    Returns (accepted, detail).

    Requires **full-sample** expectancy > 0, PF > min_pf, pnl_wo_top1 > 0, n >= min_trades,
    plus either ≥ ``min_strong_factors`` stable factors **or** at least one qualifying interaction row.
    """
    m = aggregate_sample_metrics(trades_df)
    detail: dict[str, Any] = {"sample_metrics": m, "reasons": []}

    if m["n"] < min_trades:
        detail["reasons"].append("n_lt_min_trades")
        return False, detail

    exp = m.get("expectancy")
    if exp is None or exp <= 0:
        detail["reasons"].append("expectancy_non_positive")
        return False, detail

    pf = m.get("profit_factor")
    if pf is None or pf < min_pf:
        detail["reasons"].append("profit_factor_below_threshold")
        return False, detail

    pwo = m.get("pnl_wo_top1")
    if pwo is None or pwo <= 0:
        detail["reasons"].append("pnl_wo_top1_non_positive")
        return False, detail

    inter_ok = interaction_hits > 0
    if factor_analysis_df.empty:
        if not inter_ok:
            detail["reasons"].append("empty_factor_analysis_no_interactions")
            return False, detail
        detail["strong_factors"] = 0
        detail["reasons"].append("edge_gate_passed")
        return True, detail

    strong = _count_strong_factors(
        factor_analysis_df,
        min_abs_spearman=min_abs_spearman,
        min_stability=min_stability,
    )
    detail["strong_factors"] = strong

    factor_ok = strong >= min_strong_factors

    if not inter_ok and not factor_ok:
        detail["reasons"].append("insufficient_factor_and_interaction_evidence")
        return False, detail

    detail["reasons"].append("edge_gate_passed")
    return True, detail
