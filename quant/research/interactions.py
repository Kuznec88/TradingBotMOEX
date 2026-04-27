"""Pairwise interaction masks (median splits on feature columns)."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from quant.core.constants import MIN_TRADES_INTERACTION, PF_INTERACTION_MIN


def _pf(pnls: np.ndarray) -> float | None:
    gp = pnls[pnls > 0].sum()
    gl = -pnls[pnls < 0].sum()
    if gl <= 1e-12:
        return None
    return float(gp / gl)


def _pnl_wo_top1(pnls: np.ndarray) -> float:
    pnls = np.sort(pnls.astype(float))[::-1]
    if len(pnls) == 0:
        return 0.0
    return float(pnls.sum() - pnls[0])


def median_high_mask(s: pd.Series) -> pd.Series:
    return s >= s.median()


def generate_pairwise_interactions(
    df: pd.DataFrame,
    feature_cols: list[str],
    *,
    min_trades: int = MIN_TRADES_INTERACTION,
    pf_min: float = PF_INTERACTION_MIN,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for i, a in enumerate(feature_cols):
        for b in feature_cols[i + 1 :]:
            if a not in df.columns or b not in df.columns:
                continue
            sub = df[[a, b, "pnl"]].dropna()
            if len(sub) < min_trades:
                continue
            ha = median_high_mask(sub[a])
            hb = median_high_mask(sub[b])
            masks = [
                ("high_high", ha & hb),
                ("high_low", ha & ~hb),
                ("low_high", ~ha & hb),
                ("low_low", ~ha & ~hb),
            ]
            for label, m in masks:
                pn = sub.loc[m, "pnl"].values
                if len(pn) < min_trades:
                    continue
                pf = _pf(pn)
                pwo = _pnl_wo_top1(pn)
                if pf is None or pf <= pf_min or pwo <= 0:
                    continue
                rows.append(
                    {
                        "factor_a": a,
                        "factor_b": b,
                        "interaction": label,
                        "n": len(pn),
                        "avg_pnl": float(np.mean(pn)),
                        "profit_factor": pf,
                        "pnl_without_top_1": pwo,
                        "winrate": float(np.mean(pn > 0)),
                    }
                )
    return pd.DataFrame(rows)


def generate_triple_interactions(
    df: pd.DataFrame,
    feature_cols: list[str],
    *,
    min_trades: int = MIN_TRADES_INTERACTION,
    pf_min: float = PF_INTERACTION_MIN,
    max_factors: int = 8,
) -> pd.DataFrame:
    """
    Median splits on three factors → 8 octants (high/low per axis).
    Caps ``max_factors`` (default 8 → C(8,3)=56 triples) to keep runtime bounded.
    """
    cols = feature_cols[: max(3, min(len(feature_cols), max_factors))]
    rows: list[dict[str, Any]] = []
    ncols = len(cols)
    for i in range(ncols):
        a = cols[i]
        for j in range(i + 1, ncols):
            b = cols[j]
            for k in range(j + 1, ncols):
                cname = cols[k]
                if a not in df.columns or b not in df.columns or cname not in df.columns:
                    continue
                sub = df[[a, b, cname, "pnl"]].dropna()
                if len(sub) < min_trades:
                    continue
                ha = median_high_mask(sub[a])
                hb = median_high_mask(sub[b])
                hc = median_high_mask(sub[cname])
                masks = [
                    ("hhh", ha & hb & hc),
                    ("hhl", ha & hb & ~hc),
                    ("hlh", ha & ~hb & hc),
                    ("hll", ha & ~hb & ~hc),
                    ("lhh", ~ha & hb & hc),
                    ("lhl", ~ha & hb & ~hc),
                    ("llh", ~ha & ~hb & hc),
                    ("lll", ~ha & ~hb & ~hc),
                ]
                for label, m in masks:
                    pn = sub.loc[m, "pnl"].values
                    if len(pn) < min_trades:
                        continue
                    pf = _pf(pn)
                    pwo = _pnl_wo_top1(pn)
                    if pf is None or pf <= pf_min or pwo <= 0:
                        continue
                    rows.append(
                        {
                            "factor_a": a,
                            "factor_b": b,
                            "factor_c": cname,
                            "interaction": label,
                            "n": len(pn),
                            "avg_pnl": float(np.mean(pn)),
                            "profit_factor": pf,
                            "pnl_without_top_1": pwo,
                            "winrate": float(np.mean(pn > 0)),
                        }
                    )
    return pd.DataFrame(rows)
