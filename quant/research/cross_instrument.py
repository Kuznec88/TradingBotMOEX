"""Compare factor-level Spearman across instruments after a batch run."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def build_cross_instrument_factor_table(batch_rows: list[dict[str, Any]]) -> pd.DataFrame | None:
    """
    Reads ``factor_analysis.csv`` from each successful per-frame ``out_dir`` and pivots
    ``spearman_pnl`` by instrument. Adds cross-sectional std/mean for stability screening.
    """
    pieces: list[pd.DataFrame] = []
    for r in batch_rows:
        if r.get("error"):
            continue
        od = r.get("out_dir")
        if not od:
            continue
        fa = Path(od) / "factor_analysis.csv"
        if not fa.is_file():
            continue
        try:
            fa_df = pd.read_csv(fa)
        except Exception:
            continue
        if "factor" not in fa_df.columns or "spearman_pnl" not in fa_df.columns:
            continue
        sub = fa_df[["factor", "spearman_pnl"]].copy()
        sub["instrument"] = str(r.get("instrument", ""))
        pieces.append(sub)
    if not pieces:
        return None
    all_ = pd.concat(pieces, ignore_index=True)
    pivot = all_.pivot_table(index="factor", columns="instrument", values="spearman_pnl", aggfunc="first")
    pivot["spearman_std_across_instruments"] = pivot.std(axis=1)
    pivot["spearman_mean_across_instruments"] = pivot.mean(axis=1)
    return pivot.reset_index()
