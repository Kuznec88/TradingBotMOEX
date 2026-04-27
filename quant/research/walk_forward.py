"""Chronological bucket metrics on trades (no bar-level re-backtest)."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from quant.research.pnl_metrics import aggregate_sample_metrics


def chronological_trade_bucket_metrics(
    trades_df: pd.DataFrame,
    *,
    n_buckets: int,
) -> dict[str, Any]:
    """
    Sort trades by ``timestamp``, split into ``n_buckets`` contiguous groups with nearly equal count,
    compute :func:`aggregate_sample_metrics` per bucket.
    """
    if n_buckets < 2:
        return {"note": "n_buckets_lt_2", "buckets": []}
    if trades_df.empty or "timestamp" not in trades_df.columns or "pnl" not in trades_df.columns:
        return {"note": "empty_or_missing_columns", "buckets": []}
    d = trades_df.copy()
    d["_ts"] = pd.to_datetime(d["timestamp"], errors="coerce")
    d = d.sort_values("_ts").dropna(subset=["_ts"])
    n = len(d)
    if n < n_buckets:
        return {"note": "too_few_trades_for_buckets", "n": n, "n_buckets": n_buckets, "buckets": []}
    splits = np.array_split(np.arange(n, dtype=int), n_buckets)
    buckets: list[dict[str, Any]] = []
    for i, ix in enumerate(splits):
        part = d.iloc[ix]
        m = aggregate_sample_metrics(part)
        t0 = part["_ts"].min()
        t1 = part["_ts"].max()
        buckets.append(
            {
                "bucket": i,
                "t_start": t0.isoformat() if hasattr(t0, "isoformat") else str(t0),
                "t_end": t1.isoformat() if hasattr(t1, "isoformat") else str(t1),
                "n": m.get("n"),
                "expectancy": m.get("expectancy"),
                "profit_factor": m.get("profit_factor"),
                "pnl_wo_top1": m.get("pnl_wo_top1"),
                "sum_pnl": m.get("sum_pnl"),
                "winrate": m.get("winrate"),
            }
        )
    return {
        "n_buckets": n_buckets,
        "note": "chronological_equal_count_trade_buckets",
        "buckets": buckets,
    }
