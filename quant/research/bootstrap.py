"""Bootstrap resampling of trade PnLs for PF and total PnL distribution (nonparametric)."""

from __future__ import annotations

from typing import Any

import numpy as np

from quant.research.pnl_metrics import profit_factor, pnl_without_top_one


def bootstrap_pnl_distribution(
    pnls: np.ndarray,
    *,
    n_bootstrap: int = 2000,
    random_state: int = 42,
) -> dict[str, Any]:
    """
    With-replacement bootstrap of trades. Returns mean PF and percentiles of sum PnL and PF.
    """
    rng = np.random.default_rng(random_state)
    pnls = np.asarray(pnls, dtype=float)
    n = len(pnls)
    if n < 5:
        return {"note": "too_few_trades", "n": n}

    sums = np.empty(n_bootstrap, dtype=float)
    pfs = np.empty(n_bootstrap, dtype=float)
    pwos = np.empty(n_bootstrap, dtype=float)

    for i in range(n_bootstrap):
        samp = rng.choice(pnls, size=n, replace=True)
        sums[i] = float(np.sum(samp))
        pf = profit_factor(samp)
        pfs[i] = pf if pf is not None and np.isfinite(pf) else np.nan
        pwos[i] = pnl_without_top_one(samp)

    return {
        "n": n,
        "n_bootstrap": n_bootstrap,
        "sum_pnl_mean": float(np.nanmean(sums)),
        "sum_pnl_pctiles": {
            "p5": float(np.percentile(sums, 5)),
            "p50": float(np.percentile(sums, 50)),
            "p95": float(np.percentile(sums, 95)),
        },
        "profit_factor_mean": float(np.nanmean(pfs)),
        "profit_factor_pctiles": {
            "p5": float(np.nanpercentile(pfs, 5)),
            "p50": float(np.nanpercentile(pfs, 50)),
            "p95": float(np.nanpercentile(pfs, 95)),
        },
        "pnl_wo_top1_pctiles": {
            "p5": float(np.percentile(pwos, 5)),
            "p50": float(np.percentile(pwos, 50)),
            "p95": float(np.percentile(pwos, 95)),
        },
    }
