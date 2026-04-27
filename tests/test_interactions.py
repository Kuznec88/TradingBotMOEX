"""Triple interaction generator smoke test."""

from __future__ import annotations

import numpy as np
import pandas as pd

from quant.research.interactions import generate_triple_interactions


def test_triple_interactions_runs() -> None:
    rng = np.random.default_rng(0)
    n = 80
    df = pd.DataFrame(
        {
            "raw_a": rng.standard_normal(n),
            "raw_b": rng.standard_normal(n),
            "raw_c": rng.standard_normal(n),
            "pnl": rng.standard_normal(n) * 10,
        }
    )
    cols = ["raw_a", "raw_b", "raw_c"]
    out = generate_triple_interactions(df, cols, min_trades=5, pf_min=0.5)
    assert isinstance(out, pd.DataFrame)
