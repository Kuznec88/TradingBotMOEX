"""Sanity: engineered columns align with index length (no accidental trim)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from quant.features.alpha_candidates import attach_alpha_features


def test_attach_alpha_same_length() -> None:
    n = 120
    rng = np.random.default_rng(0)
    df = pd.DataFrame(
        {
            "open": 100 + rng.random(n),
            "high": 101 + rng.random(n),
            "low": 99 + rng.random(n),
            "close": 100 + rng.random(n),
            "volume": rng.integers(1, 100, n),
        },
        index=pd.date_range("2025-01-01", periods=n, freq="h", tz="UTC"),
    )
    df["atr"] = (df["high"] - df["low"]).rolling(14, min_periods=5).mean().bfill()
    out = attach_alpha_features(df)
    assert len(out) == len(df)
    alpha_cols = [c for c in out.columns if c.startswith("alpha_")]
    assert len(alpha_cols) >= 35
