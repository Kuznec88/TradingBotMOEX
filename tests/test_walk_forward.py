"""Chronological trade bucket metrics."""

from __future__ import annotations

import pandas as pd
import pytest

from quant.research.walk_forward import chronological_trade_bucket_metrics


def test_buckets_equal_count() -> None:
    ts = pd.date_range("2025-01-01", periods=12, freq="h")
    df = pd.DataFrame(
        {
            "timestamp": [str(x) for x in ts],
            "pnl": [1.0, -1.0, 2.0, -2.0, 1.0, -1.0, 3.0, -1.0, 1.0, -1.0, 0.5, -0.5],
        }
    )
    out = chronological_trade_bucket_metrics(df, n_buckets=4)
    assert out["n_buckets"] == 4
    assert len(out["buckets"]) == 4
    assert sum(b["n"] for b in out["buckets"]) == 12


def test_too_few_trades() -> None:
    df = pd.DataFrame({"timestamp": ["2025-01-01"], "pnl": [1.0]})
    out = chronological_trade_bucket_metrics(df, n_buckets=4)
    assert out.get("note") == "too_few_trades_for_buckets"
