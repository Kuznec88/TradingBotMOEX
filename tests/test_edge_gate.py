"""Edge gate: PnL-aware + factor evidence."""

from __future__ import annotations

import pandas as pd

from quant.research.edge_gate import edge_hypothesis_accepted


def _make_trades(n: int, *, pnl_val: float = 5.0) -> pd.DataFrame:
    rows = [{"pnl": pnl_val if i % 2 == 0 else -2.0, "timestamp": f"2025-01-{i+1:02d}T10:00:00Z"} for i in range(n)]
    return pd.DataFrame(rows)


def _strong_factor_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factor": "raw_x",
                "spearman_pnl": 0.2,
                "stability_score": 0.5,
                "sign_consistent": True,
            },
            {
                "factor": "raw_y",
                "spearman_pnl": -0.18,
                "stability_score": 0.4,
                "sign_consistent": True,
            },
        ]
    )


def test_gate_fails_small_n() -> None:
    t = _make_trades(10)
    ok, d = edge_hypothesis_accepted(t, _strong_factor_df(), interaction_hits=0)
    assert ok is False
    assert "n_lt_min_trades" in d["reasons"]


def test_gate_passes_economics_and_factors() -> None:
    # 35 trades with positive mean, PF > 1, pnl_wo_top1 > 0
    wins = [15.0] * 20
    losses = [-3.0] * 15
    pnls = wins + losses
    t = pd.DataFrame({"pnl": pnls, "timestamp": pd.date_range("2025-01-01", periods=len(pnls), freq="D")})
    ok, d = edge_hypothesis_accepted(t, _strong_factor_df(), interaction_hits=0)
    assert ok is True
    assert d["strong_factors"] >= 2
