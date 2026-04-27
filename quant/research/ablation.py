"""Leave-one-legacy-factor-out signal_score ablation (post_enrich alpha aligned with research backtest)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from fix_engine.backtest.experiment_metrics import extended_metrics
from fix_engine.strategy.signal_scoring import SignalScorePolicy, SignalScoreWeights, renormalize_weights_drop
from fix_engine.strategy.swing_breakout import SwingBreakoutParams, load_ohlcv_csv
from fix_engine.strategy.swing_v2 import SwingV2Config, run_backtest_v2

from quant.core.constants import FACTOR_LABELS
from quant.features.alpha_candidates import attach_alpha_features

FACTOR_KEYS = tuple(FACTOR_LABELS.values())


def _metrics_row(name: str, res, p: SwingBreakoutParams) -> dict[str, Any]:
    m = extended_metrics(res, p)
    return {
        "variant": name,
        "n_trades": m.get("n_trades"),
        "profit_factor": m.get("profit_factor"),
        "total_pnl_net_rub": m.get("total_pnl_net_rub"),
        "pnl_without_top_1_trade": m.get("pnl_without_top_1_trade"),
        "winrate": m.get("winrate"),
    }


def run_ablation_study(
    csv_path: str,
    *,
    min_score: float = 0.6,
    continuation_vol_mult: float = 1.35,
) -> pd.DataFrame:
    df = load_ohlcv_csv(csv_path)
    p = SwingBreakoutParams(volume_spike_required=False, htf_resample_rule="", htf_gate_mode="soft")
    rows: list[dict[str, Any]] = []

    base = SwingV2Config(
        enable_continuation=False,
        enable_ema_pullback=True,
        continuation_vol_mult=continuation_vol_mult,
        score_policy=SignalScorePolicy(
            min_score=min_score,
            weights=SignalScoreWeights(combine_impulse=False),
            apply_context_quality=True,
            hard_reject_context_below=None,
        ),
    )
    r0 = run_backtest_v2(df, p, cfg=base, log_trades=False, post_enrich_fn=attach_alpha_features)
    m0 = _metrics_row("baseline_all_factors", r0, p)
    m0["delta_pf_vs_baseline"] = 0.0
    m0["delta_pnl_wo_top1_vs_baseline"] = 0.0
    m0["classification"] = "baseline"
    rows.append(m0)

    for fk in FACTOR_KEYS:
        w = renormalize_weights_drop(SignalScoreWeights(), frozenset({fk}))
        cfg = SwingV2Config(
            enable_continuation=False,
            enable_ema_pullback=True,
            continuation_vol_mult=continuation_vol_mult,
            score_policy=SignalScorePolicy(
                min_score=min_score,
                weights=w,
                apply_context_quality=True,
                hard_reject_context_below=None,
            ),
        )
        r = run_backtest_v2(df, p, cfg=cfg, log_trades=False, post_enrich_fn=attach_alpha_features)
        mr = _metrics_row(f"drop_{fk}", r, p)
        mr["delta_pf_vs_baseline"] = (mr.get("profit_factor") or 0) - (m0.get("profit_factor") or 0)
        mr["delta_pnl_wo_top1_vs_baseline"] = (mr.get("pnl_without_top_1_trade") or 0) - (
            m0.get("pnl_without_top_1_trade") or 0
        )
        pf_b = m0.get("profit_factor")
        pf_a = mr.get("profit_factor")
        if pf_b is not None and pf_a is not None and pf_b > 0:
            if pf_a > pf_b * 1.02:
                mr["classification"] = "harmful"
            elif pf_a < pf_b * 0.98:
                mr["classification"] = "useful"
            else:
                mr["classification"] = "neutral"
        else:
            mr["classification"] = "unreliable"
        rows.append(mr)

    return pd.DataFrame(rows)
