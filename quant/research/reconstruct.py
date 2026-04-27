"""Optional: re-evaluate signal_score weights when edge gate passes."""

from __future__ import annotations

from typing import Any

from fix_engine.backtest.experiment_metrics import extended_metrics
from fix_engine.strategy.signal_scoring import SignalScorePolicy, SignalScoreWeights
from fix_engine.strategy.swing_breakout import SwingBreakoutParams, load_ohlcv_csv
from fix_engine.strategy.swing_v2 import SwingV2Config, run_backtest_v2

from quant.features.alpha_candidates import attach_alpha_features


def reevaluate_with_weights(
    csv_path: str,
    weights: SignalScoreWeights,
    *,
    min_score: float = 0.55,
    top_n_per_day: int = 0,
    continuation_vol_mult: float = 1.35,
) -> dict[str, Any]:
    df = load_ohlcv_csv(csv_path)
    p = SwingBreakoutParams(volume_spike_required=False, htf_resample_rule="", htf_gate_mode="soft")
    pol = SignalScorePolicy(
        min_score=min_score,
        use_top_n_per_day=top_n_per_day > 0,
        top_n_per_day=max(1, top_n_per_day),
        weights=weights,
        apply_context_quality=True,
        hard_reject_context_below=None,
    )
    cfg = SwingV2Config(
        enable_continuation=False,
        enable_ema_pullback=True,
        continuation_vol_mult=continuation_vol_mult,
        score_policy=pol,
    )
    res = run_backtest_v2(df, p, cfg=cfg, log_trades=False, post_enrich_fn=attach_alpha_features)
    m = extended_metrics(res, p)
    return {
        "profit_factor": m.get("profit_factor"),
        "total_pnl_net_rub": m.get("total_pnl_net_rub"),
        "pnl_without_top_1_trade": m.get("pnl_without_top_1_trade"),
        "n_trades": m.get("n_trades"),
    }


def weights_from_rf_importance(importance: dict[str, float]) -> SignalScoreWeights:
    """Map raw_* columns to SignalScoreWeights (legacy signal factors only)."""
    key_map = {
        "raw_impulse_strength": "w_impulse_strength",
        "raw_atr_expansion": "w_atr_expansion",
        "raw_retest_speed": "w_retest",
        "raw_htf_score": "w_htf",
        "raw_ema_distance": "w_heat",
        "raw_candle_structure": "w_body",
    }
    vec = {key_map.get(k, k): max(0.0, float(v)) for k, v in importance.items() if k in key_map}
    s = sum(vec.values())
    if s <= 1e-12:
        return SignalScoreWeights()
    fac = 1.0 / s
    return SignalScoreWeights(
        combine_impulse=False,
        w_impulse_strength=vec.get("w_impulse_strength", 0) * fac,
        w_atr_expansion=vec.get("w_atr_expansion", 0) * fac,
        w_retest=vec.get("w_retest", 0) * fac,
        w_htf=vec.get("w_htf", 0) * fac,
        w_heat=vec.get("w_heat", 0) * fac,
        w_body=vec.get("w_body", 0) * fac,
    )
