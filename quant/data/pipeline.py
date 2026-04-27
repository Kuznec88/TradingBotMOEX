"""Enriched OHLCV + backtest for research (single entrypoint, no lookahead in features)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from fix_engine.strategy.signal_scoring import SignalScorePolicy, SignalScoreWeights
from fix_engine.strategy.swing_breakout import (
    SwingBreakoutParams,
    load_ohlcv_csv,
    run_backtest,
)
from fix_engine.strategy.swing_v2 import (
    SwingV2Config,
    build_extra_intent_fn,
    compute_indicators,
    enrich_regime_and_vwap,
)
from fix_engine.strategy.signal_scoring import build_intent_filter_fn
from fix_engine.strategy.swing_breakout import BacktestResult

from quant.data.trade_dataset import trades_to_dataframe
from quant.features.alpha_candidates import attach_alpha_features


def get_enriched_ohlcv(
    csv_path: str | Path,
    *,
    score_filter: bool = True,
    min_score: float = 0.52,
    enable_continuation: bool = False,
) -> tuple[pd.DataFrame, SwingBreakoutParams, SwingV2Config]:
    df = load_ohlcv_csv(str(csv_path))
    # HTF 1h + 4h включены; OR по ТФ (не требуем одновременного окна на обоих — иначе мало сделок на LTF)
    p = SwingBreakoutParams(
        volume_spike_required=False,
        htf_resample_rule="1h",
        htf_secondary_resample_rule="4h",
        htf_dual_require_both=False,
        htf_gate_mode="soft",
        htf_score_min=0.30,
        htf_pv_score_influence=0.12,
    )
    pol = None
    if score_filter:
        pol = SignalScorePolicy(
            min_score=float(min_score),
            weights=SignalScoreWeights(combine_impulse=False),
            apply_context_quality=True,
            hard_reject_context_below=None,
        )
    c = SwingV2Config(
        enable_continuation=enable_continuation,
        enable_ema_pullback=True,
        continuation_vol_mult=1.35,
        score_policy=pol,
    )
    df = compute_indicators(df, n=p.n_levels, ma_period=p.ma_period, params=p)
    df = enrich_regime_and_vwap(df, p, c)
    df = attach_alpha_features(df)
    return df, p, c


def run_research_backtest(
    csv_path: str | Path,
    *,
    instrument: str | None = None,
    score_filter: bool = True,
    min_score: float = 0.52,
    enable_continuation: bool = False,
) -> tuple[pd.DataFrame, BacktestResult, pd.DataFrame, SwingBreakoutParams]:
    """
    Returns:
        trades_df — one row per trade with raw_* + alpha_* at entry bar; ``pnl`` is **RUB per 1 contract**
            (backtest ``qty=1``).
        result — BacktestResult
        enriched_df — full OHLCV + indicators + alpha (for audit)
        params
    """
    df, p, c = get_enriched_ohlcv(
        csv_path,
        score_filter=score_filter,
        min_score=min_score,
        enable_continuation=enable_continuation,
    )
    extra = build_extra_intent_fn(p, c)
    filt: Any = None
    if c.score_policy is not None:
        filt = build_intent_filter_fn(c.score_policy)
    res = run_backtest(
        df,
        p,
        qty=1.0,
        log_trades=False,
        extra_intent_fn=extra,
        intent_filter_fn=filt,
        pre_indicators=True,
    )
    inst = instrument or Path(csv_path).stem
    trades_df = trades_to_dataframe(res, instrument=inst, enriched_df=df)
    return trades_df, res, df, p
