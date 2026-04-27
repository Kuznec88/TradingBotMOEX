"""Research constants: legacy signal factors + engineered alpha columns."""

from __future__ import annotations

# From entry meta (breakout strategy diagnostics)
RAW_FACTOR_KEYS = (
    "impulse_strength_ratio",
    "atr_slope",
    "retest_bars_to_touch",
    "htf_score_raw",
    "ema_dist_atr_signed",
    "candle_body_frac",
)

FACTOR_LABELS = {
    "impulse_strength_ratio": "impulse_strength",
    "atr_slope": "atr_expansion",
    "retest_bars_to_touch": "retest_speed",
    "htf_score_raw": "htf_score",
    "ema_dist_atr_signed": "ema_distance",
    "candle_body_frac": "candle_structure",
}

# Research/backtest: ``pnl`` and ``pnl_net_rub`` at qty=1 are **RUB per 1 contract**
# (1 point × rub_per_point × 1 lot). Account PnL ≈ per_contract_pnl × contracts.
DEFAULT_PLANNED_CONTRACTS = 50

MIN_TRADES_RELIABLE = 30
MIN_TRADES_INTERACTION = 30
PF_INTERACTION_MIN = 1.2

# Prefix for columns added by quant.features.alpha_candidates
ALPHA_PREFIX = "alpha_"

# v2: ``alpha_v2_*``, v3: ``alpha_v3_*`` (still matched by ``startswith("alpha_")`` in factor discovery)
#
# Names (without prefix) for documentation / model columns — v1 only
ALPHA_FEATURE_NAMES = (
    "liq_sweep_bear",
    "liq_sweep_bull",
    "atr_ratio_short_long",
    "vol_roll_pctile",
    "compression",
    "expand_after_compress",
    "hour_norm",
    "session_bucket",
    "wick_upper_ratio",
    "wick_lower_ratio",
    "range_position",
)
