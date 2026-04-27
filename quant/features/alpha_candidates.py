"""
Entry-time-safe engineered features (use only information available at bar close).

All outputs are numeric; most clipped to ~[0,1] for comparability.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _clip01(x: pd.Series) -> pd.Series:
    return x.clip(0.0, 1.0)


def attach_alpha_features(df: pd.DataFrame, *, atr_period_short: int = 14, atr_period_long: int = 100) -> pd.DataFrame:
    """
    Append columns prefixed with ``alpha_``. Must run after ``compute_indicators`` so ``atr`` exists.
    """
    out = df.copy()
    o = out["open"].astype(float)
    h = out["high"].astype(float)
    l_ = out["low"].astype(float)
    c = out["close"].astype(float)
    rng = (h - l_).replace(0, np.nan)

    prev_h = h.shift(1)
    prev_l = l_.shift(1)
    # 1–2: liquidity sweep / false breakout (single-bar vs previous bar extreme)
    out["alpha_liq_sweep_bear"] = ((h > prev_h) & (c < prev_h)).astype(float)
    out["alpha_liq_sweep_bull"] = ((l_ < prev_l) & (c > prev_l)).astype(float)

    # ATR regime: short vs long span (Wilder ATR already in ``atr`` — reuse as short; long = smoothed ATR)
    atr = out.get("atr")
    if atr is None:
        tr = pd.concat([h - l_, (h - c.shift(1)).abs(), (l_ - c.shift(1)).abs()], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1.0 / max(1, atr_period_short), adjust=False).mean()
    atr = atr.astype(float)
    atr_long = atr.rolling(max(5, atr_period_long), min_periods=max(5, atr_period_long // 2)).mean()
    ratio = atr / atr_long.replace(0, np.nan)
    out["alpha_atr_ratio_short_long"] = ratio.clip(0.1, 5.0) / 5.0  # ~0..1

    # Rolling vol percentile (ATR relative to recent distribution)
    atr_med = atr.rolling(100, min_periods=20).median()
    vol_score = atr / atr_med.replace(0, np.nan)
    out["alpha_vol_roll_pctile"] = vol_score.rolling(50, min_periods=10).rank(pct=True).clip(0, 1)

    # Compression: narrow range vs recent
    range_bar = h - l_
    comp = 1.0 - range_bar.rolling(50, min_periods=10).rank(pct=True)
    out["alpha_compression"] = _clip01(comp)

    # Expansion after compression (lagged signal)
    compress_lag = out["alpha_compression"].shift(1)
    range_exp = range_bar / range_bar.rolling(20, min_periods=5).median().replace(0, np.nan)
    out["alpha_expand_after_compress"] = (
        (compress_lag > 0.65) & (range_exp > 1.1)
    ).astype(float)

    # Time (index = bar time)
    idx = out.index
    try:
        hours = pd.Series([pd.Timestamp(ts).hour + pd.Timestamp(ts).minute / 60.0 for ts in idx], index=out.index)
    except Exception:
        hours = pd.Series(np.nan, index=out.index)
    out["alpha_hour_norm"] = (hours % 24.0) / 24.0

    # MOEX-oriented session bucket 0..3 (numeric; not one-hot to keep matrix small)
    def _sess(h: float) -> float:
        if np.isnan(h):
            return 2.0
        if h < 7:
            return 0.0  # night
        if h < 10:
            return 1.0  # open
        if h < 18:
            return 2.0  # main
        return 3.0  # evening

    out["alpha_session_bucket"] = hours.apply(lambda x: _sess(float(x)) if pd.notna(x) else 2.0) / 3.0

    # Wick vs body (rejection strength)
    body = (c - o).abs()
    wick_u = h - np.maximum(o, c)
    wick_l = np.minimum(o, c) - l_
    out["alpha_wick_upper_ratio"] = _clip01((wick_u / rng).fillna(0))
    out["alpha_wick_lower_ratio"] = _clip01((wick_l / rng).fillna(0))
    out["alpha_body_ratio"] = _clip01((body / rng).fillna(0))

    # Range positioning (local Donchian)
    win = 20
    hh = h.rolling(win, min_periods=5).max()
    ll = l_.rolling(win, min_periods=5).min()
    denom = (hh - ll).replace(0, np.nan)
    pos = (c - ll) / denom
    out["alpha_range_position"] = _clip01(pos.fillna(0.5))

    # HTF price–volume согласованность (из compute_indicators) — для research-факторов
    if "htf_pv_quality" in out.columns:
        out["alpha_htf_pv_quality"] = pd.to_numeric(out["htf_pv_quality"], errors="coerce").fillna(0.5).clip(0.0, 1.0)

    from quant.features.alpha_v2 import attach_alpha_v2_features
    from quant.features.alpha_v3 import attach_alpha_v3_features

    return attach_alpha_v3_features(attach_alpha_v2_features(out))
