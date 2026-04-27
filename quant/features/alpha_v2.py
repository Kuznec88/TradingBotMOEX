"""
Liquidity- and regime-aware features (v2). All columns use only information available at bar close.

Prefixed ``alpha_v2_`` to distinguish from v1 ``alpha_*``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _clip01(x: pd.Series) -> pd.Series:
    return x.clip(0.0, 1.0)


def attach_alpha_v2_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expects OHLCV + ``atr`` (Wilder) from ``compute_indicators`` / v1 alpha pass.
    """
    out = df.copy()
    o = out["open"].astype(float)
    h = out["high"].astype(float)
    l_ = out["low"].astype(float)
    c = out["close"].astype(float)
    rng = (h - l_).replace(0, np.nan)
    atr = out.get("atr")
    if atr is None:
        tr = pd.concat([h - l_, (h - c.shift(1)).abs(), (l_ - c.shift(1)).abs()], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1.0 / 14.0, adjust=False).mean()
    atr = atr.astype(float).replace(0, np.nan)

    prev_h = h.shift(1)
    prev_l = l_.shift(1)
    # False breakout strength: pierce + close back through reference (failure to hold)
    pierce_up = (h > prev_h) & prev_h.notna()
    fail_up = pierce_up & (c < prev_h)
    pierce_dn = (l_ < prev_l) & prev_l.notna()
    fail_dn = pierce_dn & (c > prev_l)
    depth_up = ((h - np.maximum(o, c)) / rng.replace(0, np.nan)).clip(0, 1).fillna(0)
    depth_dn = ((np.minimum(o, c) - l_) / rng.replace(0, np.nan)).clip(0, 1).fillna(0)
    out["alpha_v2_false_breakout_up"] = (fail_up.astype(float) * (0.5 + 0.5 * depth_up)).clip(0, 1)
    out["alpha_v2_false_breakout_dn"] = (fail_dn.astype(float) * (0.5 + 0.5 * depth_dn)).clip(0, 1)

    # Post-event 3-bar path (lagged, no future): net move over bars t-3..t inclusive vs ATR
    ret_3 = c - c.shift(3)
    out["alpha_v2_ret_3bar_atr"] = (ret_3 / atr).clip(-6, 6) / 6.0 + 0.5
    out["alpha_v2_ret_3bar_atr"] = _clip01(out["alpha_v2_ret_3bar_atr"])

    # Volatility regime: tertiles of rolling ATR vs its long MA
    atr_ma = atr.rolling(100, min_periods=30).mean()
    ratio = (atr / atr_ma.replace(0, np.nan)).clip(0.05, 5.0)
    rk = ratio.rolling(120, min_periods=40).rank(pct=True).fillna(0.5)
    reg = pd.Series(1.0, index=out.index, dtype=float)
    reg = reg.mask(rk < 1.0 / 3.0, 0.0)
    reg = reg.mask(rk > 2.0 / 3.0, 2.0)
    out["alpha_v2_vol_regime"] = (reg / 2.0).astype(float)

    # Donchian distance (20) normalized by ATR
    win = 20
    hh = h.rolling(win, min_periods=5).max()
    ll = l_.rolling(win, min_periods=5).min()
    out["alpha_v2_dist_to_don_hi_atr"] = _clip01(((hh - c) / atr).clip(0, 8) / 8.0)
    out["alpha_v2_dist_to_don_lo_atr"] = _clip01(((c - ll) / atr).clip(0, 8) / 8.0)

    # Z-score of close vs rolling mean (entry-safe: uses only past including current)
    ma = c.rolling(50, min_periods=15).mean()
    sd = c.rolling(50, min_periods=15).std().replace(0, np.nan)
    z = (c - ma) / sd
    out["alpha_v2_price_zscore"] = _clip01((z.clip(-4, 4) / 8.0 + 0.5))

    # Session / time (cyclical + finer bucket)
    idx = out.index
    try:
        hours = pd.Series(
            [pd.Timestamp(ts).hour + pd.Timestamp(ts).minute / 60.0 for ts in idx],
            index=out.index,
        )
    except Exception:
        hours = pd.Series(np.nan, index=out.index)
    hrad = (hours % 24.0) / 24.0 * 2 * np.pi
    out["alpha_v2_hour_sin"] = np.sin(hrad)
    out["alpha_v2_hour_cos"] = np.cos(hrad)

    def _fine_sess(hr: float) -> float:
        if np.isnan(hr):
            return 0.5
        if hr < 7:
            return 0.0
        if hr < 10:
            return 0.2
        if hr < 13:
            return 0.4
        if hr < 16:
            return 0.6
        if hr < 19:
            return 0.8
        return 1.0

    out["alpha_v2_session_fine"] = hours.apply(lambda x: _fine_sess(float(x)) if pd.notna(x) else 0.5)

    return out
