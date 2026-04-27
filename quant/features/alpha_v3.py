"""
Extended microstructure features (v3). Chained after v1 + v2. Entry-time safe.

Prefixed ``alpha_v3_``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _clip01(x: pd.Series) -> pd.Series:
    return x.clip(0.0, 1.0)


def attach_alpha_v3_features(df: pd.DataFrame) -> pd.DataFrame:
    """Expects OHLCV + ``atr`` and prior alpha columns."""
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

    # Two-bar false-breakout narrative: extreme 2 bars ago, failure now
    ph2 = h.shift(2)
    pl2 = l_.shift(2)
    sweep_up_2 = (h.shift(1) > ph2) & ph2.notna()
    fail_up = sweep_up_2 & (c < ph2)
    sweep_dn_2 = (l_.shift(1) < pl2) & pl2.notna()
    fail_dn = sweep_dn_2 & (c > pl2)
    out["alpha_v3_fail_break_2bar_up"] = fail_up.astype(float)
    out["alpha_v3_fail_break_2bar_dn"] = fail_dn.astype(float)

    # 5-bar return vs ATR (distinct from v2 3-bar)
    ret5 = c - c.shift(5)
    out["alpha_v3_ret5_atr_norm"] = _clip01((ret5 / atr).clip(-8, 8) / 16.0 + 0.5)

    # True range as fraction of ATR — regime of bar "size"
    tr = pd.concat([h - l_, (h - c.shift(1)).abs(), (l_ - c.shift(1)).abs()], axis=1).max(axis=1)
    out["alpha_v3_tr_over_atr_pctile"] = (tr / atr.replace(0, np.nan)).rolling(60, min_periods=15).rank(pct=True).clip(0, 1)

    # Donchian 10 (vs 20 in v2)
    w10 = 10
    hh10 = h.rolling(w10, min_periods=3).max()
    ll10 = l_.rolling(w10, min_periods=3).min()
    out["alpha_v3_dist_don10_hi_atr"] = _clip01(((hh10 - c) / atr).clip(0, 8) / 8.0)
    out["alpha_v3_dist_don10_lo_atr"] = _clip01(((c - ll10) / atr).clip(0, 8) / 8.0)

    # Shorter z-score window
    ma20 = c.rolling(20, min_periods=8).mean()
    sd20 = c.rolling(20, min_periods=8).std().replace(0, np.nan)
    z20 = (c - ma20) / sd20
    out["alpha_v3_zscore_20"] = _clip01(z20.clip(-4, 4) / 8.0 + 0.5)

    # Minute-of-day (session microstructure)
    idx = out.index
    try:
        mins = pd.Series(
            [pd.Timestamp(ts).hour * 60 + pd.Timestamp(ts).minute for ts in idx],
            index=out.index,
        )
        out["alpha_v3_minute_of_day_norm"] = (mins % 1440) / 1440.0
    except Exception:
        out["alpha_v3_minute_of_day_norm"] = 0.5

    # Local extremes vs rolling window (50)
    rw = 50
    roll_max_h = h.rolling(rw, min_periods=10).max()
    roll_min_l = l_.rolling(rw, min_periods=10).min()
    out["alpha_v3_dist_local_high_atr"] = _clip01(((roll_max_h - h) / atr).clip(0, 6) / 6.0)
    out["alpha_v3_dist_local_low_atr"] = _clip01(((l_ - roll_min_l) / atr).clip(0, 6) / 6.0)

    # Position in 30-bar range
    rw30 = 30
    hh30 = h.rolling(rw30, min_periods=8).max()
    ll30 = l_.rolling(rw30, min_periods=8).min()
    denom = (hh30 - ll30).replace(0, np.nan)
    out["alpha_v3_pos_in_range_30"] = _clip01(((c - ll30) / denom).fillna(0.5))

    # Candle shape (explicit shadows + signed body)
    body = c - o
    wick_u = h - np.maximum(o, c)
    wick_l = np.minimum(o, c) - l_
    out["alpha_v3_upper_shadow_frac"] = _clip01((wick_u / rng).fillna(0))
    out["alpha_v3_lower_shadow_frac"] = _clip01((wick_l / rng).fillna(0))
    body_r = (body.abs() / rng).fillna(0)
    out["alpha_v3_body_signed_ratio"] = np.sign(body) * _clip01(body_r)
    pin = (np.maximum(wick_u, wick_l) - body.abs()) / rng.replace(0, np.nan)
    out["alpha_v3_pin_bar_score"] = _clip01(pin.fillna(0))

    return out
