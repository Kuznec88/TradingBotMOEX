"""
Архитектура v2/v3: ансамбль входов + опционально unified signal_score (signal_scoring.py).

Приоритет входов на баре: основной FSM (retest) → затем extra (continuation vs pullback — лучший по score+priority).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any, Callable

import numpy as np
import pandas as pd

from fix_engine.strategy.signal_scoring import (
    SignalScorePolicy,
    build_intent_filter_fn,
    compute_unified_signal_score,
    priority_weight,
)
from fix_engine.strategy.swing_breakout import (
    RetestEntryIntent,
    SwingBreakoutParams,
    SwingRetestFSMState,
    compute_indicators,
    load_ohlcv_csv,
    run_backtest,
)
from fix_engine.strategy import swing_breakout as sb


@dataclass
class SwingV2Config:
    enable_continuation: bool = False
    enable_ema_pullback: bool = False
    continuation_vol_mult: float = 1.35
    ema_pullback_period: int = 20
    pullback_require_trend_regime: bool = True
    # momentum persistence: сколько подряд бычьих тел перед continuation
    continuation_bull_bars: int = 2
    # фильтр signal_score (None = без фильтра)
    score_policy: SignalScorePolicy | None = None


def enrich_regime_and_vwap(df: pd.DataFrame, p: SwingBreakoutParams, cfg: SwingV2Config) -> pd.DataFrame:
    out = df.copy()
    atr = out["atr"].astype(float)
    med = atr.rolling(100, min_periods=20).median()
    ratio = atr / med.replace(0, np.nan)
    reg = np.where(ratio > 1.08, 1, np.where(ratio < 0.92, -1, 0))
    out["market_regime"] = reg
    out["regime_size_mult"] = np.where(reg == 1, 1.12, np.where(reg == -1, 0.85, 1.0)).astype(float)
    pv = (out["close"] * out["volume"].astype(float)).cumsum()
    vv = out["volume"].astype(float).cumsum().replace(0, np.nan)
    out["vwap_approx"] = pv / vv
    ema_n = max(5, int(cfg.ema_pullback_period))
    em = out["close"].ewm(span=ema_n, adjust=False).mean()
    out["ema_pullback"] = em
    out["ema_slope"] = (em - em.shift(2)) / atr.replace(0, np.nan)
    return out


def _diag_nan(row: pd.Series, name: str) -> float:
    v = row.get(name)
    return float(v) if v is not None and pd.notna(v) else float("nan")


def _synthetic_long_market(
    row: pd.Series,
    p: SwingBreakoutParams,
    *,
    entry_reason: str,
    size_mult: float,
) -> RetestEntryIntent | None:
    tick = max(float(p.tick_size), 1e-12)
    atr_v = float(row.get("atr", 0) or 0)
    if not math.isfinite(atr_v) or atr_v <= 0:
        return None
    cl = float(row["close"])
    hi = float(row["high"])
    lo = float(row["low"])
    dist = sb._adaptive_stop_distance(atr_v, tick, p)
    if dist <= 1e-12:
        return None
    sl = cl - dist
    risk = cl - sl
    if risk <= 1e-12:
        return None
    tp = cl + p.risk_reward_multiple * risk
    tp = sb._round_to_tick(tp, tick)
    rng_bar = hi - lo
    prev_max = row.get("impulse_prev_range_max")
    prev_max_f = float(prev_max) if pd.notna(prev_max) else float("nan")
    ratio_imp = (rng_bar / prev_max_f) if (np.isfinite(prev_max_f) and prev_max_f > 1e-12) else float("nan")
    rr_m = abs(tp - cl) / max(risk, 1e-12)
    return RetestEntryIntent(
        side=1,
        limit_price=sb._round_to_tick(cl, tick),
        stop_price=sl,
        take_profit_price=tp,
        risk_per_unit=risk,
        use_market_entry=True,
        entry_reason=entry_reason,
        diag_atr_slope=_diag_nan(row, "atr_slope"),
        diag_range_impulse_ratio=ratio_imp,
        diag_retest_bars_to_touch=0,
        diag_range_atr_ratio=_diag_nan(row, "range_atr_ratio"),
        diag_rr_multiple=rr_m,
        size_mult=max(0.2, min(1.5, float(size_mult))),
        setup_tag="",
        signal_score=1.0,
    )


def _bullish_bar(row: pd.Series) -> bool:
    return float(row["close"]) > float(row["open"])


def _try_continuation(
    i: int,
    row: pd.Series,
    df: pd.DataFrame,
    params: SwingBreakoutParams,
    cfg: SwingV2Config,
    base_sm: float,
) -> RetestEntryIntent | None:
    if i < 3:
        return None
    av = row.get("avg_volume")
    vol = float(row.get("volume", 0) or 0)
    av_f = float(av) if pd.notna(av) else float("nan")
    vol_spike = bool(np.isfinite(av_f) and av_f > 0 and vol > av_f * params.k_volume * cfg.continuation_vol_mult)
    atr_v = float(row.get("atr", 0) or 0)
    sl = float(row.get("atr_slope", 0) or 0)
    if sl < 1.0:
        return None
    nb = max(1, int(cfg.continuation_bull_bars))
    ok_bull = True
    for k in range(nb):
        if i - 1 - k < 0:
            ok_bull = False
            break
        if not _bullish_bar(df.iloc[i - 1 - k]):
            ok_bull = False
            break
    if not ok_bull:
        return None
    recent_hi = float(df["high"].iloc[max(0, i - 4) : i + 1].max())
    if float(row["low"]) < recent_hi - 0.5 * atr_v:
        return None
    hN = row.get("high_N")
    ma = row.get("ma")
    if (
        not vol_spike
        or pd.isna(hN)
        or float(row["close"]) <= float(hN)
        or pd.isna(ma)
        or float(row["close"]) <= float(ma)
        or float(row.get("market_regime", 0)) < 0
    ):
        return None
    return _synthetic_long_market(row, params, entry_reason="v2_continuation", size_mult=base_sm * 0.95)


def _try_pullback(
    row: pd.Series,
    params: SwingBreakoutParams,
    cfg: SwingV2Config,
    base_sm: float,
) -> RetestEntryIntent | None:
    if cfg.pullback_require_trend_regime and float(row.get("market_regime", 0)) != 1:
        return None
    es = row.get("ema_slope")
    if es is None or pd.isna(es) or float(es) <= 0:
        return None
    ema = row.get("ema_pullback")
    atr_v = float(row.get("atr", 0) or 0)
    if ema is None or pd.isna(ema) or atr_v <= 0:
        return None
    ema_f = float(ema)
    lo = float(row["low"])
    cl = float(row["close"])
    hi = float(row["high"])
    depth = (ema_f - lo) / atr_v
    if depth < 0.3 or depth > 0.65:
        return None
    if not (float(row["low"]) <= ema_f * 1.002 and cl > ema_f):
        return None
    mid = 0.5 * (hi + lo)
    if not (cl > float(row["open"]) and cl >= mid):
        return None
    return _synthetic_long_market(row, params, entry_reason="v2_ema_pullback", size_mult=base_sm * 0.9)


def build_extra_intent_fn(
    p: SwingBreakoutParams,
    cfg: SwingV2Config,
) -> Callable[..., RetestEntryIntent | None]:
    def extra(
        *,
        i: int,
        row: pd.Series,
        df: pd.DataFrame,
        params: SwingBreakoutParams,
        governor: Any,
        bar_time: Any,
        main_fsm_state: SwingRetestFSMState,
        **_: Any,
    ) -> RetestEntryIntent | None:
        if governor is not None and not governor.allow_new_entry():
            return None
        htf_sm = float(row.get("htf_score_long", 1.0))
        reg_sm = float(row.get("regime_size_mult", 1.0))
        base_sm = htf_sm * reg_sm

        cands: list[tuple[RetestEntryIntent, str]] = []
        if cfg.enable_continuation:
            c = _try_continuation(i, row, df, params, cfg, base_sm)
            if c is not None:
                cands.append((replace(c, setup_tag="continuation"), "continuation"))
        if cfg.enable_ema_pullback:
            pl = _try_pullback(row, params, cfg, base_sm)
            if pl is not None:
                cands.append((replace(pl, setup_tag="pullback"), "pullback"))
        if not cands:
            return None
        if len(cands) == 1:
            return cands[0][0]

        def rank(t: tuple[RetestEntryIntent, str]) -> tuple[float, float]:
            it, tag = t
            sc, _ = compute_unified_signal_score(intent=it, row=row, params=params)
            return (float(priority_weight(tag)), sc)

        best = max(cands, key=rank)
        return best[0]

    return extra


def run_backtest_v2(
    ohlcv: pd.DataFrame,
    params: SwingBreakoutParams | None = None,
    *,
    cfg: SwingV2Config | None = None,
    qty: float = 1.0,
    log_trades: bool = False,
    post_enrich_fn: Any = None,
) -> Any:
    """post_enrich_fn: optional df -> df (e.g. attach research alpha features after indicators)."""
    p = params or SwingBreakoutParams()
    c = cfg or SwingV2Config()
    df = ohlcv.copy()
    df = compute_indicators(df, n=p.n_levels, ma_period=p.ma_period, params=p)
    df = enrich_regime_and_vwap(df, p, c)
    if post_enrich_fn is not None:
        df = post_enrich_fn(df)
    extra = build_extra_intent_fn(p, c)
    filt: Callable[..., RetestEntryIntent | None] | None = None
    if c.score_policy is not None:
        filt = build_intent_filter_fn(c.score_policy)
    return run_backtest(
        df,
        p,
        qty=qty,
        log_trades=log_trades,
        extra_intent_fn=extra,
        intent_filter_fn=filt,
        pre_indicators=True,
    )


def run_backtest_late_retest_only(
    ohlcv: pd.DataFrame,
    params: SwingBreakoutParams | None = None,
    *,
    max_retest_bars_fast: int = 7,
    min_retest_bars_for_touch: int = 3,
    qty: float = 1.0,
    log_trades: bool = False,
    score_policy: SignalScorePolicy | None = None,
) -> Any:
    p0 = params or SwingBreakoutParams()
    p = replace(
        p0,
        max_retest_bars_fast=max_retest_bars_fast,
        min_retest_bars_for_touch=min_retest_bars_for_touch,
    )
    df = compute_indicators(ohlcv.copy(), n=p.n_levels, ma_period=p.ma_period, params=p)
    df = enrich_regime_and_vwap(df, p, SwingV2Config())
    filt = build_intent_filter_fn(score_policy) if score_policy is not None else None
    return run_backtest(
        df,
        p,
        qty=qty,
        log_trades=log_trades,
        intent_filter_fn=filt,
        pre_indicators=True,
    )
