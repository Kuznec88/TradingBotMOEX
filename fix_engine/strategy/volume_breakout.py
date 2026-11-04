"""Вход на пробое с объёмом: закрытие за экстремумом N прошлых баров при всплеске объёма, без ретеста."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from fix_engine.strategy import swing_breakout as sb
from fix_engine.strategy.swing_breakout import RetestEntryIntent, SwingBreakoutParams


def volume_breakout_intent(row: pd.Series, params: SwingBreakoutParams) -> RetestEntryIntent | None:
    close = float(row["close"])
    high_n = float(row.get("high_N", math.nan))
    low_n = float(row.get("low_N", math.nan))
    avg_volume = float(row.get("avg_volume", math.nan))
    atr = float(row.get("atr", math.nan))
    if not all(math.isfinite(v) for v in (high_n, low_n, avg_volume, atr)) or atr <= 0:
        return None
    if float(row["volume"]) <= params.k_volume * avg_volume:
        return None
    if close > high_n:
        side = 1
    elif close < low_n:
        side = -1
    else:
        return None
    tick = max(float(params.tick_size), 1e-12)
    risk = sb._adaptive_stop_distance(atr, tick, params)
    return RetestEntryIntent(
        side=side,
        limit_price=sb._round_to_tick(close, tick),
        stop_price=close - side * risk,
        take_profit_price=sb._round_to_tick(close + side * params.risk_reward_multiple * risk, tick),
        risk_per_unit=risk,
        use_market_entry=True,
        entry_reason="volume_breakout",
        setup_tag="volume_breakout",
    )


def volume_breakout_extra_intent(
    *,
    row: pd.Series,
    params: SwingBreakoutParams,
    governor: Any,
    bar_time: pd.Timestamp | None,
    **_: Any,
) -> RetestEntryIntent | None:
    if governor is not None and not governor.allow_new_entry():
        return None
    if not sb._session_allows(bar_time, params):
        return None
    return volume_breakout_intent(row, params)
