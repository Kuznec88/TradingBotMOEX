"""Сигнал пробоя с объёмом."""

from __future__ import annotations

import pandas as pd

from fix_engine.strategy.swing_breakout import SwingBreakoutParams
from fix_engine.strategy.volume_breakout import volume_breakout_intent

PARAMS = SwingBreakoutParams(tick_size=1.0, k_volume=1.5, adaptive_stop_atr_mult=2.0, stop_buffer_ticks=2.0, risk_reward_multiple=3.0)


def _row(close: float, volume: float = 200.0) -> pd.Series:
    return pd.Series({"close": close, "volume": volume, "high_N": 110.0, "low_N": 90.0, "avg_volume": 100.0, "atr": 5.0})


def test_long_on_close_above_range_with_volume() -> None:
    intent = volume_breakout_intent(_row(111.0), PARAMS)
    assert intent is not None
    assert intent.side == 1
    assert intent.use_market_entry
    assert intent.stop_price == 101.0
    assert intent.take_profit_price == 141.0


def test_short_on_close_below_range_with_volume() -> None:
    intent = volume_breakout_intent(_row(89.0), PARAMS)
    assert intent is not None
    assert intent.side == -1
    assert intent.stop_price == 99.0
    assert intent.take_profit_price == 59.0


def test_no_entry_without_volume() -> None:
    assert volume_breakout_intent(_row(111.0, volume=150.0), PARAMS) is None


def test_no_entry_inside_range() -> None:
    assert volume_breakout_intent(_row(100.0), PARAMS) is None


def test_no_entry_while_indicators_warm_up() -> None:
    row = _row(111.0)
    row["high_N"] = float("nan")
    assert volume_breakout_intent(row, PARAMS) is None
