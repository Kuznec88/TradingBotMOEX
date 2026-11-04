"""Выходы бэктеста не должны опираться на порядок high/low внутри бара."""

from __future__ import annotations

from dataclasses import replace

import pandas as pd

from fix_engine.strategy.swing_breakout import RetestEntryIntent, SwingBreakoutParams, run_backtest

ENTRY_BAR = 130
ENTRY = 1000.0
STOP = 900.0
RISK = 100.0


def _params(**overrides: object) -> SwingBreakoutParams:
    base = SwingBreakoutParams(
        tick_size=1.0,
        slippage_ticks=0,
        commission_rate=0.0,
        htf_resample_rule="",
        htf_secondary_resample_rule="",
        partial_exit_fraction=0.0,
        trail_after_r_multiple=1.0,
        trail_gap_r_multiple=0.5,
        swing_trade_start_hhmm="",
        swing_trade_end_hhmm="",
        swing_lunch_skip=False,
    )
    return replace(base, **overrides)


def _candles(after_entry: list[tuple[float, float, float]], *, entry_high: float = ENTRY) -> pd.DataFrame:
    """Плоская цена до входа, на входе бар с заданным high, затем бары (high, low, close)."""
    rows = [(ENTRY, ENTRY, ENTRY, ENTRY)] * ENTRY_BAR
    rows.append((ENTRY, entry_high, ENTRY, ENTRY))
    rows.extend((close, high, low, close) for high, low, close in after_entry)
    rows.extend([(rows[-1][3],) * 4] * 3)
    frame = pd.DataFrame(rows, columns=["open", "high", "low", "close"])
    frame["volume"] = 100
    frame.index = pd.date_range("2025-01-06 07:00", periods=len(frame), freq="5min", tz="UTC")
    return frame


def _run(candles: pd.DataFrame, params: SwingBreakoutParams) -> list:
    def enter_once(*, i: int, **_: object) -> RetestEntryIntent | None:
        if i != ENTRY_BAR:
            return None
        return RetestEntryIntent(
            side=1,
            limit_price=ENTRY,
            stop_price=STOP,
            take_profit_price=ENTRY + 20 * RISK,
            risk_per_unit=RISK,
            use_market_entry=True,
        )

    return run_backtest(candles, params, log_trades=False, extra_intent_fn=enter_once).trades


def test_trailing_stop_is_not_hit_by_the_bar_that_moved_it() -> None:
    # Бар дошёл до +3R и откатил до +1.5R. Новый трейлинг (+2.5R) действует только со следующего бара.
    trades = _run(_candles([(1300.0, 1150.0, 1200.0), (1210.0, 1100.0, 1120.0)]), _params())
    assert trades[0].exit_reason == "trailing_stop"
    assert trades[0].exit_bar == ENTRY_BAR + 2
    assert trades[0].exit_price == 1250.0


def test_trailing_ignores_high_before_market_entry() -> None:
    # Максимум бара входа был до входа по close и не должен включать трейлинг.
    trades = _run(_candles([(1050.0, 950.0, 1000.0), (1010.0, 890.0, 900.0)], entry_high=1500.0), _params())
    assert trades[0].exit_reason == "stop_loss"
    assert trades[0].exit_price == STOP


def test_partial_exit_is_skipped_when_bar_also_touches_stop() -> None:
    trades = _run(_candles([(1150.0, 890.0, 1000.0)]), _params(partial_exit_fraction=0.5, trail_after_r_multiple=0.0))
    assert [t.exit_reason for t in trades] == ["stop_loss"]


def test_partial_exit_applies_when_stop_is_not_touched() -> None:
    trades = _run(
        _candles([(1150.0, 950.0, 1100.0), (1110.0, 890.0, 900.0)]),
        _params(partial_exit_fraction=0.5, trail_after_r_multiple=0.0),
    )
    assert [t.exit_reason for t in trades] == ["partial_tp", "stop_loss"]
    assert trades[0].exit_price == ENTRY + RISK
