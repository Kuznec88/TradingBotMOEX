"""Метрики лаборатории и склейка контрактов."""

from __future__ import annotations

import pandas as pd
import pytest

from fix_engine.tools.fetch_continuous_futures import stitch_contracts
from lab import engine


def _positions(net: list[float]) -> pd.DataFrame:
    times = pd.date_range("2024-01-31", periods=len(net), freq="ME", tz="UTC")
    return pd.DataFrame(
        {
            "asset": "SR",
            "frame": "1h",
            "side": "LONG",
            "entry_time": times,
            "exit_time": times,
            "entry_price": 30000.0,
            "exit_price": 30000.0,
            "gross_rub": [v + 10 for v in net],
            "fees_rub": 10.0,
            "net_rub": net,
            "gross_bp": [(v + 10) / 3 for v in net],
            "net_bp": [v / 3 for v in net],
            "exit_reason": "stop_loss",
            "had_partial": False,
        }
    )


def test_summarize_counts_and_drawdown() -> None:
    s = engine.summarize(_positions([100.0, -50.0, -30.0, 40.0]), months=4.0)
    assert s["positions"] == 4
    assert s["per_month"] == 1.0
    assert s["net_rub"] == 60.0
    assert s["gross_rub"] == 100.0
    assert s["fees_rub"] == 40.0
    assert s["win_rate"] == 0.5
    assert s["profit_factor"] == pytest.approx(140.0 / 80.0)
    assert s["max_drawdown_rub"] == 80.0


def test_summarize_empty() -> None:
    assert engine.summarize(pd.DataFrame(columns=engine.POSITION_COLUMNS), months=4.0)["positions"] == 0


def test_monthly_net_groups_by_exit_month() -> None:
    months = engine.monthly_net(_positions([100.0, -50.0]))
    assert months["net_rub"].tolist() == [100.0, -50.0]
    assert months["positions"].tolist() == [1, 1]


def test_htf_mode_avoids_same_timeframe_on_1h() -> None:
    params = engine.bot_defaults()
    assert engine.apply_htf_mode(params, "hard", "1h").htf_resample_rule == "4h"
    assert engine.apply_htf_mode(params, "hard", "15m").htf_resample_rule == "1h"
    assert engine.apply_htf_mode(params, "off", "15m").htf_resample_rule == ""
    assert engine.apply_htf_mode(params, "soft", "5m").htf_gate_mode == "soft"


def test_holdout_checks_are_counted(tmp_path) -> None:
    config = {"frame": "1h"}
    assert engine.log_holdout_check(config, {"positions": 1}, log_dir=tmp_path) == 1
    assert engine.log_holdout_check(config, {"positions": 1}, log_dir=tmp_path) == 2
    assert engine.log_holdout_check({"frame": "5m"}, {}, log_dir=tmp_path) == 1
    assert engine.holdout_check_count(log_dir=tmp_path) == 3


def test_stitch_removes_gap_between_contracts() -> None:
    old = pd.DataFrame({"time_utc": [1, 2], "open": [100.0, 101.0], "high": [100.0, 101.0], "low": [100.0, 101.0], "close": [100.0, 110.0]})
    new = pd.DataFrame({"time_utc": [3, 4], "open": [120.0, 121.0], "high": [120.0, 121.0], "low": [120.0, 121.0], "close": [120.0, 125.0]})
    out = stitch_contracts([old, new])
    assert out["close"].tolist() == [110.0, 120.0, 120.0, 125.0]
