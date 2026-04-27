"""Tests for PnL aggregates and expectancy."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quant.research.pnl_metrics import (
    aggregate_sample_metrics,
    expectancy_decomposition,
    profit_factor,
    pnl_without_top_one,
    sizing_scale_for_6m_target_rub,
)


def test_expectancy_matches_mean() -> None:
    p = np.array([10.0, -5.0, 3.0, -2.0])
    dec = expectancy_decomposition(p)
    assert dec["mean_pnl"] == pytest.approx(float(np.mean(p)))
    assert dec["expectancy_from_components"] == pytest.approx(float(np.mean(p)))


def test_profit_factor() -> None:
    p = np.array([10.0, 5.0, -4.0, -6.0])
    pf = profit_factor(p)
    assert pf is not None
    assert pf == pytest.approx(15.0 / 10.0)


def test_pnl_without_top_one() -> None:
    p = np.array([1.0, 2.0, 100.0])
    assert pnl_without_top_one(p) == pytest.approx(3.0)


def test_aggregate_sample_metrics() -> None:
    df = pd.DataFrame({"pnl": [10.0, -5.0, 8.0], "timestamp": pd.date_range("2025-01-01", periods=3, freq="h")})
    m = aggregate_sample_metrics(df)
    assert m["n"] == 3
    assert m["expectancy"] == pytest.approx(13.0 / 3.0)
    assert m["pnl_wo_top1"] == pytest.approx(3.0)


def test_sizing_scale_positive_edge() -> None:
    df = pd.DataFrame({"pnl": [100.0, -20.0], "timestamp": pd.date_range("2025-01-01", periods=2, freq="30d")})
    # target and projection are both per 1 contract (RUB)
    s = sizing_scale_for_6m_target_rub(df, target_6m_pnl_rub=4_000.0, projected_6m_pnl_rub=1_000.0)
    assert s["implied_qty_multiplier"] == pytest.approx(4.0)


def test_sizing_scale_planned_contracts_account_view() -> None:
    df = pd.DataFrame({"pnl": [100.0, -20.0], "timestamp": pd.date_range("2025-01-01", periods=2, freq="30d")})
    s = sizing_scale_for_6m_target_rub(
        df,
        target_6m_pnl_rub=800.0,
        projected_6m_pnl_rub=1_000.0,
        planned_contracts=50.0,
    )
    assert s["target_6m_account_rub"] == pytest.approx(40_000.0)
    assert s["projected_6m_account_rub_at_planned_contracts"] == pytest.approx(50_000.0)


def test_sizing_scale_negative_edge() -> None:
    df = pd.DataFrame({"pnl": [-1.0], "timestamp": pd.date_range("2025-01-01", periods=1, freq="d")})
    s = sizing_scale_for_6m_target_rub(df, target_6m_pnl_rub=40_000.0, projected_6m_pnl_rub=-500.0)
    assert s["implied_qty_multiplier"] is None
    assert s["note"] == "negative_or_zero_edge_scaling_wont_fix"
