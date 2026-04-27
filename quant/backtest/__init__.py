"""Thin facade over fix_engine backtest + research alpha features (v1→v2→v3)."""

from fix_engine.strategy.swing_v2 import run_backtest_v2

from quant.features.alpha_candidates import attach_alpha_features

__all__ = ["run_backtest_v2", "attach_alpha_features"]
