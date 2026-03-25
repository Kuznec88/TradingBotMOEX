from __future__ import annotations

from dataclasses import dataclass

from fix_engine.market_data.models import MarketData
from fix_engine.strategy.base import Strategy


@dataclass(frozen=True)
class StrategyEngine:
    """
    Thin orchestrator for exactly one strategy instance.
    Intentionally minimal: no confirmations, no batching, no extra state.
    """

    strategy: Strategy

    def on_market_data(self, data: MarketData) -> None:
        self.strategy.on_market_data(data)

    def on_execution_report(self, state: dict[str, object]) -> None:
        self.strategy.on_execution_report(state)

