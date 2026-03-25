from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from fix_engine.execution_gateway import ExecutionGateway
from fix_engine.market_data.models import MarketData


@dataclass(frozen=True)
class ExecutionEngine:
    """
    Facade over `ExecutionGateway` to keep runtime wiring simple.
    """

    gateway: ExecutionGateway

    def on_market_data(self, data: MarketData) -> None:
        self.gateway.on_market_data(data)

    def set_on_execution_report(self, cb: Callable[[object, str], None]) -> None:
        # `ExecutionGateway` takes callback in ctor; this exists for future extraction
        # without changing call-sites yet.
        _ = cb

