from __future__ import annotations

from typing import Protocol

from fix_engine.market_data.models import MarketData


class Strategy(Protocol):
    def on_market_data(self, data: MarketData) -> None: ...

    def on_execution_report(self, state: dict[str, object]) -> None: ...

