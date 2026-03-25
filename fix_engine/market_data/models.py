from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class MarketData:
    symbol: str
    bid: float
    ask: float
    last: float
    volume: float
    timestamp: datetime
    bid_size: float = 0.0
    ask_size: float = 0.0

    @property
    def mid_price(self) -> float:
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> float:
        return self.ask - self.bid
