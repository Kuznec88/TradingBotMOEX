from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class MarketType(str, Enum):
    EQUITIES = "EQUITIES"
    FORTS = "FORTS"


@dataclass(frozen=True)
class OrderRequest:
    symbol: str
    side: str | int
    qty: float
    price: float | None = None
    market: MarketType = MarketType.EQUITIES
    lot_size: int = 1
