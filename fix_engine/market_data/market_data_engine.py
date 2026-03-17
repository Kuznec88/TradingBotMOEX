from __future__ import annotations

import logging
from datetime import datetime, timezone
from threading import RLock
from typing import Callable

from market_data.models import MarketData


class MarketDataEngine:
    """
    Centralized market data hub:
    - normalizes raw input into MarketData
    - stores latest snapshot by symbol
    - notifies subscribers (strategies)
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger
        self._lock = RLock()
        self._latest: dict[str, MarketData] = {}
        self._subscribers: list[Callable[[MarketData], None]] = []

    def subscribe(self, callback: Callable[[MarketData], None]) -> None:
        with self._lock:
            self._subscribers.append(callback)

    def update_market_data(self, raw_data: dict[str, object]) -> MarketData:
        data = self._normalize(raw_data)
        with self._lock:
            self._latest[data.symbol] = data
        self.notify_subscribers(data)
        return data

    def get_latest(self, symbol: str) -> MarketData | None:
        with self._lock:
            return self._latest.get(symbol)

    def notify_subscribers(self, data: MarketData) -> None:
        with self._lock:
            subscribers = list(self._subscribers)

        if self._logger is not None:
            self._logger.info(
                "[MARKET] %s bid=%s ask=%s mid=%s spread=%s",
                data.symbol,
                data.bid,
                data.ask,
                round(data.mid_price, 6),
                round(data.spread, 6),
            )

        for callback in subscribers:
            callback(data)

    @staticmethod
    def _normalize(raw_data: dict[str, object]) -> MarketData:
        symbol = str(raw_data.get("symbol") or raw_data.get("ticker") or "").strip().upper()
        if not symbol:
            raise ValueError("raw_data must include symbol/ticker.")

        bid = float(raw_data.get("bid", 0.0))
        ask = float(raw_data.get("ask", 0.0))
        last_raw = raw_data.get("last")
        last = float(last_raw) if last_raw is not None else (bid + ask) / 2
        volume = float(raw_data.get("volume", 0.0))

        timestamp_raw = raw_data.get("timestamp")
        if isinstance(timestamp_raw, datetime):
            ts = timestamp_raw
        elif isinstance(timestamp_raw, str) and timestamp_raw:
            ts = datetime.fromisoformat(timestamp_raw)
        else:
            ts = datetime.now(timezone.utc)

        return MarketData(
            symbol=symbol,
            bid=bid,
            ask=ask,
            last=last,
            volume=volume,
            timestamp=ts,
        )
