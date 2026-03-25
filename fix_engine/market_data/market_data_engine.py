from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from threading import RLock
from typing import Callable

from market_data.models import MarketData
from structured_logging import log_event


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
        self._snapshot_interval_sec = 5.0
        self._last_snapshot_log_ts: dict[str, float] = {}
        self._last_mid_by_symbol: dict[str, float] = {}
        self._spike_threshold = 0.01
        self._md_tick_count = 0
        self._last_md_monotonic: float | None = None

    def subscribe(self, callback: Callable[[MarketData], None]) -> None:
        with self._lock:
            self._subscribers.append(callback)

    def update_market_data(self, raw_data: dict[str, object]) -> MarketData:
        data = self._normalize(raw_data)
        with self._lock:
            self._latest[data.symbol] = data
            self._md_tick_count += 1
            self._last_md_monotonic = time.monotonic()
        self.notify_subscribers(data)
        return data

    def get_health_metrics(self) -> dict[str, float | int | bool]:
        """Counters for MD health logging (thread-safe snapshot)."""
        with self._lock:
            tick_count = self._md_tick_count
            last_mono = self._last_md_monotonic
        now = time.monotonic()
        if last_mono is None:
            return {
                "tick_count": tick_count,
                "time_since_last_update_ms": 999999.0,
                "no_data": tick_count == 0,
            }
        return {
            "tick_count": tick_count,
            "time_since_last_update_ms": max(0.0, (now - last_mono) * 1000.0),
            "no_data": False,
        }

    def get_latest(self, symbol: str) -> MarketData | None:
        with self._lock:
            return self._latest.get(symbol)

    def notify_subscribers(self, data: MarketData) -> None:
        with self._lock:
            subscribers = list(self._subscribers)

        if self._logger is not None:
            self._log_market_data(data)
            tot = float(data.bid_size) + float(data.ask_size) + 1e-12
            imbalance = (float(data.bid_size) - float(data.ask_size)) / tot
            log_event(
                self._logger,
                level=logging.INFO,
                component="MarketData",
                event="MD_UPDATE",
                symbol=data.symbol,
                bid=float(data.bid),
                ask=float(data.ask),
                spread=float(data.spread),
                imbalance=float(imbalance),
            )

        for callback in subscribers:
            callback(data)

    def _log_market_data(self, data: MarketData) -> None:
        now = time.monotonic()
        symbol = data.symbol
        last_log = self._last_snapshot_log_ts.get(symbol, 0.0)
        should_snapshot = (now - last_log) >= self._snapshot_interval_sec
        prev_mid = self._last_mid_by_symbol.get(symbol)
        self._last_mid_by_symbol[symbol] = float(data.mid_price)
        if prev_mid and prev_mid > 0:
            jump = abs(float(data.mid_price) - prev_mid) / prev_mid
            if jump >= self._spike_threshold:
                self._logger.warning(
                    "market_data_spike",
                    extra={
                        "component": "MarketDataEngine",
                        "event": "market_data_spike",
                        "correlation_id": "",
                        "symbol": symbol,
                        "bid": float(data.bid),
                        "ask": float(data.ask),
                        "spread": float(data.spread),
                        "last_price": float(data.last),
                        "mid_price": float(data.mid_price),
                        "jump_ratio": jump,
                    },
                )
        if should_snapshot:
            self._last_snapshot_log_ts[symbol] = now
            self._logger.info(
                "market_data_snapshot",
                extra={
                    "component": "MarketDataEngine",
                    "event": "market_data_snapshot",
                    "correlation_id": "",
                    "symbol": symbol,
                    "bid": float(data.bid),
                    "ask": float(data.ask),
                    "spread": float(data.spread),
                    "last_price": float(data.last),
                    "mid_price": float(data.mid_price),
                },
            )

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
        bid_size = float(raw_data.get("bid_size", 0.0))
        ask_size = float(raw_data.get("ask_size", 0.0))

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
            bid_size=bid_size,
            ask_size=ask_size,
        )
