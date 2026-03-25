"""Periodic MD health: updates/sec, staleness, missing_market_data."""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

from structured_logging import log_event

if TYPE_CHECKING:
    from market_data.market_data_engine import MarketDataEngine


class MdHealthMonitor:
    def __init__(
        self,
        *,
        market_data_engine: MarketDataEngine,
        logger: logging.Logger,
        interval_sec: float = 5.0,
    ) -> None:
        self._mde = market_data_engine
        self._logger = logger
        self._interval = max(1.0, float(interval_sec))
        self._running = False
        self._thread: threading.Thread | None = None
        self._last_tick_count = 0

    def start(self) -> None:
        with threading.Lock():
            if self._running:
                return
            self._running = True
            self._last_tick_count = 0
            self._thread = threading.Thread(target=self._run, name="md-health", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._running = False
        t = self._thread
        self._thread = None
        if t is not None:
            t.join(timeout=self._interval + 2.0)

    def _run(self) -> None:
        while self._running:
            time.sleep(self._interval)
            if not self._running:
                break
            snap = self._mde.get_health_metrics()
            tick_count = int(snap["tick_count"])
            delta = max(0, tick_count - self._last_tick_count)
            self._last_tick_count = tick_count
            ups = float(delta) / self._interval
            stale_ms = float(snap["time_since_last_update_ms"])
            log_event(
                self._logger,
                level=logging.INFO,
                component="MdHealth",
                event="MD_HEALTH",
                updates_per_second=round(ups, 4),
                time_since_last_update_ms=round(stale_ms, 3),
                tick_count=tick_count,
            )
            if snap.get("no_data") or stale_ms > 30_000.0:
                log_event(
                    self._logger,
                    level=logging.WARNING,
                    component="MdHealth",
                    event="missing_market_data",
                    detail="missing_market_data",
                    time_since_last_update_ms=round(stale_ms, 3),
                    tick_count=tick_count,
                )
