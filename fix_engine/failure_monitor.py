from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

from fix_engine.execution_gateway import ExecutionGateway
from fix_engine.market_data.models import MarketData
from fix_engine.order_manager import OrderManager


class FailureMonitor:
    """
    Basic runtime anomaly detection:
    - missing market data updates
    - stuck orders
    - no execution reports
    """

    def __init__(
        self,
        *,
        logger: logging.Logger,
        order_manager: OrderManager,
        gateway: ExecutionGateway,
        watch_symbol: str,
        max_market_data_staleness_sec: int,
        max_order_stuck_sec: int,
        max_no_execution_report_sec: int,
        action_on_anomaly: str = "ALERT",  # ALERT | STOP
        check_interval_sec: int = 1,
    ) -> None:
        self.logger = logger
        self.order_manager = order_manager
        self.gateway = gateway
        self.watch_symbol = watch_symbol.upper()
        self.max_market_data_staleness_sec = max(1, int(max_market_data_staleness_sec))
        self.max_order_stuck_sec = max(1, int(max_order_stuck_sec))
        self.max_no_execution_report_sec = max(1, int(max_no_execution_report_sec))
        self.action_on_anomaly = action_on_anomaly.upper()
        self.check_interval_sec = max(1, int(check_interval_sec))

        self._lock = threading.RLock()
        self._running = False
        self._thread: threading.Thread | None = None
        self._last_md_ts: datetime | None = None
        self._last_exec_report_ts: datetime | None = None
        self._last_alert_at: dict[str, float] = {}
        self._alert_cooldown_sec = 5.0

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._run, name="failure-monitor", daemon=True)
            self._thread.start()
            self.logger.info(
                "failure_monitor_started",
                extra={
                    "component": "FailureMonitor",
                    "event": "failure_monitor_started",
                    "correlation_id": "",
                },
            )

    def stop(self) -> None:
        with self._lock:
            self._running = False
            thread = self._thread
            self._thread = None
        if thread is not None:
            thread.join(timeout=2.0)
        self.logger.info(
            "failure_monitor_stopped",
            extra={
                "component": "FailureMonitor",
                "event": "failure_monitor_stopped",
                "correlation_id": "",
            },
        )

    def on_market_data(self, data: MarketData) -> None:
        if data.symbol.upper() != self.watch_symbol:
            return
        with self._lock:
            self._last_md_ts = datetime.now(timezone.utc)

    def on_execution_report(self) -> None:
        with self._lock:
            self._last_exec_report_ts = datetime.now(timezone.utc)

    def _run(self) -> None:
        while True:
            with self._lock:
                if not self._running:
                    return
            self._check_once()
            time.sleep(self.check_interval_sec)

    def _check_once(self) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            last_md_ts = self._last_md_ts
            last_exec_ts = self._last_exec_report_ts

        open_orders = self.order_manager.get_open_orders()
        if last_md_ts is None:
            self._emit_anomaly("missing_market_data", "No market data received yet")
        else:
            md_age = (now - last_md_ts).total_seconds()
            if md_age > self.max_market_data_staleness_sec:
                self._emit_anomaly(
                    "stale_market_data",
                    f"symbol={self.watch_symbol} age_sec={md_age:.1f} limit={self.max_market_data_staleness_sec}",
                )

        if open_orders:
            oldest_age = max((now - o.created_at).total_seconds() for o in open_orders)
            if oldest_age > self.max_order_stuck_sec:
                ids = ",".join(o.cl_ord_id for o in open_orders[:5])
                self._emit_anomaly(
                    "stuck_orders",
                    f"open_count={len(open_orders)} oldest_age_sec={oldest_age:.1f} ids={ids}",
                )

            if last_exec_ts is None:
                self._emit_anomaly(
                    "no_execution_reports",
                    f"open_count={len(open_orders)} no execution reports received yet",
                )
            else:
                exec_age = (now - last_exec_ts).total_seconds()
                if exec_age > self.max_no_execution_report_sec:
                    self._emit_anomaly(
                        "no_execution_reports",
                        f"open_count={len(open_orders)} last_exec_age_sec={exec_age:.1f} "
                        f"limit={self.max_no_execution_report_sec}",
                    )

    def _emit_anomaly(self, key: str, message: str) -> None:
        now_monotonic = time.monotonic()
        last = self._last_alert_at.get(key, 0.0)
        if now_monotonic - last < self._alert_cooldown_sec:
            return
        self._last_alert_at[key] = now_monotonic

        self.logger.error(
            "anomaly_detected",
            extra={
                "component": "FailureMonitor",
                "event": "anomaly_detected",
                "correlation_id": "",
                "anomaly_type": key,
                "details": message,
            },
        )
        if self.action_on_anomaly == "STOP":
            self.logger.critical(
                "kill_switch_triggered",
                extra={
                    "component": "FailureMonitor",
                    "event": "kill_switch_triggered",
                    "correlation_id": "",
                    "reason": f"anomaly:{key}",
                },
            )
            self.gateway.set_trading_enabled(False, reason=f"anomaly:{key}")
