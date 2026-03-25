from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from threading import RLock

from fix_engine.order_manager import OrderManager
from fix_engine.order_models import MarketType


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reason: str = ""


class RiskManager:
    """
    Basic production-style pre-trade risk controls:
    - max exposure per instrument
    - max trades per rolling window
    - cooldown after consecutive losses
    """

    def __init__(
        self,
        order_manager: OrderManager,
        max_exposure_per_instrument: float,
        max_trades_in_window: int,
        trades_window_seconds: int,
        cooldown_after_consecutive_losses: int,
        cooldown_seconds: int,
        logger: logging.Logger | None = None,
    ) -> None:
        self._order_manager = order_manager
        self._max_exposure = float(max_exposure_per_instrument)
        self._max_trades = int(max_trades_in_window)
        self._window_sec = int(trades_window_seconds)
        self._cooldown_after_losses = int(cooldown_after_consecutive_losses)
        self._cooldown_sec = int(cooldown_seconds)
        self._logger = logger

        self._lock = RLock()
        self._trade_timestamps: deque[datetime] = deque()
        self._consecutive_losses = 0
        self._cooldown_until: datetime | None = None

    def pre_check_order(self, *, symbol: str, qty: float, market: MarketType) -> RiskDecision:
        now = datetime.now(timezone.utc)
        symbol = symbol.upper()
        qty = abs(float(qty))

        with self._lock:
            if self._cooldown_until and now < self._cooldown_until:
                self._log_risk_event(
                    "risk_reject",
                    reason="cooldown_active",
                    symbol=symbol,
                    market=market.value,
                    qty=qty,
                    limit=self._cooldown_sec,
                )
                return RiskDecision(
                    allowed=False,
                    reason=f"cooldown_active_until={self._cooldown_until.isoformat()}",
                )

            self._evict_old_trades(now)
            if self._max_trades > 0 and len(self._trade_timestamps) >= self._max_trades:
                self._log_risk_event(
                    "risk_reject",
                    reason="max_trades_window_exceeded",
                    symbol=symbol,
                    market=market.value,
                    qty=qty,
                    current_exposure=len(self._trade_timestamps),
                    limit=self._max_trades,
                )
                return RiskDecision(
                    allowed=False,
                    reason=f"max_trades_window_exceeded market={market.value} "
                    f"count={len(self._trade_timestamps)} window_sec={self._window_sec}",
                )

            current_position = abs(self._order_manager.get_position(symbol))
            open_orders = self._order_manager.get_open_orders()
            open_qty_same_symbol = sum(abs(o.remaining_qty) for o in open_orders if o.symbol.upper() == symbol)
            projected_exposure = current_position + open_qty_same_symbol + qty
            if self._max_exposure > 0 and projected_exposure > self._max_exposure:
                self._log_risk_event(
                    "exposure_limit_hit",
                    reason="max_exposure_exceeded",
                    symbol=symbol,
                    market=market.value,
                    qty=qty,
                    current_exposure=projected_exposure,
                    limit=self._max_exposure,
                )
                return RiskDecision(
                    allowed=False,
                    reason=(
                        f"max_exposure_exceeded symbol={symbol} "
                        f"projected={projected_exposure:.4f} limit={self._max_exposure:.4f}"
                    ),
                )

            return RiskDecision(allowed=True)

    def on_order_accepted(self) -> None:
        with self._lock:
            now = datetime.now(timezone.utc)
            self._trade_timestamps.append(now)
            self._evict_old_trades(now)

    def on_trade_result(self, net_pnl: float) -> None:
        with self._lock:
            if net_pnl < 0:
                self._consecutive_losses += 1
                if self._cooldown_after_losses > 0 and self._consecutive_losses >= self._cooldown_after_losses:
                    self._cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=self._cooldown_sec)
                    self._log_risk_event(
                        "kill_switch_triggered",
                        reason="cooldown_after_consecutive_losses",
                        current_exposure=self._consecutive_losses,
                        limit=self._cooldown_after_losses,
                    )
            elif net_pnl > 0:
                self._consecutive_losses = 0

    def _log_risk_event(self, event: str, **fields: object) -> None:
        if self._logger is None:
            return
        self._logger.warning(
            event,
            extra={
                "component": "RiskManager",
                "event": event,
                "correlation_id": "",
                **fields,
            },
        )

    def _evict_old_trades(self, now: datetime) -> None:
        if self._window_sec <= 0:
            self._trade_timestamps.clear()
            return
        while self._trade_timestamps:
            delta = (now - self._trade_timestamps[0]).total_seconds()
            if delta <= self._window_sec:
                break
            self._trade_timestamps.popleft()
