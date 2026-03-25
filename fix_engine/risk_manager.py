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
        max_abs_position_per_symbol: float = 0.0,
        max_daily_loss_abs: float = 0.0,
        kill_switch_drawdown_abs: float = 0.0,
        logger: logging.Logger | None = None,
    ) -> None:
        self._order_manager = order_manager
        self._max_exposure = float(max_exposure_per_instrument)
        self._max_trades = int(max_trades_in_window)
        self._window_sec = int(trades_window_seconds)
        self._cooldown_after_losses = int(cooldown_after_consecutive_losses)
        self._cooldown_sec = int(cooldown_seconds)
        self._max_abs_position = float(max_abs_position_per_symbol)
        self._max_daily_loss_abs = float(max_daily_loss_abs)
        self._kill_switch_drawdown_abs = float(kill_switch_drawdown_abs)
        self._logger = logger

        self._lock = RLock()
        self._trade_timestamps: deque[datetime] = deque()
        self._consecutive_losses = 0
        self._cooldown_until: datetime | None = None

        # Daily PnL / drawdown guard (based on realized net PnL reported via on_trade_result()).
        self._day_key: str | None = None
        self._day_realized: float = 0.0
        self._equity_peak: float = 0.0
        self._trading_halted: bool = False
        self._halt_reason: str = ""

    def pre_check_order(self, *, symbol: str, qty: float, market: MarketType, side: str | None = None) -> RiskDecision:
        now = datetime.now(timezone.utc)
        symbol = symbol.upper()
        qty = abs(float(qty))

        with self._lock:
            self._roll_day(now)
            if self._trading_halted:
                self._log_risk_event(
                    "risk_reject",
                    reason="trading_halted",
                    symbol=symbol,
                    market=market.value,
                    qty=qty,
                    halt_reason=self._halt_reason,
                )
                return RiskDecision(allowed=False, reason=f"trading_halted: {self._halt_reason}")

            if self._max_daily_loss_abs > 0.0 and self._day_realized <= -abs(self._max_daily_loss_abs):
                self._trading_halted = True
                self._halt_reason = "max_daily_loss_abs"
                self._log_risk_event(
                    "kill_switch_triggered",
                    reason=self._halt_reason,
                    day_realized=self._day_realized,
                    limit=self._max_daily_loss_abs,
                )
                return RiskDecision(allowed=False, reason=f"max_daily_loss_abs reached: day_realized={self._day_realized:.4f}")

            if self._kill_switch_drawdown_abs > 0.0:
                dd = max(0.0, self._equity_peak - self._day_realized)
                if dd >= abs(self._kill_switch_drawdown_abs):
                    self._trading_halted = True
                    self._halt_reason = "kill_switch_drawdown_abs"
                    self._log_risk_event(
                        "kill_switch_triggered",
                        reason=self._halt_reason,
                        day_realized=self._day_realized,
                        equity_peak=self._equity_peak,
                        drawdown=dd,
                        limit=self._kill_switch_drawdown_abs,
                    )
                    return RiskDecision(allowed=False, reason=f"kill_switch_drawdown_abs hit: dd={dd:.4f}")

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

            current_position_signed = float(self._order_manager.get_position(symbol))
            current_position_abs = abs(current_position_signed)
            open_orders = self._order_manager.get_open_orders()
            open_qty_same_symbol = sum(abs(o.remaining_qty) for o in open_orders if o.symbol.upper() == symbol)
            projected_exposure = current_position_abs + open_qty_same_symbol + qty
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

            if self._max_abs_position > 0.0 and side:
                s = str(side).strip().upper()
                # FIX-style: 1=BUY, 2=SELL
                d = qty if s in {"1", "BUY"} else (-qty if s in {"2", "SELL"} else 0.0)
                projected_abs_pos = abs(current_position_signed + d)
                if projected_abs_pos > self._max_abs_position:
                    self._log_risk_event(
                        "risk_reject",
                        reason="max_abs_position_exceeded",
                        symbol=symbol,
                        market=market.value,
                        qty=qty,
                        side=s,
                        current_position=current_position_signed,
                        projected_abs_position=projected_abs_pos,
                        limit=self._max_abs_position,
                    )
                    return RiskDecision(
                        allowed=False,
                        reason=(
                            f"max_abs_position_exceeded symbol={symbol} "
                            f"projected_abs={projected_abs_pos:.4f} limit={self._max_abs_position:.4f}"
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
            now = datetime.now(timezone.utc)
            self._roll_day(now)
            pnl = float(net_pnl)
            self._day_realized += pnl
            if self._day_realized > self._equity_peak:
                self._equity_peak = self._day_realized

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

            if self._max_daily_loss_abs > 0.0 and self._day_realized <= -abs(self._max_daily_loss_abs):
                self._trading_halted = True
                self._halt_reason = "max_daily_loss_abs"
            if self._kill_switch_drawdown_abs > 0.0:
                dd = max(0.0, self._equity_peak - self._day_realized)
                if dd >= abs(self._kill_switch_drawdown_abs):
                    self._trading_halted = True
                    self._halt_reason = "kill_switch_drawdown_abs"

    def _roll_day(self, now: datetime) -> None:
        day_key = now.astimezone(timezone.utc).date().isoformat()
        if self._day_key != day_key:
            self._day_key = day_key
            self._day_realized = 0.0
            self._equity_peak = 0.0
            self._trading_halted = False
            self._halt_reason = ""

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
