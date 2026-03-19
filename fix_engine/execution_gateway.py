from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Callable

import quickfix as fix

from execution_engine import ExecutionEngine
from market_data.models import MarketData
from order_manager import OrderManager
from order_models import MarketType, OrderRequest
from risk_manager import RiskManager


@dataclass
class _PendingSimOrder:
    cl_ord_id: str
    symbol: str
    side: str
    qty: float
    remaining: float
    limit_price: float | None
    market: MarketType


class ExecutionGateway:
    """Routes orders by market to corresponding execution engine."""

    def __init__(
        self,
        equities_engine: ExecutionEngine,
        forts_engine: ExecutionEngine,
        order_manager: OrderManager,
        logger: logging.Logger,
        simulation_mode: bool = False,
        get_latest_market_data: Callable[[str], MarketData | None] | None = None,
        on_execution_report: Callable[[fix.Message, str], None] | None = None,
        risk_manager: RiskManager | None = None,
        simulation_slippage_bps: float = 2.0,
        simulation_latency_network_ms: int = 20,
        simulation_latency_exchange_ms: int = 30,
        simulation_fill_participation: float = 0.25,
        simulation_rng: random.Random | None = None,
    ) -> None:
        self.equities_engine = equities_engine
        self.forts_engine = forts_engine
        self.order_manager = order_manager
        self.logger = logger
        self.simulation_mode = simulation_mode
        self.get_latest_market_data = get_latest_market_data
        self.on_execution_report = on_execution_report
        self.risk_manager = risk_manager
        self.simulation_slippage_bps = simulation_slippage_bps
        self.simulation_latency_network_ms = simulation_latency_network_ms
        self.simulation_latency_exchange_ms = simulation_latency_exchange_ms
        self.simulation_fill_participation = simulation_fill_participation
        self._simulation_rng = simulation_rng or random.Random()
        self._pending_orders: dict[str, _PendingSimOrder] = {}
        self._lock = RLock()
        self._trading_enabled = True

    def send_order(self, request: OrderRequest) -> str:
        if not self._trading_enabled:
            raise RuntimeError("TradingHalted: execution gateway is disabled by failure handling")
        qty = request.qty * max(request.lot_size, 1)
        if self.risk_manager is not None:
            decision = self.risk_manager.pre_check_order(
                symbol=request.symbol,
                qty=qty,
                market=request.market,
            )
            if not decision.allowed:
                self.logger.warning(
                    "[RISK][REJECT] market=%s symbol=%s qty=%s reason=%s",
                    request.market.value,
                    request.symbol,
                    qty,
                    decision.reason,
                )
                raise RuntimeError(f"RiskRejected: {decision.reason}")

        if self.simulation_mode:
            cl_ord_id = self._simulate_send_order(request, qty)
            if self.risk_manager is not None:
                self.risk_manager.on_order_accepted()
            return cl_ord_id

        if request.market == MarketType.FORTS:
            cl_ord_id = self.forts_engine.send_order(request.symbol, request.side, qty, request.price)
        else:
            cl_ord_id = self.equities_engine.send_order(request.symbol, request.side, qty, request.price)
        if self.risk_manager is not None:
            self.risk_manager.on_order_accepted()
        return cl_ord_id

    def cancel_order(self, cl_ord_id: str, market: MarketType = MarketType.EQUITIES) -> str:
        if self.simulation_mode:
            with self._lock:
                pending = self._pending_orders.pop(cl_ord_id, None)
            if pending is not None:
                self._emit_execution_report(
                    cl_ord_id=cl_ord_id,
                    symbol=pending.symbol,
                    side=pending.side,
                    order_qty=pending.qty,
                    cum_qty=pending.qty - pending.remaining,
                    leaves_qty=pending.remaining,
                    ord_status="4",
                    exec_type="4",
                    last_qty=0.0,
                    last_px=0.0,
                    text="Canceled in simulation",
                )
            else:
                old, new = self.order_manager.set_status(cl_ord_id, "CANCELED")
                self.logger.info("[SIM][%s] cancel %s -> %s for %s", market.value, old, new, cl_ord_id)
            return f"SIM-CANCEL-{datetime.now(timezone.utc).strftime('%y%m%d%H%M%S')}"

        if market == MarketType.FORTS:
            return self.forts_engine.cancel_order(cl_ord_id)
        return self.equities_engine.cancel_order(cl_ord_id)

    def on_market_data(self, data: MarketData) -> None:
        if not self.simulation_mode:
            return
        self._simulate_match_pending_orders(data)

    def set_trading_enabled(self, enabled: bool, reason: str = "") -> None:
        self._trading_enabled = bool(enabled)
        self.logger.warning(
            "[TRADING] enabled=%s reason=%s",
            self._trading_enabled,
            reason,
        )

    def _simulate_send_order(self, request: OrderRequest, qty: float) -> str:
        order = self.order_manager.create_order(
            symbol=request.symbol,
            side=request.side,
            qty=qty,
            price=request.price,
        )
        old, new = self.order_manager.set_status(order.cl_ord_id, "SENT")
        side = self._normalize_side(request.side)
        pending = _PendingSimOrder(
            cl_ord_id=order.cl_ord_id,
            symbol=request.symbol.upper(),
            side=side,
            qty=qty,
            remaining=qty,
            limit_price=request.price,
            market=request.market,
        )
        with self._lock:
            self._pending_orders[order.cl_ord_id] = pending

        self._sleep_with_latency()
        market_data = self.get_latest_market_data(request.symbol) if self.get_latest_market_data else None
        if market_data is not None:
            self._simulate_match_pending_orders(market_data)

        self.logger.info(
            "[SIM][%s] order accepted cl_ord_id=%s symbol=%s side=%s qty=%s limit=%s",
            request.market.value,
            order.cl_ord_id,
            pending.symbol,
            pending.side,
            pending.qty,
            pending.limit_price,
        )
        self.logger.info("[STATE] order_id=%s %s -> %s", order.cl_ord_id, old, new)
        return order.cl_ord_id

    def _simulate_match_pending_orders(self, data: MarketData) -> None:
        symbol = data.symbol.upper()
        with self._lock:
            pending_list = [p for p in self._pending_orders.values() if p.symbol == symbol and p.remaining > 0]
        if not pending_list:
            return

        available_qty = max(1.0, float(data.volume) * max(0.01, self.simulation_fill_participation))
        for pending in pending_list:
            if not self._is_crossed(pending, data):
                continue

            fill_qty = min(pending.remaining, available_qty)
            fill_px = self._fill_price(pending, data)
            cum_qty = pending.qty - pending.remaining + fill_qty
            leaves_qty = max(0.0, pending.qty - cum_qty)
            is_full = leaves_qty <= 1e-9
            ord_status = "2" if is_full else "1"
            exec_type = "2" if is_full else "1"

            self._sleep_with_latency()
            self._emit_execution_report(
                cl_ord_id=pending.cl_ord_id,
                symbol=pending.symbol,
                side=pending.side,
                order_qty=pending.qty,
                cum_qty=cum_qty,
                leaves_qty=leaves_qty,
                ord_status=ord_status,
                exec_type=exec_type,
                last_qty=fill_qty,
                last_px=fill_px,
                text="Simulated fill",
            )

            with self._lock:
                if pending.cl_ord_id in self._pending_orders:
                    self._pending_orders[pending.cl_ord_id].remaining = leaves_qty
                    if is_full:
                        self._pending_orders.pop(pending.cl_ord_id, None)

    def _is_crossed(self, pending: _PendingSimOrder, data: MarketData) -> bool:
        if pending.limit_price is None:
            return True
        if pending.side == "1":
            return pending.limit_price >= data.ask
        return pending.limit_price <= data.bid

    def _fill_price(self, pending: _PendingSimOrder, data: MarketData) -> float:
        slip = self.simulation_slippage_bps / 10000.0
        if pending.side == "1":
            base = data.ask
            px = base * (1.0 + slip)
            if pending.limit_price is not None:
                px = min(px, pending.limit_price)
            return px
        base = data.bid
        px = base * (1.0 - slip)
        if pending.limit_price is not None:
            px = max(px, pending.limit_price)
        return px

    def _sleep_with_latency(self) -> None:
        total_ms = max(0, self.simulation_latency_network_ms) + max(0, self.simulation_latency_exchange_ms)
        jitter = self._simulation_rng.randint(0, 10)
        time.sleep((total_ms + jitter) / 1000.0)

    def _emit_execution_report(
        self,
        *,
        cl_ord_id: str,
        symbol: str,
        side: str,
        order_qty: float,
        cum_qty: float,
        leaves_qty: float,
        ord_status: str,
        exec_type: str,
        last_qty: float,
        last_px: float,
        text: str,
    ) -> None:
        if self.on_execution_report is None:
            old, new = self.order_manager.set_status(
                cl_ord_id,
                "FILLED" if ord_status == "2" else "PARTIALLY_FILLED",
            )
            self.logger.info("[SIM] status fallback %s -> %s for %s", old, new, cl_ord_id)
            return

        msg = fix.Message()
        msg.getHeader().setField(fix.MsgType(fix.MsgType_ExecutionReport))
        msg.setField(fix.ClOrdID(cl_ord_id))
        msg.setField(fix.ExecID(f"{cl_ord_id}|{exec_type}|SIM"))
        msg.setField(fix.OrderID(f"SIM-{cl_ord_id}"))
        msg.setField(fix.Symbol(symbol))
        msg.setField(fix.Side(side))
        msg.setField(fix.OrderQty(float(order_qty)))
        msg.setField(fix.CumQty(float(cum_qty)))
        msg.setField(fix.LeavesQty(float(leaves_qty)))
        msg.setField(fix.AvgPx(float(last_px) if cum_qty > 0 else 0.0))
        msg.setField(fix.StringField(39, ord_status))
        msg.setField(fix.StringField(150, exec_type))
        msg.setField(fix.LastQty(float(last_qty)))
        msg.setField(fix.LastPx(float(last_px)))
        msg.setField(fix.Text(text))
        self.on_execution_report(msg, "SIMULATION")

    @staticmethod
    def _normalize_side(side: str | int) -> str:
        if isinstance(side, int):
            return "1" if side == 1 else "2"
        s = str(side).strip().upper()
        if s in {"1", "BUY", "B"}:
            return "1"
        return "2"
