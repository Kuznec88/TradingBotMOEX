from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Callable
import re

import quickfix as fix

from execution_engine import ExecutionEngine
from market_data.models import MarketData
from order_manager import OrderManager
from order_models import MarketType, OrderRequest
from position_manager import PositionManager
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
        position_manager: PositionManager | None = None,
        simulation_slippage_bps: float = 2.0,
        simulation_slippage_max_bps: float = 25.0,
        simulation_volatility_slippage_multiplier: float = 1.5,
        simulation_latency_network_ms: int = 20,
        simulation_latency_exchange_ms: int = 30,
        simulation_latency_jitter_ms: int = 40,
        simulation_fill_participation: float = 0.25,
        simulation_touch_fill_probability: float = 0.15,
        simulation_passive_fill_probability_scale: float = 0.5,
        simulation_adverse_selection_bias: float = 0.35,
        simulation_rng: random.Random | None = None,
        account_by_market: dict[MarketType, str] | None = None,
    ) -> None:
        self.equities_engine = equities_engine
        self.forts_engine = forts_engine
        self.order_manager = order_manager
        self.logger = logger
        self.simulation_mode = simulation_mode
        self.get_latest_market_data = get_latest_market_data
        self.on_execution_report = on_execution_report
        self.risk_manager = risk_manager
        self.position_manager = position_manager
        self.simulation_slippage_bps = max(0.0, float(simulation_slippage_bps))
        self.simulation_slippage_max_bps = max(self.simulation_slippage_bps, float(simulation_slippage_max_bps))
        self.simulation_volatility_slippage_multiplier = max(0.0, float(simulation_volatility_slippage_multiplier))
        self.simulation_latency_network_ms = simulation_latency_network_ms
        self.simulation_latency_exchange_ms = simulation_latency_exchange_ms
        self.simulation_latency_jitter_ms = max(0, int(simulation_latency_jitter_ms))
        self.simulation_fill_participation = simulation_fill_participation
        self.simulation_touch_fill_probability = max(0.0, min(1.0, float(simulation_touch_fill_probability)))
        self.simulation_passive_fill_probability_scale = max(
            0.0, min(1.0, float(simulation_passive_fill_probability_scale))
        )
        self.simulation_adverse_selection_bias = max(0.0, min(1.0, float(simulation_adverse_selection_bias)))
        self._simulation_rng = simulation_rng or random.Random()
        self.account_by_market = account_by_market or {}
        self._pending_orders: dict[str, _PendingSimOrder] = {}
        self._last_mid_by_symbol: dict[str, float] = {}
        self._lock = RLock()
        self._trading_enabled = True

    def send_order(self, request: OrderRequest) -> str:
        if not self._trading_enabled:
            raise RuntimeError("TradingHalted: execution gateway is disabled by failure handling")
        account = self._resolve_account(request)
        if not account:
            self.logger.error(
                "[ORDER][REJECT] account_missing market=%s symbol=%s side=%s qty=%s",
                request.market.value,
                request.symbol,
                request.side,
                request.qty,
            )
            raise RuntimeError(f"AccountMissing: market={request.market.value}")
        if (not self.simulation_mode) and (not self._is_valid_account_format(account)):
            self.logger.error(
                "[ORDER][REJECT] account_invalid_format market=%s symbol=%s account=%s",
                request.market.value,
                request.symbol,
                account,
            )
            raise RuntimeError(f"AccountInvalidFormat: market={request.market.value} account={account}")
        qty = request.qty * max(request.lot_size, 1)
        if self.position_manager is not None:
            inventory_decision = self.position_manager.pre_check_order(
                symbol=request.symbol,
                side=request.side,
                qty=qty,
            )
            if not inventory_decision.allowed:
                self.logger.warning(
                    "[INVENTORY][REJECT] market=%s symbol=%s side=%s qty=%s reason=%s",
                    request.market.value,
                    request.symbol,
                    request.side,
                    qty,
                    inventory_decision.reason,
                )
                raise RuntimeError(f"InventoryRejected: {inventory_decision.reason}")
        if self.risk_manager is not None and not request.bypass_risk:
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
            cl_ord_id = self._simulate_send_order(request, qty, account)
            if self.risk_manager is not None and not request.bypass_risk:
                self.risk_manager.on_order_accepted()
            return cl_ord_id

        if request.market == MarketType.FORTS:
            cl_ord_id = self.forts_engine.send_order(
                request.symbol,
                request.side,
                qty,
                account=account,
                price=request.price,
            )
        else:
            cl_ord_id = self.equities_engine.send_order(
                request.symbol,
                request.side,
                qty,
                account=account,
                price=request.price,
            )
        if self.risk_manager is not None and not request.bypass_risk:
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

    def simulation_config_snapshot(self) -> dict[str, float | int]:
        return {
            "simulation_slippage_bps": float(self.simulation_slippage_bps),
            "simulation_slippage_max_bps": float(self.simulation_slippage_max_bps),
            "simulation_volatility_slippage_multiplier": float(self.simulation_volatility_slippage_multiplier),
            "simulation_latency_network_ms": int(self.simulation_latency_network_ms),
            "simulation_latency_exchange_ms": int(self.simulation_latency_exchange_ms),
            "simulation_latency_jitter_ms": int(self.simulation_latency_jitter_ms),
            "simulation_fill_participation": float(self.simulation_fill_participation),
            "simulation_touch_fill_probability": float(self.simulation_touch_fill_probability),
            "simulation_passive_fill_probability_scale": float(self.simulation_passive_fill_probability_scale),
            "simulation_adverse_selection_bias": float(self.simulation_adverse_selection_bias),
        }

    def apply_simulation_config(
        self,
        *,
        simulation_slippage_bps: float,
        simulation_slippage_max_bps: float,
        simulation_volatility_slippage_multiplier: float,
        simulation_latency_network_ms: int,
        simulation_latency_exchange_ms: int,
        simulation_latency_jitter_ms: int,
        simulation_fill_participation: float,
        simulation_touch_fill_probability: float,
        simulation_passive_fill_probability_scale: float,
        simulation_adverse_selection_bias: float,
        profile_name: str = "",
        source: str = "runtime",
    ) -> None:
        with self._lock:
            self.simulation_slippage_bps = max(0.0, float(simulation_slippage_bps))
            self.simulation_slippage_max_bps = max(self.simulation_slippage_bps, float(simulation_slippage_max_bps))
            self.simulation_volatility_slippage_multiplier = max(0.0, float(simulation_volatility_slippage_multiplier))
            self.simulation_latency_network_ms = max(0, int(simulation_latency_network_ms))
            self.simulation_latency_exchange_ms = max(0, int(simulation_latency_exchange_ms))
            self.simulation_latency_jitter_ms = max(0, int(simulation_latency_jitter_ms))
            self.simulation_fill_participation = max(0.01, float(simulation_fill_participation))
            self.simulation_touch_fill_probability = max(0.0, min(1.0, float(simulation_touch_fill_probability)))
            self.simulation_passive_fill_probability_scale = max(
                0.0, min(1.0, float(simulation_passive_fill_probability_scale))
            )
            self.simulation_adverse_selection_bias = max(0.0, min(1.0, float(simulation_adverse_selection_bias)))

        cfg = self.simulation_config_snapshot()
        self.logger.info(
            "[SIM][PROFILE] applied profile=%s source=%s slippage_bps=%s slippage_max_bps=%s vol_slippage_mult=%s "
            "latency_network_ms=%s latency_exchange_ms=%s latency_jitter_ms=%s fill_participation=%s "
            "touch_fill_probability=%s passive_fill_scale=%s adverse_selection_bias=%s",
            profile_name or "CUSTOM",
            source,
            cfg["simulation_slippage_bps"],
            cfg["simulation_slippage_max_bps"],
            cfg["simulation_volatility_slippage_multiplier"],
            cfg["simulation_latency_network_ms"],
            cfg["simulation_latency_exchange_ms"],
            cfg["simulation_latency_jitter_ms"],
            cfg["simulation_fill_participation"],
            cfg["simulation_touch_fill_probability"],
            cfg["simulation_passive_fill_probability_scale"],
            cfg["simulation_adverse_selection_bias"],
        )

    def set_trading_enabled(self, enabled: bool, reason: str = "") -> None:
        self._trading_enabled = bool(enabled)
        self.logger.warning(
            "[TRADING] enabled=%s reason=%s",
            self._trading_enabled,
            reason,
        )

    def _simulate_send_order(self, request: OrderRequest, qty: float, account: str) -> str:
        order = self.order_manager.create_order(
            symbol=request.symbol,
            side=request.side,
            qty=qty,
            account=account,
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

        accept_latency_ms = self._sleep_with_latency()
        market_data = self.get_latest_market_data(request.symbol) if self.get_latest_market_data else None
        if market_data is not None:
            self._simulate_match_pending_orders(market_data)

        self.logger.info(
            "simulation_order_accepted",
            extra={
                "component": "ExecutionGateway",
                "event": "simulation_order_accepted",
                "correlation_id": order.cl_ord_id,
                "market": request.market.value,
                "order_id": order.cl_ord_id,
                "symbol": pending.symbol,
                "side": pending.side,
                "quantity": pending.qty,
                "price": pending.limit_price,
                "account": order.account,
                "simulated_latency_ms": accept_latency_ms,
            },
        )
        self.logger.info("[STATE] order_id=%s %s -> %s", order.cl_ord_id, old, new)
        return order.cl_ord_id

    def _simulate_match_pending_orders(self, data: MarketData) -> None:
        symbol = data.symbol.upper()
        with self._lock:
            pending_list = [p for p in self._pending_orders.values() if p.symbol == symbol and p.remaining > 0]
        if not pending_list:
            self._last_mid_by_symbol[symbol] = float(data.mid_price)
            return

        abs_return, move_dir = self._compute_short_term_move(symbol, data)
        available_qty = max(1.0, float(data.volume) * max(0.01, self.simulation_fill_participation))
        for pending in pending_list:
            if not self._is_crossed(pending, data, abs_return=abs_return, move_dir=move_dir):
                continue

            fill_qty = min(pending.remaining, available_qty)
            fill_px, slip_bps = self._fill_price(pending, data, abs_return=abs_return, move_dir=move_dir)
            cum_qty = pending.qty - pending.remaining + fill_qty
            leaves_qty = max(0.0, pending.qty - cum_qty)
            is_full = leaves_qty <= 1e-9
            ord_status = "2" if is_full else "1"
            exec_type = "2" if is_full else "1"

            fill_latency_ms = self._sleep_with_latency()
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
                text=f"Simulated fill latency_ms={fill_latency_ms} slippage_bps={slip_bps:.4f}",
            )

            with self._lock:
                if pending.cl_ord_id in self._pending_orders:
                    self._pending_orders[pending.cl_ord_id].remaining = leaves_qty
                    if is_full:
                        self._pending_orders.pop(pending.cl_ord_id, None)
        self._last_mid_by_symbol[symbol] = float(data.mid_price)

    def _is_crossed(self, pending: _PendingSimOrder, data: MarketData, *, abs_return: float, move_dir: int) -> bool:
        if pending.limit_price is None:
            return True
        if pending.side == "1":
            if pending.limit_price >= data.ask:
                return True
            # At-touch passive buy: probabilistic queue fill.
            if abs(pending.limit_price - data.bid) <= 1e-9:
                probability = self._passive_fill_probability(
                    side=pending.side,
                    abs_return=abs_return,
                    move_dir=move_dir,
                )
                return self._simulation_rng.random() < probability
            return False
        if pending.limit_price <= data.bid:
            return True
        # At-touch passive sell: probabilistic queue fill.
        if abs(pending.limit_price - data.ask) <= 1e-9:
            probability = self._passive_fill_probability(
                side=pending.side,
                abs_return=abs_return,
                move_dir=move_dir,
            )
            return self._simulation_rng.random() < probability
        return False

    def _fill_price(
        self, pending: _PendingSimOrder, data: MarketData, *, abs_return: float, move_dir: int
    ) -> tuple[float, float]:
        slip_bps = self._dynamic_slippage_bps(side=pending.side, abs_return=abs_return, move_dir=move_dir)
        slip = slip_bps / 10000.0
        if pending.side == "1":
            base = data.ask
            px = base * (1.0 + slip)
            if pending.limit_price is not None:
                px = min(px, pending.limit_price)
            return px, slip_bps
        base = data.bid
        px = base * (1.0 - slip)
        if pending.limit_price is not None:
            px = max(px, pending.limit_price)
        return px, slip_bps

    def _sleep_with_latency(self) -> int:
        total_ms = max(0, self.simulation_latency_network_ms) + max(0, self.simulation_latency_exchange_ms)
        jitter = self._simulation_rng.randint(0, self.simulation_latency_jitter_ms)
        elapsed = total_ms + jitter
        time.sleep(elapsed / 1000.0)
        return elapsed

    def _compute_short_term_move(self, symbol: str, data: MarketData) -> tuple[float, int]:
        current_mid = float(data.mid_price)
        prev_mid = self._last_mid_by_symbol.get(symbol)
        if prev_mid is None or prev_mid <= 0:
            return 0.0, 0
        delta = current_mid - prev_mid
        abs_return = abs(delta) / prev_mid
        move_dir = 1 if delta > 0 else (-1 if delta < 0 else 0)
        return abs_return, move_dir

    def _passive_fill_probability(self, *, side: str, abs_return: float, move_dir: int) -> float:
        base = self.simulation_touch_fill_probability * self.simulation_passive_fill_probability_scale
        vol_component = min(1.0, abs_return / 0.002)
        adverse = (side == "1" and move_dir < 0) or (side == "2" and move_dir > 0)
        if adverse:
            probability = base * (1.0 + self.simulation_adverse_selection_bias * (1.0 + vol_component))
        else:
            probability = base * (1.0 - 0.5 * self.simulation_adverse_selection_bias) * (1.0 - 0.5 * vol_component)
        return max(0.0, min(1.0, probability))

    def _dynamic_slippage_bps(self, *, side: str, abs_return: float, move_dir: int) -> float:
        vol_boost = 1.0 + self.simulation_volatility_slippage_multiplier * min(3.0, abs_return * 1000.0)
        adverse = (side == "1" and move_dir < 0) or (side == "2" and move_dir > 0)
        adverse_boost = 1.0 + self.simulation_adverse_selection_bias if adverse else 1.0
        slip_bps = self.simulation_slippage_bps * vol_boost * adverse_boost
        return min(self.simulation_slippage_max_bps, max(self.simulation_slippage_bps, slip_bps))

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

    def _resolve_account(self, request: OrderRequest) -> str:
        explicit = str(request.account).strip()
        if explicit:
            return explicit
        by_market = str(self.account_by_market.get(request.market, "")).strip()
        if by_market:
            return by_market
        if self.simulation_mode:
            return f"SIM-{request.market.value}-ACCOUNT"
        return ""

    @staticmethod
    def _is_valid_account_format(account: str) -> bool:
        value = str(account).strip()
        if not value:
            return False
        # Conservative FIX-safe account format for real mode.
        return re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9+._-]{1,31}", value) is not None
