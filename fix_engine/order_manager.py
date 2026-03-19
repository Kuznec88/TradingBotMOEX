from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Dict, List

import quickfix as fix


@dataclass
class ManagedOrder:
    cl_ord_id: str
    symbol: str
    side: str
    qty: float
    price: float | None
    status: str
    filled_qty: float
    remaining_qty: float
    created_at: datetime


class OrderManager:
    """Thread-safe in-memory order lifecycle tracker."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._counter = 0
        self._orders: Dict[str, ManagedOrder] = {}
        self._positions: Dict[str, float] = {}
        self._outbound_messages: Dict[str, str] = {}

    def next_cl_ord_id(self) -> str:
        with self._lock:
            self._counter += 1
            # MOEX UAT rejects oversized ClOrdID; keep it short and deterministic.
            # Format length: 1 + 12 + 4 = 17 chars (e.g., M2603172033480001).
            ts = datetime.now(timezone.utc).strftime("%y%m%d%H%M%S")
            return f"M{ts}{self._counter % 10000:04d}"

    def create_order(
        self,
        symbol: str,
        side: str | int,
        qty: float,
        price: float | None = None,
    ) -> ManagedOrder:
        with self._lock:
            cl_ord_id = self.next_cl_ord_id()
            while cl_ord_id in self._orders:
                cl_ord_id = self.next_cl_ord_id()

            side_value = self._normalize_side(side)
            order = ManagedOrder(
                cl_ord_id=cl_ord_id,
                symbol=symbol,
                side=side_value,
                qty=float(qty),
                price=float(price) if price is not None else None,
                status="NEW",
                filled_qty=0.0,
                remaining_qty=float(qty),
                created_at=datetime.now(timezone.utc),
            )
            self._orders[cl_ord_id] = order
            return order

    def on_execution_report(self, message: fix.Message) -> dict[str, str]:
        with self._lock:
            cl_ord_id = self._safe_get(message, 11)
            if not cl_ord_id:
                return {"error": "ExecutionReport missing ClOrdID"}

            order = self._orders.get(cl_ord_id)
            if order is None:
                # Drop Copy may send order updates created outside this process.
                side = self._normalize_side(self._safe_get(message, 54) or "1")
                qty = self._to_float(self._safe_get(message, 38), 0.0)
                order = ManagedOrder(
                    cl_ord_id=cl_ord_id,
                    symbol=self._safe_get(message, 55),
                    side=side,
                    qty=qty,
                    price=self._to_float_or_none(self._safe_get(message, 44)),
                    status="NEW",
                    filled_qty=0.0,
                    remaining_qty=qty,
                    created_at=datetime.now(timezone.utc),
                )
                self._orders[cl_ord_id] = order

            old_status = order.status
            cum_qty = self._to_float(self._safe_get(message, 14), order.filled_qty)
            leaves_qty = self._to_float(self._safe_get(message, 151), max(order.qty - cum_qty, 0.0))
            last_qty = self._to_float(self._safe_get(message, 32), 0.0)
            exec_type = self._safe_get(message, 150)
            ord_status = self._safe_get(message, 39)

            order.filled_qty = cum_qty
            order.remaining_qty = leaves_qty
            order.status = self._map_status(exec_type=exec_type, ord_status=ord_status, order=order)

            if last_qty > 0:
                sign = 1.0 if order.side == "1" else -1.0
                self._positions[order.symbol] = self._positions.get(order.symbol, 0.0) + sign * last_qty

            return {
                "cl_ord_id": order.cl_ord_id,
                "symbol": order.symbol,
                "side": order.side,
                "account": self._safe_get(message, 1),
                "exec_type": exec_type,
                "ord_status": ord_status,
                "status_old": old_status,
                "status_new": order.status,
                "filled_qty": f"{order.filled_qty:.4f}",
                "remaining_qty": f"{order.remaining_qty:.4f}",
                "last_qty": f"{last_qty:.4f}",
                "last_px": self._safe_get(message, 31),
                "avg_px": self._safe_get(message, 6),
                "ord_rej_reason": self._safe_get(message, 103),
                "text": self._safe_get(message, 58),
                "exec_id": self._safe_get(message, 17),
                "raw_inbound_fix": message.toString(),
            }

    def remember_outbound_message(self, cl_ord_id: str, raw_fix: str) -> None:
        with self._lock:
            self._outbound_messages[cl_ord_id] = raw_fix

    def get_outbound_message(self, cl_ord_id: str) -> str:
        with self._lock:
            return self._outbound_messages.get(cl_ord_id, "")

    def set_status(self, cl_ord_id: str, new_status: str) -> tuple[str, str]:
        with self._lock:
            order = self._orders[cl_ord_id]
            old = order.status
            order.status = new_status
            return old, new_status

    def get_open_orders(self) -> List[ManagedOrder]:
        with self._lock:
            return [
                order
                for order in self._orders.values()
                if order.status not in {"FILLED", "CANCELED", "REJECTED"}
            ]

    def get_position(self, symbol: str) -> float:
        with self._lock:
            return self._positions.get(symbol, 0.0)

    def get_order(self, cl_ord_id: str) -> ManagedOrder | None:
        with self._lock:
            return self._orders.get(cl_ord_id)

    @staticmethod
    def _normalize_side(side: str | int) -> str:
        if isinstance(side, int):
            if side == 1:
                return "1"
            if side == 2:
                return "2"
            raise ValueError("Side int must be 1 (Buy) or 2 (Sell).")

        side_upper = str(side).strip().upper()
        if side_upper in {"1", "BUY", "B"}:
            return "1"
        if side_upper in {"2", "SELL", "S"}:
            return "2"
        raise ValueError("Side must be Buy/1 or Sell/2.")

    @staticmethod
    def _safe_get(message: fix.Message, tag: int) -> str:
        return message.getField(tag) if message.isSetField(tag) else ""

    @staticmethod
    def _to_float(value: str, default: float) -> float:
        if not value:
            return default
        try:
            return float(value)
        except ValueError:
            return default

    @staticmethod
    def _to_float_or_none(value: str) -> float | None:
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    @staticmethod
    def _map_status(exec_type: str, ord_status: str, order: ManagedOrder) -> str:
        if exec_type == "8" or ord_status == "8":
            return "REJECTED"
        if exec_type == "4" or ord_status == "4":
            return "CANCELED"
        if exec_type == "2" or ord_status == "2":
            return "FILLED"
        if exec_type == "1" or ord_status == "1":
            return "PARTIALLY_FILLED"
        if exec_type in {"0", "A"} or ord_status in {"0", "A"}:
            return "PENDING_NEW"
        if order.filled_qty >= order.qty and order.qty > 0:
            return "FILLED"
        if order.filled_qty > 0:
            return "PARTIALLY_FILLED"
        return order.status
