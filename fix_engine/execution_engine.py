from __future__ import annotations

import logging
from typing import Callable

import quickfix as fix
import quickfix44 as fix44

from order_manager import ManagedOrder, OrderManager


class ExecutionEngine:
    """
    Sends orders/cancels via TRADE session and updates OrderManager lifecycle.
    """

    def __init__(
        self,
        order_manager: OrderManager,
        get_trade_session_id: Callable[[], fix.SessionID],
        logger: logging.Logger,
    ) -> None:
        self.order_manager = order_manager
        self.get_trade_session_id = get_trade_session_id
        self.logger = logger

    def send_order(
        self,
        symbol: str,
        side: str | int,
        qty: float,
        price: float | None = None,
    ) -> str:
        order = self.order_manager.create_order(symbol, side, qty, price)
        message = self._build_new_order(order)
        self._send(message)

        old, new = self.order_manager.set_status(order.cl_ord_id, "PENDING_NEW")
        self.logger.info("[ORDER] %s %s qty=%s price=%s", order.side, order.symbol, order.qty, order.price)
        self.logger.info("[STATE] order_id=%s %s -> %s", order.cl_ord_id, old, new)
        return order.cl_ord_id

    def cancel_order(self, cl_ord_id: str) -> str:
        order = self.order_manager.get_order(cl_ord_id)
        if order is None:
            raise ValueError(f"Order not found: {cl_ord_id}")

        cancel_id = self.order_manager.next_cl_ord_id()
        message = fix44.OrderCancelRequest(
            fix.OrigClOrdID(order.cl_ord_id),
            fix.ClOrdID(cancel_id),
            fix.Symbol(order.symbol),
            fix.Side(order.side),
            fix.TransactTime(),
        )
        message.setField(fix.OrderQty(order.qty))
        self._send(message)

        self.logger.info("[CANCEL] orig=%s cancel_id=%s symbol=%s", order.cl_ord_id, cancel_id, order.symbol)
        return cancel_id

    def _build_new_order(self, order: ManagedOrder) -> fix44.NewOrderSingle:
        ord_type = fix.OrdType_LIMIT if order.price is not None else fix.OrdType_MARKET
        message = fix44.NewOrderSingle(
            fix.ClOrdID(order.cl_ord_id),
            fix.HandlInst("1"),
            fix.Symbol(order.symbol),
            fix.Side(order.side),
            fix.TransactTime(),
            fix.OrdType(ord_type),
        )
        message.setField(fix.OrderQty(order.qty))
        if order.price is not None:
            message.setField(fix.Price(order.price))
        message.setField(fix.TimeInForce(fix.TimeInForce_DAY))
        return message

    def _send(self, message: fix.Message) -> None:
        session_id = self.get_trade_session_id()
        sent = fix.Session.sendToTarget(message, session_id)
        if not sent:
            raise RuntimeError("QuickFIX sendToTarget returned False.")
