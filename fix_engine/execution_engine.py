from __future__ import annotations

import logging
import time
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
        trading_session_id: str = "TQBR",
        trading_session_sub_id: str | None = None,
        trading_account: str | None = None,
    ) -> None:
        self.order_manager = order_manager
        self.get_trade_session_id = get_trade_session_id
        self.logger = logger
        self.trading_session_id = trading_session_id
        self.trading_session_sub_id = trading_session_sub_id
        self.trading_account = trading_account

    def send_order(
        self,
        symbol: str,
        side: str | int,
        qty: float,
        account: str | None = None,
        price: float | None = None,
    ) -> str:
        start_ns = time.perf_counter_ns()
        try:
            session_id = self.get_trade_session_id()
        except Exception as exc:
            self.logger.error(
                "[ORDER][ERROR] trade_session_not_ready symbol=%s side=%s qty=%s err=%s",
                symbol,
                side,
                qty,
                exc,
            )
            raise RuntimeError("TRADE session is not logged on.") from exc

        resolved_account = (account or self.trading_account or "").strip()
        if not resolved_account:
            self.logger.error(
                "[ORDER][ERROR] account_missing symbol=%s side=%s qty=%s",
                symbol,
                side,
                qty,
            )
            raise ValueError("Trading account is required (FIX Tag 1 / Account).")
        order = self.order_manager.create_order(symbol, side, qty, resolved_account, price)
        self.logger.info(
            "order_created",
            extra={
                "component": "ExecutionEngine",
                "event": "order_created",
                "correlation_id": order.cl_ord_id,
                "order_id": order.cl_ord_id,
                "symbol": order.symbol,
                "side": order.side,
                "price": order.price,
                "quantity": order.qty,
                "filled_quantity": order.filled_qty,
                "remaining_quantity": order.remaining_qty,
                "account": order.account,
            },
        )
        message = self._build_new_order(order)
        self.order_manager.remember_outbound_message(order.cl_ord_id, message.toString())
        self._send(message, session_id=session_id)

        old, new = self.order_manager.set_status(order.cl_ord_id, "PENDING_NEW")
        self.logger.info(
            "order_sent",
            extra={
                "component": "ExecutionEngine",
                "event": "order_sent",
                "correlation_id": order.cl_ord_id,
                "order_id": order.cl_ord_id,
                "symbol": order.symbol,
                "side": order.side,
                "price": order.price,
                "quantity": order.qty,
                "filled_quantity": order.filled_qty,
                "remaining_quantity": order.remaining_qty,
                "account": order.account,
                "status_old": old,
                "status_new": new,
                "processing_time_ms": round((time.perf_counter_ns() - start_ns) / 1_000_000.0, 3),
            },
        )
        return order.cl_ord_id

    def cancel_order(self, cl_ord_id: str) -> str:
        order = self.order_manager.get_order(cl_ord_id)
        if order is None:
            raise ValueError(f"Order not found: {cl_ord_id}")

        cancel_id = self.order_manager.next_cl_ord_id()
        message = fix44.OrderCancelRequest()
        message.setField(fix.OrigClOrdID(order.cl_ord_id))
        message.setField(fix.ClOrdID(cancel_id))
        message.setField(fix.Symbol(order.symbol))
        message.setField(fix.Side(order.side))
        message.setField(fix.TransactTime())
        message.setField(fix.OrderQty(order.qty))
        self._send(message)

        self.logger.info(
            "order_canceled",
            extra={
                "component": "ExecutionEngine",
                "event": "order_canceled",
                "correlation_id": order.cl_ord_id,
                "order_id": order.cl_ord_id,
                "cancel_id": cancel_id,
                "symbol": order.symbol,
                "side": order.side,
                "price": order.price,
                "quantity": order.qty,
                "filled_quantity": order.filled_qty,
                "remaining_quantity": order.remaining_qty,
                "account": order.account,
                "reason": "cancel_request",
            },
        )
        return cancel_id

    def _build_new_order(self, order: ManagedOrder) -> fix44.NewOrderSingle:
        ord_type = fix.OrdType_LIMIT if order.price is not None else fix.OrdType_MARKET
        message = fix44.NewOrderSingle()
        message.setField(fix.ClOrdID(order.cl_ord_id))
        message.setField(fix.HandlInst("1"))
        message.setField(fix.Symbol(order.symbol))
        message.setField(fix.Side(order.side))
        message.setField(fix.TransactTime())
        message.setField(fix.OrdType(ord_type))
        message.setField(fix.OrderQty(order.qty))
        if order.price is not None:
            message.setField(fix.Price(order.price))
        message.setField(fix.TimeInForce(fix.TimeInForce_DAY))
        account = (order.account or self.trading_account or "").strip()
        if not account:
            raise ValueError("Trading account is required (FIX Tag 1 / Account).")
        message.setField(fix.Account(account))

        # MOEX UAT expects TradingSessions group with exactly one element.
        ts_group = fix.Group(386, 336)
        ts_group.setField(fix.TradingSessionID(self.trading_session_id))
        if self.trading_session_sub_id:
            ts_group.setField(fix.TradingSessionSubID(self.trading_session_sub_id))
        message.addGroup(ts_group)
        return message

    def _send(self, message: fix.Message, session_id: fix.SessionID | None = None) -> None:
        if session_id is None:
            session_id = self.get_trade_session_id()
        sent = fix.Session.sendToTarget(message, session_id)
        if not sent:
            raise RuntimeError("QuickFIX sendToTarget returned False.")
