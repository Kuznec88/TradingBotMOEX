"""T-Invest gRPC: лимитные ордера и поток состояний заявок (LIVE / sandbox orders).

Используется при ExecutionMode=LIVE: ExecutionGateway маршрутизирует заявки сюда,
исполнения приходят из order_state_stream и преобразуются в SyntheticExecutionReport.
"""

from __future__ import annotations

import threading
import uuid
from decimal import Decimal
from typing import Callable

from fix_engine.fix_shim import SyntheticExecutionReport
from fix_engine.order_manager import OrderManager


class TinkoffBrokerController:
    """
    Общий контроллер: unary post/cancel под lock, отдельный поток order_state_stream.
    """

    def __init__(
        self,
        *,
        token: str,
        host: str,
        instrument_id: str,
        symbol: str,
        order_manager: OrderManager,
        logger: logging.Logger,
        on_execution_report: Callable[[object, str], None],
        shares_per_lot: int = 1,
        tick_size: float = 0.01,
        use_sandbox_orders: bool = False,
        account_ids_for_stream: list[str] | None = None,
    ) -> None:
        self._token = token.strip()
        self._host = host.strip()
        self._instrument_id = instrument_id.strip()
        self._symbol = symbol.upper().strip()
        self._order_manager = order_manager
        self._logger = logger
        self._on_execution_report = on_execution_report
        self._shares_per_lot = max(1, int(shares_per_lot))
        self._tick_size = float(tick_size) if float(tick_size) > 0 else 0.01
        self._use_sandbox_orders = bool(use_sandbox_orders)

        self._unary_lock = threading.Lock()
        self._cl_to_broker: dict[str, str] = {}
        self._broker_to_cl: dict[str, str] = {}
        self._req_to_cl: dict[str, str] = {}
        self._cl_to_account: dict[str, str] = {}
        self._last_lots_executed: dict[str, int] = {}

        accounts = [a.strip() for a in (account_ids_for_stream or []) if a.strip()]
        self._stream_accounts = accounts

        self._stop = threading.Event()
        self._stream_thread: threading.Thread | None = None

    def start_order_stream(self) -> None:
        if not self._stream_accounts:
            self._logger.warning("[TBANK][ORDERS] order_state_stream skipped (no account ids)")
            return
        if self._stream_thread is not None and self._stream_thread.is_alive():
            return

        def _run() -> None:
            try:
                from t_tech.invest import Client
                from t_tech.invest.schemas import OrderStateStreamRequest
            except Exception as exc:
                self._logger.error("[TBANK][ORDERS] import failed: %s", exc)
                return

            self._logger.info(
                "[TBANK][ORDERS] starting order_state_stream accounts=%s",
                self._stream_accounts,
            )
            try:
                with Client(self._token, target=self._host) as client:
                    req = OrderStateStreamRequest(
                        accounts=list(self._stream_accounts),
                        ping_delay_millis=5000,
                    )
                    for ev in client.orders_stream.order_state_stream(request=req):
                        if self._stop.is_set():
                            break
                        st = getattr(ev, "order_state", None)
                        if st is None:
                            continue
                        try:
                            self._handle_order_state(st)
                        except Exception as exc:
                            self._logger.warning("[TBANK][ORDERS] handle_order_state: %s", exc, exc_info=True)
            except Exception as exc:
                if not self._stop.is_set():
                    self._logger.error("[TBANK][ORDERS] stream ended with error: %s", exc, exc_info=True)

        self._stop.clear()
        self._stream_thread = threading.Thread(target=_run, name="tbank-order-state-stream", daemon=True)
        self._stream_thread.start()

    def stop_order_stream(self) -> None:
        self._stop.set()
        if self._stream_thread is not None:
            self._stream_thread.join(timeout=5.0)

    def send_limit_order(
        self,
        *,
        account_id: str,
        symbol: str,
        side: str,
        qty: float,
        price: float | None,
    ) -> str:
        from t_tech.invest import Client
        from t_tech.invest.schemas import OrderDirection, OrderType
        from t_tech.invest.utils import decimal_to_quotation

        if price is None:
            raise RuntimeError("TinkoffLiveEngine: limit price is required (strategy uses bid/ask limits).")

        order = self._order_manager.create_order(
            symbol=symbol.upper(),
            side=side,
            qty=float(qty),
            account=account_id,
            price=float(price),
        )
        cl_ord_id = order.cl_ord_id

        lots = self._qty_to_lots(qty)
        direction = OrderDirection.ORDER_DIRECTION_BUY if str(side).strip() in {"1", "BUY", "B"} else OrderDirection.ORDER_DIRECTION_SELL
        px = self._round_price_to_tick(Decimal(str(float(price))))
        quotation = decimal_to_quotation(px)
        # T-Invest requires order_id to be UUID (or empty). We keep our internal cl_ord_id
        # and use a UUID as request id to correlate stream updates.
        req_id = str(uuid.uuid4())

        try:
            with Client(self._token, target=self._host) as client:
                with self._unary_lock:
                    if self._use_sandbox_orders:
                        resp = client.sandbox.post_sandbox_order(
                            account_id=account_id,
                            instrument_id=self._instrument_id,
                            quantity=lots,
                            price=quotation,
                            direction=direction,
                            order_type=OrderType.ORDER_TYPE_LIMIT,
                            order_id=req_id,
                        )
                    else:
                        resp = client.orders.post_order(
                            account_id=account_id,
                            instrument_id=self._instrument_id,
                            quantity=lots,
                            price=quotation,
                            direction=direction,
                            order_type=OrderType.ORDER_TYPE_LIMIT,
                            order_id=req_id,
                        )
        except Exception as exc:
            self._logger.error(
                "[TBANK][ORDERS] post_order failed cl_ord_id=%s account=%s lots=%s: %s",
                cl_ord_id,
                account_id,
                lots,
                exc,
                exc_info=True,
            )
            try:
                self._order_manager.set_status(cl_ord_id, "REJECTED")
            except Exception:
                pass
            raise

        broker_id = str(getattr(resp, "order_id", "") or "").strip()
        if broker_id:
            self._cl_to_broker[cl_ord_id] = broker_id
            self._broker_to_cl[broker_id] = cl_ord_id
        self._req_to_cl[req_id] = cl_ord_id
        self._cl_to_account[cl_ord_id] = account_id

        self._logger.info(
            "[TBANK][ORDERS] post_order OK cl_ord_id=%s req_id=%s broker_order_id=%s lots=%s px=%s status=%s",
            cl_ord_id,
            req_id,
            broker_id or "?",
            lots,
            float(price),
            int(getattr(resp, "execution_report_status", 0) or 0),
        )

        self._emit_from_post_response(resp, cl_ord_id=cl_ord_id, symbol=symbol.upper())
        return cl_ord_id

    def cancel_order_by_cl_id(self, cl_ord_id: str) -> str:
        from t_tech.invest import Client

        broker_id = self._cl_to_broker.get(cl_ord_id)
        account_id = self._cl_to_account.get(cl_ord_id, "")
        if not broker_id or not account_id:
            self._logger.warning("[TBANK][ORDERS] cancel: unknown cl_ord_id=%s", cl_ord_id)
            return f"CANCEL-UNKNOWN-{cl_ord_id}"

        try:
            with Client(self._token, target=self._host) as client:
                with self._unary_lock:
                    if self._use_sandbox_orders:
                        _ = client.sandbox.cancel_sandbox_order(account_id=account_id, order_id=broker_id)
                    else:
                        _ = client.orders.cancel_order(account_id=account_id, order_id=broker_id)
        except Exception as exc:
            self._logger.error("[TBANK][ORDERS] cancel failed cl_ord_id=%s: %s", cl_ord_id, exc, exc_info=True)
            raise
        self._logger.info("[TBANK][ORDERS] cancel sent cl_ord_id=%s broker_order_id=%s", cl_ord_id, broker_id)
        return f"CANCEL-{broker_id}"

    def _qty_to_lots(self, qty: float) -> int:
        raw = float(qty) / float(self._shares_per_lot)
        lots = int(round(raw))
        return max(1, lots)

    def _round_price_to_tick(self, price: Decimal) -> Decimal:
        tick = Decimal(str(self._tick_size))
        if tick <= 0:
            return price
        # round to nearest tick
        q = (price / tick).quantize(Decimal("1"))
        return q * tick

    def _emit_from_post_response(self, resp: object, *, cl_ord_id: str, symbol: str) -> None:
        from t_tech.invest.utils import money_to_decimal

        st = int(getattr(resp, "execution_report_status", 0) or 0)
        lots_req = int(getattr(resp, "lots_requested", 0) or 0)
        lots_done = int(getattr(resp, "lots_executed", 0) or 0)
        self._last_lots_executed[cl_ord_id] = lots_done

        px_obj = getattr(resp, "executed_order_price", None)
        last_px = 0.0
        if px_obj is not None:
            try:
                last_px = float(money_to_decimal(px_obj))
            except Exception:
                last_px = 0.0

        ord_status, exec_type, last_qty = self._map_post_status(
            st,
            lots_requested=lots_req,
            lots_executed=lots_done,
            order_qty=float(self._order_manager.get_order(cl_ord_id).qty) if self._order_manager.get_order(cl_ord_id) else float(lots_req * self._shares_per_lot),
        )
        order = self._order_manager.get_order(cl_ord_id)
        order_qty = float(order.qty) if order else float(lots_req * self._shares_per_lot)
        cum_qty = min(order_qty, float(lots_done * self._shares_per_lot))
        leaves_qty = max(0.0, order_qty - cum_qty)

        msg = self._synthetic_er(
            cl_ord_id=cl_ord_id,
            symbol=symbol,
            side=order.side if order else "1",
            account=self._cl_to_account.get(cl_ord_id, ""),
            order_qty=order_qty,
            cum_qty=cum_qty,
            leaves_qty=leaves_qty,
            ord_status=ord_status,
            exec_type=exec_type,
            last_qty=last_qty,
            last_px=last_px,
            text="post_order_response",
        )
        self._on_execution_report(msg, "TBANK")

    def _handle_order_state(self, st: object) -> None:
        from t_tech.invest.schemas import OrderExecutionReportStatus
        from t_tech.invest.utils import money_to_decimal

        broker_id = str(getattr(st, "order_id", "") or "").strip()
        req_id = str(getattr(st, "order_request_id", "") or "").strip()
        cl_ord_id = self._req_to_cl.get(req_id, "") if req_id else ""
        if not cl_ord_id:
            cl_ord_id = self._broker_to_cl.get(broker_id, "")
        if not cl_ord_id:
            # Не наша заявка (другой клиент / ручные ордера) — игнорируем.
            return

        order = self._order_manager.get_order(cl_ord_id)
        if order is None:
            return

        sym = str(getattr(st, "ticker", "") or self._symbol).upper()
        st_code = int(getattr(st, "execution_report_status", 0) or 0)
        lot_sz = int(getattr(st, "lot_size", 0) or 0) or self._shares_per_lot
        lots_exec = int(getattr(st, "lots_executed", 0) or 0)
        prev = int(self._last_lots_executed.get(cl_ord_id, 0))
        delta_lots = max(0, lots_exec - prev)

        px_obj = getattr(st, "executed_order_price", None)
        last_px = 0.0
        if px_obj is not None:
            try:
                last_px = float(money_to_decimal(px_obj))
            except Exception:
                last_px = 0.0

        order_qty = float(order.qty)
        cum_qty = min(order_qty, float(lots_exec * lot_sz))
        leaves_qty = max(0.0, order_qty - cum_qty)
        last_qty = float(delta_lots * lot_sz) if delta_lots > 0 else 0.0

        ord_status, exec_type, _ = self._map_stream_status(
            st_code,
            lots_left=int(getattr(st, "lots_left", 0) or 0),
            lots_executed=lots_exec,
            order_qty=order_qty,
            lot_size=lot_sz,
        )

        # Дубли post_order (тот же lots_executed) и пустые тики — отбрасываем; terminal без объёма оставляем.
        if delta_lots <= 0 and st_code not in {
            OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED,
            OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED,
        }:
            return

        msg = self._synthetic_er(
            cl_ord_id=cl_ord_id,
            symbol=sym,
            side=order.side,
            account=str(getattr(st, "account_id", "") or self._cl_to_account.get(cl_ord_id, "")),
            order_qty=order_qty,
            cum_qty=cum_qty,
            leaves_qty=leaves_qty,
            ord_status=ord_status,
            exec_type=exec_type,
            last_qty=last_qty,
            last_px=last_px,
            text="order_state_stream",
        )
        self._on_execution_report(msg, "TBANK")
        self._last_lots_executed[cl_ord_id] = lots_exec

    def _map_post_status(
        self,
        st: int,
        *,
        lots_requested: int,
        lots_executed: int,
        order_qty: float,
    ) -> tuple[str, str, float]:
        from t_tech.invest.schemas import OrderExecutionReportStatus

        if st == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED:
            return "8", "8", 0.0
        if st == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL:
            return "2", "2", float(order_qty)
        if st == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL:
            return "1", "1", float(min(order_qty, float(lots_executed * self._shares_per_lot)))
        if st == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED:
            return "4", "4", 0.0
        if st == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW:
            return "0", "0", 0.0
        # UNSPECIFIED / прочее — считаем принятым.
        return "A", "0", 0.0

    def _map_stream_status(
        self,
        st: int,
        *,
        lots_left: int,
        lots_executed: int,
        order_qty: float,
        lot_size: int,
    ) -> tuple[str, str, float]:
        from t_tech.invest.schemas import OrderExecutionReportStatus

        if st == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED:
            return "8", "8", 0.0
        if st == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED:
            return "4", "4", 0.0
        if st == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL:
            return "2", "2", float(order_qty)
        if st == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL:
            return "1", "1", float(min(order_qty, float(lots_executed * lot_size)))
        if st == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW:
            return "0", "0", 0.0
        return "A", "0", 0.0

    def _synthetic_er(
        self,
        *,
        cl_ord_id: str,
        symbol: str,
        side: str,
        account: str,
        order_qty: float,
        cum_qty: float,
        leaves_qty: float,
        ord_status: str,
        exec_type: str,
        last_qty: float,
        last_px: float,
        text: str,
    ) -> SyntheticExecutionReport:
        fields = {
            35: "8",
            1: account or "TBANK",
            11: cl_ord_id,
            17: f"{cl_ord_id}|{exec_type}|TBANK",
            37: self._cl_to_broker.get(cl_ord_id, cl_ord_id),
            55: symbol,
            54: str(side),
            38: str(float(order_qty)),
            14: str(float(cum_qty)),
            151: str(float(leaves_qty)),
            6: str(float(last_px) if cum_qty > 0 else 0.0),
            39: ord_status,
            150: exec_type,
            32: str(float(last_qty)),
            31: str(float(last_px)),
            58: text,
        }
        return SyntheticExecutionReport(fields)


class TinkoffLiveEngine:
    """Один экземпляр на счёт (акции / фьючерсы); интерфейс как у NoopExecutionEngine."""

    def __init__(self, controller: TinkoffBrokerController, account_id: str) -> None:
        self._ctrl = controller
        self._account = str(account_id).strip()

    def send_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        *,
        account: str = "",
        price: float | None = None,
    ) -> str:
        acc = str(account).strip() or self._account
        return self._ctrl.send_limit_order(
            account_id=acc,
            symbol=str(symbol),
            side=side,
            qty=float(qty),
            price=price,
        )

    def cancel_order(self, cl_ord_id: str) -> str:
        return self._ctrl.cancel_order_by_cl_id(cl_ord_id)


def make_tinkoff_engines_for_runtime(
    *,
    token: str,
    host: str,
    instrument_id: str,
    symbol: str,
    order_manager: OrderManager,
    logger: logging.Logger,
    on_execution_report: Callable[[object, str], None],
    trading_account_equities: str,
    trading_account_forts: str,
    shares_per_lot: int,
    tick_size: float,
    use_sandbox_orders: bool,
) -> tuple[TinkoffLiveEngine, TinkoffLiveEngine, TinkoffBrokerController]:
    eq = str(trading_account_equities).strip()
    fo = str(trading_account_forts).strip()
    accounts = [a for a in (eq, fo) if a]
    ctrl = TinkoffBrokerController(
        token=token,
        host=host,
        instrument_id=instrument_id,
        symbol=symbol,
        order_manager=order_manager,
        logger=logger,
        on_execution_report=on_execution_report,
        shares_per_lot=shares_per_lot,
        tick_size=tick_size,
        use_sandbox_orders=use_sandbox_orders,
        account_ids_for_stream=accounts,
    )
    if not eq:
        raise RuntimeError("TradingAccountEquities is required for LIVE Tinkoff execution.")
    engine_eq = TinkoffLiveEngine(ctrl, eq)
    engine_fo = TinkoffLiveEngine(ctrl, fo if fo else eq)
    return engine_eq, engine_fo, ctrl
