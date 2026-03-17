from __future__ import annotations

import logging
from typing import Callable

import quickfix as fix

from dropcopy_client import DropCopyClient
from order_manager import OrderManager


class TradeClient:
    """Trade session helper used for sending orders/cancels."""

    TARGET_COMP_ID = "IFIX-EQ-UAT"

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.session_id: fix.SessionID | None = None

    def is_trade_session(self, session_id: fix.SessionID) -> bool:
        return session_id.getTargetCompID().getValue() == self.TARGET_COMP_ID

    def bind_session(self, session_id: fix.SessionID) -> None:
        self.session_id = session_id
        self.logger.info("[TRADE] session bound: %s", session_id.toString())


class MoexFixApplication(fix.Application):
    """
    Single QuickFIX Application handling both sessions.
    Routes messages to TRADE or DROP_COPY by SessionID.
    """

    def __init__(
        self,
        password: str,
        logger: logging.Logger,
        order_manager: OrderManager,
        on_execution_report: Callable[[fix.Message, str], None],
        on_market_data: Callable[[fix.Message, str], None] | None = None,
    ) -> None:
        super().__init__()
        self.password = password
        self.logger = logger
        self.order_manager = order_manager
        self.trade_client = TradeClient(logger)
        self.dropcopy_client = DropCopyClient(logger)
        self.on_execution_report = on_execution_report
        self.on_market_data = on_market_data
        self._logged_on_trade = False
        self._logged_on_dropcopy = False

    def onCreate(self, session_id: fix.SessionID) -> None:
        role = self._session_role(session_id)
        self.logger.info("[%s] Session created: %s", role, session_id.toString())

    def onLogon(self, session_id: fix.SessionID) -> None:
        role = self._session_role(session_id)
        if role == "TRADE":
            self.trade_client.bind_session(session_id)
            self._logged_on_trade = True
        else:
            self.dropcopy_client.bind_session(session_id)
            self._logged_on_dropcopy = True
        self.logger.info("[%s] Logon successful", role)

    def onLogout(self, session_id: fix.SessionID) -> None:
        role = self._session_role(session_id)
        if role == "TRADE":
            self._logged_on_trade = False
        else:
            self._logged_on_dropcopy = False
        self.logger.warning("[%s] Logout", role)

    def toAdmin(self, message: fix.Message, session_id: fix.SessionID) -> None:
        msg_type = self._msg_type(message)
        role = self._session_role(session_id)
        if msg_type == fix.MsgType_Logon:
            message.setField(fix.Password(self.password))
            message.setField(fix.ResetSeqNumFlag(True))
            self.logger.info("[%s][ADMIN] Logon tags set: 554,141", role)
        else:
            self.logger.debug("[%s][toAdmin %s] %s", role, msg_type, message.toString())

    def fromAdmin(self, message: fix.Message, session_id: fix.SessionID) -> None:
        role = self._session_role(session_id)
        msg_type = self._msg_type(message)
        if msg_type == fix.MsgType_Reject:
            self.logger.error("[%s][ADMIN REJECT] %s", role, self._message_to_dict(message))
            return
        if msg_type == fix.MsgType_Logout:
            self.logger.warning("[%s][ADMIN LOGOUT] %s", role, self._message_to_dict(message))
            return
        if msg_type == fix.MsgType_TestRequest:
            self.logger.warning("[%s][ADMIN TEST] %s", role, self._message_to_dict(message))
            return
        if msg_type == fix.MsgType_Heartbeat:
            self.logger.debug("[%s][ADMIN HEARTBEAT]", role)
            return
        self.logger.info("[%s][fromAdmin %s] %s", role, msg_type, message.toString())

    def toApp(self, message: fix.Message, session_id: fix.SessionID) -> None:
        role = self._session_role(session_id)
        self.logger.info("[%s][toApp %s] %s", role, self._msg_type(message), message.toString())

    def fromApp(self, message: fix.Message, session_id: fix.SessionID) -> None:
        role = self._session_role(session_id)
        msg_type = self._msg_type(message)
        try:
            if msg_type == fix.MsgType_ExecutionReport:
                self.on_execution_report(message, role)
            elif msg_type == "AE":
                # TradeCaptureReport, if enabled by venue for this feed.
                self.logger.info("[%s][TradeCaptureReport] %s", role, self._message_to_dict(message))
            elif msg_type in {fix.MsgType_MarketDataSnapshotFullRefresh, fix.MsgType_MarketDataIncrementalRefresh}:
                if self.on_market_data is not None:
                    self.on_market_data(message, role)
                else:
                    self.logger.info("[%s][MarketData %s] %s", role, msg_type, self._message_to_dict(message))
            else:
                self.logger.info("[%s][fromApp %s] %s", role, msg_type, self._message_to_dict(message))
        except Exception as exc:
            self.logger.exception("[%s][fromApp %s] processing error: %s", role, msg_type, exc)

    def is_trade_ready(self) -> bool:
        return self._logged_on_trade and self.trade_client.session_id is not None

    def get_trade_session_id(self) -> fix.SessionID:
        if not self.is_trade_ready():
            raise RuntimeError("TRADE session is not logged on.")
        return self.trade_client.session_id  # type: ignore[return-value]

    def _session_role(self, session_id: fix.SessionID) -> str:
        if self.trade_client.is_trade_session(session_id):
            return "TRADE"
        if self.dropcopy_client.is_dropcopy_session(session_id):
            return "DROP_COPY"
        return "UNKNOWN"

    @staticmethod
    def _msg_type(message: fix.Message) -> str:
        tag = fix.MsgType()
        message.getHeader().getField(tag)
        return tag.getValue()

    @staticmethod
    def _message_to_dict(message: fix.Message) -> dict[str, str]:
        data: dict[str, str] = {}
        raw = message.toString().strip()
        for item in raw.split("\x01"):
            if not item or "=" not in item:
                continue
            key, value = item.split("=", 1)
            data[key] = value
        return data
