from __future__ import annotations

import logging

import quickfix as fix


class DropCopyClient:
    """Drop Copy session helper: receives fills/order updates from venue feed."""

    TARGET_COMP_ID = "IFIX-DC-EQ-UAT"

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.session_id: fix.SessionID | None = None

    def is_dropcopy_session(self, session_id: fix.SessionID) -> bool:
        return session_id.getTargetCompID().getValue() == self.TARGET_COMP_ID

    def bind_session(self, session_id: fix.SessionID) -> None:
        self.session_id = session_id
        self.logger.info("[DROP_COPY] session bound: %s", session_id.toString())
