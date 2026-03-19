from __future__ import annotations

import logging

import quickfix as fix


class FortsClient:
    """FORTS session helper for derivatives market."""

    TARGET_COMP_ID = "FG"

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.session_id: fix.SessionID | None = None

    def is_forts_session(self, session_id: fix.SessionID) -> bool:
        return session_id.getTargetCompID().getValue() == self.TARGET_COMP_ID

    def bind_session(self, session_id: fix.SessionID) -> None:
        self.session_id = session_id
        self.logger.info("[FORTS] session bound: %s", session_id.toString())
