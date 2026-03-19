from __future__ import annotations

import json
import logging
import queue
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from typing import Any


@dataclass
class StructuredLoggingRuntime:
    logger: logging.Logger
    listener: QueueListener


class JsonFormatter(logging.Formatter):
    _reserved = {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "taskName",
    }

    def __init__(self, simulation: bool) -> None:
        super().__init__()
        self._simulation = bool(simulation)

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="microseconds"),
            "level": record.levelname,
            "component": getattr(record, "component", record.name),
            "event": getattr(record, "event", "log"),
            "correlation_id": getattr(record, "correlation_id", ""),
            "simulation": bool(getattr(record, "simulation", self._simulation)),
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in self._reserved:
                continue
            if key in payload:
                continue
            payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = record.stack_info
        return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def configure_structured_logging(
    *,
    base_dir: Path,
    simulation: bool,
    logger_name: str = "fix_engine",
    level: int = logging.INFO,
) -> StructuredLoggingRuntime:
    log_dir = base_dir / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    log_queue: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
    queue_handler = QueueHandler(log_queue)
    logger.addHandler(queue_handler)

    formatter = JsonFormatter(simulation=simulation)
    file_handler = logging.FileHandler(log_dir / "fix_client.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    listener = QueueListener(log_queue, file_handler, console_handler, respect_handler_level=True)
    listener.start()
    return StructuredLoggingRuntime(logger=logger, listener=listener)


def log_event(
    logger: logging.Logger,
    *,
    level: int,
    component: str,
    event: str,
    correlation_id: str = "",
    **fields: Any,
) -> None:
    extra = {
        "component": component,
        "event": event,
        "correlation_id": correlation_id,
    }
    extra.update(fields)
    logger.log(level, event, extra=extra)
