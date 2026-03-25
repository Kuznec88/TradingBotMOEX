from __future__ import annotations

from pathlib import Path

from fix_engine.structured_logging import StructuredLoggingRuntime, configure_structured_logging


def setup_logging(base_dir: Path, *, paper_execution: bool) -> StructuredLoggingRuntime:
    return configure_structured_logging(base_dir=base_dir, paper_execution=paper_execution, logger_name="fix_engine")

