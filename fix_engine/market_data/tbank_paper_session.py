from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from fix_engine.md_health_monitor import MdHealthMonitor
from fix_engine.market_data.market_data_engine import MarketDataEngine
from fix_engine.structured_logging import StructuredLoggingRuntime, log_event


def run_tbank_paper_session(
    *,
    base_dir: Path,
    cfg_for_optional: Path,
    execution_mode: str,
    gateway: object,
    market_data_engine: MarketDataEngine,
    logger: logging.Logger,
    failure_monitor: object | None,
    logging_runtime: StructuredLoggingRuntime,
    read_str: Callable[[Path, str, str], str],
    read_int: Callable[[Path, str, int], int],
    read_float: Callable[[Path, str, float], float],
    read_bool: Callable[[Path, str, bool], bool],
) -> None:
    from fix_engine.tbank_preflight import load_sandbox_token, verify_market_data_readonly
    from fix_engine.tbank_sandbox_feed import run_tbank_sandbox_market_data
    from fix_engine.tools.export_session_metrics import print_post_run_summary

    md_health: MdHealthMonitor | None = None
    if execution_mode != "PAPER_REAL_MARKET":
        raise RuntimeError(f"Sandbox paper session requires ExecutionMode=PAPER_REAL_MARKET, got {execution_mode!r}")

    uses_local_sim_execution = bool(getattr(gateway, "_uses_local_sim_execution", False))
    if not uses_local_sim_execution:
        raise RuntimeError("FATAL: gateway must use local synthetic execution for paper sandbox run")

    log_event(
        logger,
        level=logging.INFO,
        component="Preflight",
        event="paper_execution_guard",
        execution_mode=str(getattr(gateway, "execution_mode", execution_mode)),
        uses_local_sim_execution=uses_local_sim_execution,
    )

    tbank_host = read_str(cfg_for_optional, "TBankSandboxHost", "invest-public-api.tinkoff.ru:443")
    tbank_token = os.getenv("TINKOFF_TOKEN", "").strip() or os.getenv("TINKOFF_SANDBOX_TOKEN", "").strip()
    if not tbank_token:
        local_secrets = base_dir / "settings.local.cfg"
        if local_secrets.exists():
            tbank_token = read_str(local_secrets, "TBankSandboxToken", "").strip()
    if not tbank_token:
        tbank_token = read_str(cfg_for_optional, "TBankSandboxToken", "").strip()

    tbank_instrument_id = read_str(cfg_for_optional, "TBankInstrumentId", "")
    tbank_symbol = read_str(cfg_for_optional, "TBankSymbol", "SBER")
    tbank_depth = read_int(cfg_for_optional, "TBankOrderBookDepth", 1)
    tbank_include_trades = read_bool(cfg_for_optional, "TBankIncludeTrades", True)
    tbank_run_duration_sec = read_int(cfg_for_optional, "TBankRunDurationSec", 0)
    tbank_md_reconnect_initial_sec = read_float(cfg_for_optional, "TBankMdReconnectInitialSec", 1.0)
    tbank_md_reconnect_max_sec = read_float(cfg_for_optional, "TBankMdReconnectMaxSec", 60.0)
    tbank_md_stale_reconnect_sec = read_float(cfg_for_optional, "TBankMdStaleReconnectSec", 90.0)

    env_dur = os.getenv("TBANK_RUN_DURATION_SEC", "").strip()
    if env_dur:
        try:
            tbank_run_duration_sec = int(env_dur)
        except ValueError:
            logger.warning("TBANK_RUN_DURATION_SEC ignored (not an integer): %s", env_dur)

    marker_path = base_dir / "log" / "session_start_marker.txt"
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(datetime.now(timezone.utc).isoformat(), encoding="utf-8")

    tok = load_sandbox_token(base_dir, read_str)
    if not tok.strip():
        raise RuntimeError("PREFLIGHT: T-Invest token not loaded (TINKOFF_TOKEN / settings.local.cfg TBankSandboxToken)")

    verify_market_data_readonly(
        token=tok,
        host=tbank_host,
        instrument_id=tbank_instrument_id,
        logger=logger,
    )
    log_event(
        logger,
        level=logging.INFO,
        component="Preflight",
        event="stream_ready",
        detail="Tinkoff market-data unary OK; starting MarketDataStream (orders disabled)",
    )

    md_health = MdHealthMonitor(
        market_data_engine=market_data_engine,
        logger=logger,
        interval_sec=5.0,
    )
    md_health.start()
    if failure_monitor is not None:
        start = getattr(failure_monitor, "start", None)
        if callable(start):
            start()

    logger.info(
        "[TBANK] data_provider=%s host=%s instrument_id=%s symbol=%s run_duration_sec=%s",
        "TINKOFF",
        tbank_host,
        tbank_instrument_id,
        tbank_symbol,
        tbank_run_duration_sec,
    )
    try:
        run_tbank_sandbox_market_data(
            token=tbank_token,
            host=tbank_host,
            instrument_id=tbank_instrument_id,
            symbol=tbank_symbol,
            orderbook_depth=tbank_depth,
            include_trades=tbank_include_trades,
            on_raw_market_data=market_data_engine.update_market_data,
            logger=logger,
            run_duration_sec=tbank_run_duration_sec,
            reconnect_initial_delay_sec=tbank_md_reconnect_initial_sec,
            reconnect_max_delay_sec=tbank_md_reconnect_max_sec,
            stale_reconnect_sec=tbank_md_stale_reconnect_sec,
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        if md_health is not None:
            md_health.stop()
        if failure_monitor is not None:
            stop = getattr(failure_monitor, "stop", None)
            if callable(stop):
                stop()
        logging_runtime.listener.stop()
        try:
            print_post_run_summary(base_dir)
        except Exception as exc:
            logger.warning("post_run_summary_failed: %s", exc)

