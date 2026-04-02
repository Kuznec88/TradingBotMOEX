from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from fix_engine.md_health_monitor import MdHealthMonitor
from fix_engine.market_data.market_data_engine import MarketDataEngine
from fix_engine.structured_logging import StructuredLoggingRuntime, log_event


def run_tbank_paper_session(
    *,
    base_dir: Path,
    execution_mode: str,
    gateway: object,
    market_data_engine: MarketDataEngine,
    logger: logging.Logger,
    failure_monitor: object | None,
    logging_runtime: StructuredLoggingRuntime,
    tbank_broker: object | None = None,
    swing_runner: object | None = None,
) -> None:
    from fix_engine.config.settings import (
        read_bool_merged,
        read_float_merged,
        read_int_merged,
        read_str_merged,
    )
    from fix_engine.tbank_preflight import load_sandbox_token, verify_market_data_readonly
    from fix_engine.tbank_sandbox_feed import run_tbank_sandbox_market_data
    from fix_engine.tools.common_cfg_dir import TBANK_INVEST_GRPC_HOST_PROD
    from fix_engine.tools.export_session_metrics import print_post_run_summary

    md_health: MdHealthMonitor | None = None
    if execution_mode not in {"PAPER_REAL_MARKET", "LIVE"}:
        raise RuntimeError(
            f"Tinkoff market-data session requires ExecutionMode=PAPER_REAL_MARKET or LIVE, got {execution_mode!r}"
        )

    uses_local_sim_execution = bool(getattr(gateway, "_uses_local_sim_execution", False))
    if execution_mode == "PAPER_REAL_MARKET" and not uses_local_sim_execution:
        raise RuntimeError("FATAL: PAPER_REAL_MARKET requires local synthetic execution (paper fills)")
    if execution_mode == "LIVE" and uses_local_sim_execution:
        raise RuntimeError("FATAL: LIVE requires broker execution (set ExecutionMode=LIVE and wire Tinkoff engines)")

    log_event(
        logger,
        level=logging.INFO,
        component="Preflight",
        event="paper_execution_guard",
        execution_mode=str(getattr(gateway, "execution_mode", execution_mode)),
        uses_local_sim_execution=uses_local_sim_execution,
        broker_orders=execution_mode == "LIVE",
    )

    tbank_host = read_str_merged(base_dir, "TBankSandboxHost", TBANK_INVEST_GRPC_HOST_PROD)

    tbank_instrument_id = read_str_merged(base_dir, "TBankInstrumentId", "")
    tbank_symbol = read_str_merged(base_dir, "TBankSymbol", "SBER")
    tbank_depth = read_int_merged(base_dir, "TBankOrderBookDepth", 1)
    tbank_include_trades = read_bool_merged(base_dir, "TBankIncludeTrades", True)
    tbank_run_duration_sec = read_int_merged(base_dir, "TBankRunDurationSec", 0)
    tbank_md_reconnect_initial_sec = read_float_merged(base_dir, "TBankMdReconnectInitialSec", 1.0)
    tbank_md_reconnect_max_sec = read_float_merged(base_dir, "TBankMdReconnectMaxSec", 60.0)
    tbank_md_stale_reconnect_sec = read_float_merged(base_dir, "TBankMdStaleReconnectSec", 90.0)

    env_dur = os.getenv("TBANK_RUN_DURATION_SEC", "").strip()
    if env_dur:
        try:
            tbank_run_duration_sec = int(env_dur)
        except ValueError:
            logger.warning("TBANK_RUN_DURATION_SEC ignored (not an integer): %s", env_dur)

    marker_path = base_dir / "log" / "session_start_marker.txt"
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(datetime.now(timezone.utc).isoformat(), encoding="utf-8")

    tbank_token = load_sandbox_token(base_dir)
    if not tbank_token.strip():
        raise RuntimeError(
            "PREFLIGHT: T-Invest token not loaded (settings.local.cfg: TBankSandboxToken=...)"
        )

    verify_market_data_readonly(
        token=tbank_token,
        host=tbank_host,
        instrument_id=tbank_instrument_id,
        logger=logger,
    )
    log_event(
        logger,
        level=logging.INFO,
        component="Preflight",
        event="stream_ready",
        detail=(
            "Tinkoff market-data unary OK; starting MarketDataStream"
            + ("; LIVE broker orders enabled" if execution_mode == "LIVE" else "; paper fills local")
        ),
    )

    md_health = MdHealthMonitor(
        market_data_engine=market_data_engine,
        logger=logger,
        interval_sec=5.0,
    )
    md_health.start()
    if tbank_broker is not None:
        start_stream = getattr(tbank_broker, "start_order_stream", None)
        if callable(start_stream):
            start_stream()
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
        if swing_runner is not None:
            stop_swing = getattr(swing_runner, "stop", None)
            if callable(stop_swing):
                stop_swing()
        if tbank_broker is not None:
            stop_stream = getattr(tbank_broker, "stop_order_stream", None)
            if callable(stop_stream):
                stop_stream()
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

