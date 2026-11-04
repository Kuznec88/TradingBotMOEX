from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path
from typing import IO, Literal, TextIO, cast

from fix_engine.config.settings import (
    read_bool_merged as _read_bool,
    read_default_optional_setting_merged as _read_default_optional_setting,
    read_execution_mode_merged as _read_execution_mode,
    read_float_merged as _read_float,
    read_int_merged as _read_int,
    read_str_merged as _read_str,
)
from fix_engine.config.swing_params import load_swing_params
from fix_engine.data.instruments import assert_no_open_position, fetch_front_future
from fix_engine.data.preflight import load_sandbox_token
from fix_engine.data.engine import MarketDataEngine
from fix_engine.execution.gateway import ExecutionGateway
from fix_engine.execution.noop_engine import NoopExecutionEngine
from fix_engine.failure_monitor import FailureMonitor
from fix_engine.strategy.swing_live_runner import SwingLiveRunner
from fix_engine.metrics.logging_setup import setup_logging
from fix_engine.order_manager import OrderManager
from fix_engine.order_models import MarketType
from fix_engine.position_manager import PositionManager
from fix_engine.quote_history_store import QuoteHistoryStore
from fix_engine.risk.manager import RiskManager
from fix_engine.structured_logging import StructuredLoggingRuntime


def _acquire_runtime_lock(base_dir: Path, logger: logging.Logger) -> TextIO | None | Literal[False]:
    """Возвращает открытый file handle при успешной блокировке, None если lock недоступен, False если уже запущен другой экземпляр."""
    try:
        import msvcrt  # type: ignore

        lock_path = base_dir / "log" / "fix_engine_runtime.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(lock_path, "a+", encoding="utf-8")
        try:
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            logger.error("[PREFLIGHT] Another instance is already running (lock=%s). Exiting.", lock_path)
            try:
                fh.close()
            except Exception:
                pass
            return False
        return fh
    except Exception:
        return None


def _release_runtime_lock(fh: IO[str] | None) -> None:
    if fh is None:
        return
    try:
        import msvcrt  # type: ignore

        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
    except Exception:
        pass
    try:
        fh.close()
    except Exception:
        pass


def run() -> None:
    base_dir = Path(__file__).resolve().parents[1]
    # Настройки: settings.local.cfg → settings.runtime.cfg → settings.cfg (как в tools/common_cfg_dir).
    execution_mode = _read_execution_mode(base_dir)
    logging_runtime = setup_logging(base_dir, paper_execution=(execution_mode != "LIVE"))
    logger = logging_runtime.logger

    _lock_fh = _acquire_runtime_lock(base_dir, logger)
    if _lock_fh is False:
        return
    _lock_fh = cast(TextIO | None, _lock_fh)

    try:
        _run_locked(base_dir, execution_mode, logging_runtime, logger, _lock_fh)
    finally:
        _release_runtime_lock(_lock_fh)


def _run_locked(
    base_dir: Path,
    execution_mode: str,
    logging_runtime: StructuredLoggingRuntime,
    logger: logging.Logger,
    _lock_fh: TextIO | None,
) -> None:
    _ = _lock_fh  # удерживается живым до finally в run()

    data_provider = _read_str(base_dir, "DataProvider", "TINKOFF").upper()
    futures_prefix = _read_str(base_dir, "TBankFuturesPrefix", "").strip()
    if not futures_prefix:
        raise RuntimeError("TBankFuturesPrefix is required (for example SR)")
    contract = fetch_front_future(
        token=str(load_sandbox_token(base_dir) or "").strip(),
        host=_read_str(base_dir, "TBankSandboxHost", ""),
        prefix=futures_prefix,
        roll_days=_read_int(base_dir, "TBankFuturesRollDays", 7),
    )
    logger.info(
        "[PREFLIGHT] contract=%s uid=%s tick=%s no_entries_after=%s",
        contract.ticker,
        contract.uid,
        contract.tick,
        contract.no_entries_after.isoformat(),
    )
    order_manager = OrderManager()
    market_data_engine = MarketDataEngine(logger=logger)

    quote_history_enabled = _read_bool(base_dir, "QuoteHistoryEnabled", True)
    quote_history_db = base_dir / _read_str(base_dir, "QuoteHistoryDbPath", "quote_history.db")
    quote_history_retention = _read_float(base_dir, "QuoteHistoryRetentionDays", 14.0)
    quote_history_sample_ms = _read_float(base_dir, "QuoteHistorySampleIntervalMs", 1000.0)
    quote_store: QuoteHistoryStore | None = None
    if quote_history_enabled:
        quote_store = QuoteHistoryStore(
            quote_history_db,
            retention_days=quote_history_retention,
            sample_interval_ms=quote_history_sample_ms,
        )

    risk_manager = RiskManager(
        order_manager=order_manager,
        max_exposure_per_instrument=_read_float(base_dir, "RiskMaxExposurePerInstrument", 1_000_000.0),
        max_trades_in_window=_read_int(base_dir, "RiskMaxTradesInWindow", 200),
        trades_window_seconds=_read_int(base_dir, "RiskTradesWindowSeconds", 60),
        cooldown_after_consecutive_losses=_read_int(base_dir, "RiskCooldownAfterConsecutiveLosses", 3),
        cooldown_seconds=_read_int(base_dir, "RiskCooldownSeconds", 30),
        max_abs_position_per_symbol=_read_float(base_dir, "RiskMaxAbsPositionPerSymbol", 0.0),
        max_daily_loss_abs=_read_float(base_dir, "RiskMaxDailyLossAbs", 0.0),
        kill_switch_drawdown_abs=_read_float(base_dir, "RiskKillSwitchDrawdownAbs", 0.0),
        logger=logger,
    )

    failure_handling_enabled = _read_bool(base_dir, "FailureHandlingEnabled", True)
    max_inventory_per_symbol = _read_float(base_dir, "MaxInventoryPerSymbol", 10.0)
    default_soft_inventory = max_inventory_per_symbol * 0.8 if max_inventory_per_symbol > 0 else 0.0
    position_manager = PositionManager(
        order_manager=order_manager,
        max_abs_inventory_per_symbol=max_inventory_per_symbol,
        soft_abs_inventory_per_symbol=_read_float(base_dir, "MaxInventorySoftPerSymbol", default_soft_inventory),
    )

    failure_monitor: FailureMonitor | None = None
    swing_runner: SwingLiveRunner | None = None

    def on_execution_report(message: object, source: str) -> None:
        if failure_monitor is not None:
            failure_monitor.on_execution_report()
        state = order_manager.on_execution_report(message)
        if swing_runner is not None:
            swing_runner.on_execution_report(state)
        _ = source  # reserved for future per-source handling

    tbank_broker = None
    equities_engine = NoopExecutionEngine()
    forts_engine = NoopExecutionEngine()

    account_eq = _read_str(base_dir, "TradingAccountEquities", "").strip()
    account_fo = _read_str(base_dir, "TradingAccountForts", "").strip()

    if execution_mode == "LIVE" and data_provider in {"TINKOFF", "TINKOFF_SANDBOX"}:
        from fix_engine.execution.tbank_broker import make_tinkoff_engines_for_runtime
        from fix_engine.data.preflight import ensure_sandbox_account_id, is_sandbox_uuid_account_id
        from fix_engine.tools.common_cfg_dir import TBANK_INVEST_GRPC_HOST_PROD

        tok = load_sandbox_token(base_dir)
        if not str(tok).strip():
            raise RuntimeError(
                "ExecutionMode=LIVE requires TBankSandboxToken in settings.local.cfg."
            )
        host = _read_str(base_dir, "TBankSandboxHost", TBANK_INVEST_GRPC_HOST_PROD)
        inst = contract.uid
        sym = contract.ticker
        tick_size = contract.tick
        shares_per_lot = _read_int(base_dir, "TBankSharesPerLot", 1)
        use_sandbox_orders = _read_bool(base_dir, "TBankUseSandboxOrders", False)
        if not use_sandbox_orders:
            # Стопы живут только внутри бота: при сбое реальная позиция осталась бы без защиты.
            raise RuntimeError("Real account orders are disabled until broker-side stop orders are implemented.")
        if use_sandbox_orders:
            account_eq = ensure_sandbox_account_id(
                token=str(tok).strip(),
                host=host,
                logger=logger,
                preferred=account_eq,
                bundle_dir=base_dir,
            )
            if not account_fo or not is_sandbox_uuid_account_id(account_fo):
                account_fo = account_eq
        elif not account_eq:
            raise RuntimeError("ExecutionMode=LIVE requires TradingAccountEquities (Tinkoff account id).")
        assert_no_open_position(
            token=str(tok).strip(),
            host=host,
            account_id=account_fo or account_eq,
            instrument_uid=inst,
            use_sandbox=use_sandbox_orders,
        )
        equities_engine, forts_engine, tbank_broker = make_tinkoff_engines_for_runtime(
            token=str(tok).strip(),
            host=host,
            instrument_id=inst,
            symbol=sym,
            order_manager=order_manager,
            logger=logger,
            on_execution_report=on_execution_report,
            trading_account_equities=account_eq,
            trading_account_forts=account_fo,
            shares_per_lot=shares_per_lot,
            tick_size=tick_size,
            use_sandbox_orders=use_sandbox_orders,
        )
        logger.info(
            "[PREFLIGHT] LIVE Tinkoff orders: host=%s instrument_id=%s symbol=%s TBankSharesPerLot=%s sandbox_orders=%s",
            host,
            inst,
            sym,
            shares_per_lot,
            use_sandbox_orders,
        )

    gateway = ExecutionGateway(
        equities_engine=equities_engine,
        forts_engine=forts_engine,
        order_manager=order_manager,
        logger=logger,
        simulation_mode=False,
        get_latest_market_data=market_data_engine.get_latest,
        on_execution_report=on_execution_report,
        risk_manager=risk_manager,
        position_manager=position_manager,
        simulation_slippage_bps=_read_float(base_dir, "PaperSlippageBps", 2.0),
        simulation_slippage_max_bps=_read_float(base_dir, "PaperSlippageMaxBps", 25.0),
        simulation_volatility_slippage_multiplier=_read_float(base_dir, "PaperVolatilitySlippageMultiplier", 1.5),
        simulation_latency_network_ms=_read_int(base_dir, "PaperNetworkLatencyMs", 20),
        simulation_latency_exchange_ms=_read_int(base_dir, "PaperExchangeLatencyMs", 30),
        simulation_latency_jitter_ms=_read_int(base_dir, "PaperLatencyJitterMs", 40),
        simulation_fill_participation=_read_float(base_dir, "PaperFillParticipation", 0.25),
        simulation_touch_fill_probability=_read_float(base_dir, "PaperTouchFillProbability", 0.15),
        simulation_passive_fill_probability_scale=_read_float(base_dir, "PaperPassiveFillProbabilityScale", 0.5),
        simulation_adverse_selection_bias=_read_float(base_dir, "PaperAdverseSelectionBias", 0.35),
        simulation_slippage_model=(_read_default_optional_setting(base_dir, "PaperSlippageModel") or "DYNAMIC_BPS").upper(),
        simulation_fixed_slippage_abs=_read_float(base_dir, "PaperFixedSlippageAbs", 0.0),
        simulation_spread_slippage_fraction=_read_float(base_dir, "PaperSpreadSlippageFraction", 0.25),
        simulation_decision_to_send_min_ms=_read_int(base_dir, "DecisionToSendLatencyMinMs", 5),
        simulation_decision_to_send_max_ms=_read_int(base_dir, "DecisionToSendLatencyMaxMs", 20),
        simulation_send_to_fill_min_ms=_read_int(base_dir, "SendToFillLatencyMinMs", 50),
        simulation_send_to_fill_max_ms=_read_int(base_dir, "SendToFillLatencyMaxMs", 200),
        account_by_market={
            MarketType.EQUITIES: account_eq,
            MarketType.FORTS: account_fo or account_eq,
        },
        execution_mode=execution_mode,
        stream_book_fills=_read_bool(base_dir, "PaperStreamBookFills", False),
    )

    if quote_store is not None:
        market_data_engine.subscribe(quote_store.on_market_data)
    market_data_engine.subscribe(gateway.on_market_data)
    symbol = contract.ticker
    lot_value = _read_default_optional_setting(base_dir, "lot_size")
    lot_size = float(lot_value) if lot_value else _read_float(base_dir, "MarketMakingLotSize", 1.0)
    market_raw = (_read_default_optional_setting(base_dir, "market") or _read_str(base_dir, "MarketMakingMarket", "EQUITIES")).upper()
    market = MarketType(market_raw) if market_raw in {"EQUITIES", "FORTS"} else MarketType.EQUITIES
    tick_size = contract.tick

    if failure_handling_enabled:
        failure_monitor = FailureMonitor(
            logger=logger,
            order_manager=order_manager,
            gateway=gateway,
            watch_symbol=symbol,
            max_market_data_staleness_sec=_read_int(base_dir, "MaxMarketDataStalenessSec", 60),
            max_order_stuck_sec=_read_int(base_dir, "MaxOrderStuckSec", 30),
            max_no_execution_report_sec=_read_int(base_dir, "MaxNoExecutionReportSec", 15),
            action_on_anomaly=_read_str(base_dir, "FailureAction", "ALERT"),
            check_interval_sec=_read_int(base_dir, "FailureCheckIntervalSec", 1),
        )
        market_data_engine.subscribe(failure_monitor.on_market_data)

    from fix_engine.tools.common_cfg_dir import TBANK_INVEST_GRPC_HOST_PROD

    tok = str(load_sandbox_token(base_dir) or "").strip()
    if not tok:
        raise RuntimeError(
            "TradingStrategy=SWING_BREAKOUT requires TBankSandboxToken in settings.local.cfg."
        )
    inst = contract.uid
    host = _read_str(base_dir, "TBankSandboxHost", TBANK_INVEST_GRPC_HOST_PROD)
    swing_interval = _read_str(base_dir, "SwingCandleInterval", "1h").strip().lower()
    if swing_interval not in {"1h", "4h"}:
        raise RuntimeError(f"SwingCandleInterval must be 1h or 4h, got {swing_interval!r}")

    rub_per_point = contract.tick_rub / contract.tick
    swing_params = load_swing_params(base_dir, tick_size=tick_size)
    # Путь журнала сделок считается от fix_engine, а не от каталога запуска.
    trades_log = swing_params.trades_log_csv_path and str(base_dir / swing_params.trades_log_csv_path)
    swing_params = replace(swing_params, rub_per_point=rub_per_point, notional_scale=rub_per_point, trades_log_csv_path=trades_log)
    swing_runner = SwingLiveRunner(
        base_dir=base_dir,
        gateway=gateway,
        logger=logger,
        symbol=symbol,
        market=market,
        lot_size=lot_size,
        tick_size=tick_size,
        params=swing_params,
        candle_interval=swing_interval,
        poll_interval_sec=_read_float(base_dir, "SwingPollIntervalSec", 30.0),
        get_latest=market_data_engine.get_latest,
        instrument_id=inst,
        token=tok,
        host=host.strip(),
        on_trade_closed=risk_manager.on_trade_result,
        no_entries_after=contract.no_entries_after,
    )
    swing_runner.start()
    market_data_engine.subscribe(swing_runner.on_market_data)
    logger.info(
        "[RUNTIME] TradingStrategy=SWING_BREAKOUT interval=%s poll=%.1fs symbol=%s",
        swing_interval,
        _read_float(base_dir, "SwingPollIntervalSec", 30.0),
        symbol,
    )

    if data_provider in {"TINKOFF", "TINKOFF_SANDBOX"}:
        from fix_engine.data.tbank_session import run_tbank_paper_session

        run_tbank_paper_session(
            base_dir=base_dir,
            execution_mode=execution_mode,
            gateway=gateway,
            market_data_engine=market_data_engine,
            logger=logger,
            failure_monitor=failure_monitor,
            logging_runtime=logging_runtime,
            instrument_id=contract.uid,
            symbol=contract.ticker,
            tbank_broker=tbank_broker,
            swing_runner=swing_runner,
        )
        return

    raise RuntimeError(f"Unsupported DataProvider={data_provider!r}. Use TINKOFF (or legacy TINKOFF_SANDBOX).")

