from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock
from typing import IO, Literal, TextIO, cast

from fix_engine.analytics_api import TradingAnalyticsAPI
from fix_engine.config.settings import (
    read_bool_merged as _read_bool,
    read_default_optional_setting_merged as _read_default_optional_setting,
    read_execution_mode_merged as _read_execution_mode,
    read_float_merged as _read_float,
    read_int_merged as _read_int,
    read_str_merged as _read_str,
)
from fix_engine.economics_store import EconomicsStore
from fix_engine.execution.noop_engine import NoopExecutionEngine
from fix_engine.execution_gateway import ExecutionGateway
from fix_engine.failure_monitor import FailureMonitor
from fix_engine.strategy.momentum_mm import BasicMarketMaker
from fix_engine.strategy.swing_live_runner import SwingLiveRunner
from fix_engine.market_data.market_data_engine import MarketDataEngine
from fix_engine.market_data.models import MarketData
from fix_engine.metrics.adverse_selection import FillAdverseSelectionTracker
from fix_engine.metrics.logging_setup import setup_logging
from fix_engine.order_manager import OrderManager
from fix_engine.order_models import MarketType
from fix_engine.position_manager import PositionManager
from fix_engine.quote_history_store import QuoteHistoryStore
from fix_engine.risk.manager import RiskManager
from fix_engine.structured_logging import StructuredLoggingRuntime
from fix_engine.broker_fee_model import BrokerFeeCalculator
from fix_engine.unit_economics import UnitEconomicsCalculator, _d


def _format_fix_for_log(raw_fix: str) -> str:
    return raw_fix.replace("\x01", "|")


def _source_to_market(source: str) -> MarketType:
    return MarketType.FORTS if source == "FORTS" else MarketType.EQUITIES


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

    economics_store = EconomicsStore(base_dir / "trade_economics.db")
    analytics_api = TradingAnalyticsAPI(economics_store)
    adverse_tracker = FillAdverseSelectionTracker(economics_store)

    fee_equities_bps = _read_float(base_dir, "FeesBpsEquities", 4.0)
    fee_forts_bps = _read_float(base_dir, "FeesBpsForts", 0.0)
    fee_fixed_equities = _read_float(base_dir, "FeesFixedEquities", 0.0)
    fee_fixed_forts = _read_float(base_dir, "FeesFixedForts", 0.0)
    use_broker_fee_model = _read_bool(base_dir, "BrokerFeeModelEnabled", True)
    broker_fee_model: BrokerFeeCalculator | None = None
    if use_broker_fee_model:
        t1_rub = _read_float(base_dir, "FeesFortsTier1EndRub", 12_000_000.0)
        t2_rub = _read_float(base_dir, "FeesFortsTier2EndRub", 17_000_000.0)
        r1_bps = _read_float(base_dir, "FeesFortsTier1Bps", 2.5)
        r2_bps = _read_float(base_dir, "FeesFortsTier2Bps", 2.0)
        r3_bps = _read_float(base_dir, "FeesFortsTier3Bps", 1.5)
        broker_fee_model = BrokerFeeCalculator(
            equities_bps=_d(fee_equities_bps),
            forts_flat_bps=_d(fee_forts_bps),
            fixed_equities=_d(fee_fixed_equities),
            fixed_forts=_d(fee_fixed_forts),
            forts_tiered=_read_bool(base_dir, "FeesFortsTiered", True),
            forts_tier1_end_rub=_d(t1_rub),
            forts_tier2_end_rub=_d(t2_rub),
            forts_tier1_rate=_d(r1_bps) / _d(10000),
            forts_tier2_rate=_d(r2_bps) / _d(10000),
            forts_tier3_rate=_d(r3_bps) / _d(10000),
        )
    economics = UnitEconomicsCalculator(
        fee_bps_by_market={
            MarketType.EQUITIES: _d(fee_equities_bps),
            MarketType.FORTS: _d(fee_forts_bps),
        },
        fixed_fee_by_market={
            MarketType.EQUITIES: _d(fee_fixed_equities),
            MarketType.FORTS: _d(fee_fixed_forts),
        },
        broker_fee_calculator=broker_fee_model,
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
    market_maker: BasicMarketMaker | None = None
    swing_runner: SwingLiveRunner | None = None

    def on_execution_report(message: object, source: str) -> None:
        if failure_monitor is not None:
            failure_monitor.on_execution_report()
        state = order_manager.on_execution_report(message)
        if swing_runner is not None:
            swing_runner.on_execution_report(state)
        if market_maker is not None:
            market_maker.on_execution_report(state)
        _ = source  # reserved for future per-source handling

    tbank_broker = None
    equities_engine = NoopExecutionEngine()
    forts_engine = NoopExecutionEngine()

    if execution_mode == "LIVE" and data_provider in {"TINKOFF", "TINKOFF_SANDBOX"}:
        from fix_engine.execution.tbank_broker import make_tinkoff_engines_for_runtime
        from fix_engine.tbank_preflight import load_sandbox_token
        from fix_engine.tools.common_cfg_dir import TBANK_INVEST_GRPC_HOST_PROD

        tok = load_sandbox_token(base_dir)
        if not str(tok).strip():
            raise RuntimeError(
                "ExecutionMode=LIVE requires TBankSandboxToken in settings.local.cfg."
            )
        host = _read_str(base_dir, "TBankSandboxHost", TBANK_INVEST_GRPC_HOST_PROD)
        inst = _read_str(base_dir, "TBankInstrumentId", "").strip()
        if not inst:
            raise RuntimeError("ExecutionMode=LIVE requires TBankInstrumentId (instrument UID).")
        sym = _read_str(base_dir, "TBankSymbol", "SBER").upper()
        tick_value = _read_default_optional_setting(base_dir, "tick_size")
        mm_tick = float(tick_value) if tick_value else _read_float(base_dir, "MMTickSize", 0.01)
        shares_per_lot = _read_int(base_dir, "TBankSharesPerLot", 1)
        use_sandbox_orders = _read_bool(base_dir, "TBankUseSandboxOrders", False)
        eq_acc = _read_str(base_dir, "TradingAccountEquities", "").strip()
        if not eq_acc:
            raise RuntimeError("ExecutionMode=LIVE requires TradingAccountEquities (Tinkoff account id).")
        fo_acc = _read_str(base_dir, "TradingAccountForts", "").strip()
        equities_engine, forts_engine, tbank_broker = make_tinkoff_engines_for_runtime(
            token=str(tok).strip(),
            host=host,
            instrument_id=inst,
            symbol=sym,
            order_manager=order_manager,
            logger=logger,
            on_execution_report=on_execution_report,
            trading_account_equities=eq_acc,
            trading_account_forts=fo_acc,
            shares_per_lot=shares_per_lot,
            tick_size=mm_tick,
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
            MarketType.EQUITIES: _read_str(base_dir, "TradingAccountEquities", ""),
            MarketType.FORTS: _read_str(base_dir, "TradingAccountForts", ""),
        },
        execution_mode=execution_mode,
        stream_book_fills=_read_bool(base_dir, "PaperStreamBookFills", False),
    )

    if quote_store is not None:
        market_data_engine.subscribe(quote_store.on_market_data)
    market_data_engine.subscribe(gateway.on_market_data)
    market_data_engine.subscribe(
        lambda data: economics.on_market_data(market=MarketType.EQUITIES, symbol=data.symbol, mid_price=_d(data.mid_price))
    )
    market_data_engine.subscribe(adverse_tracker.on_market_data)

    trading_strategy = _read_str(base_dir, "TradingStrategy", "SWING_BREAKOUT").strip().upper()
    mm_symbol = _read_str(base_dir, "TBankSymbol", "SBER").upper()
    lot_value = _read_default_optional_setting(base_dir, "lot_size")
    mm_lot = float(lot_value) if lot_value else _read_float(base_dir, "MarketMakingLotSize", 1.0)
    mm_market_raw = (_read_default_optional_setting(base_dir, "market") or _read_str(base_dir, "MarketMakingMarket", "EQUITIES")).upper()
    mm_market = MarketType(mm_market_raw) if mm_market_raw in {"EQUITIES", "FORTS"} else MarketType.EQUITIES
    tick_value = _read_default_optional_setting(base_dir, "tick_size")
    mm_tick = float(tick_value) if tick_value else _read_float(base_dir, "MMTickSize", 0.01)

    if trading_strategy == "MOMENTUM_MM":
        market_maker = BasicMarketMaker(
            symbol=mm_symbol,
            lot_size=mm_lot,
            market=mm_market,
            gateway=gateway,
            position_manager=position_manager,
            logger=logger,
            tick_size=mm_tick,
            strategy_mode=_read_str(base_dir, "strategy_mode", "SAFE").upper(),
            spread_threshold=_read_float(base_dir, "spread_threshold", 1.0),
            max_spread=_read_float(base_dir, "max_spread", 5.0),
            dynamic_spread_enabled=_read_bool(base_dir, "dynamic_spread_enabled", True),
            dynamic_spread_window_ticks=_read_int(base_dir, "dynamic_spread_window_ticks", 200),
            dynamic_spread_mult=_read_float(base_dir, "dynamic_spread_mult", 1.5),
            move_threshold=_read_float(base_dir, "move_threshold", 0.0),
            velocity_threshold=_read_float(base_dir, "velocity_threshold", 0.0),
            delta_threshold=_read_float(base_dir, "delta_threshold", 1.0),
            normalized_delta_threshold=_read_float(base_dir, "normalized_delta_threshold", 0.0),
            score_threshold=_read_float(base_dir, "score_threshold", 0.72),
            w_spread=_read_float(base_dir, "w_spread", 0.30),
            w_delta=_read_float(base_dir, "w_delta", 0.70),
            penalty_wide_spread=_read_float(base_dir, "penalty_wide_spread", 0.25),
            penalty_low_delta=_read_float(base_dir, "penalty_low_delta", 0.25),
            position_sizing_enabled=_read_bool(base_dir, "PositionSizingEnabled", True),
            sizing_risk_per_trade_abs=_read_float(base_dir, "SizingRiskPerTradeAbs", 8.0),
            sizing_stop_loss_ticks=_read_float(base_dir, "SizingStopLossTicks", 4.0),
            sizing_min_qty=_read_float(base_dir, "SizingMinQty", 1.0),
            sizing_max_qty=_read_float(base_dir, "SizingMaxQty", mm_lot),
            risk_manager=risk_manager,
            no_trade_fallback_minutes=_read_float(base_dir, "no_trade_fallback_minutes", 3.0),
            fallback_relax_step=_read_float(base_dir, "fallback_relax_step", 0.85),
            fallback_min_delta=_read_float(base_dir, "fallback_min_delta", 0.0),
            fallback_min_score=_read_float(base_dir, "fallback_min_score", 0.10),
            fallback_max_spread=_read_float(base_dir, "fallback_max_spread", 8.0),
            metrics_log_every=_read_int(base_dir, "metrics_log_every", 200),
            cooldown_ms=_read_int(base_dir, "cooldown_ms", 1200),
            flat_filter_window_sec=_read_float(base_dir, "FlatFilterWindowSec", 10.0),
            flat_threshold_points=_read_float(base_dir, "FlatThresholdPoints", 5.0),
            volatility_filter_window_sec=_read_float(base_dir, "VolatilityFilterWindowSec", 3.0),
            volatility_max_range_points=_read_float(base_dir, "VolatilityMaxRangePoints", 25.0),
            max_trades_per_min=_read_int(base_dir, "MaxTradesPerMin", 3),
            entry_cooldown_seconds=_read_float(base_dir, "EntryCooldownSeconds", 20.0),
            kill_zone_n=_read_int(base_dir, "KillZoneN", 5),
            kill_zone_stop_threshold=_read_int(base_dir, "KillZoneStopThreshold", 2),
            kill_zone_pause_seconds=_read_float(base_dir, "KillZonePauseSeconds", 180.0),
            kill_zone_pause_2_sec=_read_float(base_dir, "KillZonePause2Sec", 60.0),
            kill_zone_pause_3_sec=_read_float(base_dir, "KillZonePause3Sec", 120.0),
            kill_zone_consecutive_only=_read_bool(base_dir, "KillZoneConsecutiveOnly", True),
            entry_skip_log_throttle_sec=_read_float(base_dir, "EntrySkipLogThrottleSec", 0.5),
            strict_entry_enabled=_read_bool(base_dir, "StrictEntryEnabled", False),
            strict_spread_max_ticks=_read_float(base_dir, "StrictSpreadMaxTicks", 1.0),
            strong_imbalance_high_min=_read_float(base_dir, "StrongImbalanceHighMin", 0.75),
            strong_imbalance_med_min=_read_float(base_dir, "StrongImbalanceMedMin", 0.60),
            strict_delta_score_min=_read_float(base_dir, "StrictDeltaScoreMin", 0.70),
            strict_delta_score_med=_read_float(base_dir, "StrictDeltaScoreMed", 0.90),
            strict_min_trend_ticks=_read_float(base_dir, "StrictMinTrendTicks", 0.50),
            delta_spike_threshold=_read_float(base_dir, "DeltaSpikeThreshold", 300.0),
            continuation_ticks=_read_int(base_dir, "ContinuationTicks", 3),
            continuation_small_drop=_read_float(base_dir, "ContinuationSmallDrop", 25.0),
            trend_align_score_boost=_read_float(base_dir, "TrendAlignScoreBoost", 0.05),
            exit_on_score_enabled=_read_bool(base_dir, "exit_on_score_enabled", False),
            exit_threshold=_read_float(base_dir, "exit_threshold", 0.0),
            max_holding_sec=_read_float(base_dir, "max_holding_sec", 0.0),
            max_exit_spread=_read_float(base_dir, "max_exit_spread", 0.0) or None,
            force_time_exit_enabled=_read_bool(base_dir, "force_time_exit_enabled", False),
            force_time_exit_after_sec=_read_float(base_dir, "force_time_exit_after_sec", 0.0),
            min_entry_spread=_read_float(base_dir, "min_entry_spread", 1.0),
            max_entry_spread=_read_float(base_dir, "max_entry_spread", 3.0),
            entry_imbalance_threshold=_read_float(base_dir, "imbalance_threshold", 0.2),
            entry_imbalance_threshold_long=_read_float(
                base_dir, "imbalance_threshold_long", _read_float(base_dir, "imbalance_threshold", 0.2)
            ),
            entry_imbalance_threshold_short=_read_float(
                base_dir, "imbalance_threshold_short", _read_float(base_dir, "imbalance_threshold", 0.2)
            ),
            min_abs_imbalance=_read_float(base_dir, "min_abs_imbalance", 0.1),
            trend_lookback_ticks=_read_int(base_dir, "trend_lookback_ticks", 8),
            min_trend_move_ticks=_read_float(base_dir, "min_trend_move_ticks", 1.0),
            take_profit_ticks=_read_float(base_dir, "take_profit_ticks", 2.0),
            stop_loss_ticks=_read_float(base_dir, "stop_loss_ticks", 1.0),
            stop_loss_min_hold_bars=_read_int(base_dir, "stop_loss_min_hold_bars", 2),
            stop_loss_min_age_ms=_read_float(base_dir, "stop_loss_min_age_ms", 400.0),
            trailing_start_ticks=_read_float(base_dir, "trailing_start_ticks", 2.0),
            trailing_gap_ticks=_read_float(base_dir, "trailing_gap_ticks", 1.0),
            exit_imbalance_reversal=_read_float(base_dir, "exit_imbalance_reversal", 0.2),
            flow_w_delta=_read_float(base_dir, "flow_w_delta", 0.45),
            flow_w_imbalance=_read_float(base_dir, "flow_w_imbalance", 0.35),
            flow_w_trend=_read_float(base_dir, "flow_w_trend", 0.20),
            flow_decay_exit_threshold=_read_float(base_dir, "flow_decay_exit_threshold", 0.40),
            flow_decay_hold_threshold=_read_float(base_dir, "flow_decay_hold_threshold", 0.80),
            flow_decay_tp_disable_pnl_ticks=_read_float(base_dir, "flow_decay_tp_disable_pnl_ticks", 1.0),
            one_position_only=_read_bool(base_dir, "one_position_only", _read_bool(base_dir, "MMOnePositionOnly", True)),
            entry_decision_sink=economics_store.insert_entry_decisions,
            economics_store=economics_store,
        )
        market_data_engine.subscribe(market_maker.on_market_data)
        logger.info("[RUNTIME] TradingStrategy=MOMENTUM_MM (orderbook/tick MM)")
    else:
        from fix_engine.strategy.swing_breakout import SwingBreakoutParams
        from fix_engine.tbank_preflight import load_sandbox_token
        from fix_engine.tools.common_cfg_dir import TBANK_INVEST_GRPC_HOST_PROD

        tok = str(load_sandbox_token(base_dir) or "").strip()
        if not tok:
            raise RuntimeError(
                "TradingStrategy=SWING_BREAKOUT requires TBankSandboxToken in settings.local.cfg."
            )
        inst = _read_str(base_dir, "TBankInstrumentId", "").strip()
        if not inst:
            raise RuntimeError("SWING_BREAKOUT requires TBankInstrumentId")
        host = _read_str(base_dir, "TBankSandboxHost", TBANK_INVEST_GRPC_HOST_PROD)
        swing_interval = _read_str(base_dir, "SwingCandleInterval", "15m").strip().lower()
        if swing_interval not in {"5m", "15m"}:
            raise RuntimeError(f"SwingCandleInterval must be 5m or 15m, got {swing_interval!r}")

        swing_params = SwingBreakoutParams(
            n_levels=_read_int(base_dir, "SwingNLevels", 20),
            k_volume=_read_float(base_dir, "SwingKVolume", 1.5),
            ma_period=_read_int(base_dir, "SwingMAPeriod", 50),
            take_profit_rub=_read_float(base_dir, "SwingTpRub", 200.0),
            stop_loss_rub=_read_float(base_dir, "SwingSlRub", 100.0),
            rub_per_point=_read_float(base_dir, "SwingRubPerPoint", 1.0),
            tick_size=mm_tick,
            slippage_ticks=_read_int(base_dir, "SwingSlippageTicks", 1),
            commission_rate=_read_float(base_dir, "SwingCommissionRate", 0.0004),
            notional_scale=_read_float(base_dir, "SwingNotionalScale", 1.0),
            trailing_activation_rub=_read_float(base_dir, "SwingTrailingActivationRub", 0.0),
            trailing_gap_rub=_read_float(base_dir, "SwingTrailingGapRub", 50.0),
        )
        swing_runner = SwingLiveRunner(
            base_dir=base_dir,
            gateway=gateway,
            logger=logger,
            symbol=mm_symbol,
            market=mm_market,
            lot_size=mm_lot,
            tick_size=mm_tick,
            params=swing_params,
            candle_interval=swing_interval,
            poll_interval_sec=_read_float(base_dir, "SwingPollIntervalSec", 30.0),
            get_latest=market_data_engine.get_latest,
            instrument_id=inst,
            token=tok,
            host=host.strip(),
        )
        swing_runner.start()
        market_data_engine.subscribe(swing_runner.on_market_data)
        logger.info(
            "[RUNTIME] TradingStrategy=SWING_BREAKOUT interval=%s poll=%.1fs symbol=%s",
            swing_interval,
            _read_float(base_dir, "SwingPollIntervalSec", 30.0),
            mm_symbol,
        )

    if data_provider in {"TINKOFF", "TINKOFF_SANDBOX"}:
        from fix_engine.market_data.tbank_paper_session import run_tbank_paper_session

        run_tbank_paper_session(
            base_dir=base_dir,
            execution_mode=execution_mode,
            gateway=gateway,
            market_data_engine=market_data_engine,
            logger=logger,
            failure_monitor=failure_monitor,
            logging_runtime=logging_runtime,
            tbank_broker=tbank_broker,
            swing_runner=swing_runner,
        )
        return

    raise RuntimeError(f"Unsupported DataProvider={data_provider!r}. Use TINKOFF (or legacy TINKOFF_SANDBOX).")

