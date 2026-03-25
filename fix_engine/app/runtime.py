from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock

from fix_engine.analytics_api import TradingAnalyticsAPI
from fix_engine.config.settings import (
    read_bool as _read_bool,
    read_default_optional_setting as _read_default_optional_setting,
    read_execution_mode as _read_execution_mode,
    read_float as _read_float,
    read_int as _read_int,
    read_str as _read_str,
)
from fix_engine.economics_store import EconomicsStore
from fix_engine.execution.noop_engine import NoopExecutionEngine
from fix_engine.execution_gateway import ExecutionGateway
from fix_engine.failure_monitor import FailureMonitor
from fix_engine.strategy.momentum_mm import BasicMarketMaker
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
from fix_engine.unit_economics import UnitEconomicsCalculator, _d


def _format_fix_for_log(raw_fix: str) -> str:
    return raw_fix.replace("\x01", "|")


def _source_to_market(source: str) -> MarketType:
    return MarketType.FORTS if source == "FORTS" else MarketType.EQUITIES


def run() -> None:
    base_dir = Path(__file__).resolve().parents[1]
    settings_path = base_dir / "settings.cfg"
    runtime_cfg_path = base_dir / "settings.runtime.cfg"
    cfg_for_optional = runtime_cfg_path if runtime_cfg_path.exists() else settings_path
    execution_mode = _read_execution_mode(cfg_for_optional)
    logging_runtime = setup_logging(base_dir, paper_execution=(execution_mode != "LIVE"))
    logger = logging_runtime.logger

    data_provider = _read_str(cfg_for_optional, "DataProvider", "TINKOFF").upper()
    order_manager = OrderManager()
    market_data_engine = MarketDataEngine(logger=logger)

    quote_history_enabled = _read_bool(cfg_for_optional, "QuoteHistoryEnabled", True)
    quote_history_db = base_dir / _read_str(cfg_for_optional, "QuoteHistoryDbPath", "quote_history.db")
    quote_history_retention = _read_float(cfg_for_optional, "QuoteHistoryRetentionDays", 14.0)
    quote_history_sample_ms = _read_float(cfg_for_optional, "QuoteHistorySampleIntervalMs", 1000.0)
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

    fee_equities_bps = _read_float(cfg_for_optional, "FeesBpsEquities", 0.0)
    fee_forts_bps = _read_float(cfg_for_optional, "FeesBpsForts", 0.0)
    fee_fixed_equities = _read_float(cfg_for_optional, "FeesFixedEquities", 0.0)
    fee_fixed_forts = _read_float(cfg_for_optional, "FeesFixedForts", 0.0)
    economics = UnitEconomicsCalculator(
        fee_bps_by_market={
            MarketType.EQUITIES: _d(fee_equities_bps),
            MarketType.FORTS: _d(fee_forts_bps),
        },
        fixed_fee_by_market={
            MarketType.EQUITIES: _d(fee_fixed_equities),
            MarketType.FORTS: _d(fee_fixed_forts),
        },
    )

    risk_manager = RiskManager(
        order_manager=order_manager,
        max_exposure_per_instrument=_read_float(cfg_for_optional, "RiskMaxExposurePerInstrument", 1_000_000.0),
        max_trades_in_window=_read_int(cfg_for_optional, "RiskMaxTradesInWindow", 200),
        trades_window_seconds=_read_int(cfg_for_optional, "RiskTradesWindowSeconds", 60),
        cooldown_after_consecutive_losses=_read_int(cfg_for_optional, "RiskCooldownAfterConsecutiveLosses", 3),
        cooldown_seconds=_read_int(cfg_for_optional, "RiskCooldownSeconds", 30),
        max_abs_position_per_symbol=_read_float(cfg_for_optional, "RiskMaxAbsPositionPerSymbol", 0.0),
        max_daily_loss_abs=_read_float(cfg_for_optional, "RiskMaxDailyLossAbs", 0.0),
        kill_switch_drawdown_abs=_read_float(cfg_for_optional, "RiskKillSwitchDrawdownAbs", 0.0),
        logger=logger,
    )

    failure_handling_enabled = _read_bool(cfg_for_optional, "FailureHandlingEnabled", True)
    max_inventory_per_symbol = _read_float(cfg_for_optional, "MaxInventoryPerSymbol", 10.0)
    default_soft_inventory = max_inventory_per_symbol * 0.8 if max_inventory_per_symbol > 0 else 0.0
    position_manager = PositionManager(
        order_manager=order_manager,
        max_abs_inventory_per_symbol=max_inventory_per_symbol,
        soft_abs_inventory_per_symbol=_read_float(cfg_for_optional, "MaxInventorySoftPerSymbol", default_soft_inventory),
    )

    failure_monitor: FailureMonitor | None = None
    market_maker: BasicMarketMaker | None = None

    def on_execution_report(message: object, source: str) -> None:
        if failure_monitor is not None:
            failure_monitor.on_execution_report()
        state = order_manager.on_execution_report(message)
        if market_maker is not None:
            market_maker.on_execution_report(state)
        _ = source  # reserved for future per-source handling

    equities_engine = NoopExecutionEngine()
    forts_engine = NoopExecutionEngine()

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
        simulation_slippage_bps=_read_float(cfg_for_optional, "PaperSlippageBps", 2.0),
        simulation_slippage_max_bps=_read_float(cfg_for_optional, "PaperSlippageMaxBps", 25.0),
        simulation_volatility_slippage_multiplier=_read_float(cfg_for_optional, "PaperVolatilitySlippageMultiplier", 1.5),
        simulation_latency_network_ms=_read_int(cfg_for_optional, "PaperNetworkLatencyMs", 20),
        simulation_latency_exchange_ms=_read_int(cfg_for_optional, "PaperExchangeLatencyMs", 30),
        simulation_latency_jitter_ms=_read_int(cfg_for_optional, "PaperLatencyJitterMs", 40),
        simulation_fill_participation=_read_float(cfg_for_optional, "PaperFillParticipation", 0.25),
        simulation_touch_fill_probability=_read_float(cfg_for_optional, "PaperTouchFillProbability", 0.15),
        simulation_passive_fill_probability_scale=_read_float(cfg_for_optional, "PaperPassiveFillProbabilityScale", 0.5),
        simulation_adverse_selection_bias=_read_float(cfg_for_optional, "PaperAdverseSelectionBias", 0.35),
        simulation_slippage_model=(_read_default_optional_setting(cfg_for_optional, "PaperSlippageModel") or "DYNAMIC_BPS").upper(),
        simulation_fixed_slippage_abs=_read_float(cfg_for_optional, "PaperFixedSlippageAbs", 0.0),
        simulation_spread_slippage_fraction=_read_float(cfg_for_optional, "PaperSpreadSlippageFraction", 0.25),
        simulation_decision_to_send_min_ms=_read_int(cfg_for_optional, "DecisionToSendLatencyMinMs", 5),
        simulation_decision_to_send_max_ms=_read_int(cfg_for_optional, "DecisionToSendLatencyMaxMs", 20),
        simulation_send_to_fill_min_ms=_read_int(cfg_for_optional, "SendToFillLatencyMinMs", 50),
        simulation_send_to_fill_max_ms=_read_int(cfg_for_optional, "SendToFillLatencyMaxMs", 200),
        account_by_market={
            MarketType.EQUITIES: _read_str(cfg_for_optional, "TradingAccountEquities", ""),
            MarketType.FORTS: _read_str(cfg_for_optional, "TradingAccountForts", ""),
        },
        execution_mode=execution_mode,
        stream_book_fills=_read_bool(cfg_for_optional, "PaperStreamBookFills", False),
    )

    if quote_store is not None:
        market_data_engine.subscribe(quote_store.on_market_data)
    market_data_engine.subscribe(gateway.on_market_data)
    market_data_engine.subscribe(
        lambda data: economics.on_market_data(market=MarketType.EQUITIES, symbol=data.symbol, mid_price=_d(data.mid_price))
    )
    market_data_engine.subscribe(adverse_tracker.on_market_data)

    # Momentum-only strategy is always enabled.
    mm_symbol = _read_str(cfg_for_optional, "TBankSymbol", "SBER").upper()
    lot_value = _read_default_optional_setting(cfg_for_optional, "lot_size")
    mm_lot = float(lot_value) if lot_value else _read_float(cfg_for_optional, "MarketMakingLotSize", 1.0)

    mm_market_raw = (_read_default_optional_setting(cfg_for_optional, "market") or _read_str(cfg_for_optional, "MarketMakingMarket", "EQUITIES")).upper()
    mm_market = MarketType(mm_market_raw) if mm_market_raw in {"EQUITIES", "FORTS"} else MarketType.EQUITIES
    tick_value = _read_default_optional_setting(cfg_for_optional, "tick_size")
    market_maker = BasicMarketMaker(
        symbol=mm_symbol,
        lot_size=mm_lot,
        market=mm_market,
        gateway=gateway,
        position_manager=position_manager,
        logger=logger,
        tick_size=float(tick_value) if tick_value else _read_float(cfg_for_optional, "MMTickSize", 0.01),
        strategy_mode=_read_str(cfg_for_optional, "strategy_mode", "SAFE").upper(),
        spread_threshold=_read_float(cfg_for_optional, "spread_threshold", 1.0),  # back-compat
        max_spread=_read_float(cfg_for_optional, "max_spread", 5.0),
        dynamic_spread_enabled=_read_bool(cfg_for_optional, "dynamic_spread_enabled", True),
        dynamic_spread_window_ticks=_read_int(cfg_for_optional, "dynamic_spread_window_ticks", 200),
        dynamic_spread_mult=_read_float(cfg_for_optional, "dynamic_spread_mult", 1.5),
        move_threshold=_read_float(cfg_for_optional, "move_threshold", 0.0),  # legacy/ignored
        velocity_threshold=_read_float(cfg_for_optional, "velocity_threshold", 0.0),  # legacy/ignored
        delta_threshold=_read_float(cfg_for_optional, "delta_threshold", 1.0),
        normalized_delta_threshold=_read_float(cfg_for_optional, "normalized_delta_threshold", 0.0),  # legacy/ignored
        score_threshold=_read_float(cfg_for_optional, "score_threshold", 0.72),
        w_spread=_read_float(cfg_for_optional, "w_spread", 0.30),
        w_delta=_read_float(cfg_for_optional, "w_delta", 0.70),
        penalty_wide_spread=_read_float(cfg_for_optional, "penalty_wide_spread", 0.25),
        penalty_low_delta=_read_float(cfg_for_optional, "penalty_low_delta", 0.25),
        position_sizing_enabled=_read_bool(cfg_for_optional, "PositionSizingEnabled", True),
        sizing_risk_per_trade_abs=_read_float(cfg_for_optional, "SizingRiskPerTradeAbs", 8.0),
        sizing_stop_loss_ticks=_read_float(cfg_for_optional, "SizingStopLossTicks", 4.0),
        sizing_min_qty=_read_float(cfg_for_optional, "SizingMinQty", 1.0),
        sizing_max_qty=_read_float(cfg_for_optional, "SizingMaxQty", mm_lot),
        risk_manager=risk_manager,
        no_trade_fallback_minutes=_read_float(cfg_for_optional, "no_trade_fallback_minutes", 3.0),
        fallback_relax_step=_read_float(cfg_for_optional, "fallback_relax_step", 0.85),
        fallback_min_delta=_read_float(cfg_for_optional, "fallback_min_delta", 0.0),
        fallback_min_score=_read_float(cfg_for_optional, "fallback_min_score", 0.10),
        fallback_max_spread=_read_float(cfg_for_optional, "fallback_max_spread", 8.0),
        metrics_log_every=_read_int(cfg_for_optional, "metrics_log_every", 200),
        cooldown_ms=_read_int(cfg_for_optional, "cooldown_ms", 1200),
        # Exit behavior: prefer TP/SL + trailing; score-exit disabled by default
        exit_on_score_enabled=_read_bool(cfg_for_optional, "exit_on_score_enabled", False),
        exit_threshold=_read_float(cfg_for_optional, "exit_threshold", 0.0),
        max_holding_sec=_read_float(cfg_for_optional, "max_holding_sec", 0.0),
        max_exit_spread=_read_float(cfg_for_optional, "max_exit_spread", 0.0) or None,
        force_time_exit_enabled=_read_bool(cfg_for_optional, "force_time_exit_enabled", False),
        force_time_exit_after_sec=_read_float(cfg_for_optional, "force_time_exit_after_sec", 0.0),
        # Entry quality filters
        min_entry_spread=_read_float(cfg_for_optional, "min_entry_spread", 1.0),
        max_entry_spread=_read_float(cfg_for_optional, "max_entry_spread", 3.0),
        entry_imbalance_threshold=_read_float(cfg_for_optional, "imbalance_threshold", 0.2),
        entry_imbalance_threshold_long=_read_float(
            cfg_for_optional, "imbalance_threshold_long", _read_float(cfg_for_optional, "imbalance_threshold", 0.2)
        ),
        entry_imbalance_threshold_short=_read_float(
            cfg_for_optional, "imbalance_threshold_short", _read_float(cfg_for_optional, "imbalance_threshold", 0.2)
        ),
        min_abs_imbalance=_read_float(cfg_for_optional, "min_abs_imbalance", 0.1),
        trend_lookback_ticks=_read_int(cfg_for_optional, "trend_lookback_ticks", 8),
        min_trend_move_ticks=_read_float(cfg_for_optional, "min_trend_move_ticks", 1.0),
        # TP/SL/trailing
        take_profit_ticks=_read_float(cfg_for_optional, "take_profit_ticks", 2.0),
        stop_loss_ticks=_read_float(cfg_for_optional, "stop_loss_ticks", 1.0),
        trailing_start_ticks=_read_float(cfg_for_optional, "trailing_start_ticks", 2.0),
        trailing_gap_ticks=_read_float(cfg_for_optional, "trailing_gap_ticks", 1.0),
        exit_imbalance_reversal=_read_float(cfg_for_optional, "exit_imbalance_reversal", 0.2),
        one_position_only=_read_bool(cfg_for_optional, "one_position_only", _read_bool(cfg_for_optional, "MMOnePositionOnly", True)),
        entry_decision_sink=economics_store.insert_entry_decisions,
        economics_store=economics_store,
    )
    market_data_engine.subscribe(market_maker.on_market_data)

    if data_provider in {"TINKOFF", "TINKOFF_SANDBOX"}:
        from fix_engine.market_data.tbank_paper_session import run_tbank_paper_session

        run_tbank_paper_session(
            base_dir=base_dir,
            cfg_for_optional=cfg_for_optional,
            execution_mode=execution_mode,
            gateway=gateway,
            market_data_engine=market_data_engine,
            logger=logger,
            failure_monitor=failure_monitor,
            logging_runtime=logging_runtime,
            read_str=_read_str,
            read_int=_read_int,
            read_float=_read_float,
            read_bool=_read_bool,
        )
        return

    raise RuntimeError(f"Unsupported DataProvider={data_provider!r}. Use TINKOFF (or legacy TINKOFF_SANDBOX).")

