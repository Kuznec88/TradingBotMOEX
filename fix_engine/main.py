from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock

from analytics_api import TradingAnalyticsAPI
from economics_store import EconomicsStore
from execution_gateway import ExecutionGateway
from failure_monitor import FailureMonitor
from market_making import BasicMarketMaker
from market_data.market_data_engine import MarketDataEngine
from market_data.models import MarketData
from quote_history_store import QuoteHistoryStore
from md_health_monitor import MdHealthMonitor
from order_models import MarketType
from order_manager import OrderManager
from position_manager import PositionManager
from risk_manager import RiskManager
from structured_logging import StructuredLoggingRuntime, configure_structured_logging, log_event
from unit_economics import UnitEconomicsCalculator, _d


class SimpleStrategy:
    """Very small MA(5) strategy for integration demo."""

    def __init__(
        self,
        *,
        logger: logging.Logger,
        min_spread_threshold: float,
        max_volatility_threshold: float,
        volatility_window: int,
    ) -> None:
        self.logger = logger
        self.min_spread_threshold = float(min_spread_threshold)
        self.max_volatility_threshold = float(max_volatility_threshold)
        self.volatility_window = max(2, int(volatility_window))
        self.prices_by_symbol: dict[str, list[float]] = {}
        self._latest_signal_by_symbol: dict[str, str] = {}
        self._lock = RLock()

    def on_market_data(self, data: MarketData) -> None:
        with self._lock:
            prices = self.prices_by_symbol.setdefault(data.symbol, [])
            prices.append(data.mid_price)
            if len(prices) <= 5:
                self._latest_signal_by_symbol[data.symbol] = "HOLD"
                return

            if data.spread < self.min_spread_threshold:
                self._latest_signal_by_symbol[data.symbol] = "HOLD"
                log_event(
                    self.logger,
                    level=logging.INFO,
                    component="Strategy",
                    event="trade_skipped",
                    symbol=data.symbol,
                    reason="spread_too_low",
                    bid=float(data.bid),
                    ask=float(data.ask),
                    spread=float(data.spread),
                    threshold=float(self.min_spread_threshold),
                )
                return

            volatility = self._estimate_volatility(prices)
            if volatility > self.max_volatility_threshold:
                self._latest_signal_by_symbol[data.symbol] = "HOLD"
                log_event(
                    self.logger,
                    level=logging.INFO,
                    component="Strategy",
                    event="trade_skipped",
                    symbol=data.symbol,
                    reason="volatility_too_high",
                    bid=float(data.bid),
                    ask=float(data.ask),
                    spread=float(data.spread),
                    volatility=float(volatility),
                    threshold=float(self.max_volatility_threshold),
                )
                return

            avg = sum(prices[-5:]) / 5
            # Use both mid_price and spread for a tiny quality filter.
            if data.mid_price > avg and data.spread >= 0:
                self._latest_signal_by_symbol[data.symbol] = "BUY"
            elif data.mid_price < avg and data.spread >= 0:
                self._latest_signal_by_symbol[data.symbol] = "SELL"
            else:
                self._latest_signal_by_symbol[data.symbol] = "HOLD"
            if self._latest_signal_by_symbol[data.symbol] in {"BUY", "SELL"}:
                log_event(
                    self.logger,
                    level=logging.INFO,
                    component="Strategy",
                    event="signal_generated",
                    symbol=data.symbol,
                    signal=self._latest_signal_by_symbol[data.symbol],
                    bid=float(data.bid),
                    ask=float(data.ask),
                    spread=float(data.spread),
                    mid_price=float(data.mid_price),
                )
                log_event(
                    self.logger,
                    level=logging.INFO,
                    component="Strategy",
                    event="ENTRY_SIGNAL",
                    symbol=data.symbol,
                    signal=self._latest_signal_by_symbol[data.symbol],
                    bid=float(data.bid),
                    ask=float(data.ask),
                    spread=float(data.spread),
                    mid_price=float(data.mid_price),
                )

    def pop_signal(self, symbol: str) -> str:
        with self._lock:
            return self._latest_signal_by_symbol.pop(symbol.upper(), "HOLD")

    def _estimate_volatility(self, prices: list[float]) -> float:
        window_prices = prices[-self.volatility_window :]
        if len(window_prices) < 2:
            return 0.0
        returns: list[float] = []
        for i in range(1, len(window_prices)):
            prev_px = window_prices[i - 1]
            cur_px = window_prices[i]
            if prev_px <= 0:
                continue
            returns.append((cur_px - prev_px) / prev_px)
        if len(returns) < 2:
            return 0.0
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / len(returns)
        return math.sqrt(variance)


class FillAdverseSelectionTracker:
    def __init__(self, store: EconomicsStore) -> None:
        self._store = store
        self._lock = RLock()
        self._pending: dict[str, dict[str, object]] = {}
        self._horizons = {
            "px_10ms": timedelta(milliseconds=10),
            "px_100ms": timedelta(milliseconds=100),
            "px_500ms": timedelta(milliseconds=500),
            "px_1s": timedelta(seconds=1),
        }

    def register_fill(
        self,
        *,
        trade_id: str,
        side: str,
        qty: float,
        fill_price: float,
        symbol: str,
        fill_ts: datetime,
    ) -> None:
        with self._lock:
            self._pending[trade_id] = {
                "trade_id": trade_id,
                "side": side,
                "qty": float(qty),
                "fill_price": float(fill_price),
                "symbol": symbol.upper(),
                "fill_ts": fill_ts,
                "px_10ms": None,
                "px_100ms": None,
                "px_500ms": None,
                "px_1s": None,
            }

    def on_market_data(self, data: MarketData) -> None:
        now_ts = data.timestamp if data.timestamp.tzinfo else data.timestamp.replace(tzinfo=timezone.utc)
        finished: list[str] = []
        with self._lock:
            for trade_id, row in self._pending.items():
                if row["symbol"] != data.symbol.upper():
                    continue
                fill_ts = row["fill_ts"]
                for key, horizon in self._horizons.items():
                    if row[key] is not None:
                        continue
                    if now_ts >= fill_ts + horizon:
                        row[key] = float(data.mid_price)
                if (
                    row["px_10ms"] is not None
                    and row["px_100ms"] is not None
                    and row["px_500ms"] is not None
                    and row["px_1s"] is not None
                ):
                    adverse_pnl, adverse_fill = self._compute_adverse(row)
                    self._store.update_adverse_selection(
                        trade_id=trade_id,
                        px_10ms=float(row["px_10ms"]),
                        px_100ms=float(row["px_100ms"]),
                        px_500ms=float(row["px_500ms"]),
                        px_1s=float(row["px_1s"]),
                        adverse_pnl=adverse_pnl,
                        adverse_fill=adverse_fill,
                    )
                    finished.append(trade_id)
            for trade_id in finished:
                self._pending.pop(trade_id, None)

    @staticmethod
    def _compute_adverse(row: dict[str, object]) -> tuple[float, bool]:
        side = str(row["side"])
        qty = float(row["qty"])
        fill = float(row["fill_price"])
        px_1s = float(row["px_1s"])
        if side == "1":
            adverse_pnl = (px_1s - fill) * qty
        else:
            adverse_pnl = (fill - px_1s) * qty
        return adverse_pnl, adverse_pnl < 0.0


def _format_fix_for_log(raw_fix: str) -> str:
    # SOH delimiter -> visible pipe for human-readable troubleshooting.
    return raw_fix.replace("\x01", "|")


def setup_logging(base_dir: Path, *, paper_execution: bool) -> StructuredLoggingRuntime:
    return configure_structured_logging(base_dir=base_dir, paper_execution=paper_execution, logger_name="fix_engine")


def _read_execution_mode(cfg_path: Path) -> str:
    """LIVE = real routing (disabled in this build); PAPER_REAL_MARKET = real MD + local fills (synthetic or stream book)."""
    value = (_read_default_optional_setting(cfg_path, "ExecutionMode") or "").strip().upper()
    if value == "SIMULATION":
        return "PAPER_REAL_MARKET"
    if value in {"LIVE", "PAPER_REAL_MARKET"}:
        return value
    return "PAPER_REAL_MARKET"


def _read_default_optional_setting(cfg_path: Path, key: str) -> str | None:
    for raw_line in cfg_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("[") and line.endswith("]"):
            if line[1:-1].strip().upper() == "SESSION":
                break
            continue
        k, v = line.split("=", 1)
        if k.strip() == key:
            value = v.strip()
            return value or None
    return None


def _read_float(cfg_path: Path, key: str, default: float) -> float:
    value = _read_default_optional_setting(cfg_path, key)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _read_int(cfg_path: Path, key: str, default: int) -> int:
    value = _read_default_optional_setting(cfg_path, key)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _read_bool(cfg_path: Path, key: str, default: bool) -> bool:
    value = _read_default_optional_setting(cfg_path, key)
    if value is None:
        return default
    return value.strip().upper() in {"Y", "YES", "TRUE", "1", "ON"}


def _read_csv_list(cfg_path: Path, key: str, default: list[str]) -> list[str]:
    value = _read_default_optional_setting(cfg_path, key)
    if not value:
        return list(default)
    items = [part.strip() for part in value.split(",")]
    return [x for x in items if x]


def _read_str(cfg_path: Path, key: str, default: str) -> str:
    value = _read_default_optional_setting(cfg_path, key)
    if value is None:
        return default
    return value.strip() or default


def _source_to_market(source: str) -> MarketType:
    return MarketType.FORTS if source == "FORTS" else MarketType.EQUITIES


class _NoopExecutionEngine:
    def send_order(self, *args: object, **kwargs: object) -> str:
        raise RuntimeError("Real exchange order routing is removed. Use paper execution only.")

    def cancel_order(self, *args: object, **kwargs: object) -> str:
        raise RuntimeError("Real exchange order routing is removed. Use paper execution only.")


def run() -> None:
    base_dir = Path(__file__).resolve().parent
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
        logger.info(
            "[QUOTE_HISTORY] enabled path=%s retention_days=%s sample_ms=%s",
            quote_history_db,
            quote_history_retention,
            quote_history_sample_ms,
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
    account_check_pending: set[str] = set()
    analytics_counters: dict[str, int] = {
        "total_trades_processed_for_analytics": 0,
        "total_records_written": 0,
    }
    analytics_lock = RLock()

    def _strategy_inventory_allowed(symbol: str, side: str, qty: float) -> bool:
        inv_decision = position_manager.pre_check_order(symbol=symbol, side=side, qty=qty)
        if inv_decision.allowed:
            return True
        log_event(
            logger,
            level=logging.INFO,
            component="Strategy",
            event="trade_skipped",
            reason="inventory_control",
            symbol=symbol.upper(),
            side=side,
            quantity=qty,
            details=inv_decision.reason,
        )
        return False

    def on_execution_report(message: object, source: str) -> None:
        recv_ts = time.perf_counter_ns()
        if failure_monitor is not None:
            failure_monitor.on_execution_report()
        state = order_manager.on_execution_report(message)
        if market_maker is not None:
            market_maker.on_execution_report(state)
        cl_ord_id = state.get("cl_ord_id", "")
        order = order_manager.get_order(cl_ord_id)
        status_new = state.get("status_new", "")
        status_event = {
            "NEW": "order_created",
            "PENDING_NEW": "order_acknowledged",
            "SENT": "order_sent",
            "PARTIALLY_FILLED": "order_partially_filled",
            "FILLED": "order_filled",
            "CANCELED": "order_canceled",
            "REJECTED": "order_rejected",
        }.get(status_new, "order_state_changed")
        if order is not None:
            log_event(
                logger,
                level=logging.INFO,
                component="OrderLifecycle",
                event=status_event,
                correlation_id=cl_ord_id,
                order_id=cl_ord_id,
                symbol=order.symbol,
                side=order.side,
                price=order.price,
                quantity=order.qty,
                filled_quantity=order.filled_qty,
                remaining_quantity=order.remaining_qty,
                account=order.account,
                reason=state.get("text", ""),
                source=source,
            )
        if cl_ord_id in account_check_pending:
            account_used = order.account if order is not None else state.get("account", "")
            if status_new == "REJECTED":
                logger.error(
                    "[CHECKACC][FAIL] cl_ord_id=%s account=%s reason=%s ord_rej_reason=%s",
                    cl_ord_id,
                    account_used,
                    state.get("text", ""),
                    state.get("ord_rej_reason", ""),
                )
                account_check_pending.discard(cl_ord_id)
            elif status_new in {"PENDING_NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED"}:
                logger.info(
                    "[CHECKACC][PASS] cl_ord_id=%s account=%s status=%s",
                    cl_ord_id,
                    account_used,
                    status_new,
                )
                account_check_pending.discard(cl_ord_id)
        if state.get("status_new") == "REJECTED":
            raw_request = order_manager.get_outbound_message(state.get("cl_ord_id", ""))
            raw_response = state.get("raw_inbound_fix", "")
            logger.error(
                "[REJECT][%s] cl_ord_id=%s exec_id=%s ord_status=%s exec_type=%s ord_rej_reason=%s text=%s",
                source,
                state.get("cl_ord_id", ""),
                state.get("exec_id", ""),
                state.get("ord_status", ""),
                state.get("exec_type", ""),
                state.get("ord_rej_reason", ""),
                state.get("text", ""),
            )
            logger.error("[REJECT][%s][REQUEST_RAW] %s", source, _format_fix_for_log(raw_request))
            logger.error("[REJECT][%s][RESPONSE_RAW] %s", source, _format_fix_for_log(raw_response))

        last_qty = _d(state.get("last_qty", "0"))
        if last_qty > 0:
            if status_new not in {"PARTIALLY_FILLED", "FILLED"}:
                log_event(
                    logger,
                    level=logging.ERROR,
                    component="AnalyticsPipeline",
                    event="unexpected_fill_event",
                    correlation_id=cl_ord_id,
                    trade_id=state.get("exec_id", ""),
                    order_id=cl_ord_id,
                    symbol=state.get("symbol", ""),
                    side=state.get("side", ""),
                    status_new=status_new,
                    last_qty=float(last_qty),
                )
            raw_last_px = (state.get("last_px", "") or "").strip()
            raw_avg_px = (state.get("avg_px", "") or "").strip()
            fill_px_str = raw_last_px or raw_avg_px or "0"
            fill_px = _d(fill_px_str)
            trade_id = state.get("exec_id", "")
            symbol = state.get("symbol", "")
            md = market_data_engine.get_latest(symbol)
            bid_px = _d(md.bid) if md is not None else fill_px
            ask_px = _d(md.ask) if md is not None else fill_px
            mid_px = _d(md.mid_price) if md is not None else fill_px
            expected_px = bid_px if state.get("side", "1") == "1" else ask_px
            fill_ts = datetime.now(timezone.utc)
            time_in_book_ms = 0.0
            if order is not None and order.created_at is not None:
                try:
                    time_in_book_ms = max(0.0, (fill_ts - order.created_at).total_seconds() * 1000.0)
                except Exception:
                    time_in_book_ms = 0.0
            log_event(
                logger,
                level=logging.INFO,
                component="AnalyticsPipeline",
                event="analytics_calc_start",
                correlation_id=cl_ord_id,
                trade_id=trade_id,
                order_id=cl_ord_id,
                symbol=symbol,
                side=state.get("side", "1"),
                entry_price=float(fill_px),
                exit_price=float(fill_px),
                status_new=status_new,
            )
            log_event(
                logger,
                level=logging.INFO,
                component="Execution",
                event="execution",
                correlation_id=cl_ord_id,
                trade_id=trade_id,
                order_id=cl_ord_id,
                execution_price=float(fill_px),
                execution_qty=float(last_qty),
                side=state.get("side", "1"),
                liquidity_flag=state.get("liquidity_flag", ""),
                expected_price=float(expected_px),
                slippage=float((fill_px - expected_px) if state.get("side", "1") == "1" else (expected_px - fill_px)),
            )
            trade_record = economics.process_fill(
                market=_source_to_market(source),
                symbol=symbol,
                side=state.get("side", "1"),
                qty=last_qty,
                price=fill_px,
                expected_price=expected_px,
                bid=bid_px,
                ask=ask_px,
                mid_price=mid_px,
                fill_ts=fill_ts,
                cl_ord_id=state.get("cl_ord_id", ""),
                exec_id=state.get("exec_id", ""),
                time_in_book_ms=time_in_book_ms,
            )
            if trade_record is not None:
                with analytics_lock:
                    analytics_counters["total_trades_processed_for_analytics"] += 1
                if source == "SYNTHETIC":
                    log_event(
                        logger,
                        level=logging.INFO,
                        component="SyntheticExecution",
                        event="SYNTHETIC_EXECUTION",
                        correlation_id=cl_ord_id,
                        fill_price=float(fill_px),
                        pnl=float(trade_record.get("net_pnl", 0.0)),
                        symbol=str(trade_record.get("symbol", "")),
                        side=str(state.get("side", "")),
                    )
                log_event(
                    logger,
                    level=logging.INFO,
                    component="AnalyticsPipeline",
                    event="trade_created",
                    correlation_id=cl_ord_id,
                    trade_id=str(trade_record.get("exec_id", "")),
                    order_id=str(trade_record.get("cl_ord_id", "")),
                    symbol=str(trade_record.get("symbol", "")),
                    side=str(trade_record.get("side", "")),
                    entry_price=float(trade_record.get("price", 0.0)),
                    exit_price=float(trade_record.get("price", 0.0)),
                )
                trade_analytics_rows = trade_record.get("trade_analytics_rows", [])
                round_trip_rows = trade_record.get("round_trip_rows", [])
                log_event(
                    logger,
                    level=logging.INFO,
                    component="AnalyticsPipeline",
                    event="analytics_db_write_start",
                    correlation_id=cl_ord_id,
                    trade_id=str(trade_record.get("exec_id", "")),
                    order_id=str(trade_record.get("cl_ord_id", "")),
                    symbol=str(trade_record.get("symbol", "")),
                    side=str(trade_record.get("side", "")),
                    trade_analytics_rows=len(trade_analytics_rows),
                    round_trip_rows=len(round_trip_rows),
                )
                economics_store.insert_trade(trade_record)
                economics_store.insert_trade_analytics(trade_analytics_rows)
                economics_store.insert_round_trips(round_trip_rows)
                written_records = 1 + len(trade_analytics_rows) + len(round_trip_rows)
                with analytics_lock:
                    analytics_counters["total_records_written"] += written_records
                write_check = economics_store.analytics_presence_for_trade(str(trade_record.get("exec_id", "")))
                expected_trade_rows = max(1, len(trade_analytics_rows))
                expected_rt_rows = len(round_trip_rows)
                if (
                    write_check["trade_analytics_rows"] < expected_trade_rows
                    or write_check["round_trip_rows"] < expected_rt_rows
                ):
                    log_event(
                        logger,
                        level=logging.ERROR,
                        component="AnalyticsPipeline",
                        event="analytics_missing_after_trade",
                        correlation_id=cl_ord_id,
                        trade_id=str(trade_record.get("exec_id", "")),
                        order_id=str(trade_record.get("cl_ord_id", "")),
                        symbol=str(trade_record.get("symbol", "")),
                        side=str(trade_record.get("side", "")),
                        entry_price=float(trade_record.get("price", 0.0)),
                        exit_price=float(trade_record.get("price", 0.0)),
                        expected_trade_rows=expected_trade_rows,
                        expected_round_trip_rows=expected_rt_rows,
                        write_check=write_check,
                    )
                log_event(
                    logger,
                    level=logging.INFO,
                    component="AnalyticsPipeline",
                    event="analytics_db_write_done",
                    correlation_id=cl_ord_id,
                    trade_id=str(trade_record.get("exec_id", "")),
                    order_id=str(trade_record.get("cl_ord_id", "")),
                    symbol=str(trade_record.get("symbol", "")),
                    side=str(trade_record.get("side", "")),
                    write_check=write_check,
                    fill_role=str(trade_record.get("fill_role", "")),
                    closed_qty=float(_d(str(trade_record.get("closed_qty", "0")))),
                    opened_qty=float(_d(str(trade_record.get("opened_qty", "0")))),
                    db_insert_succeeded=(
                        write_check["trade_analytics_rows"] >= expected_trade_rows
                        and write_check["round_trip_rows"] >= expected_rt_rows
                    ),
                    total_trades_processed_for_analytics=analytics_counters["total_trades_processed_for_analytics"],
                    total_records_written=analytics_counters["total_records_written"],
                )
                log_event(
                    logger,
                    level=logging.INFO,
                    component="AnalyticsPipeline",
                    event="analytics_calc_done",
                    correlation_id=cl_ord_id,
                    trade_id=str(trade_record.get("exec_id", "")),
                    order_id=str(trade_record.get("cl_ord_id", "")),
                    symbol=str(trade_record.get("symbol", "")),
                    side=str(trade_record.get("side", "")),
                    fill_role=str(trade_record.get("fill_role", "")),
                    closed_qty=float(_d(str(trade_record.get("closed_qty", "0")))),
                    opened_qty=float(_d(str(trade_record.get("opened_qty", "0")))),
                    trade_analytics_rows=len(trade_analytics_rows),
                    round_trip_rows=len(round_trip_rows),
                )
                for row in round_trip_rows:
                    immediate_move_10ms = economics_store.entry_move_10ms(
                        trade_id=str(row.get("entry_trade_id", "")),
                        side=str(row.get("side", "")),
                        entry_price=float(row.get("entry_price", 0.0)),
                    )
                    immediate_move = (
                        immediate_move_10ms if abs(immediate_move_10ms) > 1e-12 else float(row.get("immediate_move", 0.0))
                    )
                    log_event(
                        logger,
                        level=logging.INFO,
                        component="TradeAnalysis",
                        event="ROUND_TRIP",
                        correlation_id=str(row.get("entry_trade_id", "")),
                        pnl=float(row.get("total_pnl", 0.0)),
                        mfe=float(row.get("mfe", 0.0)),
                        mae=float(row.get("mae", 0.0)),
                        adverse_flag=bool(immediate_move < 0.0),
                        symbol=str(row.get("symbol", "")),
                        side=str(row.get("side", "")),
                    )
                    log_event(
                        logger,
                        level=logging.INFO,
                        component="TradeAnalysis",
                        event="trade_outcome",
                        correlation_id=str(row.get("entry_trade_id", "")),
                        symbol=str(row.get("symbol", "")),
                        side=str(row.get("side", "")),
                        pnl=float(row.get("total_pnl", 0.0)),
                        mae=float(row.get("mae", 0.0)),
                        mfe=float(row.get("mfe", 0.0)),
                        duration_ms=float(row.get("duration_ms", 0.0)),
                        immediate_move=float(immediate_move),
                        immediate_move_10ms=float(immediate_move_10ms),
                    )
                if market_maker is not None and round_trip_rows:
                    try:
                        market_maker.on_round_trip_outcomes(round_trip_rows)
                    except Exception as exc:
                        log_event(
                            logger,
                            level=logging.ERROR,
                            component="Strategy",
                            event="adaptive_learning_update_failed",
                            correlation_id=cl_ord_id,
                            error=str(exc),
                        )
                risk_manager.on_trade_result(float(trade_record["net_pnl"]))
                metrics = analytics_api.get_metrics()
                adverse_tracker.register_fill(
                    trade_id=trade_id,
                    side=state.get("side", "1"),
                    qty=float(last_qty),
                    fill_price=float(fill_px),
                    symbol=symbol,
                    fill_ts=fill_ts,
                )
                log_event(
                    logger,
                    level=logging.INFO,
                    component="PnL",
                    event="pnl_snapshot",
                    correlation_id=cl_ord_id,
                    market=trade_record["market"],
                    symbol=trade_record["symbol"],
                    gross_pnl=float(trade_record["gross_pnl"]),
                    fees=float(trade_record["fees"]),
                    net_pnl=float(trade_record["net_pnl"]),
                    spread_pnl=float(trade_record.get("spread_pnl", "0")),
                    slippage_pnl=float(trade_record.get("slippage", "0")),
                    adverse_pnl=float(trade_record.get("adverse_pnl", "0")),
                    holding_pnl=float(trade_record.get("holding_pnl", "0")),
                    cumulative_pnl=float(metrics["cumulative_pnl"]),
                    high_slippage=abs(float(trade_record.get("slippage", "0"))) > 0.05,
                    adverse_fill=float(trade_record.get("adverse_pnl", "0")) < 0.0,
                    large_loss=float(trade_record["net_pnl"]) < -0.5,
                )
                current_pos = order_manager.get_position(state.get("symbol", ""))
                last_md = market_data_engine.get_latest(state.get("symbol", ""))
                unrealized = 0.0
                if order is not None and last_md is not None and order.filled_qty > 0:
                    mark = float(last_md.mid_price)
                    if order.side == "1":
                        unrealized = (mark - float(fill_px)) * current_pos
                    else:
                        unrealized = (float(fill_px) - mark) * abs(current_pos)
                log_event(
                    logger,
                    level=logging.INFO,
                    component="Position",
                    event="position_update",
                    correlation_id=cl_ord_id,
                    symbol=state.get("symbol", ""),
                    position_size=float(current_pos),
                    avg_price=float(fill_px),
                    realized_pnl=float(metrics["total_pnl"]),
                    unrealized_pnl=float(unrealized),
                )
        if order is not None and order.created_at is not None and last_qty > 0:
            latency_ms = (time.perf_counter_ns() - recv_ts) / 1_000_000.0
            log_event(
                logger,
                level=logging.INFO,
                component="Performance",
                event="order_to_fill_latency",
                correlation_id=cl_ord_id,
                order_id=cl_ord_id,
                latency_ms=round(latency_ms, 3),
            )

    equities_engine = _NoopExecutionEngine()
    forts_engine = _NoopExecutionEngine()
    sim_slippage_bps = _read_float(cfg_for_optional, "PaperSlippageBps", 2.0)
    sim_slippage_max_bps = _read_float(cfg_for_optional, "PaperSlippageMaxBps", 25.0)
    sim_vol_slippage_multiplier = _read_float(cfg_for_optional, "PaperVolatilitySlippageMultiplier", 1.5)
    sim_latency_network_ms = _read_int(cfg_for_optional, "PaperNetworkLatencyMs", 20)
    sim_latency_exchange_ms = _read_int(cfg_for_optional, "PaperExchangeLatencyMs", 30)
    sim_latency_jitter_ms = _read_int(cfg_for_optional, "PaperLatencyJitterMs", 40)
    sim_fill_participation = _read_float(cfg_for_optional, "PaperFillParticipation", 0.25)
    sim_touch_fill_probability = _read_float(cfg_for_optional, "PaperTouchFillProbability", 0.15)
    sim_passive_fill_scale = _read_float(cfg_for_optional, "PaperPassiveFillProbabilityScale", 0.5)
    sim_adverse_selection_bias = _read_float(cfg_for_optional, "PaperAdverseSelectionBias", 0.35)
    paper_slippage_model = (_read_default_optional_setting(cfg_for_optional, "PaperSlippageModel") or "DYNAMIC_BPS").upper()
    paper_fixed_slippage_abs = _read_float(cfg_for_optional, "PaperFixedSlippageAbs", 0.0)
    paper_spread_slippage_fraction = _read_float(cfg_for_optional, "PaperSpreadSlippageFraction", 0.25)
    decision_to_send_min_ms = _read_int(cfg_for_optional, "DecisionToSendLatencyMinMs", 5)
    decision_to_send_max_ms = _read_int(cfg_for_optional, "DecisionToSendLatencyMaxMs", 20)
    send_to_fill_min_ms = _read_int(cfg_for_optional, "SendToFillLatencyMinMs", 50)
    send_to_fill_max_ms = _read_int(cfg_for_optional, "SendToFillLatencyMaxMs", 200)
    paper_stream_book_fills = _read_bool(cfg_for_optional, "PaperStreamBookFills", False)
    max_loss_per_trade = _read_float(cfg_for_optional, "RiskMaxLossPerTrade", 0.0)
    virtual_account_enabled = _read_bool(cfg_for_optional, "VirtualAccountEnabled", False)
    virtual_account_start_balance = _read_float(cfg_for_optional, "VirtualAccountStartBalance", 0.0)
    virtual_account_max_loss_fraction = _read_float(cfg_for_optional, "VirtualAccountMaxLossFraction", 0.0)
    entry_forecast_profit_enabled = _read_bool(cfg_for_optional, "MMEntryForecastProfitEnabled", False)
    entry_forecast_alignment_min = _read_float(cfg_for_optional, "MMEntryForecastAlignmentMin", 0.0)
    if (
        virtual_account_enabled
        and virtual_account_start_balance > 0.0
        and virtual_account_max_loss_fraction > 0.0
    ):
        # Derive stop-loss (PnL units) from virtual account constraints.
        max_loss_per_trade = float(virtual_account_start_balance) * float(virtual_account_max_loss_fraction)
    mm_volatility_window_ticks = _read_int(cfg_for_optional, "MMVolatilityWindowTicks", 20)
    mm_max_short_term_volatility = _read_float(cfg_for_optional, "MMMaxShortTermVolatility", 0.0)
    mm_cancel_on_high_volatility = _read_bool(cfg_for_optional, "MMCancelOnHighVolatility", True)
    mm_resting_order_timeout_sec = _read_float(cfg_for_optional, "MMRestingOrderTimeoutSec", 2.0)
    mm_tick_size = _read_float(cfg_for_optional, "MMTickSize", 0.01)
    mm_replace_threshold_ticks = _read_int(cfg_for_optional, "MMReplaceThresholdTicks", 2)
    mm_replace_cancel_threshold_ticks = _read_float(
        cfg_for_optional, "MMReplaceCancelThresholdTicks", float(mm_replace_threshold_ticks)
    )
    mm_replace_keep_threshold_ticks = _read_float(
        cfg_for_optional, "MMReplaceKeepThresholdTicks", max(0.0, float(mm_replace_threshold_ticks) * 0.5)
    )
    mm_replace_persist_ms = _read_int(cfg_for_optional, "MMReplacePersistMs", 220)
    mm_adverse_move_cancel_ticks = _read_int(cfg_for_optional, "MMAdverseMoveCancelTicks", 3)
    mm_fast_cancel_keep_ticks = _read_float(cfg_for_optional, "MMFastCancelKeepTicks", 1.5)
    mm_fast_cancel_persist_ms = _read_int(cfg_for_optional, "MMFastCancelPersistMs", 180)
    mm_price_tolerance_ticks = _read_float(cfg_for_optional, "MMPriceToleranceTicks", 0.0)
    mm_price_tolerance_pct = _read_float(cfg_for_optional, "MMPriceTolerancePct", 0.0)
    mm_min_order_lifetime_ms = _read_int(cfg_for_optional, "MMMinOrderLifetimeMs", 200)
    mm_cancel_replace_cooldown_ms = _read_int(cfg_for_optional, "MMCancelReplaceCooldownMs", 120)
    mm_entry_min_spread = _read_float(cfg_for_optional, "MMEntryMinSpread", 0.0)
    mm_entry_stability_window_ticks = _read_int(cfg_for_optional, "MMEntryStabilityWindowTicks", 8)
    mm_entry_max_bid_ask_move_ticks = _read_float(cfg_for_optional, "MMEntryMaxBidAskMoveTicks", 2.0)
    mm_entry_anti_trend_threshold = _read_float(cfg_for_optional, "MMEntryAntiTrendThreshold", 0.0)
    mm_entry_direction_window_ticks = _read_int(cfg_for_optional, "MMEntryDirectionWindowTicks", 12)
    mm_entry_direction_min_move_ticks = _read_float(cfg_for_optional, "MMEntryDirectionMinMoveTicks", 1.0)
    mm_trade_cooldown_ms = _read_int(cfg_for_optional, "MMTradeCooldownMs", 300)
    mm_entry_min_place_interval_ms = _read_int(cfg_for_optional, "MMEntryMinPlaceIntervalMs", 200)
    mm_one_position_only = _read_bool(cfg_for_optional, "MMOnePositionOnly", True)
    mm_reversal_enabled = _read_bool(cfg_for_optional, "MMReversalEnabled", True)
    mm_reversal_confirmation_updates = _read_int(cfg_for_optional, "MMReversalConfirmationUpdates", 3)
    mm_reversal_min_trend_strength = _read_float(cfg_for_optional, "MMReversalMinTrendStrength", 0.0008)
    mm_reversal_min_hold_ms = _read_int(cfg_for_optional, "MMReversalMinHoldMs", 120000)
    mm_reversal_cooldown_ms = _read_int(cfg_for_optional, "MMReversalCooldownMs", 120000)
    mm_weekly_trend_enabled = _read_bool(cfg_for_optional, "MMWeeklyTrendEnabled", True)
    mm_weekly_trend_days = _read_float(cfg_for_optional, "MMWeeklyTrendDays", 7.0)
    mm_weekly_trend_threshold_pct = _read_float(cfg_for_optional, "MMWeeklyTrendThresholdPct", 0.5)
    mm_weekly_trend_refresh_sec = _read_float(cfg_for_optional, "MMWeeklyTrendRefreshSec", 30.0)
    mm_volume_move_corr_enabled = _read_bool(cfg_for_optional, "MMVolumeMoveCorrEnabled", False)
    mm_volume_move_corr_lookback_days = _read_float(cfg_for_optional, "MMVolumeMoveCorrLookbackDays", 14.0)
    mm_volume_move_corr_bar_minutes = _read_int(cfg_for_optional, "MMVolumeMoveCorrBarMinutes", 5)
    mm_volume_move_corr_min_samples = _read_int(cfg_for_optional, "MMVolumeMoveCorrMinSamples", 500)
    mm_volume_move_corr_threshold = _read_float(cfg_for_optional, "MMVolumeMoveCorrThreshold", 0.15)
    mm_volume_move_corr_refresh_sec = _read_float(cfg_for_optional, "MMVolumeMoveCorrRefreshSec", 60.0)
    mm_five_min_entry_gate_enabled = _read_bool(cfg_for_optional, "MMFiveMinEntryGateEnabled", True)
    mm_five_min_impulse_min_ticks = _read_float(cfg_for_optional, "MMFiveMinImpulseMinTicks", 0.5)
    mm_five_min_volume_ratio_min = _read_float(cfg_for_optional, "MMFiveMinVolumeRatioMin", 1.05)
    mm_trend_continuation_enabled = _read_bool(cfg_for_optional, "MMTrendContinuationEnabled", True)
    mm_trend_continuation_confirm_updates = _read_int(cfg_for_optional, "MMTrendContinuationConfirmUpdates", 3)
    mm_trend_continuation_min_move_ticks = _read_float(cfg_for_optional, "MMTrendContinuationMinMoveTicks", 1.0)
    mm_flow_bias_window_ticks = _read_int(cfg_for_optional, "MMFlowBiasWindowTicks", 12)
    mm_long_flow_bias_min = _read_float(cfg_for_optional, "MMLongFlowBiasMin", 0.05)
    mm_long_trend_bias_min_ticks = _read_float(cfg_for_optional, "MMLongTrendBiasMinTicks", 0.75)
    mm_long_impulse_override_ticks = _read_float(cfg_for_optional, "MMLongImpulseOverrideTicks", 3.0)
    mm_short_flow_bias_max = _read_float(cfg_for_optional, "MMShortFlowBiasMax", 0.20)
    mm_short_trend_bias_min_ticks = _read_float(cfg_for_optional, "MMShortTrendBiasMinTicks", 0.50)
    mm_volume_flow_override_ticks = _read_float(cfg_for_optional, "MMVolumeFlowOverrideTicks", 1.50)
    mm_volume_spike_entry_enabled = _read_bool(cfg_for_optional, "MMVolumeSpikeEntryEnabled", True)
    mm_volume_spike_window_ticks = _read_int(cfg_for_optional, "MMVolumeSpikeWindowTicks", 24)
    mm_volume_spike_multiplier = _read_float(cfg_for_optional, "MMVolumeSpikeMultiplier", 2.0)
    mm_volume_spike_flow_bias_abs_min = _read_float(cfg_for_optional, "MMVolumeSpikeFlowBiasAbsMin", 0.20)
    mm_momentum_exit_enabled = _read_bool(cfg_for_optional, "MMMomentumExitEnabled", True)
    mm_ignore_duplicate_ticks_ms = _read_int(cfg_for_optional, "MMIgnoreDuplicateTicksMs", 120)
    mm_decision_min_mid_move_ticks = _read_float(cfg_for_optional, "MMDecisionMinMidMoveTicks", 0.5)
    mm_tray_price_every_sec = _read_float(cfg_for_optional, "MMTrayPriceEverySec", 3.0)
    mm_take_profit_per_trade = _read_float(cfg_for_optional, "MMTakeProfitPerTrade", 4.0)
    mm_dynamic_profit_target_enabled = _read_bool(cfg_for_optional, "MMDynamicProfitTargetEnabled", True)
    mm_dynamic_profit_target_vol_multiplier = _read_float(
        cfg_for_optional, "MMDynamicProfitTargetVolMultiplier", 1.8
    )
    mm_dynamic_profit_target_momentum_weight = _read_float(
        cfg_for_optional, "MMDynamicProfitTargetMomentumWeight", 0.35
    )
    mm_dynamic_profit_target_flow_weight = _read_float(
        cfg_for_optional, "MMDynamicProfitTargetFlowWeight", 0.60
    )
    mm_dynamic_profit_target_trend_weight = _read_float(
        cfg_for_optional, "MMDynamicProfitTargetTrendWeight", 0.45
    )
    mm_dynamic_profit_target_min = _read_float(cfg_for_optional, "MMDynamicProfitTargetMin", 2.0)
    mm_dynamic_profit_target_max = _read_float(cfg_for_optional, "MMDynamicProfitTargetMax", 30.0)
    mm_breakeven_trailing_offset_ticks = _read_float(cfg_for_optional, "MMBreakevenTrailingOffsetTicks", 5.0)
    mm_entry_score_threshold = _read_float(cfg_for_optional, "MMEntryScoreThreshold", 0.6)
    mm_entry_score_spread_threshold = _read_float(cfg_for_optional, "MMEntryScoreSpreadThreshold", 0.02)
    mm_entry_score_w_spread = _read_float(cfg_for_optional, "MMEntryScoreWSpread", 0.35)
    mm_entry_score_w_stability = _read_float(cfg_for_optional, "MMEntryScoreWStability", 0.25)
    mm_entry_score_w_trend = _read_float(cfg_for_optional, "MMEntryScoreWTrend", 0.25)
    mm_entry_score_w_imbalance = _read_float(cfg_for_optional, "MMEntryScoreWImbalance", 0.15)
    mm_entry_score_cooldown_penalty_max = _read_float(
        cfg_for_optional, "MMEntryScoreCooldownPenaltyMax", 0.35
    )
    mm_adaptive_learning_enabled = _read_bool(cfg_for_optional, "MMAdaptiveEntryLearningEnabled", False)
    mm_adaptive_learning_window = _read_int(cfg_for_optional, "MMAdaptiveEntryLearningWindow", 100)
    mm_adaptive_learning_min_bin_trades = _read_int(cfg_for_optional, "MMAdaptiveEntryLearningMinBinTrades", 10)
    mm_adaptive_learning_step_up = _read_float(cfg_for_optional, "MMAdaptiveEntryLearningStepUp", 0.01)
    mm_adaptive_learning_step_down = _read_float(cfg_for_optional, "MMAdaptiveEntryLearningStepDown", 0.01)
    mm_adaptive_learning_max_step_per_update = _read_float(
        cfg_for_optional, "MMAdaptiveEntryLearningMaxStepPerUpdate", 0.02
    )
    mm_adaptive_learning_threshold_min = _read_float(cfg_for_optional, "MMAdaptiveEntryLearningThresholdMin", 0.4)
    mm_adaptive_learning_threshold_max = _read_float(cfg_for_optional, "MMAdaptiveEntryLearningThresholdMax", 0.95)
    mm_adaptive_learning_drift_alert = _read_float(cfg_for_optional, "MMAdaptiveEntryLearningDriftAlert", 0.20)
    mm_adaptive_learning_perf_alert_delta = _read_float(
        cfg_for_optional, "MMAdaptiveEntryLearningPerfAlertDelta", 0.15
    )
    mm_trend_window_ticks = _read_int(cfg_for_optional, "MMTrendWindowTicks", 12)
    mm_trend_strength_threshold = _read_float(cfg_for_optional, "MMTrendStrengthThreshold", 0.0)
    mm_cancel_on_strong_trend = _read_bool(cfg_for_optional, "MMCancelOnStrongTrend", False)
    mm_post_fill_horizon_ms = _read_int(cfg_for_optional, "MMPostFillHorizonMs", 200)
    mm_adverse_fill_window = _read_int(cfg_for_optional, "MMAdverseFillWindow", 25)
    mm_adverse_fill_rate_threshold = _read_float(cfg_for_optional, "MMAdverseFillRateThreshold", 0.65)
    mm_defensive_quote_offset_ticks = _read_int(cfg_for_optional, "MMDefensiveQuoteOffsetTicks", 1)
    mm_decision_confirmation_updates = _read_int(cfg_for_optional, "MMDecisionConfirmationUpdates", 2)
    mm_min_decision_interval_ms = _read_int(cfg_for_optional, "MMMinDecisionIntervalMs", 150)
    mm_decision_batch_ticks = _read_int(cfg_for_optional, "MMDecisionBatchTicks", 3)
    mm_cancel_impact_horizon_ms = _read_int(cfg_for_optional, "MMCancelImpactHorizonMs", 500)
    mm_cancel_reason_summary_every = _read_int(cfg_for_optional, "MMCancelReasonSummaryEvery", 20)
    mm_disable_price_move_cancel = _read_bool(cfg_for_optional, "MMDisablePriceMoveCancel", False)
    mm_microprice_edge_threshold = _read_float(cfg_for_optional, "MMMicropriceEdgeThreshold", 0.0002)
    mm_spread_median_window_ticks = _read_int(cfg_for_optional, "MMSpreadMedianWindowTicks", 64)
    mm_anti_adverse_window_ms = _read_int(cfg_for_optional, "MMAntiAdverseWindowMs", 50)
    mm_kpi_eval_every_trades = _read_int(cfg_for_optional, "MMKpiEvalEveryTrades", 50)
    mm_kpi_threshold_step = _read_float(cfg_for_optional, "MMKpiThresholdStep", 0.0001)
    mm_position_policy_enabled = _read_bool(cfg_for_optional, "MMPositionPolicyEnabled", False)
    mm_adaptive_targets_path_raw = _read_str(cfg_for_optional, "MMAdaptiveTargetsPath", "")
    mm_adaptive_targets_path = ""
    if mm_adaptive_targets_path_raw.strip():
        _mm_at = Path(mm_adaptive_targets_path_raw.strip())
        mm_adaptive_targets_path = str(_mm_at if _mm_at.is_absolute() else base_dir / _mm_at)
    mm_learning_patch_path_raw = _read_str(cfg_for_optional, "MMLearningPatchStatePath", "")
    mm_learning_patch_path = ""
    if mm_learning_patch_path_raw.strip():
        _mm_lp = Path(mm_learning_patch_path_raw.strip())
        mm_learning_patch_path = str(_mm_lp if _mm_lp.is_absolute() else base_dir / _mm_lp)
    mm_strategy_mode = (_read_str(cfg_for_optional, "MMStrategyMode", "MOMENTUM_BREAKOUT").strip().upper() or "MOMENTUM_BREAKOUT")
    mm_momentum_params_path_raw = _read_str(cfg_for_optional, "MMMomentumParamsPath", "")
    mm_momentum_params_path = ""
    if mm_momentum_params_path_raw.strip():
        _mm_mo = Path(mm_momentum_params_path_raw.strip())
        mm_momentum_params_path = str(_mm_mo if _mm_mo.is_absolute() else base_dir / _mm_mo)
    trading_account_equities = _read_str(cfg_for_optional, "TradingAccountEquities", "")
    trading_account_forts = _read_str(cfg_for_optional, "TradingAccountForts", "")
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
        simulation_slippage_bps=sim_slippage_bps,
        simulation_slippage_max_bps=sim_slippage_max_bps,
        simulation_volatility_slippage_multiplier=sim_vol_slippage_multiplier,
        simulation_latency_network_ms=sim_latency_network_ms,
        simulation_latency_exchange_ms=sim_latency_exchange_ms,
        simulation_latency_jitter_ms=sim_latency_jitter_ms,
        simulation_fill_participation=sim_fill_participation,
        simulation_touch_fill_probability=sim_touch_fill_probability,
        simulation_passive_fill_probability_scale=sim_passive_fill_scale,
        simulation_adverse_selection_bias=sim_adverse_selection_bias,
        simulation_slippage_model=paper_slippage_model,
        simulation_fixed_slippage_abs=paper_fixed_slippage_abs,
        simulation_spread_slippage_fraction=paper_spread_slippage_fraction,
        simulation_decision_to_send_min_ms=decision_to_send_min_ms,
        simulation_decision_to_send_max_ms=decision_to_send_max_ms,
        simulation_send_to_fill_min_ms=send_to_fill_min_ms,
        simulation_send_to_fill_max_ms=send_to_fill_max_ms,
        account_by_market={
            MarketType.EQUITIES: trading_account_equities,
            MarketType.FORTS: trading_account_forts,
        },
        execution_mode=execution_mode,
        stream_book_fills=paper_stream_book_fills,
    )
    logger.info(
        "[PAPER][EXEC] execution_mode=%s fill_config=%s",
        execution_mode,
        gateway.simulation_config_snapshot(),
    )
    if execution_mode == "PAPER_REAL_MARKET":
        logger.warning(
            "[SAFETY] PAPER_REAL_MARKET enabled: real market data + local execution only; real order routing disabled."
        )
        if paper_stream_book_fills:
            logger.info(
                "[EXEC] PaperStreamBookFills=Y: fills priced from live bid/ask (PaperSlippage*/latency/ adverse sim keys ignored)."
            )
    if failure_handling_enabled:
        failure_monitor = FailureMonitor(
            logger=logger,
            order_manager=order_manager,
            gateway=gateway,
            watch_symbol=(_read_default_optional_setting(cfg_for_optional, "FailureWatchSymbol") or "SBER"),
            max_market_data_staleness_sec=_read_int(cfg_for_optional, "MaxMarketDataStalenessSec", 15),
            max_order_stuck_sec=_read_int(cfg_for_optional, "MaxOrderStuckSec", 30),
            max_no_execution_report_sec=_read_int(cfg_for_optional, "MaxNoExecutionReportSec", 15),
            action_on_anomaly=(_read_default_optional_setting(cfg_for_optional, "FailureAction") or "ALERT"),
            check_interval_sec=_read_int(cfg_for_optional, "FailureCheckIntervalSec", 1),
        )
    if quote_store is not None:
        market_data_engine.subscribe(quote_store.on_market_data)
    market_data_engine.subscribe(gateway.on_market_data)
    market_data_engine.subscribe(
        lambda data: economics.on_market_data(
            market=MarketType.EQUITIES,
            symbol=data.symbol,
            mid_price=_d(data.mid_price),
        )
    )
    market_data_engine.subscribe(adverse_tracker.on_market_data)
    if failure_monitor is not None:
        market_data_engine.subscribe(failure_monitor.on_market_data)

    mm_enabled = _read_bool(cfg_for_optional, "MarketMakingEnabled", False)
    if mm_enabled:
        mm_symbol = (_read_default_optional_setting(cfg_for_optional, "MarketMakingSymbol") or "SBER").upper()
        mm_lot = _read_float(cfg_for_optional, "MarketMakingLotSize", 1.0)
        mm_market_raw = (_read_default_optional_setting(cfg_for_optional, "MarketMakingMarket") or "EQUITIES").upper()
        mm_market = MarketType(mm_market_raw) if mm_market_raw in {"EQUITIES", "FORTS"} else MarketType.EQUITIES
        market_maker = BasicMarketMaker(
            symbol=mm_symbol,
            lot_size=mm_lot,
            market=mm_market,
            gateway=gateway,
            position_manager=position_manager,
            logger=logger,
            max_loss_per_trade=max_loss_per_trade,
            virtual_account_enabled=virtual_account_enabled,
            virtual_account_start_balance=virtual_account_start_balance,
            virtual_account_max_loss_fraction=virtual_account_max_loss_fraction,
            entry_forecast_profit_enabled=entry_forecast_profit_enabled,
            entry_forecast_alignment_min=entry_forecast_alignment_min,
            volatility_window_ticks=mm_volatility_window_ticks,
            max_short_term_volatility=mm_max_short_term_volatility,
            cancel_on_high_volatility=mm_cancel_on_high_volatility,
            resting_order_timeout_sec=mm_resting_order_timeout_sec,
            tick_size=mm_tick_size,
            replace_threshold_ticks=mm_replace_threshold_ticks,
            replace_cancel_threshold_ticks=mm_replace_cancel_threshold_ticks,
            replace_keep_threshold_ticks=mm_replace_keep_threshold_ticks,
            replace_persist_ms=mm_replace_persist_ms,
            adverse_move_cancel_ticks=mm_adverse_move_cancel_ticks,
            fast_cancel_keep_ticks=mm_fast_cancel_keep_ticks,
            fast_cancel_persist_ms=mm_fast_cancel_persist_ms,
            price_tolerance_ticks=mm_price_tolerance_ticks,
            price_tolerance_pct=mm_price_tolerance_pct,
            min_order_lifetime_ms=mm_min_order_lifetime_ms,
            cancel_replace_cooldown_ms=mm_cancel_replace_cooldown_ms,
            trend_window_ticks=mm_trend_window_ticks,
            trend_strength_threshold=mm_trend_strength_threshold,
            cancel_on_strong_trend=mm_cancel_on_strong_trend,
            post_fill_horizon_ms=mm_post_fill_horizon_ms,
            adverse_fill_window=mm_adverse_fill_window,
            adverse_fill_rate_threshold=mm_adverse_fill_rate_threshold,
            defensive_quote_offset_ticks=mm_defensive_quote_offset_ticks,
            decision_confirmation_updates=mm_decision_confirmation_updates,
            min_decision_interval_ms=mm_min_decision_interval_ms,
            decision_batch_ticks=mm_decision_batch_ticks,
            entry_min_spread=mm_entry_min_spread,
            entry_stability_window_ticks=mm_entry_stability_window_ticks,
            entry_max_bid_ask_move_ticks=mm_entry_max_bid_ask_move_ticks,
            entry_anti_trend_threshold=mm_entry_anti_trend_threshold,
            entry_direction_window_ticks=mm_entry_direction_window_ticks,
            entry_direction_min_move_ticks=mm_entry_direction_min_move_ticks,
            trade_cooldown_ms=mm_trade_cooldown_ms,
            entry_min_place_interval_ms=mm_entry_min_place_interval_ms,
            one_position_only=mm_one_position_only,
            reversal_enabled=mm_reversal_enabled,
            reversal_confirmation_updates=mm_reversal_confirmation_updates,
            reversal_min_trend_strength=mm_reversal_min_trend_strength,
            reversal_min_hold_ms=mm_reversal_min_hold_ms,
            reversal_cooldown_ms=mm_reversal_cooldown_ms,
            weekly_trend_enabled=mm_weekly_trend_enabled,
            weekly_trend_db_path=str(quote_history_db),
            weekly_trend_days=mm_weekly_trend_days,
            weekly_trend_threshold_pct=mm_weekly_trend_threshold_pct,
            weekly_trend_refresh_sec=mm_weekly_trend_refresh_sec,
            volume_move_corr_enabled=mm_volume_move_corr_enabled,
            volume_move_corr_db_path=str(quote_history_db),
            volume_move_corr_lookback_days=mm_volume_move_corr_lookback_days,
            volume_move_corr_bar_minutes=mm_volume_move_corr_bar_minutes,
            volume_move_corr_min_samples=mm_volume_move_corr_min_samples,
            volume_move_corr_threshold=mm_volume_move_corr_threshold,
            volume_move_corr_refresh_sec=mm_volume_move_corr_refresh_sec,
            five_min_entry_gate_enabled=mm_five_min_entry_gate_enabled,
            five_min_impulse_min_ticks=mm_five_min_impulse_min_ticks,
            five_min_volume_ratio_min=mm_five_min_volume_ratio_min,
            trend_continuation_enabled=mm_trend_continuation_enabled,
            trend_continuation_confirm_updates=mm_trend_continuation_confirm_updates,
            trend_continuation_min_move_ticks=mm_trend_continuation_min_move_ticks,
            flow_bias_window_ticks=mm_flow_bias_window_ticks,
            long_flow_bias_min=mm_long_flow_bias_min,
            long_trend_bias_min_ticks=mm_long_trend_bias_min_ticks,
            long_impulse_override_ticks=mm_long_impulse_override_ticks,
            short_flow_bias_max=mm_short_flow_bias_max,
            short_trend_bias_min_ticks=mm_short_trend_bias_min_ticks,
            volume_flow_override_ticks=mm_volume_flow_override_ticks,
            volume_spike_entry_enabled=mm_volume_spike_entry_enabled,
            volume_spike_window_ticks=mm_volume_spike_window_ticks,
            volume_spike_multiplier=mm_volume_spike_multiplier,
            volume_spike_flow_bias_abs_min=mm_volume_spike_flow_bias_abs_min,
            momentum_exit_enabled=mm_momentum_exit_enabled,
            ignore_duplicate_ticks_ms=mm_ignore_duplicate_ticks_ms,
            decision_min_mid_move_ticks=mm_decision_min_mid_move_ticks,
            tray_price_every_sec=mm_tray_price_every_sec,
            take_profit_per_trade=mm_take_profit_per_trade,
            dynamic_profit_target_enabled=mm_dynamic_profit_target_enabled,
            dynamic_profit_target_vol_multiplier=mm_dynamic_profit_target_vol_multiplier,
            dynamic_profit_target_momentum_weight=mm_dynamic_profit_target_momentum_weight,
            dynamic_profit_target_flow_weight=mm_dynamic_profit_target_flow_weight,
            dynamic_profit_target_trend_weight=mm_dynamic_profit_target_trend_weight,
            dynamic_profit_target_min=mm_dynamic_profit_target_min,
            dynamic_profit_target_max=mm_dynamic_profit_target_max,
            breakeven_trailing_offset_ticks=mm_breakeven_trailing_offset_ticks,
            entry_score_threshold=mm_entry_score_threshold,
            entry_score_spread_threshold=mm_entry_score_spread_threshold,
            entry_score_w_spread=mm_entry_score_w_spread,
            entry_score_w_stability=mm_entry_score_w_stability,
            entry_score_w_trend=mm_entry_score_w_trend,
            entry_score_w_imbalance=mm_entry_score_w_imbalance,
            entry_score_cooldown_penalty_max=mm_entry_score_cooldown_penalty_max,
            adaptive_entry_learning_enabled=mm_adaptive_learning_enabled,
            adaptive_entry_learning_window=mm_adaptive_learning_window,
            adaptive_entry_learning_min_bin_trades=mm_adaptive_learning_min_bin_trades,
            adaptive_entry_learning_step_up=mm_adaptive_learning_step_up,
            adaptive_entry_learning_step_down=mm_adaptive_learning_step_down,
            adaptive_entry_learning_max_step_per_update=mm_adaptive_learning_max_step_per_update,
            adaptive_entry_learning_threshold_min=mm_adaptive_learning_threshold_min,
            adaptive_entry_learning_threshold_max=mm_adaptive_learning_threshold_max,
            adaptive_entry_learning_drift_alert=mm_adaptive_learning_drift_alert,
            adaptive_entry_learning_perf_alert_delta=mm_adaptive_learning_perf_alert_delta,
            cancel_impact_horizon_ms=mm_cancel_impact_horizon_ms,
            cancel_reason_summary_every=mm_cancel_reason_summary_every,
            disable_price_move_cancel=mm_disable_price_move_cancel,
            microprice_edge_threshold=mm_microprice_edge_threshold,
            spread_median_window_ticks=mm_spread_median_window_ticks,
            anti_adverse_window_ms=mm_anti_adverse_window_ms,
            kpi_eval_every_trades=mm_kpi_eval_every_trades,
            kpi_threshold_step=mm_kpi_threshold_step,
            cancel_analytics_sink=economics_store.insert_cancel_analytics,
            entry_decision_sink=economics_store.insert_entry_decisions,
            position_policy_enabled=mm_position_policy_enabled,
            adaptive_targets_path=mm_adaptive_targets_path,
            learning_patch_path=mm_learning_patch_path,
            economics_store=economics_store,
            strategy_mode=mm_strategy_mode,
            momentum_params_path=mm_momentum_params_path,
        )
        market_data_engine.subscribe(market_maker.on_market_data)
        logger.info(
            "[MM] enabled symbol=%s lot=%s market=%s max_loss_per_trade=%s mm_vol_threshold=%s mm_vol_window=%s mm_vol_cancel=%s mm_rest_timeout_sec=%s mm_tick_size=%s mm_replace_ticks=%s mm_replace_cancel_ticks=%s mm_replace_keep_ticks=%s mm_replace_persist_ms=%s mm_adverse_cancel_ticks=%s mm_fast_cancel_keep_ticks=%s mm_fast_cancel_persist_ms=%s mm_tolerance_ticks=%s mm_tolerance_pct=%s mm_min_lifetime_ms=%s mm_replace_cooldown_ms=%s mm_entry_min_spread=%s mm_entry_stability_window=%s mm_entry_max_move_ticks=%s mm_entry_anti_trend=%s mm_entry_direction_window=%s mm_entry_direction_min_move_ticks=%s mm_trade_cooldown_ms=%s mm_entry_min_interval_ms=%s mm_entry_score_threshold=%s mm_entry_score_spread_threshold=%s mm_entry_score_w_spread=%s mm_entry_score_w_stability=%s mm_entry_score_w_trend=%s mm_entry_score_w_imbalance=%s mm_entry_score_cooldown_penalty_max=%s mm_trend_window=%s mm_trend_threshold=%s mm_trend_cancel=%s mm_post_fill_ms=%s mm_adverse_window=%s mm_adverse_rate_threshold=%s mm_defensive_offset_ticks=%s mm_confirm_updates=%s mm_min_decision_ms=%s mm_batch_ticks=%s mm_cancel_impact_ms=%s mm_cancel_summary_every=%s mm_disable_price_move_cancel=%s",
            mm_symbol,
            mm_lot,
            mm_market.value,
            max_loss_per_trade,
            mm_max_short_term_volatility,
            mm_volatility_window_ticks,
            mm_cancel_on_high_volatility,
            mm_resting_order_timeout_sec,
            mm_tick_size,
            mm_replace_threshold_ticks,
            mm_replace_cancel_threshold_ticks,
            mm_replace_keep_threshold_ticks,
            mm_replace_persist_ms,
            mm_adverse_move_cancel_ticks,
            mm_fast_cancel_keep_ticks,
            mm_fast_cancel_persist_ms,
            mm_price_tolerance_ticks,
            mm_price_tolerance_pct,
            mm_min_order_lifetime_ms,
            mm_cancel_replace_cooldown_ms,
            mm_entry_min_spread,
            mm_entry_stability_window_ticks,
            mm_entry_max_bid_ask_move_ticks,
            mm_entry_anti_trend_threshold,
            mm_entry_direction_window_ticks,
            mm_entry_direction_min_move_ticks,
            mm_trade_cooldown_ms,
            mm_entry_min_place_interval_ms,
            mm_entry_score_threshold,
            mm_entry_score_spread_threshold,
            mm_entry_score_w_spread,
            mm_entry_score_w_stability,
            mm_entry_score_w_trend,
            mm_entry_score_w_imbalance,
            mm_entry_score_cooldown_penalty_max,
            mm_trend_window_ticks,
            mm_trend_strength_threshold,
            mm_cancel_on_strong_trend,
            mm_post_fill_horizon_ms,
            mm_adverse_fill_window,
            mm_adverse_fill_rate_threshold,
            mm_defensive_quote_offset_ticks,
            mm_decision_confirmation_updates,
            mm_min_decision_interval_ms,
            mm_decision_batch_ticks,
            mm_cancel_impact_horizon_ms,
            mm_cancel_reason_summary_every,
            mm_disable_price_move_cancel,
        )
        logger.info("[MM][STRATEGY_DIAGNOSTIC] strategy_mode=%s", mm_strategy_mode)
        logger.info(
            "[MM][POSITION_POLICY] MMPositionPolicyEnabled=%s MMAdaptiveTargetsPath=%s MMLearningPatchStatePath=%s",
            mm_position_policy_enabled,
            mm_adaptive_targets_path or "(bundled adaptive_learning_targets.json if present)",
            mm_learning_patch_path or "(default fix_engine/learning_patch_state.json)",
        )

    if data_provider in {"TINKOFF", "TINKOFF_SANDBOX"}:
        from tbank_preflight import load_sandbox_token, verify_market_data_readonly
        from tbank_sandbox_feed import run_tbank_sandbox_market_data
        from tools.export_session_metrics import print_post_run_summary

        md_health: MdHealthMonitor | None = None
        if execution_mode != "PAPER_REAL_MARKET":
            raise RuntimeError(
                f"Sandbox paper session requires ExecutionMode=PAPER_REAL_MARKET, got {execution_mode!r}"
            )
        if not gateway._uses_local_sim_execution:
            raise RuntimeError("FATAL: gateway must use local synthetic execution for paper sandbox run")
        log_event(
            logger,
            level=logging.INFO,
            component="Preflight",
            event="paper_execution_guard",
            execution_mode=gateway.execution_mode,
            uses_local_sim_execution=gateway._uses_local_sim_execution,
        )

        tbank_host = _read_str(cfg_for_optional, "TBankSandboxHost", "invest-public-api.tinkoff.ru:443")
        tbank_token = os.getenv("TINKOFF_TOKEN", "").strip() or os.getenv("TINKOFF_SANDBOX_TOKEN", "").strip()
        if not tbank_token:
            _local_secrets = base_dir / "settings.local.cfg"
            if _local_secrets.exists():
                tbank_token = _read_str(_local_secrets, "TBankSandboxToken", "").strip()
        if not tbank_token:
            tbank_token = _read_str(cfg_for_optional, "TBankSandboxToken", "").strip()
        tbank_instrument_id = _read_str(cfg_for_optional, "TBankInstrumentId", "")
        tbank_symbol = _read_str(cfg_for_optional, "TBankSymbol", "SBER")
        tbank_depth = _read_int(cfg_for_optional, "TBankOrderBookDepth", 1)
        tbank_include_trades = _read_bool(cfg_for_optional, "TBankIncludeTrades", True)
        tbank_run_duration_sec = _read_int(cfg_for_optional, "TBankRunDurationSec", 0)
        tbank_md_reconnect_initial_sec = _read_float(cfg_for_optional, "TBankMdReconnectInitialSec", 1.0)
        tbank_md_reconnect_max_sec = _read_float(cfg_for_optional, "TBankMdReconnectMaxSec", 60.0)
        tbank_md_stale_reconnect_sec = _read_float(cfg_for_optional, "TBankMdStaleReconnectSec", 90.0)
        _env_dur = os.getenv("TBANK_RUN_DURATION_SEC", "").strip()
        if _env_dur:
            try:
                tbank_run_duration_sec = int(_env_dur)
            except ValueError:
                logger.warning("TBANK_RUN_DURATION_SEC ignored (not an integer): %s", _env_dur)

        _marker_path = base_dir / "log" / "session_start_marker.txt"
        _marker_path.parent.mkdir(parents=True, exist_ok=True)
        _marker_path.write_text(datetime.now(timezone.utc).isoformat(), encoding="utf-8")

        _tok = load_sandbox_token(base_dir, _read_str)
        if not _tok.strip():
            raise RuntimeError("PREFLIGHT: T-Invest token not loaded (TINKOFF_TOKEN / settings.local.cfg TBankSandboxToken)")
        verify_market_data_readonly(
            token=_tok,
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
            failure_monitor.start()
        logger.info(
            "[TBANK] data_provider=%s host=%s instrument_id=%s symbol=%s run_duration_sec=%s",
            data_provider,
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
                failure_monitor.stop()
            logging_runtime.listener.stop()
            try:
                print_post_run_summary(base_dir)
            except Exception as exc:
                logger.warning("post_run_summary_failed: %s", exc)
        return

    raise RuntimeError(
        f"Unsupported DataProvider={data_provider!r}. Use TINKOFF (or legacy TINKOFF_SANDBOX)."
    )


if __name__ == "__main__":
    run()
