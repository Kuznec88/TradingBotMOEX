from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock

import quickfix as fix

from analytics_api import TradingAnalyticsAPI
from economics_store import EconomicsStore
from execution_engine import ExecutionEngine
from execution_gateway import ExecutionGateway
from failure_monitor import FailureMonitor
from market_making import BasicMarketMaker
from market_data.market_data_engine import MarketDataEngine
from market_data.models import MarketData
from order_models import MarketType, OrderRequest
from order_manager import OrderManager
from position_manager import PositionManager
from risk_manager import RiskManager
from simulation_profiles import normalize_simulation_profile, resolve_simulation_profile
from structured_logging import StructuredLoggingRuntime, configure_structured_logging, log_event
from trade_client import MoexFixApplication
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


def _safe_get(message: fix.Message, tag: int) -> str:
    return message.getField(tag) if message.isSetField(tag) else ""


def _safe_get_group(group: fix.Group, tag: int) -> str:
    return group.getField(tag) if group.isSetField(tag) else ""


def _format_fix_for_log(raw_fix: str) -> str:
    # SOH delimiter -> visible pipe for human-readable troubleshooting.
    return raw_fix.replace("\x01", "|")


def _parse_market_data_message(
    message: fix.Message,
    market_data_engine: MarketDataEngine,
    logger: logging.Logger,
) -> None:
    msg_type = _safe_get(message.getHeader(), 35)
    if msg_type not in {fix.MsgType_MarketDataSnapshotFullRefresh, fix.MsgType_MarketDataIncrementalRefresh}:
        return

    no_md_entries = int(_safe_get(message, 268) or "0")
    top_symbol = _safe_get(message, 55).upper()
    snapshots: dict[str, dict[str, float | str]] = {}

    # Use generic Group to avoid depending on generated group classes.
    # Delimiter tag:
    # - W (snapshot): 269
    # - X (incremental): 279
    delimiter = 269 if msg_type == fix.MsgType_MarketDataSnapshotFullRefresh else 279
    for idx in range(1, no_md_entries + 1):
        group = fix.Group(268, delimiter)
        message.getGroup(idx, group)

        symbol = (_safe_get_group(group, 55) or top_symbol).upper()
        if not symbol:
            continue

        entry_type = _safe_get_group(group, 269)
        px_str = _safe_get_group(group, 270)
        size_str = _safe_get_group(group, 271)
        if not px_str:
            continue

        px = float(px_str)
        size = float(size_str) if size_str else 0.0
        snapshot = snapshots.setdefault(
            symbol,
            {
                "symbol": symbol,
                "bid": 0.0,
                "ask": 0.0,
                "last": 0.0,
                "volume": 0.0,
            },
        )

        if entry_type == "0":  # bid
            snapshot["bid"] = px
        elif entry_type == "1":  # ask
            snapshot["ask"] = px
        elif entry_type in {"2", "7", "8"}:  # trade/close/open-ish
            snapshot["last"] = px
            snapshot["volume"] = size

    for symbol, snapshot in snapshots.items():
        latest = market_data_engine.get_latest(symbol)
        bid = float(snapshot["bid"]) or (latest.bid if latest else float(snapshot["last"]) or 0.0)
        ask = float(snapshot["ask"]) or (latest.ask if latest else float(snapshot["last"]) or bid)
        last = float(snapshot["last"]) or (latest.last if latest else (bid + ask) / 2)
        volume = float(snapshot["volume"]) or (latest.volume if latest else 0.0)

        market_data_engine.update_market_data(
            {
                "symbol": symbol,
                "bid": bid,
                "ask": ask,
                "last": last,
                "volume": volume,
            }
        )
        log_event(
            logger,
            level=logging.INFO,
            component="MarketData",
            event="market_data_snapshot",
            correlation_id="",
            source="FIX",
            symbol=symbol,
            bid=float(bid),
            ask=float(ask),
            spread=float(ask - bid),
            last_price=float(last),
        )


def setup_logging(base_dir: Path, *, simulation: bool) -> StructuredLoggingRuntime:
    return configure_structured_logging(base_dir=base_dir, simulation=simulation, logger_name="fix_engine")


def _resolve_fix44_dictionary(base_dir: Path) -> Path | None:
    candidates: list[Path] = []

    # Local project candidates.
    candidates.append(base_dir / "FIX44.xml")
    candidates.append(base_dir / "spec" / "FIX44.xml")

    # Candidate inside installed quickfix package.
    quickfix_dir = Path(fix.__file__).resolve().parent
    candidates.append(quickfix_dir / "FIX44.xml")
    candidates.append(quickfix_dir / "spec" / "FIX44.xml")

    # Some distributions install under share dirs.
    candidates.append(Path("/usr/share/quickfix/FIX44.xml"))
    candidates.append(Path("/usr/local/share/quickfix/FIX44.xml"))

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _load_settings_with_resolved_dictionary(settings_path: Path, logger: logging.Logger) -> fix.SessionSettings:
    text = settings_path.read_text(encoding="utf-8")

    dictionary_value = ""
    for line in text.splitlines():
        if line.strip().startswith("DataDictionary="):
            dictionary_value = line.split("=", 1)[1].strip()
            break

    dictionary_path = Path(dictionary_value) if dictionary_value else Path()
    if dictionary_value and not dictionary_path.is_absolute():
        dictionary_path = (settings_path.parent / dictionary_path).resolve()

    if not dictionary_value or not dictionary_path.exists():
        resolved = _resolve_fix44_dictionary(settings_path.parent)
        if resolved is None:
            raise RuntimeError(
                "FIX44.xml not found. Put FIX44.xml into fix_engine/ "
                "or install quickfix package that includes spec/FIX44.xml."
            )
        logger.info("Using FIX dictionary: %s", resolved)
        lines = []
        replaced = False
        for line in text.splitlines():
            if line.strip().startswith("DataDictionary="):
                lines.append(f"DataDictionary={resolved}")
                replaced = True
            else:
                lines.append(line)
        if not replaced:
            lines.append(f"DataDictionary={resolved}")
        runtime_cfg = settings_path.parent / "settings.runtime.cfg"
        runtime_cfg.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return fix.SessionSettings(str(runtime_cfg.resolve()))

    return fix.SessionSettings(str(settings_path.resolve()))


def _read_session_blocks(cfg_path: Path) -> list[dict[str, str]]:
    current: dict[str, str] | None = None
    sessions: list[dict[str, str]] = []

    for raw_line in cfg_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            if current is not None:
                sessions.append(current)
            section = line[1:-1].strip().upper()
            current = {} if section == "SESSION" else None
            continue
        if current is None or "=" not in line:
            continue
        k, v = line.split("=", 1)
        current[k.strip()] = v.strip()

    if current is not None:
        sessions.append(current)
    return sessions


def _read_session_optional_setting_from_cfg(
    cfg_path: Path,
    key: str,
    *,
    target_comp_id: str,
) -> str | None:
    """
    Parses duplicated [SESSION] blocks manually and returns key for selected session.
    This avoids SWIG iteration issues with SessionSettings.getSessions().
    """
    sessions = _read_session_blocks(cfg_path)
    for session in sessions:
        if session.get("TargetCompID", "") == target_comp_id:
            value = session.get(key, "").strip()
            return value or None
    return None


def _read_passwords_by_target(cfg_path: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for session in _read_session_blocks(cfg_path):
        target = session.get("TargetCompID", "").strip()
        password = session.get("Password", "").strip()
        if target and password:
            mapping[target] = password
    return mapping


def _read_simulation_mode(cfg_path: Path) -> bool:
    for raw_line in cfg_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("SIMULATION_MODE="):
            value = line.split("=", 1)[1].strip().upper()
            return value in {"Y", "YES", "TRUE", "1", "ON"}
    return False


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


def _source_to_market(source: str) -> MarketType:
    return MarketType.FORTS if source == "FORTS" else MarketType.EQUITIES


def run() -> None:
    base_dir = Path(__file__).resolve().parent
    settings_path = base_dir / "settings.cfg"
    initial_simulation_mode = _read_simulation_mode(settings_path)
    logging_runtime = setup_logging(base_dir, simulation=initial_simulation_mode)
    logger = logging_runtime.logger
    settings = _load_settings_with_resolved_dictionary(settings_path, logger)
    runtime_cfg_path = base_dir / "settings.runtime.cfg"
    cfg_for_optional = runtime_cfg_path if runtime_cfg_path.exists() else settings_path
    order_manager = OrderManager()
    market_data_engine = MarketDataEngine(logger=logger)
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

    def on_execution_report(message: fix.Message, source: str) -> None:
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

    def on_market_data_message(message: fix.Message, source: str) -> None:
        del source
        _parse_market_data_message(message, market_data_engine, logger)

    app = MoexFixApplication(
        password_by_target=_read_passwords_by_target(cfg_for_optional),
        logger=logger,
        order_manager=order_manager,
        on_execution_report=on_execution_report,
        on_market_data=on_market_data_message,
    )

    store_factory = fix.FileStoreFactory(settings)
    log_factory = fix.FileLogFactory(settings)
    initiator = fix.SocketInitiator(app, store_factory, settings, log_factory)
    simulation_mode = _read_simulation_mode(cfg_for_optional)
    sender_comp_id_equities = (
        _read_session_optional_setting_from_cfg(cfg_for_optional, "SenderCompID", target_comp_id="IFIX-EQ-UAT") or ""
    ).strip()
    sender_comp_id_forts = (
        _read_session_optional_setting_from_cfg(cfg_for_optional, "SenderCompID", target_comp_id="FG") or ""
    ).strip()
    trading_account_equities = (
        _read_default_optional_setting(cfg_for_optional, "EQUITIES.account")
        or _read_default_optional_setting(cfg_for_optional, "EquitiesTradingAccount")
        or _read_session_optional_setting_from_cfg(cfg_for_optional, "TradingAccount", target_comp_id="IFIX-EQ-UAT")
        or ""
    ).strip()
    trading_account_forts = (
        _read_default_optional_setting(cfg_for_optional, "FORTS.account")
        or _read_default_optional_setting(cfg_for_optional, "FortsTradingAccount")
        or _read_session_optional_setting_from_cfg(cfg_for_optional, "TradingAccount", target_comp_id="FG")
        or ""
    ).strip()
    if not trading_account_equities:
        logger.warning(
            "Equities trading account is not set. "
            "MOEX may reject orders with 'Invalid trading account'."
        )
    elif sender_comp_id_equities and trading_account_equities == sender_comp_id_equities:
        logger.warning(
            "TradingAccount is currently set to SenderCompID value (%s). "
            "MOEX usually requires a separate valid trading account/portfolio code.",
            trading_account_equities,
        )
        trading_account_equities = ""
    if not trading_account_forts:
        logger.warning(
            "FORTS trading account is not set. "
            "FORTS orders may be rejected with 'Invalid trading account'."
        )
    elif sender_comp_id_forts and trading_account_forts == sender_comp_id_forts:
        logger.warning(
            "FORTS TradingAccount is currently set to SenderCompID value (%s). "
            "Use real FORTS account code, otherwise orders will be blocked locally.",
            trading_account_forts,
        )
        trading_account_forts = ""
    equities_engine = ExecutionEngine(
        order_manager,
        lambda: app.get_session_id(MarketType.EQUITIES),
        logger,
        trading_account=trading_account_equities,
    )
    forts_engine = ExecutionEngine(
        order_manager,
        lambda: app.get_session_id(MarketType.FORTS),
        logger,
        trading_session_id="FUT",
        trading_account=trading_account_forts,
    )
    simulation_stress_params: dict[str, float | int] = {
        "simulation_slippage_bps": _read_float(cfg_for_optional, "SimulationSlippageBps", 2.0),
        "simulation_slippage_max_bps": _read_float(cfg_for_optional, "SimulationSlippageMaxBps", 25.0),
        "simulation_volatility_slippage_multiplier": _read_float(
            cfg_for_optional, "SimulationVolatilitySlippageMultiplier", 1.5
        ),
        "simulation_latency_network_ms": _read_int(cfg_for_optional, "SimulationNetworkLatencyMs", 20),
        "simulation_latency_exchange_ms": _read_int(cfg_for_optional, "SimulationExchangeLatencyMs", 30),
        "simulation_latency_jitter_ms": _read_int(cfg_for_optional, "SimulationLatencyJitterMs", 40),
        "simulation_fill_participation": _read_float(cfg_for_optional, "SimulationFillParticipation", 0.25),
        "simulation_touch_fill_probability": _read_float(cfg_for_optional, "SimulationTouchFillProbability", 0.15),
        "simulation_passive_fill_probability_scale": _read_float(
            cfg_for_optional, "SimulationPassiveFillProbabilityScale", 0.5
        ),
        "simulation_adverse_selection_bias": _read_float(cfg_for_optional, "SimulationAdverseSelectionBias", 0.35),
    }
    configured_simulation_profile = _read_default_optional_setting(cfg_for_optional, "SimulationProfile") or "STRESS"
    runtime_simulation_profile_override = os.getenv("SIMULATION_PROFILE_OVERRIDE", "")
    active_simulation_profile, resolved_simulation_params, simulation_profile_source = resolve_simulation_profile(
        base_params=simulation_stress_params,
        configured_profile=configured_simulation_profile,
        runtime_override_profile=runtime_simulation_profile_override,
    )
    sim_slippage_bps = float(resolved_simulation_params["simulation_slippage_bps"])
    sim_slippage_max_bps = float(resolved_simulation_params["simulation_slippage_max_bps"])
    sim_vol_slippage_multiplier = float(resolved_simulation_params["simulation_volatility_slippage_multiplier"])
    sim_latency_network_ms = int(resolved_simulation_params["simulation_latency_network_ms"])
    sim_latency_exchange_ms = int(resolved_simulation_params["simulation_latency_exchange_ms"])
    sim_latency_jitter_ms = int(resolved_simulation_params["simulation_latency_jitter_ms"])
    sim_fill_participation = float(resolved_simulation_params["simulation_fill_participation"])
    sim_touch_fill_probability = float(resolved_simulation_params["simulation_touch_fill_probability"])
    sim_passive_fill_scale = float(resolved_simulation_params["simulation_passive_fill_probability_scale"])
    sim_adverse_selection_bias = float(resolved_simulation_params["simulation_adverse_selection_bias"])
    strategy_min_spread = _read_float(cfg_for_optional, "StrategyMinSpreadThreshold", 0.0)
    strategy_max_volatility = _read_float(cfg_for_optional, "StrategyMaxVolatilityThreshold", 1.0)
    strategy_vol_window = _read_int(cfg_for_optional, "StrategyVolatilityWindow", 20)
    max_loss_per_trade = _read_float(cfg_for_optional, "RiskMaxLossPerTrade", 0.0)
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
    gateway = ExecutionGateway(
        equities_engine=equities_engine,
        forts_engine=forts_engine,
        order_manager=order_manager,
        logger=logger,
        simulation_mode=simulation_mode,
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
        account_by_market={
            MarketType.EQUITIES: trading_account_equities,
            MarketType.FORTS: trading_account_forts,
        },
    )
    logger.info(
        "[SIM][PROFILE] active=%s source=%s simulation_mode=%s config=%s",
        active_simulation_profile,
        simulation_profile_source,
        simulation_mode,
        gateway.simulation_config_snapshot(),
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
    strategy = SimpleStrategy(
        logger=logger,
        min_spread_threshold=strategy_min_spread,
        max_volatility_threshold=strategy_max_volatility,
        volatility_window=strategy_vol_window,
    )
    market_data_engine.subscribe(strategy.on_market_data)
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
            cancel_analytics_sink=economics_store.insert_cancel_analytics,
            entry_decision_sink=economics_store.insert_entry_decisions,
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

    try:
        initiator.start()
    except Exception as exc:
        logger.exception("Failed to start FIX initiator: %s", exc)
        logging_runtime.listener.stop()
        return
    if failure_monitor is not None:
        failure_monitor.start()

    logger.info("FIX TRADE + DROP_COPY started. Wait for TRADE onLogon before sending orders.")
    logger.info(
        "Commands:\n"
        "  m [MARKET] SYMBOL SIDE QTY\n"
        "  l [MARKET] SYMBOL SIDE QTY PRICE\n"
        "  c CL_ORD_ID\n"
        "  sig LAST_PRICE SYMBOL QTY\n"
        "  md SYMBOL BID ASK LAST VOLUME [QTY]\n"
        "  pos SYMBOL\n"
        "  open\n"
        "  checkacc [SYMBOL] [QTY]\n"
        "  metrics\n"
        "  toplosers [N]\n"
        "  topwinners [N]\n"
        "  worstslip [N]\n"
        "  pnlcomp\n"
        "  fillquality\n"
        "  lossanalysis\n"
        "  analyticscounters\n"
        "  backfillanalytics [N]\n"
        "  analyticsmissing [N]\n"
        "  entrycorr\n"
        "  cancelimpact\n"
        "  simprofile [REALISTIC|STRESS|EXTREME]\n"
        "  q"
    )

    try:
        while True:
            command = input("> ").strip()
            if not command:
                continue
            if command.lower() == "q":
                break

            parts = command.split()
            kind = parts[0].lower()
            if kind == "m" and len(parts) == 4:
                _, symbol, side, qty = parts
                try:
                    cl_ord_id = gateway.send_order(
                        OrderRequest(symbol=symbol, side=side, qty=float(qty), account="")
                    )
                    logger.info("Market order queued, ClOrdID=%s", cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed to send market order: %s", exc)
            elif kind == "m" and len(parts) == 5:
                _, market_raw, symbol, side, qty = parts
                try:
                    market = MarketType(market_raw.upper())
                    cl_ord_id = gateway.send_order(
                        OrderRequest(symbol=symbol, side=side, qty=float(qty), account="", market=market)
                    )
                    logger.info("%s market order queued, ClOrdID=%s", market.value, cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed to send market order: %s", exc)
            elif kind == "l" and len(parts) == 5:
                _, symbol, side, qty, price = parts
                try:
                    cl_ord_id = gateway.send_order(
                        OrderRequest(symbol=symbol, side=side, qty=float(qty), account="", price=float(price))
                    )
                    logger.info("Limit order queued, ClOrdID=%s", cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed to send limit order: %s", exc)
            elif kind == "l" and len(parts) == 6:
                _, market_raw, symbol, side, qty, price = parts
                try:
                    market = MarketType(market_raw.upper())
                    cl_ord_id = gateway.send_order(
                        OrderRequest(
                            symbol=symbol,
                            side=side,
                            qty=float(qty),
                            account="",
                            price=float(price),
                            market=market,
                        )
                    )
                    logger.info("%s limit order queued, ClOrdID=%s", market.value, cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed to send limit order: %s", exc)
            elif kind == "c" and len(parts) == 2:
                _, cl_ord_id = parts
                try:
                    cancel_id = gateway.cancel_order(cl_ord_id)
                    logger.info("Cancel sent. CancelClOrdID=%s", cancel_id)
                except Exception as exc:
                    logger.exception("Failed to cancel order: %s", exc)
            elif kind == "sig" and len(parts) == 4:
                _, last_price, symbol, qty = parts
                try:
                    lp = float(last_price)
                    market_data_engine.update_market_data(
                        {
                            "symbol": symbol,
                            "bid": lp,
                            "ask": lp,
                            "last": lp,
                            "volume": 0,
                        }
                    )
                    signal = strategy.pop_signal(symbol)
                    logger.info("[STRATEGY] symbol=%s last=%s signal=%s", symbol, last_price, signal)
                    if signal in {"BUY", "SELL"}:
                        qty_f = float(qty)
                        if _strategy_inventory_allowed(symbol, signal, qty_f):
                            cl_ord_id = gateway.send_order(
                                OrderRequest(
                                    symbol=symbol,
                                    side=signal,
                                    qty=qty_f,
                                    account="",
                                    market=MarketType.EQUITIES,
                                )
                            )
                            logger.info("Signal order sent, ClOrdID=%s", cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed strategy signal handling: %s", exc)
            elif kind == "md" and len(parts) in {6, 7}:
                _, symbol, bid, ask, last, volume, *rest = parts
                try:
                    market_data = market_data_engine.update_market_data(
                        {
                            "symbol": symbol,
                            "bid": float(bid),
                            "ask": float(ask),
                            "last": float(last),
                            "volume": float(volume),
                        }
                    )
                    signal = strategy.pop_signal(symbol)
                    logger.info(
                        "[STRATEGY] symbol=%s mid=%s spread=%s signal=%s",
                        market_data.symbol,
                        round(market_data.mid_price, 6),
                        round(market_data.spread, 6),
                        signal,
                    )
                    if signal in {"BUY", "SELL"}:
                        qty = float(rest[0]) if rest else 1.0
                        if _strategy_inventory_allowed(symbol, signal, qty):
                            cl_ord_id = gateway.send_order(
                                OrderRequest(
                                    symbol=symbol,
                                    side=signal,
                                    qty=qty,
                                    account="",
                                    market=MarketType.EQUITIES,
                                )
                            )
                            logger.info("MarketData signal order sent, ClOrdID=%s", cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed market data processing: %s", exc)
            elif kind == "pos" and len(parts) == 2:
                _, symbol = parts
                logger.info("Position %s = %s", symbol, order_manager.get_position(symbol))
            elif kind == "open" and len(parts) == 1:
                open_orders = order_manager.get_open_orders()
                logger.info("Open orders: %s", [o.__dict__ for o in open_orders])
            elif kind == "checkacc" and len(parts) in {1, 2, 3}:
                symbol = "SBER"
                qty = 1.0
                if len(parts) >= 2:
                    symbol = parts[1]
                if len(parts) == 3:
                    qty = float(parts[2])
                try:
                    cl_ord_id = gateway.send_order(
                        OrderRequest(
                            symbol=symbol,
                            side="BUY",
                            qty=qty,
                            account="",
                            market=MarketType.EQUITIES,
                        )
                    )
                    account_check_pending.add(cl_ord_id)
                    logger.info(
                        "[CHECKACC] sent test order cl_ord_id=%s symbol=%s qty=%s",
                        cl_ord_id,
                        symbol,
                        qty,
                    )
                except Exception as exc:
                    logger.exception("[CHECKACC][FAIL] unable to send test order: %s", exc)
            elif kind == "metrics" and len(parts) == 1:
                metrics = analytics_api.get_metrics()
                logger.info(
                    "[METRICS] cumulative_pnl=%.4f max_drawdown=%.4f success=%.0f(%.2f%%) failed=%.0f(%.2f%%) success_fail_ratio=%.3f average_trade_pnl=%.4f total_trades=%.0f",
                    metrics["cumulative_pnl"],
                    metrics["max_drawdown"],
                    metrics["successful_trades"],
                    metrics["win_rate"],
                    metrics["failed_trades"],
                    metrics["fail_rate"],
                    metrics["success_to_failure_ratio"],
                    metrics["average_trade_pnl"],
                    metrics["total_trades"],
                )
            elif kind == "toplosers":
                limit = int(parts[1]) if len(parts) == 2 else 10
                logger.info("top_losing_trades=%s", analytics_api.top_losing_trades(limit=limit))
            elif kind == "topwinners":
                limit = int(parts[1]) if len(parts) == 2 else 10
                logger.info("top_profitable_trades=%s", analytics_api.top_profitable_trades(limit=limit))
            elif kind == "worstslip":
                limit = int(parts[1]) if len(parts) == 2 else 10
                logger.info("worst_slippage_trades=%s", analytics_api.worst_slippage_trades(limit=limit))
            elif kind == "pnlcomp":
                logger.info("pnl_by_component=%s", analytics_api.pnl_by_component())
            elif kind == "fillquality":
                logger.info("fill_quality_stats=%s", analytics_api.fill_quality_stats())
            elif kind == "lossanalysis":
                logger.info("trade_outcome_analysis=%s", analytics_api.trade_outcome_analysis())
            elif kind == "analyticscounters":
                logger.info(
                    "analytics_counters=%s",
                    {
                        "total_trades_processed_for_analytics": analytics_counters[
                            "total_trades_processed_for_analytics"
                        ],
                        "total_records_written": analytics_counters["total_records_written"],
                    },
                )
            elif kind == "backfillanalytics":
                limit = int(parts[1]) if len(parts) == 2 else 100
                backfill_result = economics_store.backfill_trade_analytics(limit=limit)
                logger.info("backfill_analytics_result=%s", backfill_result)
            elif kind == "analyticsmissing":
                limit = int(parts[1]) if len(parts) == 2 else 100
                logger.info("analytics_missing_fills=%s", analytics_api.analytics_missing_fills(limit=limit))
            elif kind == "entrycorr":
                logger.info("entry_score_pnl_correlation=%s", analytics_api.entry_score_pnl_correlation())
            elif kind == "cancelimpact":
                logger.info("missed_pnl_by_cancel_reason=%s", analytics_api.missed_pnl_by_cancel_reason())
            elif kind == "simprofile":
                if len(parts) == 1:
                    logger.info(
                        "[SIM][PROFILE] active=%s config=%s",
                        active_simulation_profile,
                        gateway.simulation_config_snapshot(),
                    )
                    continue
                if len(parts) != 2:
                    logger.warning("Bad command format.")
                    continue
                if not simulation_mode:
                    logger.warning("[SIM][PROFILE] ignored: SIMULATION_MODE is disabled")
                    continue
                requested_profile = normalize_simulation_profile(parts[1])
                active_simulation_profile, runtime_params, _ = resolve_simulation_profile(
                    base_params=simulation_stress_params,
                    configured_profile=configured_simulation_profile,
                    runtime_override_profile=requested_profile,
                )
                gateway.apply_simulation_config(
                    simulation_slippage_bps=float(runtime_params["simulation_slippage_bps"]),
                    simulation_slippage_max_bps=float(runtime_params["simulation_slippage_max_bps"]),
                    simulation_volatility_slippage_multiplier=float(
                        runtime_params["simulation_volatility_slippage_multiplier"]
                    ),
                    simulation_latency_network_ms=int(runtime_params["simulation_latency_network_ms"]),
                    simulation_latency_exchange_ms=int(runtime_params["simulation_latency_exchange_ms"]),
                    simulation_latency_jitter_ms=int(runtime_params["simulation_latency_jitter_ms"]),
                    simulation_fill_participation=float(runtime_params["simulation_fill_participation"]),
                    simulation_touch_fill_probability=float(runtime_params["simulation_touch_fill_probability"]),
                    simulation_passive_fill_probability_scale=float(
                        runtime_params["simulation_passive_fill_probability_scale"]
                    ),
                    simulation_adverse_selection_bias=float(runtime_params["simulation_adverse_selection_bias"]),
                    profile_name=active_simulation_profile,
                    source="runtime_command",
                )
            else:
                logger.warning("Bad command format.")
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        if failure_monitor is not None:
            failure_monitor.stop()
        time.sleep(0.2)
        initiator.stop()
        logging_runtime.listener.stop()


if __name__ == "__main__":
    run()
