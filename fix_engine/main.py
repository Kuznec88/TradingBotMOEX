from __future__ import annotations

import logging
import math
import sys
import time
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
                self.logger.info(
                    "[STRATEGY][SKIP] symbol=%s reason=spread_below_threshold spread=%.6f threshold=%.6f",
                    data.symbol,
                    data.spread,
                    self.min_spread_threshold,
                )
                return

            volatility = self._estimate_volatility(prices)
            if volatility > self.max_volatility_threshold:
                self._latest_signal_by_symbol[data.symbol] = "HOLD"
                self.logger.info(
                    "[STRATEGY][SKIP] symbol=%s reason=volatility_too_high volatility=%.6f threshold=%.6f",
                    data.symbol,
                    volatility,
                    self.max_volatility_threshold,
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
        logger.info("[LIVE_MD] source=FIX symbol=%s bid=%s ask=%s last=%s", symbol, bid, ask, last)


def setup_logging(base_dir: Path) -> logging.Logger:
    log_dir = base_dir / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("fix_engine")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = logging.FileHandler(log_dir / "fix_client.log", encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    return logger


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
    logger = setup_logging(base_dir)
    settings_path = base_dir / "settings.cfg"
    settings = _load_settings_with_resolved_dictionary(settings_path, logger)
    runtime_cfg_path = base_dir / "settings.runtime.cfg"
    cfg_for_optional = runtime_cfg_path if runtime_cfg_path.exists() else settings_path
    order_manager = OrderManager()
    market_data_engine = MarketDataEngine(logger=logger)
    economics_store = EconomicsStore(base_dir / "trade_economics.db")
    analytics_api = TradingAnalyticsAPI(economics_store)
    fee_equities_bps = _read_float(cfg_for_optional, "FeesBpsEquities", 0.0)
    fee_forts_bps = _read_float(cfg_for_optional, "FeesBpsForts", 0.0)
    economics = UnitEconomicsCalculator(
        fee_bps_by_market={
            MarketType.EQUITIES: _d(fee_equities_bps),
            MarketType.FORTS: _d(fee_forts_bps),
        }
    )
    risk_manager = RiskManager(
        order_manager=order_manager,
        max_exposure_per_instrument=_read_float(cfg_for_optional, "RiskMaxExposurePerInstrument", 1_000_000.0),
        max_trades_in_window=_read_int(cfg_for_optional, "RiskMaxTradesInWindow", 200),
        trades_window_seconds=_read_int(cfg_for_optional, "RiskTradesWindowSeconds", 60),
        cooldown_after_consecutive_losses=_read_int(cfg_for_optional, "RiskCooldownAfterConsecutiveLosses", 3),
        cooldown_seconds=_read_int(cfg_for_optional, "RiskCooldownSeconds", 30),
    )
    failure_handling_enabled = _read_bool(cfg_for_optional, "FailureHandlingEnabled", True)
    position_manager = PositionManager(
        order_manager=order_manager,
        max_abs_inventory_per_symbol=_read_float(cfg_for_optional, "MaxInventoryPerSymbol", 10.0),
    )

    failure_monitor: FailureMonitor | None = None

    def on_execution_report(message: fix.Message, source: str) -> None:
        if failure_monitor is not None:
            failure_monitor.on_execution_report()
        state = order_manager.on_execution_report(message)
        logger.info(
            "[FILL][%s] symbol=%s filled=%s avg_price=%s",
            source,
            state.get("symbol", ""),
            state.get("last_qty", "0"),
            state.get("avg_px", ""),
        )
        logger.info(
            "[STATE] order_id=%s %s -> %s",
            state.get("cl_ord_id", ""),
            state.get("status_old", ""),
            state.get("status_new", ""),
        )
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
            raw_last_px = (state.get("last_px", "") or "").strip()
            raw_avg_px = (state.get("avg_px", "") or "").strip()
            fill_px_str = raw_last_px or raw_avg_px or "0"
            fill_px = _d(fill_px_str)
            trade_record = economics.process_fill(
                market=_source_to_market(source),
                symbol=state.get("symbol", ""),
                side=state.get("side", "1"),
                qty=last_qty,
                price=fill_px,
                cl_ord_id=state.get("cl_ord_id", ""),
                exec_id=state.get("exec_id", ""),
            )
            if trade_record is not None:
                economics_store.insert_trade(trade_record)
                risk_manager.on_trade_result(float(trade_record["net_pnl"]))
                metrics = analytics_api.get_metrics()
                logger.info(
                    "[ECON] market=%s symbol=%s gross=%s fees=%s net=%s",
                    trade_record["market"],
                    trade_record["symbol"],
                    trade_record["gross_pnl"],
                    trade_record["fees"],
                    trade_record["net_pnl"],
                )
                logger.info(
                    "[METRICS] cumulative_pnl=%.4f max_drawdown=%.4f win_rate=%.2f average_trade_pnl=%.4f total_trades=%.0f",
                    metrics["cumulative_pnl"],
                    metrics["max_drawdown"],
                    metrics["win_rate"],
                    metrics["average_trade_pnl"],
                    metrics["total_trades"],
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
    trading_account_equities = _read_session_optional_setting_from_cfg(
        cfg_for_optional, "TradingAccount", target_comp_id="IFIX-EQ-UAT"
    )
    trading_account_forts = _read_session_optional_setting_from_cfg(
        cfg_for_optional, "TradingAccount", target_comp_id="FG"
    )
    if not trading_account_equities:
        logger.warning(
            "TradingAccount is not set in TRADE session. "
            "MOEX may reject orders with 'Invalid trading account'."
        )
    elif trading_account_equities == "MU9057200001":
        logger.warning(
            "TradingAccount is currently set to SenderCompID value (%s). "
            "MOEX usually requires a separate valid trading account/portfolio code.",
            trading_account_equities,
        )
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
    sim_slippage_bps = _read_float(cfg_for_optional, "SimulationSlippageBps", 2.0)
    sim_latency_network_ms = _read_int(cfg_for_optional, "SimulationNetworkLatencyMs", 20)
    sim_latency_exchange_ms = _read_int(cfg_for_optional, "SimulationExchangeLatencyMs", 30)
    sim_fill_participation = _read_float(cfg_for_optional, "SimulationFillParticipation", 0.25)
    strategy_min_spread = _read_float(cfg_for_optional, "StrategyMinSpreadThreshold", 0.0)
    strategy_max_volatility = _read_float(cfg_for_optional, "StrategyMaxVolatilityThreshold", 1.0)
    strategy_vol_window = _read_int(cfg_for_optional, "StrategyVolatilityWindow", 20)
    gateway = ExecutionGateway(
        equities_engine=equities_engine,
        forts_engine=forts_engine,
        order_manager=order_manager,
        logger=logger,
        simulation_mode=simulation_mode,
        get_latest_market_data=market_data_engine.get_latest,
        on_execution_report=on_execution_report,
        risk_manager=risk_manager,
        simulation_slippage_bps=sim_slippage_bps,
        simulation_latency_network_ms=sim_latency_network_ms,
        simulation_latency_exchange_ms=sim_latency_exchange_ms,
        simulation_fill_participation=sim_fill_participation,
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
        )
        market_data_engine.subscribe(market_maker.on_market_data)
        logger.info("[MM] enabled symbol=%s lot=%s market=%s", mm_symbol, mm_lot, mm_market.value)

    try:
        initiator.start()
    except Exception as exc:
        logger.exception("Failed to start FIX initiator: %s", exc)
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
        "  metrics\n"
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
                    cl_ord_id = gateway.send_order(OrderRequest(symbol=symbol, side=side, qty=float(qty)))
                    logger.info("Market order queued, ClOrdID=%s", cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed to send market order: %s", exc)
            elif kind == "m" and len(parts) == 5:
                _, market_raw, symbol, side, qty = parts
                try:
                    market = MarketType(market_raw.upper())
                    cl_ord_id = gateway.send_order(
                        OrderRequest(symbol=symbol, side=side, qty=float(qty), market=market)
                    )
                    logger.info("%s market order queued, ClOrdID=%s", market.value, cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed to send market order: %s", exc)
            elif kind == "l" and len(parts) == 5:
                _, symbol, side, qty, price = parts
                try:
                    cl_ord_id = gateway.send_order(
                        OrderRequest(symbol=symbol, side=side, qty=float(qty), price=float(price))
                    )
                    logger.info("Limit order queued, ClOrdID=%s", cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed to send limit order: %s", exc)
            elif kind == "l" and len(parts) == 6:
                _, market_raw, symbol, side, qty, price = parts
                try:
                    market = MarketType(market_raw.upper())
                    cl_ord_id = gateway.send_order(
                        OrderRequest(symbol=symbol, side=side, qty=float(qty), price=float(price), market=market)
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
                        cl_ord_id = gateway.send_order(
                            OrderRequest(symbol=symbol, side=signal, qty=float(qty), market=MarketType.EQUITIES)
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
                        cl_ord_id = gateway.send_order(
                            OrderRequest(symbol=symbol, side=signal, qty=qty, market=MarketType.EQUITIES)
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
            elif kind == "metrics" and len(parts) == 1:
                metrics = analytics_api.get_metrics()
                logger.info(
                    "[METRICS] cumulative_pnl=%.4f max_drawdown=%.4f win_rate=%.2f average_trade_pnl=%.4f total_trades=%.0f",
                    metrics["cumulative_pnl"],
                    metrics["max_drawdown"],
                    metrics["win_rate"],
                    metrics["average_trade_pnl"],
                    metrics["total_trades"],
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


if __name__ == "__main__":
    run()
