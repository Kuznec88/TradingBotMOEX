from __future__ import annotations

import json
import logging
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from threading import RLock

import quickfix as fix

from analytics_api import TradingAnalyticsAPI
from economics_store import EconomicsStore
from execution_gateway import ExecutionGateway
from failure_monitor import FailureMonitor
from market_data.market_data_engine import MarketDataEngine
from market_data.models import MarketData
from market_making import BasicMarketMaker
from order_manager import ManagedOrder, OrderManager
from order_models import MarketType, OrderRequest
from position_manager import PositionManager
from risk_manager import RiskManager
from unit_economics import UnitEconomicsCalculator


class _NoopExecutionEngine:
    def send_order(self, symbol: str, side: str | int, qty: float, price: float | None = None) -> str:
        raise RuntimeError("Unexpected FIX path: simulation mode must handle orders")

    def cancel_order(self, cl_ord_id: str) -> str:
        raise RuntimeError("Unexpected FIX path: simulation mode must handle cancels")


@dataclass(frozen=True)
class E2EConfig:
    symbol: str = "SBER"
    seed: int = 42
    tick_count: int = 120
    tick_interval_sec: float = 0.03
    base_price: float = 250.0
    base_spread: float = 0.04
    lot_size: float = 1.0
    simulation_slippage_bps: float = 2.0
    simulation_latency_network_ms: int = 120
    simulation_latency_exchange_ms: int = 180
    simulation_fill_participation: float = 0.25
    volatility_spike_every: int = 25
    volatility_spike_size: float = 1.20
    max_exposure_per_instrument: float = 6.0
    max_trades_in_window: int = 500
    trades_window_seconds: int = 60
    cooldown_after_consecutive_losses: int = 1000
    cooldown_seconds: int = 5
    max_abs_inventory_per_symbol: float = 4.0
    failure_action: str = "ALERT"
    max_market_data_staleness_sec: int = 3
    max_order_stuck_sec: int = 3
    max_no_execution_report_sec: int = 3
    negative_pnl_spike_threshold: float = -100.0
    db_path: str = "e2e_validation.db"
    economics_db_path: str = "e2e_economics.db"


class DeterministicMarketDataFeed:
    def __init__(self, *, config: E2EConfig) -> None:
        self._cfg = config
        self._rng = random.Random(config.seed)
        self._tick = 0
        self._last_mid = config.base_price

    def next_tick(self) -> dict[str, object]:
        self._tick += 1
        drift = (self._rng.random() - 0.5) * 0.08
        spike = 0.0
        if self._cfg.volatility_spike_every > 0 and self._tick % self._cfg.volatility_spike_every == 0:
            spike_sign = -1.0 if (self._tick // self._cfg.volatility_spike_every) % 2 == 0 else 1.0
            spike = spike_sign * self._cfg.volatility_spike_size
        mid = max(1.0, self._last_mid + drift + spike)
        spread_noise = self._rng.random() * 0.02
        spread = max(0.01, self._cfg.base_spread + spread_noise)
        bid = mid - spread / 2.0
        ask = mid + spread / 2.0
        volume = 30.0 + float((self._tick % 7) * 5)
        self._last_mid = mid
        return {
            "symbol": self._cfg.symbol,
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "last": round(mid, 4),
            "volume": volume,
            "timestamp": datetime.now(timezone.utc),
        }


class ValidationStore:
    def __init__(self, db_path: str | Path) -> None:
        self._path = str(db_path)
        self._lock = RLock()
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self._path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    bid REAL NOT NULL,
                    ask REAL NOT NULL,
                    last REAL NOT NULL,
                    volume REAL NOT NULL,
                    ts TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    cl_ord_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty REAL NOT NULL,
                    price REAL,
                    created_at TEXT NOT NULL,
                    final_status TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cl_ord_id TEXT NOT NULL,
                    status_old TEXT NOT NULL,
                    status_new TEXT NOT NULL,
                    event_ts TEXT NOT NULL,
                    FOREIGN KEY(cl_ord_id) REFERENCES orders(cl_ord_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    exec_id TEXT PRIMARY KEY,
                    cl_ord_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty REAL NOT NULL,
                    price REAL NOT NULL,
                    net_pnl REAL NOT NULL,
                    ts TEXT NOT NULL,
                    FOREIGN KEY(cl_ord_id) REFERENCES orders(cl_ord_id)
                )
                """
            )
            conn.commit()

    def insert_market_data(self, data: MarketData) -> None:
        with self._lock, sqlite3.connect(self._path) as conn:
            conn.execute(
                """
                INSERT INTO market_data(symbol, bid, ask, last, volume, ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    data.symbol,
                    float(data.bid),
                    float(data.ask),
                    float(data.last),
                    float(data.volume),
                    data.timestamp.isoformat(),
                ),
            )
            conn.commit()

    def upsert_order(self, order: ManagedOrder) -> None:
        with self._lock, sqlite3.connect(self._path) as conn:
            conn.execute(
                """
                INSERT INTO orders(cl_ord_id, symbol, side, qty, price, created_at, final_status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cl_ord_id) DO UPDATE SET
                    final_status=excluded.final_status
                """,
                (
                    order.cl_ord_id,
                    order.symbol,
                    order.side,
                    float(order.qty),
                    float(order.price) if order.price is not None else None,
                    order.created_at.isoformat(),
                    order.status,
                ),
            )
            conn.commit()

    def insert_order_event(self, cl_ord_id: str, old: str, new: str, event_ts: datetime) -> None:
        with self._lock, sqlite3.connect(self._path) as conn:
            conn.execute(
                """
                INSERT INTO order_events(cl_ord_id, status_old, status_new, event_ts)
                VALUES (?, ?, ?, ?)
                """,
                (cl_ord_id, old, new, event_ts.isoformat()),
            )
            conn.commit()

    def insert_trade(
        self,
        *,
        exec_id: str,
        cl_ord_id: str,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        net_pnl: float,
        ts: datetime,
    ) -> None:
        with self._lock, sqlite3.connect(self._path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO trades(exec_id, cl_ord_id, symbol, side, qty, price, net_pnl, ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (exec_id, cl_ord_id, symbol, side, qty, price, net_pnl, ts.isoformat()),
            )
            conn.commit()

    def counts(self) -> dict[str, int]:
        with self._lock, sqlite3.connect(self._path) as conn:
            md_count = int(conn.execute("SELECT COUNT(1) FROM market_data").fetchone()[0])
            order_count = int(conn.execute("SELECT COUNT(1) FROM orders").fetchone()[0])
            trade_count = int(conn.execute("SELECT COUNT(1) FROM trades").fetchone()[0])
        return {"market_data": md_count, "orders": order_count, "trades": trade_count}


class PositionPnlTracker:
    def __init__(self) -> None:
        self._qty = 0.0
        self._avg_price = 0.0
        self.realized_pnl = 0.0

    @property
    def qty(self) -> float:
        return self._qty

    @property
    def avg_price(self) -> float:
        return self._avg_price

    def apply_fill(self, *, side: str, qty: float, price: float) -> None:
        sign = 1.0 if side == "1" else -1.0
        fill_qty = abs(float(qty))
        fill_px = float(price)
        if fill_qty <= 0:
            return

        if self._qty == 0 or (self._qty > 0 and sign > 0) or (self._qty < 0 and sign < 0):
            new_qty = self._qty + sign * fill_qty
            if abs(new_qty) > 1e-9:
                gross_notional = abs(self._qty) * self._avg_price + fill_qty * fill_px
                self._avg_price = gross_notional / abs(new_qty)
            self._qty = new_qty
            return

        closing_qty = min(abs(self._qty), fill_qty)
        if self._qty > 0 and sign < 0:
            self.realized_pnl += (fill_px - self._avg_price) * closing_qty
        elif self._qty < 0 and sign > 0:
            self.realized_pnl += (self._avg_price - fill_px) * closing_qty
        self._qty += sign * fill_qty

        if abs(self._qty) <= 1e-9:
            self._qty = 0.0
            self._avg_price = 0.0
        elif (self._qty > 0 and sign > 0) or (self._qty < 0 and sign < 0):
            self._avg_price = fill_px

    def unrealized_pnl(self, mark_price: float) -> float:
        if self._qty == 0:
            return 0.0
        if self._qty > 0:
            return (float(mark_price) - self._avg_price) * self._qty
        return (self._avg_price - float(mark_price)) * abs(self._qty)


class TestOrchestrator:
    def __init__(self, config: E2EConfig, logger: logging.Logger) -> None:
        self.cfg = config
        self.logger = logger
        self.order_manager = OrderManager()
        self.market_data_engine = MarketDataEngine(logger=logger)
        self.validation_store = ValidationStore(config.db_path)
        self.economics_store = EconomicsStore(config.economics_db_path)
        self.analytics_api = TradingAnalyticsAPI(self.economics_store)
        self.economics = UnitEconomicsCalculator(
            fee_bps_by_market={
                MarketType.EQUITIES: Decimal("0.00"),
                MarketType.FORTS: Decimal("0.00"),
            }
        )
        self.risk_manager = RiskManager(
            order_manager=self.order_manager,
            max_exposure_per_instrument=config.max_exposure_per_instrument,
            max_trades_in_window=config.max_trades_in_window,
            trades_window_seconds=config.trades_window_seconds,
            cooldown_after_consecutive_losses=config.cooldown_after_consecutive_losses,
            cooldown_seconds=config.cooldown_seconds,
        )
        self.position_manager = PositionManager(
            order_manager=self.order_manager,
            max_abs_inventory_per_symbol=config.max_abs_inventory_per_symbol,
        )
        self.gateway = ExecutionGateway(
            equities_engine=_NoopExecutionEngine(),  # type: ignore[arg-type]
            forts_engine=_NoopExecutionEngine(),  # type: ignore[arg-type]
            order_manager=self.order_manager,
            logger=logger,
            simulation_mode=True,
            get_latest_market_data=self.market_data_engine.get_latest,
            on_execution_report=self._on_execution_report,
            risk_manager=self.risk_manager,
            simulation_slippage_bps=config.simulation_slippage_bps,
            simulation_latency_network_ms=config.simulation_latency_network_ms,
            simulation_latency_exchange_ms=config.simulation_latency_exchange_ms,
            simulation_fill_participation=config.simulation_fill_participation,
            simulation_rng=random.Random(config.seed),
        )
        self.market_maker = BasicMarketMaker(
            symbol=config.symbol,
            lot_size=config.lot_size,
            market=MarketType.EQUITIES,
            gateway=self.gateway,
            position_manager=self.position_manager,
            logger=logger,
        )
        self.failure_monitor = FailureMonitor(
            logger=logger,
            order_manager=self.order_manager,
            gateway=self.gateway,
            watch_symbol=config.symbol,
            max_market_data_staleness_sec=config.max_market_data_staleness_sec,
            max_order_stuck_sec=config.max_order_stuck_sec,
            max_no_execution_report_sec=config.max_no_execution_report_sec,
            action_on_anomaly=config.failure_action,
            check_interval_sec=1,
        )
        self.feed = DeterministicMarketDataFeed(config=config)
        self.pnl_tracker = PositionPnlTracker()

        self._known_orders: set[str] = set()
        self._rejected_orders = 0
        self._risk_rejected_orders = 0
        self._illegal_transitions: list[str] = []
        self._timestamp_errors: list[str] = []
        self._missing_exec_reports: list[str] = []
        self._negative_pnl_spikes: list[str] = []
        self._position_mismatch: list[str] = []
        self._anomalies: list[str] = []
        self._order_last_event_ts: dict[str, datetime] = {}
        self._order_exec_count: dict[str, int] = {}

        self._install_subscriptions()
        self._install_gateway_audit_hook()

    def _install_subscriptions(self) -> None:
        self.market_data_engine.subscribe(self.market_maker.on_market_data)
        self.market_data_engine.subscribe(self.gateway.on_market_data)
        self.market_data_engine.subscribe(self.failure_monitor.on_market_data)
        self.market_data_engine.subscribe(self.validation_store.insert_market_data)

    def _install_gateway_audit_hook(self) -> None:
        original_send_order = self.gateway.send_order

        def audited_send_order(request: OrderRequest) -> str:
            try:
                cl_ord_id = original_send_order(request)
                order = self.order_manager.get_order(cl_ord_id)
                if order is not None:
                    self.validation_store.upsert_order(order)
                    self._known_orders.add(order.cl_ord_id)
                    self.logger.info(
                        "[E2E][ORDER_CREATE] order_id=%s symbol=%s side=%s qty=%s",
                        order.cl_ord_id,
                        order.symbol,
                        order.side,
                        order.qty,
                    )
                return cl_ord_id
            except RuntimeError as exc:
                if "RiskRejected" in str(exc):
                    self._risk_rejected_orders += 1
                raise

        self.gateway.send_order = audited_send_order  # type: ignore[method-assign]

    def _on_execution_report(self, message: fix.Message, source: str) -> None:
        self.failure_monitor.on_execution_report()
        event_ts = datetime.now(timezone.utc)
        state = self.order_manager.on_execution_report(message)
        if "error" in state:
            self._anomalies.append(f"bad_exec_report:{state['error']}")
            self.logger.error("[E2E][ANOMALY] %s", self._anomalies[-1])
            return

        cl_ord_id = state.get("cl_ord_id", "")
        old = state.get("status_old", "")
        new = state.get("status_new", "")
        self.logger.info(
            "[E2E][EXEC] source=%s order_id=%s %s->%s",
            source,
            cl_ord_id,
            old,
            new,
        )
        self._validate_transition(cl_ord_id, old, new)
        self.validation_store.insert_order_event(cl_ord_id, old, new, event_ts)

        order = self.order_manager.get_order(cl_ord_id)
        if order is not None:
            self.validation_store.upsert_order(order)

        last_event_ts = self._order_last_event_ts.get(cl_ord_id)
        if last_event_ts is not None and event_ts < last_event_ts:
            msg = f"timestamp_regression order_id={cl_ord_id}"
            self._timestamp_errors.append(msg)
            self.logger.error("[E2E][ANOMALY] %s", msg)
        self._order_last_event_ts[cl_ord_id] = event_ts
        self._order_exec_count[cl_ord_id] = self._order_exec_count.get(cl_ord_id, 0) + 1

        if new == "REJECTED":
            self._rejected_orders += 1

        last_qty = float(state.get("last_qty", "0") or "0")
        if last_qty <= 0:
            return
        last_px = float(state.get("last_px", "0") or "0")
        side = state.get("side", "1")
        symbol = state.get("symbol", self.cfg.symbol)

        self.pnl_tracker.apply_fill(side=side, qty=last_qty, price=last_px)
        trade_record = self.economics.process_fill(
            market=MarketType.EQUITIES,
            symbol=symbol,
            side=side,
            qty=Decimal(str(last_qty)),
            price=Decimal(str(last_px)),
            cl_ord_id=cl_ord_id,
            exec_id=state.get("exec_id", ""),
        )
        if trade_record is None:
            return
        self.economics_store.insert_trade(trade_record)
        net_pnl = float(trade_record["net_pnl"])
        if net_pnl <= self.cfg.negative_pnl_spike_threshold:
            msg = f"negative_pnl_spike order_id={cl_ord_id} net_pnl={net_pnl:.4f}"
            self._negative_pnl_spikes.append(msg)
            self.logger.error("[E2E][ANOMALY] %s", msg)
        self.validation_store.insert_trade(
            exec_id=trade_record["exec_id"],
            cl_ord_id=trade_record["cl_ord_id"],
            symbol=trade_record["symbol"],
            side=trade_record["side"],
            qty=float(trade_record["qty"]),
            price=float(trade_record["price"]),
            net_pnl=float(trade_record["net_pnl"]),
            ts=event_ts,
        )

    def _validate_transition(self, cl_ord_id: str, old: str, new: str) -> None:
        allowed: dict[str, set[str]] = {
            "NEW": {"SENT", "PENDING_NEW", "REJECTED", "CANCELED"},
            "SENT": {"SENT", "PENDING_NEW", "PARTIALLY_FILLED", "FILLED", "REJECTED", "CANCELED"},
            "PENDING_NEW": {"PENDING_NEW", "PARTIALLY_FILLED", "FILLED", "REJECTED", "CANCELED"},
            "PARTIALLY_FILLED": {"PARTIALLY_FILLED", "FILLED", "REJECTED", "CANCELED"},
            "FILLED": set(),
            "CANCELED": set(),
            "REJECTED": set(),
        }
        if old not in allowed:
            msg = f"unknown_old_status order_id={cl_ord_id} old={old} new={new}"
            self._illegal_transitions.append(msg)
            self.logger.error("[E2E][ANOMALY] %s", msg)
            return
        if new not in allowed[old] and old != new:
            msg = f"illegal_transition order_id={cl_ord_id} old={old} new={new}"
            self._illegal_transitions.append(msg)
            self.logger.error("[E2E][ANOMALY] %s", msg)

    def run(self) -> dict[str, object]:
        random.seed(self.cfg.seed)
        self.failure_monitor.start()
        start_ts = datetime.now(timezone.utc)
        self.logger.info("[E2E] start symbol=%s tick_count=%s seed=%s", self.cfg.symbol, self.cfg.tick_count, self.cfg.seed)

        try:
            for _ in range(self.cfg.tick_count):
                raw = self.feed.next_tick()
                self.market_data_engine.update_market_data(raw)
                time.sleep(self.cfg.tick_interval_sec)
        finally:
            self.failure_monitor.stop()

        self._finalize_and_validate()
        end_ts = datetime.now(timezone.utc)
        metrics = self.analytics_api.get_metrics()
        last_md = self.market_data_engine.get_latest(self.cfg.symbol)
        unrealized_pnl = self.pnl_tracker.unrealized_pnl(last_md.mid_price) if last_md else 0.0

        result = {
            "session_start": start_ts.isoformat(),
            "session_end": end_ts.isoformat(),
            "duration_sec": round((end_ts - start_ts).total_seconds(), 3),
            "total_trades": int(metrics["total_trades"]),
            "total_pnl": float(metrics["total_pnl"]),
            "avg_pnl_per_trade": float(metrics["avg_pnl_per_trade"]),
            "win_rate": float(metrics["win_rate"]),
            "max_drawdown": float(metrics["max_drawdown"]),
            "rejected_orders": int(self._rejected_orders + self._risk_rejected_orders),
            "risk_rejected_orders": int(self._risk_rejected_orders),
            "position_qty": round(self.position_manager.get_inventory(self.cfg.symbol), 6),
            "avg_price": round(self.pnl_tracker.avg_price, 6),
            "realized_pnl": round(self.pnl_tracker.realized_pnl, 6),
            "unrealized_pnl": round(unrealized_pnl, 6),
            "persistence_counts": self.validation_store.counts(),
            "anomalies": {
                "illegal_transitions": self._illegal_transitions,
                "timestamp_errors": self._timestamp_errors,
                "missing_execution_reports": self._missing_exec_reports,
                "position_mismatch": self._position_mismatch,
                "negative_pnl_spikes": self._negative_pnl_spikes,
                "other": self._anomalies,
            },
        }
        self.logger.info("[E2E][RESULT] %s", json.dumps(result, ensure_ascii=True))
        return result

    def _finalize_and_validate(self) -> None:
        open_orders = self.order_manager.get_open_orders()
        now = datetime.now(timezone.utc)
        for order in open_orders:
            if order.status in {"SENT", "PENDING_NEW"}:
                age = (now - order.created_at).total_seconds()
                msg = f"stuck_order order_id={order.cl_ord_id} status={order.status} age_sec={age:.2f}"
                self._anomalies.append(msg)
                self.logger.error("[E2E][ANOMALY] %s", msg)

        for cl_ord_id in self._known_orders:
            order = self.order_manager.get_order(cl_ord_id)
            if order is None:
                continue
            if order.status in {"PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED"}:
                if self._order_exec_count.get(cl_ord_id, 0) == 0:
                    msg = f"missing_exec_reports order_id={cl_ord_id} final_status={order.status}"
                    self._missing_exec_reports.append(msg)
                    self.logger.error("[E2E][ANOMALY] %s", msg)
            self.validation_store.upsert_order(order)

        expected_qty = self.pnl_tracker.qty
        actual_qty = self.position_manager.get_inventory(self.cfg.symbol)
        if abs(expected_qty - actual_qty) > 1e-9:
            msg = (
                f"position_mismatch symbol={self.cfg.symbol} expected={expected_qty:.6f} actual={actual_qty:.6f}"
            )
            self._position_mismatch.append(msg)
            self.logger.error("[E2E][ANOMALY] %s", msg)

        counts = self.validation_store.counts()
        if counts["market_data"] == 0:
            self._anomalies.append("persistence_missing_market_data")
        if counts["orders"] == 0:
            self._anomalies.append("persistence_missing_orders")
        if counts["trades"] == 0:
            self._anomalies.append("persistence_missing_trades")
        for issue in self._anomalies:
            self.logger.error("[E2E][ANOMALY] %s", issue)


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("e2e_test")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def run_e2e(config: E2EConfig | None = None) -> dict[str, object]:
    cfg = config or E2EConfig()
    logger = _build_logger()
    orchestrator = TestOrchestrator(config=cfg, logger=logger)
    return orchestrator.run()


if __name__ == "__main__":
    result_data = run_e2e()
    print(json.dumps(result_data, ensure_ascii=True, indent=2))
