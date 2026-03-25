from __future__ import annotations

import csv
import argparse
import json
import logging
import os
import random
import sqlite3
import sys
import time
import types
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from threading import RLock

try:
    import quickfix as fix
except ModuleNotFoundError:
    class _Field:
        TAG = 0

        def __init__(self, value: object) -> None:
            self.value = value

        @property
        def tag(self) -> int:
            return int(self.TAG)

    class _Header:
        def __init__(self) -> None:
            self._fields: dict[int, str] = {}

        def setField(self, field: object) -> None:
            tag = int(getattr(field, "tag", 0))
            value = str(getattr(field, "value", ""))
            self._fields[tag] = value

    class _Message:
        def __init__(self) -> None:
            self._header = _Header()
            self._fields: dict[int, str] = {}

        def getHeader(self) -> _Header:
            return self._header

        def setField(self, field: object) -> None:
            tag = int(getattr(field, "tag", 0))
            value = str(getattr(field, "value", ""))
            self._fields[tag] = value

        def isSetField(self, tag: int) -> bool:
            return int(tag) in self._fields

        def getField(self, tag: int) -> str:
            return self._fields.get(int(tag), "")

        def toString(self) -> str:
            parts = [f"{k}={v}" for k, v in sorted(self._fields.items())]
            return "|".join(parts)

    def _mk_field(tag: int):
        return type(f"Field{tag}", (_Field,), {"TAG": int(tag)})

    _fix = types.ModuleType("quickfix")
    _fix.MsgType_ExecutionReport = "8"
    _fix.Message = _Message
    _fix.MsgType = _mk_field(35)
    _fix.ClOrdID = _mk_field(11)
    _fix.ExecID = _mk_field(17)
    _fix.OrderID = _mk_field(37)
    _fix.Symbol = _mk_field(55)
    _fix.Side = _mk_field(54)
    _fix.OrderQty = _mk_field(38)
    _fix.CumQty = _mk_field(14)
    _fix.LeavesQty = _mk_field(151)
    _fix.AvgPx = _mk_field(6)
    _fix.LastQty = _mk_field(32)
    _fix.LastPx = _mk_field(31)
    _fix.Text = _mk_field(58)

    class _StringField(_Field):
        TAG = 0

        def __init__(self, tag: int, value: object) -> None:
            self.TAG = int(tag)
            super().__init__(value)

    _fix.StringField = _StringField
    sys.modules["quickfix"] = _fix
    _fix44 = types.ModuleType("quickfix44")

    class _DummyMsg:
        def __init__(self) -> None:
            self._fields: list[object] = []
            self._groups: list[object] = []

        def setField(self, field: object) -> None:
            self._fields.append(field)

        def addGroup(self, group: object) -> None:
            self._groups.append(group)

    _fix44.NewOrderSingle = _DummyMsg
    _fix44.OrderCancelRequest = _DummyMsg
    sys.modules["quickfix44"] = _fix44
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
    def send_order(
        self,
        symbol: str,
        side: str | int,
        qty: float,
        account: str | None = None,
        price: float | None = None,
    ) -> str:
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
    simulation_slippage_bps: float = 3.0
    simulation_slippage_max_bps: float = 30.0
    simulation_volatility_slippage_multiplier: float = 2.0
    simulation_latency_network_ms: int = 120
    simulation_latency_exchange_ms: int = 180
    simulation_latency_jitter_ms: int = 60
    simulation_fill_participation: float = 0.25
    simulation_touch_fill_probability: float = 0.12
    simulation_passive_fill_probability_scale: float = 0.45
    simulation_adverse_selection_bias: float = 0.45
    volatility_spike_every: int = 25
    volatility_spike_size: float = 1.20
    max_exposure_per_instrument: float = 6.0
    max_trades_in_window: int = 500
    trades_window_seconds: int = 60
    cooldown_after_consecutive_losses: int = 1000
    cooldown_seconds: int = 5
    max_loss_per_trade: float = 0.0
    mm_volatility_window_ticks: int = 20
    mm_max_short_term_volatility: float = 0.0
    mm_cancel_on_high_volatility: bool = True
    mm_resting_order_timeout_sec: float = 2.0
    mm_tick_size: float = 0.01
    mm_replace_threshold_ticks: int = 2
    mm_replace_cancel_threshold_ticks: float = 2.0
    mm_replace_keep_threshold_ticks: float = 1.0
    mm_replace_persist_ms: int = 220
    mm_adverse_move_cancel_ticks: int = 3
    mm_fast_cancel_keep_ticks: float = 1.5
    mm_fast_cancel_persist_ms: int = 180
    mm_price_tolerance_ticks: float = 0.0
    mm_price_tolerance_pct: float = 0.0
    mm_min_order_lifetime_ms: int = 200
    mm_cancel_replace_cooldown_ms: int = 120
    mm_entry_min_spread: float = 0.0
    mm_entry_stability_window_ticks: int = 8
    mm_entry_max_bid_ask_move_ticks: float = 2.0
    mm_entry_anti_trend_threshold: float = 0.0
    mm_entry_direction_window_ticks: int = 12
    mm_entry_direction_min_move_ticks: float = 1.0
    mm_trade_cooldown_ms: int = 300
    mm_entry_min_place_interval_ms: int = 200
    mm_entry_score_threshold: float = 0.6
    mm_entry_score_spread_threshold: float = 0.02
    mm_entry_score_w_spread: float = 0.35
    mm_entry_score_w_stability: float = 0.25
    mm_entry_score_w_trend: float = 0.25
    mm_entry_score_w_imbalance: float = 0.15
    mm_entry_score_cooldown_penalty_max: float = 0.35
    mm_adaptive_entry_learning_enabled: bool = False
    mm_adaptive_entry_learning_window: int = 100
    mm_adaptive_entry_learning_min_bin_trades: int = 10
    mm_adaptive_entry_learning_step_up: float = 0.01
    mm_adaptive_entry_learning_step_down: float = 0.01
    mm_adaptive_entry_learning_max_step_per_update: float = 0.02
    mm_adaptive_entry_learning_threshold_min: float = 0.4
    mm_adaptive_entry_learning_threshold_max: float = 0.95
    mm_adaptive_entry_learning_drift_alert: float = 0.20
    mm_adaptive_entry_learning_perf_alert_delta: float = 0.15
    mm_trend_window_ticks: int = 12
    mm_trend_strength_threshold: float = 0.0
    mm_cancel_on_strong_trend: bool = False
    mm_post_fill_horizon_ms: int = 200
    mm_adverse_fill_window: int = 25
    mm_adverse_fill_rate_threshold: float = 0.65
    mm_defensive_quote_offset_ticks: int = 1
    mm_decision_confirmation_updates: int = 2
    mm_min_decision_interval_ms: int = 150
    mm_decision_batch_ticks: int = 3
    mm_cancel_impact_horizon_ms: int = 500
    mm_cancel_reason_summary_every: int = 20
    mm_disable_price_move_cancel: bool = False
    max_abs_inventory_per_symbol: float = 4.0
    soft_abs_inventory_per_symbol: float = 3.2
    failure_action: str = "ALERT"
    max_market_data_staleness_sec: int = 3
    max_order_stuck_sec: int = 3
    max_no_execution_report_sec: int = 3
    negative_pnl_spike_threshold: float = -100.0
    session_stats_snapshot_every_trades: int = 10
    frequency_window_minutes: int = 5
    frequency_log_interval_sec: float = 60.0
    low_frequency_threshold_per_min: float = 1.0
    edge_eval_every_round_trips: int = 20
    edge_adverse_rate_threshold: float = 0.5
    live_metrics_every_trades: int = 10
    db_path: str = "e2e_validation.db"
    economics_db_path: str = "e2e_economics.db"
    # Historical replay (CSV from tools/fetch_tbank_candles_history.py: time_utc, open, high, low, close, volume, …)
    historical_candles_csv: str = ""
    replay_tick_limit: int = 0
    replay_synthetic_spread: float = 0.04
    strategy_mode: str = "MICROPRICE"
    momentum_params_path: str = ""


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
        bid_size = max(1.0, volume * (0.8 + 0.6 * self._rng.random()))
        ask_size = max(1.0, volume * (0.8 + 0.6 * self._rng.random()))
        self._last_mid = mid
        return {
            "symbol": self._cfg.symbol,
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "last": round(mid, 4),
            "volume": volume,
            "bid_size": round(bid_size, 4),
            "ask_size": round(ask_size, 4),
            "timestamp": datetime.now(timezone.utc),
        }


class CandleCsvFeed:
    """One market-data tick per CSV row: synthetic bid/ask around bar mid (replay timestamps advance monotonically)."""

    def __init__(self, *, config: E2EConfig) -> None:
        raw = (config.historical_candles_csv or "").strip()
        path = Path(raw)
        if not path.is_absolute():
            path = Path(__file__).resolve().parent / path
        if not path.is_file():
            raise FileNotFoundError(f"historical_candles_csv not found: {path}")
        self._cfg = config
        self._path = path
        self._rows: list[dict[str, str]] = []
        with path.open(encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                self._rows.append(row)
        if not self._rows:
            raise ValueError(f"candle CSV is empty: {path}")
        self._i = 0
        self._t0 = datetime.now(timezone.utc)

    def __len__(self) -> int:
        return len(self._rows)

    def next_tick(self) -> dict[str, object]:
        if self._i >= len(self._rows):
            raise RuntimeError("CandleCsvFeed.next_tick past end (tick_count mismatch)")
        row = self._rows[self._i]
        self._i += 1
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        vol_raw = row.get("volume", "0") or "0"
        try:
            vol = float(vol_raw)
        except ValueError:
            vol = 0.0
        mid = (high + low) / 2.0
        sp = max(0.01, float(self._cfg.replay_synthetic_spread))
        bid = mid - sp / 2.0
        ask = mid + sp / 2.0
        ts = self._t0 + timedelta(milliseconds=50 * self._i)
        return {
            "symbol": self._cfg.symbol,
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "last": round(close, 4),
            "volume": max(1.0, vol),
            "bid_size": 100.0,
            "ask_size": 100.0,
            "timestamp": ts,
        }


def _resolve_optional_path_under_fix_engine(path_str: str) -> str:
    s = path_str.strip()
    if not s:
        return ""
    p = Path(s)
    if not p.is_absolute():
        p = Path(__file__).resolve().parent / p
    return str(p)


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
        if not trade_id:
            return
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


class TestOrchestrator:
    def __init__(self, config: E2EConfig, logger: logging.Logger) -> None:
        self.cfg = config
        self.logger = logger
        self.order_manager = OrderManager()
        self.market_data_engine = MarketDataEngine(logger=logger)
        self.validation_store = ValidationStore(config.db_path)
        self.economics_store = EconomicsStore(config.economics_db_path)
        self.analytics_api = TradingAnalyticsAPI(self.economics_store)
        self.adverse_tracker = FillAdverseSelectionTracker(self.economics_store)
        self.economics = UnitEconomicsCalculator(
            fee_bps_by_market={
                MarketType.EQUITIES: Decimal("0.00"),
                MarketType.FORTS: Decimal("0.00"),
            },
            fixed_fee_by_market={
                MarketType.EQUITIES: Decimal("0.00"),
                MarketType.FORTS: Decimal("0.00"),
            },
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
            soft_abs_inventory_per_symbol=config.soft_abs_inventory_per_symbol,
        )
        self.gateway = ExecutionGateway(
            equities_engine=_NoopExecutionEngine(),  # type: ignore[arg-type]
            forts_engine=_NoopExecutionEngine(),  # type: ignore[arg-type]
            order_manager=self.order_manager,
            logger=logger,
            simulation_mode=False,
            get_latest_market_data=self.market_data_engine.get_latest,
            on_execution_report=self._on_execution_report,
            risk_manager=self.risk_manager,
            position_manager=self.position_manager,
            simulation_slippage_bps=float(config.simulation_slippage_bps),
            simulation_slippage_max_bps=float(config.simulation_slippage_max_bps),
            simulation_volatility_slippage_multiplier=float(config.simulation_volatility_slippage_multiplier),
            simulation_latency_network_ms=int(config.simulation_latency_network_ms),
            simulation_latency_exchange_ms=int(config.simulation_latency_exchange_ms),
            simulation_latency_jitter_ms=int(config.simulation_latency_jitter_ms),
            simulation_fill_participation=float(config.simulation_fill_participation),
            simulation_touch_fill_probability=float(config.simulation_touch_fill_probability),
            simulation_passive_fill_probability_scale=float(config.simulation_passive_fill_probability_scale),
            simulation_adverse_selection_bias=float(config.simulation_adverse_selection_bias),
            simulation_rng=random.Random(config.seed),
            account_by_market={
                MarketType.EQUITIES: "SIM-EQ-ACCOUNT",
                MarketType.FORTS: "SIM-FO-ACCOUNT",
            },
            execution_mode="PAPER_REAL_MARKET",
        )
        self.logger.info(
            "[E2E][PAPER] synthetic_fill_config=%s",
            self.gateway.simulation_config_snapshot(),
        )
        self.market_maker = BasicMarketMaker(
            symbol=config.symbol,
            lot_size=config.lot_size,
            market=MarketType.EQUITIES,
            gateway=self.gateway,
            position_manager=self.position_manager,
            logger=logger,
            max_loss_per_trade=config.max_loss_per_trade,
            volatility_window_ticks=config.mm_volatility_window_ticks,
            max_short_term_volatility=config.mm_max_short_term_volatility,
            cancel_on_high_volatility=config.mm_cancel_on_high_volatility,
            resting_order_timeout_sec=config.mm_resting_order_timeout_sec,
            tick_size=config.mm_tick_size,
            replace_threshold_ticks=config.mm_replace_threshold_ticks,
            replace_cancel_threshold_ticks=config.mm_replace_cancel_threshold_ticks,
            replace_keep_threshold_ticks=config.mm_replace_keep_threshold_ticks,
            replace_persist_ms=config.mm_replace_persist_ms,
            adverse_move_cancel_ticks=config.mm_adverse_move_cancel_ticks,
            fast_cancel_keep_ticks=config.mm_fast_cancel_keep_ticks,
            fast_cancel_persist_ms=config.mm_fast_cancel_persist_ms,
            price_tolerance_ticks=config.mm_price_tolerance_ticks,
            price_tolerance_pct=config.mm_price_tolerance_pct,
            min_order_lifetime_ms=config.mm_min_order_lifetime_ms,
            cancel_replace_cooldown_ms=config.mm_cancel_replace_cooldown_ms,
            entry_min_spread=config.mm_entry_min_spread,
            entry_stability_window_ticks=config.mm_entry_stability_window_ticks,
            entry_max_bid_ask_move_ticks=config.mm_entry_max_bid_ask_move_ticks,
            entry_anti_trend_threshold=config.mm_entry_anti_trend_threshold,
            entry_direction_window_ticks=config.mm_entry_direction_window_ticks,
            entry_direction_min_move_ticks=config.mm_entry_direction_min_move_ticks,
            trade_cooldown_ms=config.mm_trade_cooldown_ms,
            entry_min_place_interval_ms=config.mm_entry_min_place_interval_ms,
            entry_score_threshold=config.mm_entry_score_threshold,
            entry_score_spread_threshold=config.mm_entry_score_spread_threshold,
            entry_score_w_spread=config.mm_entry_score_w_spread,
            entry_score_w_stability=config.mm_entry_score_w_stability,
            entry_score_w_trend=config.mm_entry_score_w_trend,
            entry_score_w_imbalance=config.mm_entry_score_w_imbalance,
            entry_score_cooldown_penalty_max=config.mm_entry_score_cooldown_penalty_max,
            adaptive_entry_learning_enabled=config.mm_adaptive_entry_learning_enabled,
            adaptive_entry_learning_window=config.mm_adaptive_entry_learning_window,
            adaptive_entry_learning_min_bin_trades=config.mm_adaptive_entry_learning_min_bin_trades,
            adaptive_entry_learning_step_up=config.mm_adaptive_entry_learning_step_up,
            adaptive_entry_learning_step_down=config.mm_adaptive_entry_learning_step_down,
            adaptive_entry_learning_max_step_per_update=config.mm_adaptive_entry_learning_max_step_per_update,
            adaptive_entry_learning_threshold_min=config.mm_adaptive_entry_learning_threshold_min,
            adaptive_entry_learning_threshold_max=config.mm_adaptive_entry_learning_threshold_max,
            adaptive_entry_learning_drift_alert=config.mm_adaptive_entry_learning_drift_alert,
            adaptive_entry_learning_perf_alert_delta=config.mm_adaptive_entry_learning_perf_alert_delta,
            trend_window_ticks=config.mm_trend_window_ticks,
            trend_strength_threshold=config.mm_trend_strength_threshold,
            cancel_on_strong_trend=config.mm_cancel_on_strong_trend,
            post_fill_horizon_ms=config.mm_post_fill_horizon_ms,
            adverse_fill_window=config.mm_adverse_fill_window,
            adverse_fill_rate_threshold=config.mm_adverse_fill_rate_threshold,
            defensive_quote_offset_ticks=config.mm_defensive_quote_offset_ticks,
            decision_confirmation_updates=config.mm_decision_confirmation_updates,
            min_decision_interval_ms=config.mm_min_decision_interval_ms,
            decision_batch_ticks=config.mm_decision_batch_ticks,
            cancel_impact_horizon_ms=config.mm_cancel_impact_horizon_ms,
            cancel_reason_summary_every=config.mm_cancel_reason_summary_every,
            disable_price_move_cancel=config.mm_disable_price_move_cancel,
            cancel_analytics_sink=self.economics_store.insert_cancel_analytics,
            entry_decision_sink=self.economics_store.insert_entry_decisions,
            economics_store=self.economics_store,
            strategy_mode=config.strategy_mode,
            momentum_params_path=_resolve_optional_path_under_fix_engine(config.momentum_params_path),
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
        if (config.historical_candles_csv or "").strip():
            self.feed = CandleCsvFeed(config=config)
            n_rows = len(self.feed)
            lim = int(config.replay_tick_limit)
            self._replay_tick_count = min(n_rows, lim) if lim > 0 else n_rows
        else:
            self.feed = DeterministicMarketDataFeed(config=config)
            self._replay_tick_count = int(config.tick_count)
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
        self._analytics_trades_processed = 0
        self._analytics_records_written = 0
        self._analytics_lock = RLock()
        self._exec_timing_by_order: dict[str, dict[str, datetime | None]] = {}
        self._frequency_window_sec = max(60.0, float(config.frequency_window_minutes) * 60.0)
        self._frequency_log_interval_sec = max(1.0, float(config.frequency_log_interval_sec))
        self._low_frequency_threshold_per_min = max(0.0, float(config.low_frequency_threshold_per_min))
        self._trade_event_times_mono: deque[float] = deque()
        self._round_trip_event_times_mono: deque[float] = deque()
        self._last_frequency_log_mono = 0.0
        self._edge_eval_every_round_trips = max(1, int(config.edge_eval_every_round_trips))
        self._edge_adverse_rate_threshold = float(config.edge_adverse_rate_threshold)
        self._edge_window_recent_round_trips: deque[dict[str, float]] = deque(maxlen=self._edge_eval_every_round_trips)
        self._live_metrics_every_trades = max(1, int(config.live_metrics_every_trades))
        self._latency_ms_sum = 0.0
        self._latency_ms_count = 0
        self._lookahead_signal_violations = 0
        self._lookahead_execution_violations = 0
        self._session_stats_snapshot_every_trades = max(1, int(config.session_stats_snapshot_every_trades))
        self._session_stats_started_monotonic = 0.0
        self._session_stats_total_round_trips = 0
        self._session_stats_sum_pnl = 0.0
        self._session_stats_win_count = 0
        self._session_stats_adverse_count = 0
        self._session_stats_sum_mfe = 0.0
        self._session_stats_sum_mae = 0.0

        self._install_subscriptions()
        self._install_gateway_audit_hook()

    def _install_subscriptions(self) -> None:
        self.market_data_engine.subscribe(self.market_maker.on_market_data)
        self.market_data_engine.subscribe(self.gateway.on_market_data)
        self.market_data_engine.subscribe(self.failure_monitor.on_market_data)
        self.market_data_engine.subscribe(self.validation_store.insert_market_data)
        self.market_data_engine.subscribe(
            lambda data: self.economics.on_market_data(
                market=MarketType.EQUITIES,
                symbol=data.symbol,
                mid_price=Decimal(str(data.mid_price)),
            )
        )
        self.market_data_engine.subscribe(self.adverse_tracker.on_market_data)

    def _install_gateway_audit_hook(self) -> None:
        original_send_order = self.gateway.send_order

        def audited_send_order(request: OrderRequest) -> str:
            try:
                cl_ord_id = original_send_order(request)
                order = self.order_manager.get_order(cl_ord_id)
                decision_ts: datetime | None = None
                if hasattr(self.market_maker, "get_entry_order_context"):
                    ctx = self.market_maker.get_entry_order_context(cl_ord_id)
                    if isinstance(ctx, dict):
                        decision_raw = str(ctx.get("timestamp", "")).strip()
                        if decision_raw:
                            try:
                                decision_ts = datetime.fromisoformat(decision_raw)
                            except ValueError:
                                self.logger.warning(
                                    "[E2E][EXEC_TIMING_PARSE_FAIL] order_id=%s decision_timestamp=%s",
                                    cl_ord_id,
                                    decision_raw,
                                )
                if order is not None:
                    self.validation_store.upsert_order(order)
                    self._known_orders.add(order.cl_ord_id)
                    self._exec_timing_by_order[order.cl_ord_id] = {
                        "decision_timestamp": decision_ts,
                        "order_sent_timestamp": order.created_at,
                        "exchange_ack_timestamp": order.exchange_ack_at,
                        "fill_timestamp": None,
                    }
                    self.logger.info(
                        "[E2E][ORDER_CREATE] order_id=%s symbol=%s side=%s qty=%s",
                        order.cl_ord_id,
                        order.symbol,
                        order.side,
                        order.qty,
                    )
                    self.logger.info(
                        "[E2E][EXEC_TIMING_CREATE] order_id=%s decision_timestamp=%s order_sent_timestamp=%s exchange_ack_timestamp=%s",
                        order.cl_ord_id,
                        decision_ts.isoformat() if decision_ts is not None else "",
                        order.created_at.isoformat(),
                        order.exchange_ack_at.isoformat() if order.exchange_ack_at is not None else "",
                    )
                return cl_ord_id
            except RuntimeError as exc:
                if "RiskRejected" in str(exc):
                    self._risk_rejected_orders += 1
                raise

        self.gateway.send_order = audited_send_order  # type: ignore[method-assign]

    def _on_execution_report(self, message: object, source: str) -> None:
        self.failure_monitor.on_execution_report()
        event_ts = datetime.now(timezone.utc)
        state = self.order_manager.on_execution_report(message)
        if "error" in state:
            self._anomalies.append(f"bad_exec_report:{state['error']}")
            self.logger.error("[E2E][ANOMALY] %s", self._anomalies[-1])
            return
        self.market_maker.on_execution_report(state)

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
            if order.created_at > event_ts:
                self._lookahead_execution_violations += 1
                self.logger.error(
                    "[E2E][LOOKAHEAD_VIOLATION] order_id=%s created_at=%s event_ts=%s",
                    cl_ord_id,
                    order.created_at.isoformat(),
                    event_ts.isoformat(),
                )
            assert order.created_at <= event_ts, "order.created_at must be <= execution event timestamp"
            self.validation_store.upsert_order(order)
            timing = self._exec_timing_by_order.setdefault(
                cl_ord_id,
                {
                    "decision_timestamp": None,
                    "order_sent_timestamp": order.created_at,
                    "exchange_ack_timestamp": order.exchange_ack_at,
                    "fill_timestamp": None,
                },
            )
            if timing.get("exchange_ack_timestamp") is None:
                # First execution report is treated as exchange acknowledgement in real mode.
                timing["exchange_ack_timestamp"] = event_ts
                self.logger.info(
                    "[E2E][EXEC_TIMING_ACK] order_id=%s exchange_ack_timestamp=%s status=%s",
                    cl_ord_id,
                    event_ts.isoformat(),
                    new,
                )

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
        now_mono = time.monotonic()
        with self._analytics_lock:
            self._trade_event_times_mono.append(now_mono)
        self._maybe_log_frequency(now_mono)
        if new not in {"PARTIALLY_FILLED", "FILLED"}:
            msg = (
                f"unexpected_fill_event trade_id={state.get('exec_id','')} order_id={cl_ord_id} "
                f"symbol={state.get('symbol','')} side={state.get('side','')} status_new={new} last_qty={last_qty}"
            )
            self._anomalies.append(msg)
            self.logger.error("[E2E][ANOMALY] %s", msg)
        last_px = float(state.get("last_px", "0") or "0")
        side = state.get("side", "1")
        symbol = state.get("symbol", self.cfg.symbol)
        md = self.market_data_engine.get_latest(symbol)
        bid_px = Decimal(str(md.bid)) if md is not None else Decimal(str(last_px))
        ask_px = Decimal(str(md.ask)) if md is not None else Decimal(str(last_px))
        mid_px = Decimal(str(md.mid_price)) if md is not None else Decimal(str(last_px))
        expected_px = bid_px if side == "1" else ask_px
        fill_ts = datetime.now(timezone.utc)
        exec_id = state.get("exec_id", "")
        trade_obs_row: dict[str, object] | None = None
        entry_ctx: dict[str, object] | None = None
        if exec_id and hasattr(self.market_maker, "get_entry_trade_context"):
            entry_ctx = self.market_maker.get_entry_trade_context(exec_id)
        if entry_ctx is not None:
            obs_row = {
                "timestamp": str(entry_ctx.get("entry_timestamp", fill_ts.isoformat())),
                "exec_id": exec_id,
                "cl_ord_id": cl_ord_id,
                "symbol": symbol,
                "side": str(entry_ctx.get("side", side)),
                "mid_price": float(entry_ctx.get("mid_price", 0.0)),
                "bid_price": float(entry_ctx.get("bid_price", 0.0)),
                "ask_price": float(entry_ctx.get("ask_price", 0.0)),
                "bid_size": float(entry_ctx.get("bid_size", 0.0)),
                "ask_size": float(entry_ctx.get("ask_size", 0.0)),
                "imbalance": float(entry_ctx.get("imbalance", 0.0)),
                "microprice_edge": float(entry_ctx.get("microprice_edge", 0.0)),
                "momentum_100ms": float(entry_ctx.get("momentum_100ms", 0.0)),
                "momentum_500ms": float(entry_ctx.get("momentum_500ms", 0.0)),
                "spread": float(entry_ctx.get("spread", 0.0)),
                "last_50ms_move": float(entry_ctx.get("last_50ms_price_move", 0.0)),
            }
            try:
                self.economics_store.insert_executed_entry_observability([obs_row])
                self.logger.info("[E2E][ENTRY_OBSERVABILITY] %s", json.dumps(obs_row, ensure_ascii=True))
            except Exception as exc:
                self.logger.error(
                    "[E2E][ENTRY_OBSERVABILITY][DB_FAIL] exec_id=%s cl_ord_id=%s err=%s",
                    exec_id,
                    cl_ord_id,
                    exc,
                )
        timing = self._exec_timing_by_order.get(cl_ord_id)
        if timing is not None:
            decision_ts_ctx = self._parse_iso_opt(str(entry_ctx.get("timestamp", ""))) if entry_ctx is not None else None
            if timing.get("decision_timestamp") is None and decision_ts_ctx is not None:
                timing["decision_timestamp"] = decision_ts_ctx
            timing["fill_timestamp"] = fill_ts
            decision_ts = timing.get("decision_timestamp")
            sent_ts = timing.get("order_sent_timestamp")
            ack_ts = timing.get("exchange_ack_timestamp")
            decision_to_send_latency_ms: float | None = None
            send_to_fill_latency_ms: float | None = None
            decision_to_fill_latency_ms: float | None = None
            if isinstance(decision_ts, datetime) and isinstance(sent_ts, datetime):
                decision_to_send_latency_ms = max(0.0, (sent_ts - decision_ts).total_seconds() * 1000.0)
            if isinstance(sent_ts, datetime):
                send_to_fill_latency_ms = max(0.0, (fill_ts - sent_ts).total_seconds() * 1000.0)
            if isinstance(decision_ts, datetime):
                decision_to_fill_latency_ms = max(0.0, (fill_ts - decision_ts).total_seconds() * 1000.0)
            lookahead_signal_violation = False
            if isinstance(decision_ts, datetime) and isinstance(sent_ts, datetime) and decision_ts > sent_ts:
                lookahead_signal_violation = True
                self._lookahead_signal_violations += 1
            self.logger.info(
                "[E2E][EXEC_TIMING_FILL] trade_id=%s order_id=%s decision_timestamp=%s order_sent_timestamp=%s exchange_ack_timestamp=%s fill_timestamp=%s decision_to_send_latency_ms=%s send_to_fill_latency_ms=%s decision_to_fill_latency_ms=%s",
                state.get("exec_id", ""),
                cl_ord_id,
                decision_ts.isoformat() if isinstance(decision_ts, datetime) else "",
                sent_ts.isoformat() if isinstance(sent_ts, datetime) else "",
                ack_ts.isoformat() if isinstance(ack_ts, datetime) else "",
                fill_ts.isoformat(),
                f"{decision_to_send_latency_ms:.3f}" if decision_to_send_latency_ms is not None else "",
                f"{send_to_fill_latency_ms:.3f}" if send_to_fill_latency_ms is not None else "",
                f"{decision_to_fill_latency_ms:.3f}" if decision_to_fill_latency_ms is not None else "",
            )
            if decision_to_fill_latency_ms is not None:
                self._latency_ms_sum += decision_to_fill_latency_ms
                self._latency_ms_count += 1
            configured_latency_ms = float(self.gateway.simulation_latency_network_ms + self.gateway.simulation_latency_exchange_ms)
            if isinstance(decision_ts, datetime) and fill_ts == decision_ts:
                self.logger.error(
                    "[E2E][EXEC_TIMING_ANOMALY] order_id=%s type=instant_fill decision_timestamp=%s fill_timestamp=%s",
                    cl_ord_id,
                    decision_ts.isoformat(),
                    fill_ts.isoformat(),
                )
                self._anomalies.append(f"instant_fill order_id={cl_ord_id}")
            if configured_latency_ms > 0.0 and send_to_fill_latency_ms is not None and send_to_fill_latency_ms <= 0.0:
                self.logger.error(
                    "[E2E][EXEC_TIMING_ANOMALY] order_id=%s type=zero_latency_fill send_to_fill_latency_ms=%.3f configured_latency_ms=%.3f",
                    cl_ord_id,
                    send_to_fill_latency_ms,
                    configured_latency_ms,
                )
                self._anomalies.append(f"zero_latency_fill order_id={cl_ord_id}")
            if (
                configured_latency_ms > 0.0
                and send_to_fill_latency_ms is not None
                and send_to_fill_latency_ms < (configured_latency_ms * 0.5)
            ):
                self.logger.warning(
                    "[E2E][EXEC_TIMING_WARN] order_id=%s type=latency_below_config send_to_fill_latency_ms=%.3f configured_latency_ms=%.3f",
                    cl_ord_id,
                    send_to_fill_latency_ms,
                    configured_latency_ms,
                )
            side_label = "LONG" if str(side).strip() in {"1", "BUY", "B"} else "SHORT"
            trade_obs_row = {
                "trade_id": exec_id,
                "round_trip_id": "",
                "side": side_label,
                "entry_timestamp": str(entry_ctx.get("entry_timestamp", "")) if entry_ctx is not None else "",
                "exit_timestamp": fill_ts.isoformat(),
                "entry_price": float(entry_ctx.get("entry_price", 0.0)) if entry_ctx is not None else 0.0,
                "exit_price": float(last_px),
                "realized_pnl": 0.0,
                "mid_price": float(entry_ctx.get("mid_price", 0.0)) if entry_ctx is not None else 0.0,
                "bid_price": float(entry_ctx.get("bid_price", 0.0)) if entry_ctx is not None else 0.0,
                "ask_price": float(entry_ctx.get("ask_price", 0.0)) if entry_ctx is not None else 0.0,
                "bid_size": float(entry_ctx.get("bid_size", 0.0)) if entry_ctx is not None else 0.0,
                "ask_size": float(entry_ctx.get("ask_size", 0.0)) if entry_ctx is not None else 0.0,
                "imbalance": float(entry_ctx.get("imbalance", 0.0)) if entry_ctx is not None else 0.0,
                "microprice_edge": float(entry_ctx.get("microprice_edge", 0.0)) if entry_ctx is not None else 0.0,
                "momentum_100ms": float(entry_ctx.get("momentum_100ms", 0.0)) if entry_ctx is not None else 0.0,
                "momentum_500ms": float(entry_ctx.get("momentum_500ms", 0.0)) if entry_ctx is not None else 0.0,
                "spread": float(entry_ctx.get("spread", 0.0)) if entry_ctx is not None else 0.0,
                "last_50ms_price_move": float(entry_ctx.get("last_50ms_price_move", 0.0)) if entry_ctx is not None else 0.0,
                "decision_timestamp": decision_ts.isoformat() if isinstance(decision_ts, datetime) else "",
                "order_sent_timestamp": sent_ts.isoformat() if isinstance(sent_ts, datetime) else "",
                "fill_timestamp": fill_ts.isoformat(),
                "latency_ms": float(decision_to_fill_latency_ms or 0.0),
                "lookahead_violation": bool(lookahead_signal_violation),
            }
        time_in_book_ms = 0.0
        if order is not None and order.created_at is not None:
            time_in_book_ms = max(0.0, (fill_ts - order.created_at).total_seconds() * 1000.0)

        self.pnl_tracker.apply_fill(side=side, qty=last_qty, price=last_px)
        self.logger.info(
            "[E2E][ANALYTICS][START] trade_id=%s order_id=%s symbol=%s side=%s entry_price=%s exit_price=%s status_new=%s",
            state.get("exec_id", ""),
            cl_ord_id,
            symbol,
            side,
            last_px,
            last_px,
            new,
        )
        trade_record = self.economics.process_fill(
            market=MarketType.EQUITIES,
            symbol=symbol,
            side=side,
            qty=Decimal(str(last_qty)),
            price=Decimal(str(last_px)),
            expected_price=expected_px,
            bid=bid_px,
            ask=ask_px,
            mid_price=mid_px,
            fill_ts=fill_ts,
            cl_ord_id=cl_ord_id,
            exec_id=state.get("exec_id", ""),
            time_in_book_ms=time_in_book_ms,
        )
        if trade_record is None:
            return
        if trade_obs_row is not None:
            trade_obs_row["realized_pnl"] = float(trade_record.get("net_pnl", 0.0))
            try:
                self.economics_store.insert_trade_observability([trade_obs_row])
            except Exception as exc:
                self.logger.error("[E2E][TRADE_OBSERVABILITY][DB_FAIL] trade_id=%s err=%s", exec_id, exc)
        with self._analytics_lock:
            self._analytics_trades_processed += 1
            if self._analytics_trades_processed % self._live_metrics_every_trades == 0:
                self._emit_live_metrics_locked()
        self.logger.info(
            "[E2E][ANALYTICS][TRADE_CREATED] trade_id=%s order_id=%s symbol=%s side=%s entry_price=%s exit_price=%s",
            trade_record.get("exec_id", ""),
            trade_record.get("cl_ord_id", ""),
            trade_record.get("symbol", ""),
            trade_record.get("side", ""),
            trade_record.get("price", ""),
            trade_record.get("price", ""),
        )
        trade_analytics_rows = trade_record.get("trade_analytics_rows", [])
        round_trip_rows = trade_record.get("round_trip_rows", [])
        self.logger.info(
            "[E2E][ANALYTICS][DB_WRITE_START] trade_id=%s order_id=%s symbol=%s side=%s trade_rows=%s rt_rows=%s",
            trade_record.get("exec_id", ""),
            trade_record.get("cl_ord_id", ""),
            trade_record.get("symbol", ""),
            trade_record.get("side", ""),
            len(trade_analytics_rows),
            len(round_trip_rows),
        )
        self.economics_store.insert_trade(trade_record)
        self.economics_store.insert_trade_analytics(trade_analytics_rows)
        self.economics_store.insert_round_trips(round_trip_rows)
        written_records = 1 + len(trade_analytics_rows) + len(round_trip_rows)
        with self._analytics_lock:
            self._analytics_records_written += written_records
        write_check = self.economics_store.analytics_presence_for_trade(str(trade_record.get("exec_id", "")))
        expected_trade_rows = max(1, len(trade_analytics_rows))
        expected_rt_rows = len(round_trip_rows)
        self.logger.info(
            "[E2E][ANALYTICS][DB_WRITE_DONE] trade_id=%s order_id=%s symbol=%s side=%s fill_role=%s closed_qty=%s opened_qty=%s check=%s expected_ta=%s expected_rt=%s counters=%s/%s",
            trade_record.get("exec_id", ""),
            trade_record.get("cl_ord_id", ""),
            trade_record.get("symbol", ""),
            trade_record.get("side", ""),
            trade_record.get("fill_role", ""),
            trade_record.get("closed_qty", ""),
            trade_record.get("opened_qty", ""),
            write_check,
            expected_trade_rows,
            expected_rt_rows,
            self._analytics_trades_processed,
            self._analytics_records_written,
        )
        self.logger.info(
            "[E2E][ANALYTICS][DONE] trade_id=%s order_id=%s symbol=%s side=%s fill_role=%s closed_qty=%s opened_qty=%s trade_rows=%s rt_rows=%s",
            trade_record.get("exec_id", ""),
            trade_record.get("cl_ord_id", ""),
            trade_record.get("symbol", ""),
            trade_record.get("side", ""),
            trade_record.get("fill_role", ""),
            trade_record.get("closed_qty", ""),
            trade_record.get("opened_qty", ""),
            len(trade_analytics_rows),
            len(round_trip_rows),
        )
        if round_trip_rows:
            try:
                self.market_maker.on_round_trip_outcomes(round_trip_rows)
            except Exception as exc:
                msg = f"adaptive_learning_update_failed err={exc}"
                self._anomalies.append(msg)
                self.logger.error("[E2E][ANOMALY] %s", msg)
            for rt_row in round_trip_rows:
                self._persist_round_trip_observability(rt_row)
                self.update_session_stats(rt_row)
        if (
            write_check.get("trade_analytics_rows", 0) < expected_trade_rows
            or write_check.get("round_trip_rows", 0) < expected_rt_rows
        ):
            msg = (
                f"analytics_missing trade_id={trade_record.get('exec_id','')} "
                f"order_id={trade_record.get('cl_ord_id','')} symbol={trade_record.get('symbol','')} side={trade_record.get('side','')} "
                f"expected_trade_rows={expected_trade_rows} expected_round_trip_rows={expected_rt_rows} check={write_check}"
            )
            self._anomalies.append(msg)
            self.logger.error("[E2E][ANOMALY] %s", msg)
        self.adverse_tracker.register_fill(
            trade_id=state.get("exec_id", ""),
            side=side,
            qty=float(last_qty),
            fill_price=float(last_px),
            symbol=symbol,
            fill_ts=fill_ts,
        )
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

    def update_session_stats(self, trade: dict[str, object]) -> None:
        """
        Incrementally updates session-level analytics from a round-trip trade.
        Emits snapshot logs every N round-trips without affecting strategy behavior.
        """
        pnl = float(trade.get("total_pnl", 0.0))
        mfe = float(trade.get("mfe", 0.0))
        mae = float(trade.get("mae", 0.0))
        immediate_move = float(trade.get("immediate_move", 0.0))
        now_mono = time.monotonic()

        snapshot_payload: dict[str, object] | None = None
        edge_payload: dict[str, object] | None = None
        edge_degradation = False
        with self._analytics_lock:
            if self._session_stats_started_monotonic <= 0:
                self._session_stats_started_monotonic = now_mono
            self._round_trip_event_times_mono.append(now_mono)
            self._session_stats_total_round_trips += 1
            self._session_stats_sum_pnl += pnl
            self._session_stats_sum_mfe += mfe
            self._session_stats_sum_mae += mae
            if pnl > 0.0:
                self._session_stats_win_count += 1
            if immediate_move < 0.0:
                self._session_stats_adverse_count += 1
            self._edge_window_recent_round_trips.append(
                {
                    "pnl": float(pnl),
                    "mfe": float(mfe),
                    "adverse": 1.0 if immediate_move < 0.0 else 0.0,
                }
            )

            rt_count = self._session_stats_total_round_trips
            if rt_count % self._session_stats_snapshot_every_trades == 0:
                elapsed_min = max(1e-9, (now_mono - self._session_stats_started_monotonic) / 60.0)
                total_trades = int(self._analytics_trades_processed)
                snapshot_payload = {
                    "total_trades": total_trades,
                    "total_round_trips": rt_count,
                    "sum_pnl": self._session_stats_sum_pnl,
                    "avg_pnl_per_trade": self._session_stats_sum_pnl / float(rt_count),
                    "win_rate": self._session_stats_win_count / float(rt_count),
                    "adverse_rate": self._session_stats_adverse_count / float(rt_count),
                    "avg_mfe": self._session_stats_sum_mfe / float(rt_count),
                    "avg_mae": self._session_stats_sum_mae / float(rt_count),
                    "trade_frequency": total_trades / elapsed_min,
                }
            if rt_count % self._edge_eval_every_round_trips == 0 and self._edge_window_recent_round_trips:
                recent = list(self._edge_window_recent_round_trips)
                avg_pnl_recent = sum(x["pnl"] for x in recent) / float(len(recent))
                adverse_rate_recent = sum(x["adverse"] for x in recent) / float(len(recent))
                avg_mfe_recent = sum(x["mfe"] for x in recent) / float(len(recent))
                edge_payload = {
                    "window_round_trips": len(recent),
                    "avg_pnl": float(avg_pnl_recent),
                    "adverse_rate": float(adverse_rate_recent),
                    "avg_mfe": float(avg_mfe_recent),
                }
                edge_degradation = (
                    avg_pnl_recent < 0.0 or adverse_rate_recent > self._edge_adverse_rate_threshold
                )

        if snapshot_payload is not None:
            self.logger.info("[E2E][SESSION_STATS] %s", json.dumps(snapshot_payload, ensure_ascii=True))
        if edge_payload is not None:
            self.logger.info("[E2E][EDGE_MONITOR] %s", json.dumps(edge_payload, ensure_ascii=True))
            if edge_degradation:
                self.logger.warning(
                    "[E2E][EDGE_DEGRADATION_DETECTED] avg_pnl=%.6f adverse_rate=%.6f avg_mfe=%.6f adverse_threshold=%.6f window_round_trips=%d",
                    float(edge_payload["avg_pnl"]),
                    float(edge_payload["adverse_rate"]),
                    float(edge_payload["avg_mfe"]),
                    self._edge_adverse_rate_threshold,
                    int(edge_payload["window_round_trips"]),
                )
        self._maybe_log_frequency(now_mono)

    def _maybe_log_frequency(self, now_mono: float) -> None:
        payload: dict[str, float | int] | None = None
        low_frequency = False
        with self._analytics_lock:
            while self._trade_event_times_mono and (now_mono - self._trade_event_times_mono[0]) > self._frequency_window_sec:
                self._trade_event_times_mono.popleft()
            while self._round_trip_event_times_mono and (
                now_mono - self._round_trip_event_times_mono[0]
            ) > self._frequency_window_sec:
                self._round_trip_event_times_mono.popleft()
            if self._last_frequency_log_mono > 0 and (now_mono - self._last_frequency_log_mono) < self._frequency_log_interval_sec:
                return
            self._last_frequency_log_mono = now_mono
            window_minutes = self._frequency_window_sec / 60.0
            trade_frequency = len(self._trade_event_times_mono) / window_minutes
            round_trip_frequency = len(self._round_trip_event_times_mono) / window_minutes
            low_frequency = trade_frequency < self._low_frequency_threshold_per_min
            payload = {
                "window_minutes": int(round(window_minutes)),
                "trade_frequency": float(trade_frequency),
                "round_trip_frequency": float(round_trip_frequency),
            }
        if payload is not None:
            self.logger.info("[E2E][FREQUENCY] %s", json.dumps(payload, ensure_ascii=True))
            if low_frequency:
                self.logger.warning(
                    "[E2E][LOW_FREQUENCY] trade_frequency=%.4f threshold=%.4f window_minutes=%d",
                    float(payload["trade_frequency"]),
                    self._low_frequency_threshold_per_min,
                    int(payload["window_minutes"]),
                )

    def _persist_round_trip_observability(self, rt_row: dict[str, object]) -> None:
        entry_trade_id = str(rt_row.get("entry_trade_id", ""))
        if not entry_trade_id:
            return
        trade_obs = self.economics_store.get_trade_observability(entry_trade_id)
        if trade_obs is None:
            return
        row = {
            "round_trip_id": str(rt_row.get("round_trip_id", "")),
            "trade_id": entry_trade_id,
            "side": str(rt_row.get("side", "")),
            "entry_timestamp": str(rt_row.get("entry_ts", "")),
            "exit_timestamp": str(rt_row.get("exit_ts", "")),
            "entry_price": float(rt_row.get("entry_price", 0.0)),
            "exit_price": float(rt_row.get("exit_price", 0.0)),
            "realized_pnl": float(rt_row.get("total_pnl", 0.0)),
            "duration_ms": float(rt_row.get("duration_ms", 0.0)),
            "mfe": float(rt_row.get("mfe", 0.0)),
            "mae": float(rt_row.get("mae", 0.0)),
            "adverse_rate": 1.0 if float(rt_row.get("immediate_move", 0.0)) < 0.0 else 0.0,
            "immediate_move": float(rt_row.get("immediate_move", 0.0)),
            "mid_price": float(trade_obs.get("mid_price", 0.0)),
            "bid_price": float(trade_obs.get("bid_price", 0.0)),
            "ask_price": float(trade_obs.get("ask_price", 0.0)),
            "bid_size": float(trade_obs.get("bid_size", 0.0)),
            "ask_size": float(trade_obs.get("ask_size", 0.0)),
            "imbalance": float(trade_obs.get("imbalance", 0.0)),
            "microprice_edge": float(trade_obs.get("microprice_edge", 0.0)),
            "momentum_100ms": float(trade_obs.get("momentum_100ms", 0.0)),
            "momentum_500ms": float(trade_obs.get("momentum_500ms", 0.0)),
            "spread": float(trade_obs.get("spread", 0.0)),
            "last_50ms_price_move": float(trade_obs.get("last_50ms_price_move", 0.0)),
            "decision_timestamp": str(trade_obs.get("decision_timestamp", "")),
            "order_sent_timestamp": str(trade_obs.get("order_sent_timestamp", "")),
            "fill_timestamp": str(trade_obs.get("fill_timestamp", "")),
            "latency_ms": float(trade_obs.get("latency_ms", 0.0)),
            "lookahead_violation": bool(trade_obs.get("lookahead_violation", False)),
        }
        try:
            self.economics_store.insert_round_trip_observability([row])
            self.economics_store.link_trade_to_round_trip(
                trade_id=entry_trade_id, round_trip_id=str(rt_row.get("round_trip_id", ""))
            )
        except Exception as exc:
            self.logger.error("[E2E][ROUND_TRIP_OBSERVABILITY][DB_FAIL] round_trip_id=%s err=%s", row["round_trip_id"], exc)

    def _emit_live_metrics_locked(self) -> None:
        rt_count = self._session_stats_total_round_trips
        avg_pnl = (self._session_stats_sum_pnl / float(rt_count)) if rt_count else 0.0
        win_rate = (self._session_stats_win_count / float(rt_count)) if rt_count else 0.0
        adverse_rate = (self._session_stats_adverse_count / float(rt_count)) if rt_count else 0.0
        avg_mfe = (self._session_stats_sum_mfe / float(rt_count)) if rt_count else 0.0
        avg_mae = (self._session_stats_sum_mae / float(rt_count)) if rt_count else 0.0
        latency_avg = (self._latency_ms_sum / float(self._latency_ms_count)) if self._latency_ms_count else 0.0
        payload = {
            "total_trades": int(self._analytics_trades_processed),
            "total_round_trips": int(rt_count),
            "avg_pnl": float(avg_pnl),
            "win_rate": float(win_rate),
            "adverse_rate": float(adverse_rate),
            "avg_mfe": float(avg_mfe),
            "avg_mae": float(avg_mae),
            "latency_avg": float(latency_avg),
            "lookahead_signal_violations": int(self._lookahead_signal_violations),
            "lookahead_execution_violations": int(self._lookahead_execution_violations),
        }
        self.logger.info("[E2E][LIVE_METRICS] %s", json.dumps(payload, ensure_ascii=True))

    @staticmethod
    def _parse_iso_opt(value: str) -> datetime | None:
        s = str(value).strip()
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None

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
        self.logger.info(
            "[E2E] start symbol=%s tick_count=%s seed=%s historical_csv=%s",
            self.cfg.symbol,
            self._replay_tick_count,
            self.cfg.seed,
            (self.cfg.historical_candles_csv or "").strip() or "",
        )

        try:
            for _ in range(self._replay_tick_count):
                raw = self.feed.next_tick()
                self.market_data_engine.update_market_data(raw)
                self._maybe_log_frequency(time.monotonic())
                time.sleep(self.cfg.tick_interval_sec)
        finally:
            self.failure_monitor.stop()

        self._finalize_and_validate()
        end_ts = datetime.now(timezone.utc)
        metrics = self.analytics_api.get_metrics()
        last_md = self.market_data_engine.get_latest(self.cfg.symbol)
        unrealized_pnl = self.pnl_tracker.unrealized_pnl(last_md.mid_price) if last_md else 0.0
        net_equity_pnl = float(metrics["total_pnl"]) + float(unrealized_pnl)

        result = {
            "session_start": start_ts.isoformat(),
            "session_end": end_ts.isoformat(),
            "duration_sec": round((end_ts - start_ts).total_seconds(), 3),
            "total_trades": int(metrics["total_trades"]),
            "successful_trades": int(metrics["successful_trades"]),
            "failed_trades": int(metrics["failed_trades"]),
            "success_rate_pct": float(metrics["win_rate"]),
            "failed_rate_pct": float(metrics["fail_rate"]),
            "success_to_failure_ratio": float(metrics["success_to_failure_ratio"]),
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
            "net_equity_pnl": round(net_equity_pnl, 6),
            "persistence_counts": self.validation_store.counts(),
            "anomalies": {
                "illegal_transitions": self._illegal_transitions,
                "timestamp_errors": self._timestamp_errors,
                "missing_execution_reports": self._missing_exec_reports,
                "position_mismatch": self._position_mismatch,
                "negative_pnl_spikes": self._negative_pnl_spikes,
                "other": self._anomalies,
            },
            "analytics_counters": {
                "total_trades_processed_for_analytics": int(self._analytics_trades_processed),
                "total_records_written": int(self._analytics_records_written),
            },
            "analytics_missing_fills_count": len(self.analytics_api.analytics_missing_fills(limit=100000)),
            "entry_score_pnl_correlation": self.analytics_api.entry_score_pnl_correlation(),
            "observability_summary": {
                "total_trades": int(self._analytics_trades_processed),
                "total_round_trips": int(self._session_stats_total_round_trips),
                "avg_pnl": (
                    float(self._session_stats_sum_pnl / float(self._session_stats_total_round_trips))
                    if self._session_stats_total_round_trips
                    else 0.0
                ),
                "win_rate": (
                    float(self._session_stats_win_count / float(self._session_stats_total_round_trips))
                    if self._session_stats_total_round_trips
                    else 0.0
                ),
                "adverse_rate": (
                    float(self._session_stats_adverse_count / float(self._session_stats_total_round_trips))
                    if self._session_stats_total_round_trips
                    else 0.0
                ),
                "avg_mfe": (
                    float(self._session_stats_sum_mfe / float(self._session_stats_total_round_trips))
                    if self._session_stats_total_round_trips
                    else 0.0
                ),
                "avg_mae": (
                    float(self._session_stats_sum_mae / float(self._session_stats_total_round_trips))
                    if self._session_stats_total_round_trips
                    else 0.0
                ),
                "latency_avg": (
                    float(self._latency_ms_sum / float(self._latency_ms_count)) if self._latency_ms_count else 0.0
                ),
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


def _build_logger(log_file: str | Path | None = None) -> logging.Logger:
    logger = logging.getLogger("e2e_test")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    if log_file is not None:
        fh = logging.FileHandler(str(log_file), encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger


def run_e2e(config: E2EConfig | None = None, *, log_file: str | Path | None = None) -> dict[str, object]:
    cfg = config or E2EConfig()
    logger = _build_logger(log_file)
    orchestrator = TestOrchestrator(config=cfg, logger=logger)
    return orchestrator.run()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="E2E / historical candle replay for BasicMarketMaker.")
    ap.add_argument("--csv", type=str, default="", help="Candle CSV (time_utc, open, high, low, close, volume, …)")
    ap.add_argument("--symbol", type=str, default="SBER")
    ap.add_argument(
        "--strategy",
        type=str,
        default="MICROPRICE",
        help="BasicMarketMaker strategy_mode (use MOMENTUM_BREAKOUT for channel replay)",
    )
    ap.add_argument(
        "--momentum-params",
        type=str,
        default="",
        help="Path to momentum JSON (relative to fix_engine/ or absolute); empty = bundled defaults",
    )
    ap.add_argument("--replay-tick-limit", type=int, default=0, help="Cap replay length; 0 = all rows")
    ap.add_argument("--spread", type=float, default=0.04, dest="replay_spread", help="Synthetic bid/ask spread for CSV replay")
    ap.add_argument("--tick-interval", type=float, default=0.0, help="Sleep between ticks (0 = fast)")
    ap.add_argument(
        "--out-dir",
        type=str,
        default="",
        help="If set, write backtest.log, validation/economics SQLite, backtest_result.json here",
    )
    ap.add_argument("--seed", type=int, default=42)
    ns = ap.parse_args()

    out_dir: Path | None = Path(ns.out_dir) if (ns.out_dir or "").strip() else None
    log_path: Path | None = None
    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)
        log_path = out_dir / "backtest.log"
        db_path = str(out_dir / "backtest_validation.db")
        econ_path = str(out_dir / "backtest_economics.db")
    else:
        db_path = "e2e_validation.db"
        econ_path = "e2e_economics.db"

    cfg = E2EConfig(
        symbol=ns.symbol,
        seed=ns.seed,
        tick_interval_sec=float(ns.tick_interval),
        historical_candles_csv=ns.csv,
        replay_tick_limit=int(ns.replay_tick_limit),
        replay_synthetic_spread=float(ns.replay_spread),
        strategy_mode=ns.strategy,
        momentum_params_path=ns.momentum_params if (ns.momentum_params or "").strip() else "",
        db_path=db_path,
        economics_db_path=econ_path,
    )
    result_data = run_e2e(cfg, log_file=log_path)
    print(json.dumps(result_data, ensure_ascii=True, indent=2))
    if out_dir is not None:
        outp = out_dir / "backtest_result.json"
        outp.write_text(json.dumps(result_data, ensure_ascii=True, indent=2), encoding="utf-8")
        print(f"Wrote {outp}", file=sys.stderr)
