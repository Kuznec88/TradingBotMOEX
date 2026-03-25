# from __future__ import annotations

# Backward-compatible shim.
# The momentum-only strategy now lives in `fix_engine.strategy.momentum_mm`.

from fix_engine.strategy.momentum_mm import BasicMarketMaker

__all__ = ["BasicMarketMaker"]

# Always export the momentum-only implementation, regardless of any legacy code
# that may have been appended below during iterative refactors.
from fix_engine.strategy.momentum_mm import BasicMarketMaker as _MomentumBasicMarketMaker  # noqa: E402

BasicMarketMaker = _MomentumBasicMarketMaker

# from __future__ import annotations

import logging
import time
from collections import deque

from fix_engine.execution_gateway import ExecutionGateway
from fix_engine.market_data.models import MarketData
from fix_engine.order_models import MarketType, OrderRequest
from fix_engine.position_manager import PositionManager


class BasicMarketMaker:
    """
    Momentum-only active trader:
    signal -> aggressive entry -> fast invalidation -> exit
    """

    def __init__(
        self,
        *,
        symbol: str,
        lot_size: float,
        market: MarketType,
        gateway: ExecutionGateway,
        position_manager: PositionManager,
        logger: logging.Logger,
        tick_size: float,
        spread_threshold: float,
        move_threshold: float,
        velocity_threshold: float,
        delta_threshold: float,
        imbalance_threshold: float,
        cooldown_ms: int,
        one_position_only: bool = True,
        entry_decision_sink: object | None = None,
        economics_store: object | None = None,
        **_legacy_kwargs: object,
    ) -> None:
        self.symbol = symbol.upper()
        self.lot_size = float(lot_size)
        self.market = market
        self.gateway = gateway
        self.position_manager = position_manager
        self.logger = logger

        self._tick_size = max(1e-12, float(tick_size))
        self._spread_threshold = max(0.0, float(spread_threshold))
        self._move_threshold = max(0.0, float(move_threshold))
        self._velocity_threshold = max(0.0, float(velocity_threshold))
        self._delta_threshold = max(0.0, float(delta_threshold))
        self._imbalance_threshold = max(0.0, float(imbalance_threshold))
        self._cooldown_sec = max(0.0, float(cooldown_ms) / 1000.0)

        self._one_position_only = bool(one_position_only)

        # Minimal position state
        self._position_qty = 0.0
        self._entry_price = 0.0
        self._entry_monotonic = 0.0
        self._immediate_ticks_seen = 0
        self._entry_mid_price = 0.0

        # Feature window (single snapshot decision uses recent move)
        self._window_ms = 200.0
        self._mid_hist: deque[tuple[float, float]] = deque(maxlen=512)  # (mono_ms, mid)
        self._last_entry_place_monotonic = 0.0

        # Explicitly unused in momentum-only mode (kept for backward compatibility with wiring)
        _ = entry_decision_sink
        _ = economics_store

    def on_execution_report(self, state: dict[str, object]) -> None:
        if str(state.get("symbol", "")).upper() != self.symbol:
            return
        status_new = str(state.get("status_new", ""))
        if status_new not in {"PARTIALLY_FILLED", "FILLED"}:
            return
        last_qty = float(state.get("last_qty", 0) or 0)
        if last_qty <= 0:
            return
        side = str(state.get("side", ""))
        last_px = state.get("last_px", None)
        avg_px = state.get("avg_px", None)
        fill_px = float(last_px or avg_px or 0.0)

        if side == "1":  # BUY
            self._position_qty += last_qty
        else:  # SELL
            self._position_qty -= last_qty

        if abs(self._entry_price) < 1e-12 and abs(self._position_qty) > 1e-12:
            self._entry_price = fill_px
            self._entry_monotonic = time.perf_counter()
            self._immediate_ticks_seen = 0
            self._entry_mid_price = 0.0

        if abs(self._position_qty) < 1e-12:
            self._position_qty = 0.0
            self._entry_price = 0.0
            self._entry_monotonic = 0.0
            self._immediate_ticks_seen = 0
            self._entry_mid_price = 0.0

    def on_market_data(self, data: MarketData) -> None:
        if data.symbol.upper() != self.symbol:
            return

        now_mono = time.perf_counter()
        now_ms = now_mono * 1000.0

        mid = float(data.mid_price)
        self._mid_hist.append((now_ms, mid))

        # Exit first: fast invalidation (first 1 tick against us).
        if abs(self._position_qty) > 1e-12:
            self._maybe_fast_invalidate(data=data, now_mono=now_mono)
            return

        # Cooldown between entry attempts
        if self._cooldown_sec > 0.0 and self._last_entry_place_monotonic > 0.0:
            if (now_mono - self._last_entry_place_monotonic) < self._cooldown_sec:
                return

        snapshot = self._compute_features(data=data, now_ms=now_ms)
        decision = self.evaluate_entry_signal(snapshot)
        self._log_decision(snapshot=snapshot, decision=decision)
        if decision == "NONE":
            return

        self._send_aggressive_entry(decision=decision, data=data, now_mono=now_mono)

    def _compute_features(self, *, data: MarketData, now_ms: float) -> dict[str, float]:
        spread_ticks = float(data.spread) / self._tick_size

        cutoff = now_ms - self._window_ms
        old_mid = float(data.mid_price)
        old_ms = now_ms
        for ts_ms, mid in self._mid_hist:
            if ts_ms >= cutoff:
                old_ms = ts_ms
                old_mid = mid
                break

        move_ticks = (float(data.mid_price) - float(old_mid)) / self._tick_size
        dt_ms = max(1.0, now_ms - old_ms)
        velocity = move_ticks / dt_ms  # ticks/ms

        bid_sz = float(getattr(data, "bid_size", 0.0) or 0.0)
        ask_sz = float(getattr(data, "ask_size", 0.0) or 0.0)
        delta = bid_sz - ask_sz
        denom = bid_sz + ask_sz
        imbalance = (delta / denom) if denom > 1e-12 else 0.0

        return {
            "spread": spread_ticks,
            "price_move_ticks": move_ticks,
            "velocity": velocity,
            "delta": delta,
            "imbalance": imbalance,
            "spread_threshold": self._spread_threshold,
            "move_threshold": self._move_threshold,
            "velocity_threshold": self._velocity_threshold,
            "delta_threshold": self._delta_threshold,
            "imbalance_threshold": self._imbalance_threshold,
        }

    def evaluate_entry_signal(self, market_snapshot: dict[str, float]) -> str:
        spread = float(market_snapshot.get("spread", 0.0))
        price_move_ticks = float(market_snapshot.get("price_move_ticks", 0.0))
        velocity = float(market_snapshot.get("velocity", 0.0))
        delta = float(market_snapshot.get("delta", 0.0))
        imbalance = float(market_snapshot.get("imbalance", 0.0))

        if spread > float(market_snapshot.get("spread_threshold", self._spread_threshold)):
            return "NONE"
        if abs(price_move_ticks) < float(market_snapshot.get("move_threshold", self._move_threshold)):
            return "NONE"
        if abs(velocity) < float(market_snapshot.get("velocity_threshold", self._velocity_threshold)):
            return "NONE"
        if abs(delta) < float(market_snapshot.get("delta_threshold", self._delta_threshold)):
            return "NONE"
        if abs(imbalance) < float(market_snapshot.get("imbalance_threshold", self._imbalance_threshold)):
            return "NONE"
        if price_move_ticks > 0.0:
            return "LONG"
        if price_move_ticks < 0.0:
            return "SHORT"
        return "NONE"

    def _log_decision(self, *, snapshot: dict[str, float], decision: str) -> None:
        self.logger.info(
            "[MM][ENTRY_DECISION] symbol=%s decision=%s spread=%.2f move_ticks=%.2f vel=%.6f delta=%.2f imb=%.3f",
            self.symbol,
            decision,
            float(snapshot.get("spread", 0.0)),
            float(snapshot.get("price_move_ticks", 0.0)),
            float(snapshot.get("velocity", 0.0)),
            float(snapshot.get("delta", 0.0)),
            float(snapshot.get("imbalance", 0.0)),
        )

    def _send_aggressive_entry(self, *, decision: str, data: MarketData, now_mono: float) -> None:
        if self._one_position_only and abs(self._position_qty) > 1e-12:
            return

        side = "1" if decision == "LONG" else "2"
        price = float(data.ask) if side == "1" else float(data.bid)

        req = OrderRequest(
            symbol=self.symbol,
            side=side,
            qty=self.lot_size,
            account="",
            price=price,
            market=self.market,
            lot_size=1,
            bypass_risk=False,
        )
        cl_ord_id = self.gateway.send_order(req)
        self._last_entry_place_monotonic = now_mono
        self.logger.info(
            "[MM][ENTRY_EXECUTE] symbol=%s side=%s qty=%s px=%.4f cl_ord_id=%s",
            self.symbol,
            side,
            self.lot_size,
            price,
            cl_ord_id,
        )

    def _maybe_fast_invalidate(self, *, data: MarketData, now_mono: float) -> None:
        if self._entry_monotonic <= 0.0:
            return
        if self._immediate_ticks_seen >= 1:
            return

        mid = float(data.mid_price)
        if self._entry_mid_price <= 0.0:
            self._entry_mid_price = mid
            self._immediate_ticks_seen = 1
            return

        if self._position_qty > 0:
            adverse = (self._entry_mid_price - mid) / self._tick_size
        else:
            adverse = (mid - self._entry_mid_price) / self._tick_size

        self._immediate_ticks_seen = 1
        if adverse >= 1.0:
            self._force_exit(data=data, now_mono=now_mono, reason="fast_invalidation")

    def _force_exit(self, *, data: MarketData, now_mono: float, reason: str) -> None:
        qty = abs(self._position_qty)
        if qty <= 1e-12:
            return
        side = "2" if self._position_qty > 0 else "1"
        price = float(data.bid) if side == "2" else float(data.ask)

        req = OrderRequest(
            symbol=self.symbol,
            side=side,
            qty=qty,
            account="",
            price=price,
            market=self.market,
            lot_size=1,
            bypass_risk=True,
        )
        cl_ord_id = self.gateway.send_order(req)
        self._last_entry_place_monotonic = now_mono
        self.logger.info(
            "[MM][EXIT] symbol=%s reason=%s side=%s qty=%.4f px=%.4f cl_ord_id=%s",
            self.symbol,
            reason,
            side,
            qty,
            price,
            cl_ord_id,
        )

# from __future__ import annotations

import logging
import time
from collections import deque

from fix_engine.execution_gateway import ExecutionGateway
from fix_engine.market_data.models import MarketData
from fix_engine.order_models import MarketType, OrderRequest
from fix_engine.position_manager import PositionManager


class _MomentumBasicMarketMaker:
    """
    Momentum-only active trader:
    signal -> aggressive entry -> fast invalidation -> exit
    """

    def __init__(
        self,
        *,
        symbol: str,
        lot_size: float,
        market: MarketType,
        gateway: ExecutionGateway,
        position_manager: PositionManager,
        logger: logging.Logger,
        tick_size: float,
        spread_threshold: float,
        move_threshold: float,
        velocity_threshold: float,
        delta_threshold: float,
        imbalance_threshold: float,
        cooldown_ms: int,
        one_position_only: bool = True,
        entry_decision_sink: object | None = None,
        economics_store: object | None = None,
        **_legacy_kwargs: object,
    ) -> None:
        self.symbol = symbol.upper()
        self.lot_size = float(lot_size)
        self.market = market
        self.gateway = gateway
        self.position_manager = position_manager
        self.logger = logger

        self._tick_size = max(1e-12, float(tick_size))
        self._spread_threshold = max(0.0, float(spread_threshold))
        self._move_threshold = max(0.0, float(move_threshold))
        self._velocity_threshold = max(0.0, float(velocity_threshold))
        self._delta_threshold = max(0.0, float(delta_threshold))
        self._imbalance_threshold = max(0.0, float(imbalance_threshold))
        self._cooldown_sec = max(0.0, float(cooldown_ms) / 1000.0)

        self._one_position_only = bool(one_position_only)

        # Minimal position state
        self._position_qty = 0.0
        self._entry_price = 0.0
        self._entry_monotonic = 0.0
        self._immediate_ticks_seen = 0
        self._entry_mid_price = 0.0

        # Feature window (single snapshot decision uses recent move)
        self._window_ms = 200.0
        self._mid_hist: deque[tuple[float, float]] = deque(maxlen=512)  # (mono_ms, mid)
        self._last_entry_place_monotonic = 0.0

        # Explicitly unused in momentum-only mode (kept for backward compatibility with wiring)
        _ = entry_decision_sink
        _ = economics_store

    def on_execution_report(self, state: dict[str, object]) -> None:
        # Track fills into a single position number.
        if str(state.get("symbol", "")).upper() != self.symbol:
            return
        status_new = str(state.get("status_new", ""))
        if status_new not in {"PARTIALLY_FILLED", "FILLED"}:
            return
        last_qty = float(state.get("last_qty", 0) or 0)
        if last_qty <= 0:
            return
        side = str(state.get("side", ""))
        last_px = state.get("last_px", None)
        avg_px = state.get("avg_px", None)
        fill_px = float(last_px or avg_px or 0.0)

        if side == "1":  # BUY
            self._position_qty += last_qty
        else:  # SELL
            self._position_qty -= last_qty

        # Track entry price only when opening from flat.
        if abs(self._entry_price) < 1e-12 and abs(self._position_qty) > 1e-12:
            self._entry_price = fill_px
            self._entry_monotonic = time.perf_counter()
            self._immediate_ticks_seen = 0
            self._entry_mid_price = 0.0

        if abs(self._position_qty) < 1e-12:
            self._position_qty = 0.0
            self._entry_price = 0.0
            self._entry_monotonic = 0.0
            self._immediate_ticks_seen = 0
            self._entry_mid_price = 0.0

    def on_market_data(self, data: MarketData) -> None:
        if data.symbol.upper() != self.symbol:
            return

        now_mono = time.perf_counter()
        now_ms = now_mono * 1000.0

        mid = float(data.mid_price)
        self._mid_hist.append((now_ms, mid))

        # Exit first: fast invalidation (first 1 tick against us).
        if abs(self._position_qty) > 1e-12:
            self._maybe_fast_invalidate(data=data, now_mono=now_mono)
            return

        # Cooldown between entry attempts
        if self._cooldown_sec > 0.0 and self._last_entry_place_monotonic > 0.0:
            if (now_mono - self._last_entry_place_monotonic) < self._cooldown_sec:
                return

        snapshot = self._compute_features(data=data, now_ms=now_ms)
        decision = self.evaluate_entry_signal(snapshot)
        self._log_decision(snapshot=snapshot, decision=decision)
        if decision == "NONE":
            return

        self._send_aggressive_entry(decision=decision, data=data, now_mono=now_mono)

    def _compute_features(self, *, data: MarketData, now_ms: float) -> dict[str, float]:
        spread_ticks = float(data.spread) / self._tick_size

        # Oldest point within window
        cutoff = now_ms - self._window_ms
        old_mid = float(data.mid_price)
        old_ms = now_ms
        for ts_ms, mid in self._mid_hist:
            if ts_ms >= cutoff:
                old_ms = ts_ms
                old_mid = mid
                break

        move_ticks = (float(data.mid_price) - float(old_mid)) / self._tick_size
        dt_ms = max(1.0, now_ms - old_ms)
        velocity = move_ticks / dt_ms  # ticks/ms

        bid_sz = float(getattr(data, "bid_size", 0.0) or 0.0)
        ask_sz = float(getattr(data, "ask_size", 0.0) or 0.0)
        delta = bid_sz - ask_sz
        denom = bid_sz + ask_sz
        imbalance = (delta / denom) if denom > 1e-12 else 0.0

        return {
            "spread": spread_ticks,
            "price_move_ticks": move_ticks,
            "velocity": velocity,
            "delta": delta,
            "imbalance": imbalance,
            "spread_threshold": self._spread_threshold,
            "move_threshold": self._move_threshold,
            "velocity_threshold": self._velocity_threshold,
            "delta_threshold": self._delta_threshold,
            "imbalance_threshold": self._imbalance_threshold,
        }

    def evaluate_entry_signal(self, market_snapshot: dict[str, float]) -> str:
        """
        Single-layer momentum decision engine.
        Input: pure snapshot of features. Output: LONG / SHORT / NONE.
        No side effects.
        """
        spread = float(market_snapshot.get("spread", 0.0))
        price_move_ticks = float(market_snapshot.get("price_move_ticks", 0.0))
        velocity = float(market_snapshot.get("velocity", 0.0))
        delta = float(market_snapshot.get("delta", 0.0))
        imbalance = float(market_snapshot.get("imbalance", 0.0))

        if spread > float(market_snapshot.get("spread_threshold", self._spread_threshold)):
            return "NONE"
        if abs(price_move_ticks) < float(market_snapshot.get("move_threshold", self._move_threshold)):
            return "NONE"
        if abs(velocity) < float(market_snapshot.get("velocity_threshold", self._velocity_threshold)):
            return "NONE"
        if abs(delta) < float(market_snapshot.get("delta_threshold", self._delta_threshold)):
            return "NONE"
        if abs(imbalance) < float(market_snapshot.get("imbalance_threshold", self._imbalance_threshold)):
            return "NONE"
        if price_move_ticks > 0.0:
            return "LONG"
        if price_move_ticks < 0.0:
            return "SHORT"
        return "NONE"

    def _log_decision(self, *, snapshot: dict[str, float], decision: str) -> None:
        self.logger.info(
            "[MM][ENTRY_DECISION] symbol=%s decision=%s spread=%.2f move_ticks=%.2f vel=%.6f delta=%.2f imb=%.3f",
            self.symbol,
            decision,
            float(snapshot.get("spread", 0.0)),
            float(snapshot.get("price_move_ticks", 0.0)),
            float(snapshot.get("velocity", 0.0)),
            float(snapshot.get("delta", 0.0)),
            float(snapshot.get("imbalance", 0.0)),
        )

    def _send_aggressive_entry(self, *, decision: str, data: MarketData, now_mono: float) -> None:
        if self._one_position_only and abs(self._position_qty) > 1e-12:
            return

        side = "1" if decision == "LONG" else "2"
        price = float(data.ask) if side == "1" else float(data.bid)

        req = OrderRequest(
            symbol=self.symbol,
            side=side,
            qty=self.lot_size,
            account="",
            price=price,
            market=self.market,
            lot_size=1,
            bypass_risk=False,
        )

        cl_ord_id = self.gateway.send_order(req)
        self._last_entry_place_monotonic = now_mono
        self.logger.info("[MM][ENTRY_EXECUTE] symbol=%s side=%s qty=%s px=%.4f cl_ord_id=%s", self.symbol, side, self.lot_size, price, cl_ord_id)

    def _maybe_fast_invalidate(self, *, data: MarketData, now_mono: float) -> None:
        # First tick after entry: if mid moved against us by >= 1 tick -> exit immediately.
        if self._entry_monotonic <= 0.0:
            return
        if self._immediate_ticks_seen >= 1:
            return

        mid = float(data.mid_price)
        if self._entry_mid_price <= 0.0:
            self._entry_mid_price = mid
            self._immediate_ticks_seen = 1
            return

        adverse = 0.0
        if self._position_qty > 0:
            adverse = (self._entry_mid_price - mid) / self._tick_size
        else:
            adverse = (mid - self._entry_mid_price) / self._tick_size

        self._immediate_ticks_seen = 1
        if adverse >= 1.0:
            self._force_exit(data=data, now_mono=now_mono, reason="fast_invalidation")

    def _force_exit(self, *, data: MarketData, now_mono: float, reason: str) -> None:
        qty = abs(self._position_qty)
        if qty <= 1e-12:
            return
        side = "2" if self._position_qty > 0 else "1"
        price = float(data.bid) if side == "2" else float(data.ask)

        req = OrderRequest(
            symbol=self.symbol,
            side=side,
            qty=qty,
            account="",
            price=price,
            market=self.market,
            lot_size=1,
            bypass_risk=True,
        )
        cl_ord_id = self.gateway.send_order(req)
        self._last_entry_place_monotonic = now_mono
        self.logger.info(
            "[MM][EXIT] symbol=%s reason=%s side=%s qty=%.4f px=%.4f cl_ord_id=%s",
            self.symbol,
            reason,
            side,
            qty,
            price,
            cl_ord_id,
        )

# from __future__ import annotations

import logging
import json
import sqlite3
from collections import defaultdict, deque
from datetime import datetime, timezone
import math
from pathlib import Path
from statistics import median
import time
from threading import RLock
from typing import Callable

from fix_engine.adaptive_learning_targets import load_adaptive_learning_targets
from fix_engine.economics_store import EconomicsStore
from fix_engine.execution_gateway import ExecutionGateway
from fix_engine.market_data.models import MarketData
from fix_engine.order_models import MarketType, OrderRequest
from fix_engine.position_manager import PositionManager
from fix_engine.position_policy import PositionPolicyRuntime
from fix_engine.structured_logging import log_event


class BasicMarketMaker:
    """
    Momentum-only active trader:
    signal -> aggressive entry -> fast invalidation -> exit
    """

    def __init__(
        self,
        *,
        symbol: str,
        lot_size: float,
        market: MarketType,
        gateway: ExecutionGateway,
        position_manager: PositionManager,
        logger: logging.Logger,
        max_loss_per_trade: float = 0.0,
        virtual_account_enabled: bool = False,
        virtual_account_start_balance: float = 0.0,
        virtual_account_max_loss_fraction: float = 0.0,
        entry_forecast_profit_enabled: bool = False,
        entry_forecast_alignment_min: float = 0.0,
        # legacy MM args kept for backward compatibility (ignored)
        volatility_window_ticks: int = 20,
        max_short_term_volatility: float = 0.0,
        cancel_on_high_volatility: bool = True,
        resting_order_timeout_sec: float = 2.0,
        tick_size: float = 0.01,
        replace_threshold_ticks: int = 2,
        replace_cancel_threshold_ticks: float = 0.0,
        replace_keep_threshold_ticks: float = 0.0,
        replace_persist_ms: int = 220,
        adverse_move_cancel_ticks: int = 3,
        fast_cancel_keep_ticks: float = 1.5,
        fast_cancel_persist_ms: int = 180,
        price_tolerance_ticks: float = 0.0,
        price_tolerance_pct: float = 0.0,
        min_order_lifetime_ms: int = 200,
        cancel_replace_cooldown_ms: int = 120,
        trend_window_ticks: int = 12,
        trend_strength_threshold: float = 0.0,
        cancel_on_strong_trend: bool = False,
        post_fill_horizon_ms: int = 200,
        adverse_fill_window: int = 25,
        adverse_fill_rate_threshold: float = 0.65,
        defensive_quote_offset_ticks: int = 1,
        decision_confirmation_updates: int = 2,
        min_decision_interval_ms: int = 150,
        decision_batch_ticks: int = 3,
        entry_min_spread: float = 0.0,
        entry_stability_window_ticks: int = 8,
        entry_max_bid_ask_move_ticks: float = 2.0,
        entry_anti_trend_threshold: float = 0.0,
        entry_direction_window_ticks: int = 12,
        entry_direction_min_move_ticks: float = 1.0,
        trade_cooldown_ms: int = 300,
        entry_min_place_interval_ms: int = 200,
        entry_score_threshold: float = 0.6,
        entry_score_spread_threshold: float = 0.02,
        entry_score_w_spread: float = 0.35,
        entry_score_w_stability: float = 0.25,
        entry_score_w_trend: float = 0.25,
        entry_score_w_imbalance: float = 0.15,
        entry_score_cooldown_penalty_max: float = 0.35,
        adaptive_entry_learning_enabled: bool = False,
        adaptive_entry_learning_window: int = 100,
        adaptive_entry_learning_min_bin_trades: int = 10,
        adaptive_entry_learning_step_up: float = 0.01,
        adaptive_entry_learning_step_down: float = 0.01,
        adaptive_entry_learning_max_step_per_update: float = 0.02,
        adaptive_entry_learning_threshold_min: float = 0.4,
        adaptive_entry_learning_threshold_max: float = 0.95,
        adaptive_entry_learning_drift_alert: float = 0.20,
        adaptive_entry_learning_perf_alert_delta: float = 0.15,
        cancel_impact_horizon_ms: int = 500,
        cancel_reason_summary_every: int = 20,
        disable_price_move_cancel: bool = False,
        microprice_edge_threshold: float = 0.0,
        spread_median_window_ticks: int = 64,
        anti_adverse_window_ms: int = 50,
        # Post-fill KPI / adaptive learning removed (momentum-only).
        cancel_analytics_sink: Callable[[list[dict[str, object]]], None] | None = None,
        entry_decision_sink: Callable[[list[dict[str, object]]], None] | None = None,
        one_position_only: bool = True,
        reversal_enabled: bool = True,
        reversal_confirmation_updates: int = 3,
        reversal_min_trend_strength: float = 0.0008,
        reversal_min_hold_ms: int = 120000,
        reversal_cooldown_ms: int = 120000,
        weekly_trend_enabled: bool = False,
        weekly_trend_db_path: str = "",
        weekly_trend_days: float = 7.0,
        weekly_trend_threshold_pct: float = 0.5,
        weekly_trend_refresh_sec: float = 30.0,
        volume_move_corr_enabled: bool = False,
        volume_move_corr_db_path: str = "",
        volume_move_corr_lookback_days: float = 14.0,
        volume_move_corr_bar_minutes: int = 5,
        volume_move_corr_min_samples: int = 500,
        volume_move_corr_threshold: float = 0.15,
        volume_move_corr_refresh_sec: float = 60.0,
        five_min_entry_gate_enabled: bool = True,
        five_min_impulse_min_ticks: float = 0.5,
        five_min_volume_ratio_min: float = 1.05,
        trend_continuation_enabled: bool = True,
        trend_continuation_confirm_updates: int = 3,
        trend_continuation_min_move_ticks: float = 1.0,
        flow_bias_window_ticks: int = 12,
        long_flow_bias_min: float = 0.05,
        long_trend_bias_min_ticks: float = 0.75,
        long_impulse_override_ticks: float = 3.0,
        short_flow_bias_max: float = 0.20,
        short_trend_bias_min_ticks: float = 0.50,
        volume_flow_override_ticks: float = 1.50,
        volume_spike_entry_enabled: bool = True,
        volume_spike_window_ticks: int = 24,
        volume_spike_multiplier: float = 2.0,
        volume_spike_flow_bias_abs_min: float = 0.20,
        momentum_exit_enabled: bool = True,
        ignore_duplicate_ticks_ms: int = 120,
        decision_min_mid_move_ticks: float = 0.5,
        cooldown_ms: int = 1200,
        tray_price_every_sec: float = 3.0,
        take_profit_per_trade: float = 0.5,
        dynamic_profit_target_enabled: bool = True,
        dynamic_profit_target_vol_multiplier: float = 1.8,
        dynamic_profit_target_momentum_weight: float = 0.35,
        dynamic_profit_target_flow_weight: float = 0.60,
        dynamic_profit_target_trend_weight: float = 0.45,
        dynamic_profit_target_min: float = 2.0,
        dynamic_profit_target_max: float = 30.0,
        breakeven_trailing_offset_ticks: float = 5.0,
        position_policy_enabled: bool = False,
        adaptive_targets_path: str = "",
        economics_store: EconomicsStore | None = None,
        strategy_mode: str = "MICROPRICE",
        momentum_params_path: str = "",
    ) -> None:
        self.symbol = symbol.upper()
        self.lot_size = float(lot_size)
        self.market = market
        self.gateway = gateway
        self.position_manager = position_manager
        self.logger = logger
        # No passive quoting state (momentum-only trader).
        self.max_loss_per_trade = abs(float(max_loss_per_trade))
        self._virtual_account_enabled = bool(virtual_account_enabled)
        self._virtual_account_start_balance = max(0.0, float(virtual_account_start_balance))
        self._virtual_account_max_loss_fraction = max(0.0, float(virtual_account_max_loss_fraction))
        self._virtual_account_halt_enabled = (
            self._virtual_account_enabled
            and self._virtual_account_start_balance > 0.0
            and self._virtual_account_max_loss_fraction > 0.0
        )
        self._entry_forecast_profit_enabled = bool(entry_forecast_profit_enabled)
        self._entry_forecast_alignment_min = float(entry_forecast_alignment_min)
        # Flat config (single-layer momentum decision engine)
        self._spread_threshold = 1.0
        self._move_threshold = 1.0
        self._velocity_threshold = 0.0
        self._delta_threshold = 0.0
        self._imbalance_threshold = 0.2

        # Keep TP configurable to avoid premature profit fixation.
        self._take_profit_per_trade = max(0.0, float(take_profit_per_trade))
        # If max_loss_per_trade is 0 in config, keep stop-loss disabled instead of fallback.
        self._stop_loss_per_trade = self.max_loss_per_trade if self.max_loss_per_trade > 0 else 0.0
        # If virtual account risk is enabled, override per-trade stop based on account constraints.
        if self._virtual_account_halt_enabled:
            stop_abs = self._virtual_account_start_balance * self._virtual_account_max_loss_fraction
            self.max_loss_per_trade = stop_abs
            self._stop_loss_per_trade = stop_abs

        self._virtual_account_max_drawdown_abs = (
            self._virtual_account_start_balance * self._virtual_account_max_loss_fraction
            if self._virtual_account_halt_enabled
            else 0.0
        )
        self._position_qty = 0.0
        self._avg_price = 0.0
        self._position_fixed_dynamic_tp = 0.0
        self._position_fixed_dynamic_tp_detail: dict[str, object] = {}
        self._breakeven_trailing_offset_ticks = max(0.0, float(breakeven_trailing_offset_ticks))
        self._position_peak_mark = 0.0
        self._position_trailing_stop_price = 0.0
        # Fast invalidation: if first tick goes against us -> exit immediately.
        self._immediate_stop_ticks = 1
        self._max_immediate_loss = 0.0
        self._hard_max_loss_per_trade = 0.0  # set after _tick_size / _stop_loss below
        self._immediate_ticks_seen = 0
        self._immediate_filter_active = False
        self._forced_exit_in_progress = False
        self._one_position_only = bool(one_position_only)
        self._reversal_enabled = bool(reversal_enabled)
        self._reversal_confirmation_updates = max(1, int(reversal_confirmation_updates))
        self._reversal_min_trend_strength = max(0.0, float(reversal_min_trend_strength))
        self._reversal_min_hold_ms = max(0.0, float(reversal_min_hold_ms))
        self._reversal_cooldown_sec = max(0.0, float(reversal_cooldown_ms) / 1000.0)
        self._reversal_candidate_side = ""
        self._reversal_candidate_count = 0
        self._position_open_monotonic = 0.0
        self._last_reversal_monotonic = 0.0
        self._weekly_trend_enabled = bool(weekly_trend_enabled)
        self._weekly_trend_db_path = str(weekly_trend_db_path or "").strip()
        self._weekly_trend_days = max(1.0, float(weekly_trend_days))
        self._weekly_trend_threshold_pct = max(0.0, float(weekly_trend_threshold_pct))
        self._weekly_trend_refresh_sec = max(5.0, float(weekly_trend_refresh_sec))
        self._weekly_trend_last_refresh_mono = 0.0
        self._weekly_trend_pct = 0.0
        self._weekly_trend_state = "UNKNOWN"
        self._volume_move_corr_enabled = bool(volume_move_corr_enabled)
        self._volume_move_corr_db_path = str(volume_move_corr_db_path or "").strip()
        self._volume_move_corr_lookback_days = max(1.0, float(volume_move_corr_lookback_days))
        self._volume_move_corr_bar_minutes = max(1, int(volume_move_corr_bar_minutes))
        self._volume_move_corr_min_samples = max(100, int(volume_move_corr_min_samples))
        self._volume_move_corr_threshold = max(0.0, min(1.0, float(volume_move_corr_threshold)))
        self._volume_move_corr_refresh_sec = max(10.0, float(volume_move_corr_refresh_sec))
        self._volume_move_corr_last_refresh_mono = 0.0
        self._volume_move_corr_value = 0.0
        self._volume_move_corr_samples = 0
        self._five_min_entry_gate_enabled = bool(five_min_entry_gate_enabled)
        self._five_min_impulse_min_ticks = max(0.0, float(five_min_impulse_min_ticks))
        self._five_min_volume_ratio_min = max(0.0, float(five_min_volume_ratio_min))
        self._five_min_impulse_ticks = 0.0
        self._five_min_volume_ratio = 0.0
        self._five_min_side_hint = "NONE"
        self._trend_continuation_enabled = bool(trend_continuation_enabled)
        self._trend_continuation_confirm_updates = max(1, int(trend_continuation_confirm_updates))
        self._trend_continuation_min_move_ticks = max(0.0, float(trend_continuation_min_move_ticks))
        self._flow_bias_window_ticks = max(2, int(flow_bias_window_ticks))
        self._flow_bias_history: deque[float] = deque(maxlen=self._flow_bias_window_ticks)
        self._trade_flow_window_size = 20
        self._trade_flow_window: deque[tuple[float, float, float]] = deque(maxlen=self._trade_flow_window_size)
        self._long_flow_bias_min = max(-1.0, min(1.0, float(long_flow_bias_min)))
        self._long_trend_bias_min_ticks = max(0.0, float(long_trend_bias_min_ticks))
        self._long_impulse_override_ticks = max(0.0, float(long_impulse_override_ticks))
        self._short_flow_bias_max = max(-1.0, min(1.0, float(short_flow_bias_max)))
        self._short_trend_bias_min_ticks = max(0.0, float(short_trend_bias_min_ticks))
        self._volume_flow_override_ticks = max(0.0, float(volume_flow_override_ticks))
        self._volume_spike_entry_enabled = bool(volume_spike_entry_enabled)
        self._volume_spike_multiplier = max(1.0, float(volume_spike_multiplier))
        self._volume_spike_flow_bias_abs_min = max(0.0, min(1.0, float(volume_spike_flow_bias_abs_min)))
        self._volume_total_history: deque[float] = deque(maxlen=max(5, int(volume_spike_window_ticks)))
        self._trend_continuation_candidate_side = ""
        self._trend_continuation_candidate_count = 0
        # Legacy: momentum exits removed; keep arg for compatibility.
        self._momentum_exit_enabled = False
        self._ignore_duplicate_ticks_sec = max(0.0, float(ignore_duplicate_ticks_ms) / 1000.0)
        # Single-layer momentum engine: no decision batching/confirmations.
        self._cooldown_sec = max(0.0, float(cooldown_ms) / 1000.0)
        self._last_tick_bid = 0.0
        self._last_tick_ask = 0.0
        self._last_tick_mid = 0.0
        self._last_tick_mono = 0.0
        # legacy decision-loop state removed
        self._tray_price_every_sec = max(0.0, float(tray_price_every_sec))
        self._last_tray_log_mono = 0.0
        self._dynamic_profit_target_enabled = bool(dynamic_profit_target_enabled)
        self._dynamic_profit_target_vol_multiplier = max(0.0, float(dynamic_profit_target_vol_multiplier))
        self._dynamic_profit_target_momentum_weight = max(0.0, float(dynamic_profit_target_momentum_weight))
        self._dynamic_profit_target_flow_weight = max(0.0, float(dynamic_profit_target_flow_weight))
        self._dynamic_profit_target_trend_weight = max(0.0, float(dynamic_profit_target_trend_weight))
        self._dynamic_profit_target_min = max(0.0, float(dynamic_profit_target_min))
        self._dynamic_profit_target_max = max(self._dynamic_profit_target_min, float(dynamic_profit_target_max))
        self._volatility_window_ticks = max(2, int(volatility_window_ticks))
        self._max_short_term_volatility = max(0.0, float(max_short_term_volatility))
        self._cancel_on_high_volatility = bool(cancel_on_high_volatility)
        self._mid_prices: deque[float] = deque(maxlen=self._volatility_window_ticks)
        self._resting_order_timeout_sec = max(0.0, float(resting_order_timeout_sec))
        self._tick_size = max(1e-9, float(tick_size))
        if self._stop_loss_per_trade > 0:
            self._hard_max_loss_per_trade = float(self._stop_loss_per_trade)
        else:
            self._hard_max_loss_per_trade = 0.0
        base_replace_ticks = max(0.0, float(replace_threshold_ticks))
        cancel_ticks = float(replace_cancel_threshold_ticks) if replace_cancel_threshold_ticks > 0 else base_replace_ticks
        keep_ticks = (
            float(replace_keep_threshold_ticks)
            if replace_keep_threshold_ticks > 0
            else max(0.0, cancel_ticks * 0.5)
        )
        if keep_ticks >= cancel_ticks and cancel_ticks > 0:
            keep_ticks = max(0.0, cancel_ticks - 0.01)
        self._replace_cancel_threshold_ticks = max(0.0, cancel_ticks)
        self._replace_keep_threshold_ticks = max(0.0, keep_ticks)
        self._replace_persist_sec = max(0.0, float(replace_persist_ms) / 1000.0)
        self._adverse_move_cancel_ticks = max(0, int(adverse_move_cancel_ticks))
        self._fast_cancel_keep_ticks = max(0.0, float(fast_cancel_keep_ticks))
        self._fast_cancel_persist_sec = max(0.0, float(fast_cancel_persist_ms) / 1000.0)
        self._price_tolerance_ticks = max(0.0, float(price_tolerance_ticks))
        self._price_tolerance_pct = max(0.0, float(price_tolerance_pct))
        self._min_order_lifetime_sec = max(0.0, float(min_order_lifetime_ms) / 1000.0)
        self._cancel_replace_cooldown_sec = max(0.0, float(cancel_replace_cooldown_ms) / 1000.0)
        self._trend_window_ticks = max(2, int(trend_window_ticks))
        self._trend_strength_threshold = max(0.0, float(trend_strength_threshold))
        self._cancel_on_strong_trend = bool(cancel_on_strong_trend)
        self._trend_mids: deque[float] = deque(maxlen=self._trend_window_ticks)
        self._post_fill_horizon_sec = max(0.01, float(post_fill_horizon_ms) / 1000.0)
        self._pending_fill_checks: list[dict[str, float | str]] = []
        self._adverse_fill_flags: deque[int] = deque(maxlen=max(5, int(adverse_fill_window)))
        self._adverse_fill_rate_threshold = min(1.0, max(0.0, float(adverse_fill_rate_threshold)))
        self._defensive_quote_offset_ticks = max(0, int(defensive_quote_offset_ticks))
        self._last_defensive_mode = False
        # legacy confirmation/batching args ignored
        self._latest_data: MarketData | None = None
        self._latest_mid_price = 0.0
        self._latest_bid = 0.0
        self._latest_ask = 0.0
        self._latest_spread = 0.0
        self._entry_min_spread = max(0.0, float(entry_min_spread))
        self._entry_stability_window_ticks = max(2, int(entry_stability_window_ticks))
        self._entry_max_bid_ask_move_ticks = max(0.0, float(entry_max_bid_ask_move_ticks))
        self._entry_anti_trend_threshold = max(0.0, float(entry_anti_trend_threshold))
        self._entry_direction_window_ticks = max(2, int(entry_direction_window_ticks))
        self._entry_direction_min_move_ticks = max(0.0, float(entry_direction_min_move_ticks))
        self._trade_cooldown_sec = max(0.0, float(trade_cooldown_ms) / 1000.0)
        self._entry_min_place_interval_sec = max(0.0, float(entry_min_place_interval_ms) / 1000.0)
        self._entry_score_threshold = max(0.0, float(entry_score_threshold))
        self._adaptive_learning_lock = RLock()
        # microprice edge threshold no longer used in momentum-only entry
        self._signal_mid_history: deque[tuple[float, float]] = deque(maxlen=600)
        self._signal_bid_history: deque[tuple[float, float]] = deque(maxlen=600)
        self._signal_ask_history: deque[tuple[float, float]] = deque(maxlen=600)
        # spread median used only for optional diagnostics, keep a small window
        self._spread_history: deque[float] = deque(maxlen=max(10, int(spread_median_window_ticks)))
        self._anti_adverse_window_sec = max(0.0, float(anti_adverse_window_ms) / 1000.0)
        # Post-fill KPI / adaptive learning removed (momentum-only).
        self._current_trade_reduction_factor = 1.0
        self._entry_eval_counter = 0
        self._entry_order_context: dict[str, dict[str, object]] = {}
        self._entry_trade_context: dict[str, dict[str, object]] = {}
        self._entry_context_fallback_by_side: dict[str, dict[str, object]] = {}
        self._bid_history: deque[float] = deque(maxlen=self._entry_stability_window_ticks)
        self._ask_history: deque[float] = deque(maxlen=self._entry_stability_window_ticks)
        self._entry_direction_mids: deque[float] = deque(maxlen=self._entry_direction_window_ticks)
        self._last_fill_monotonic = 0.0
        self._last_entry_place_monotonic = 0.0
        # No decision confirmation (we decide pre-trade, immediately).
        # cancel/replace analytics removed (momentum-only)
        self._entry_decision_sink = entry_decision_sink
        self._position_policy_enabled = bool(position_policy_enabled)
        _at_path = Path(adaptive_targets_path.strip()) if adaptive_targets_path and adaptive_targets_path.strip() else None
        if _at_path is not None and not _at_path.is_file():
            _at_path = None
        if _at_path is None:
            _bundled = Path(__file__).resolve().parent / "adaptive_learning_targets.json"
            _at_path = _bundled if _bundled.is_file() else None
        self._adaptive_targets = load_adaptive_learning_targets(_at_path)
        self._policy_runtime = PositionPolicyRuntime()
        # Learning patch disabled/removed in momentum-only mode.
        self._economics_store = economics_store
        self._restore_virtual_account_state_from_economics()
        # Single-strategy engine: VOLUME_FOLLOW (volume/flow-based trend following).
        # Keep constructor args for backward compatibility, but ignore legacy strategy modes.
        self._strategy_mode = "MOMENTUM_ACTIVE"
        self.logger.info(
            "[MM][POSITION_POLICY] enabled=%s adaptive_targets_file=%s",
            self._position_policy_enabled,
            str(_at_path) if _at_path is not None else "embedded_defaults",
        )
        self.logger.info(
            "[MM][ADAPTIVE_TARGETS] symbol=%s targets=%s",
            self.symbol,
            self._adaptive_targets.to_dict(),
        )
        self.logger.info(
            "[MM][ENTRY_RULES] symbol=%s anti_adverse_ms=%d spread_median_window_ticks=%d",
            self.symbol,
            int(self._anti_adverse_window_sec * 1000.0),
            len(self._spread_history),
        )
        self.logger.info("[MM][STRATEGY_MODE] mode=%s", self._strategy_mode)

    def on_market_data(self, data: MarketData) -> None:
        if data.symbol.upper() != self.symbol:
            return
        if data.bid <= 0 or data.ask <= 0 or data.ask < data.bid:
            return
        now_mono = time.monotonic()
        bid_f = float(data.bid)
        ask_f = float(data.ask)
        mid_f = float(data.mid_price)
        if self._ignore_duplicate_ticks_sec > 0.0 and self._last_tick_mono > 0.0:
            if (
                abs(bid_f - self._last_tick_bid) < 1e-12
                and abs(ask_f - self._last_tick_ask) < 1e-12
                and abs(mid_f - self._last_tick_mid) < 1e-12
                and (now_mono - self._last_tick_mono) <= self._ignore_duplicate_ticks_sec
            ):
                return
        prev_mid = self._last_tick_mid
        self._last_tick_bid = bid_f
        self._last_tick_ask = ask_f
        self._last_tick_mid = mid_f
        self._last_tick_mono = now_mono
        self._latest_data = data
        self._latest_mid_price = mid_f
        self._latest_bid = bid_f
        self._latest_ask = ask_f
        self._latest_spread = ask_f - bid_f
        self._bid_history.append(bid_f)
        self._ask_history.append(ask_f)
        self._entry_direction_mids.append(mid_f)
        self._signal_mid_history.append((now_mono, mid_f))
        self._signal_bid_history.append((now_mono, bid_f))
        self._signal_ask_history.append((now_mono, ask_f))
        self._spread_history.append(float(self._latest_spread))
        bid_size = float(getattr(data, "bid_size", 0.0) or 0.0)
        ask_size = float(getattr(data, "ask_size", 0.0) or 0.0)
        total_size = bid_size + ask_size
        flow_bias_tick = 0.0
        if total_size > 0.0:
            flow_bias_tick = (bid_size - ask_size) / total_size
        self._flow_bias_history.append(float(flow_bias_tick))
        self._volume_total_history.append(float(total_size))
        buy_aggr_vol = 0.0
        sell_aggr_vol = 0.0
        if prev_mid > 0.0:
            if mid_f > prev_mid:
                buy_aggr_vol = ask_size if ask_size > 0.0 else max(0.0, total_size * 0.5)
            elif mid_f < prev_mid:
                sell_aggr_vol = bid_size if bid_size > 0.0 else max(0.0, total_size * 0.5)
        self._trade_flow_window.append((now_mono, float(buy_aggr_vol), float(sell_aggr_vol)))
        if self._tray_price_every_sec > 0.0:
            if self._last_tray_log_mono <= 0.0 or (now_mono - self._last_tray_log_mono) >= self._tray_price_every_sec:
                self._last_tray_log_mono = now_mono
                self.logger.info(
                    "[MM][TRAY] symbol=%s ts=%s bid=%.4f ask=%.4f mid=%.4f spread=%.4f",
                    self.symbol,
                    data.timestamp.isoformat(),
                    self._latest_bid,
                    self._latest_ask,
                    self._latest_mid_price,
                    self._latest_spread,
                )
        # Momentum-only trader (no quoting / no passive MM).
        self._strategy_mode = "MOMENTUM_ACTIVE"

        latest = self._latest_data
        if latest is None:
            return

        # Exit first (fast invalidation / risk exits).
        if self._maybe_force_exit(latest):
            return
        if self._forced_exit_in_progress:
            return
        # Simple cooldown (ms) between entry attempts.
        if self._cooldown_sec > 0.0 and self._last_entry_place_monotonic > 0.0:
            if (now_mono - self._last_entry_place_monotonic) < self._cooldown_sec:
                return

        self._evaluate_and_execute_entry(latest=latest, now_mono=now_mono)

    def on_execution_report(self, state: dict[str, str]) -> None:
        symbol = state.get("symbol", "").upper()
        if symbol != self.symbol:
            return
        last_qty = float(state.get("last_qty", "0") or "0")
        if last_qty <= 0:
            return
        side = state.get("side", "1")
        last_px = float(state.get("last_px", "0") or "0")
        if last_px <= 0:
            return
        cl_ord_id = state.get("cl_ord_id", "")
        exec_id = state.get("exec_id", "")
        if cl_ord_id and exec_id:
            with self._adaptive_learning_lock:
                ctx = self._entry_order_context.get(cl_ord_id)
                if ctx is not None:
                    self._entry_trade_context[exec_id] = dict(ctx)
                else:
                    side_label = "BUY" if str(side).strip().upper() in {"1", "BUY", "B"} else "SELL"
                    fallback = self._entry_context_fallback_by_side.get(side_label)
                    if fallback is not None:
                        patched = dict(fallback)
                        patched["order_id"] = cl_ord_id
                        patched["side"] = side_label
                        self._entry_trade_context[exec_id] = patched
                        self.logger.info(
                            "[MM][ENTRY_CONTEXT_FALLBACK] symbol=%s exec_id=%s cl_ord_id=%s side=%s",
                            self.symbol,
                            exec_id,
                            cl_ord_id,
                            side_label,
                        )
        prev_qty = self._position_qty
        self._last_fill_monotonic = time.monotonic()
        self._apply_fill(side=side, qty=last_qty, price=last_px)
        new_qty = self._position_qty
        if (abs(prev_qty) <= 1e-9 and abs(new_qty) > 1e-9) or (prev_qty * new_qty < 0.0):
            self._position_open_monotonic = time.monotonic()
            self._position_fixed_dynamic_tp = 0.0
            self._position_fixed_dynamic_tp_detail = {}
            self._position_peak_mark = float(last_px)
            self._position_trailing_stop_price = 0.0
            self._immediate_ticks_seen = 0
            self._immediate_filter_active = True
            if self._dynamic_profit_target_enabled:
                latest = self._latest_data
                if latest is not None:
                    dyn_tp, detail = self._compute_dynamic_tp_for_position(
                        mark=float(latest.mid_price),
                        now_mono=time.monotonic(),
                        qty=new_qty,
                        data=latest,
                    )
                    self._position_fixed_dynamic_tp = float(dyn_tp)
                    self._position_fixed_dynamic_tp_detail = dict(detail)
                    self.logger.info(
                        "[MM][DYN_TP_FIXED] symbol=%s side=%s tp=%.4f detail=%s",
                        self.symbol,
                        "BUY" if new_qty > 0 else "SELL",
                        self._position_fixed_dynamic_tp,
                        self._position_fixed_dynamic_tp_detail,
                    )
        elif abs(new_qty) <= 1e-9:
            self._position_open_monotonic = 0.0
            self._reversal_candidate_side = ""
            self._reversal_candidate_count = 0
            self._position_fixed_dynamic_tp = 0.0
            self._position_fixed_dynamic_tp_detail = {}
            self._position_peak_mark = 0.0
            self._position_trailing_stop_price = 0.0
            self._immediate_ticks_seen = 0
            self._immediate_filter_active = False
        if self._position_policy_enabled:
            if abs(prev_qty) <= 1e-9 and abs(new_qty) > 1e-9:
                lat_ms = (
                    (time.monotonic() - self._last_entry_place_monotonic) * 1000.0
                    if self._last_entry_place_monotonic > 0.0
                    else 0.0
                )
                self._policy_runtime.on_position_open(
                    entry_price=last_px,
                    qty=new_qty,
                    direction_label="BUY" if new_qty > 0.0 else "SELL",
                    spread_at_entry=float(self._latest_spread),
                    latency_ms=lat_ms,
                    entry_mono=time.monotonic(),
                    entry_exec_id=exec_id,
                )
            elif abs(new_qty) <= 1e-9 and abs(prev_qty) > 1e-9:
                self._policy_runtime.on_position_flat()
        if abs(self._position_qty) <= 1e-9:
            self._forced_exit_in_progress = False
            self._immediate_ticks_seen = 0
            self._immediate_filter_active = False

    def on_round_trip_outcomes(self, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        with self._adaptive_learning_lock:
            for row in rows:
                entry_trade_id = str(row.get("entry_trade_id", ""))
                if not entry_trade_id:
                    continue
                context = self._entry_trade_context.pop(entry_trade_id, None)
                if context is None:
                    continue
                immediate_move = float(row.get("immediate_move", 0.0))
                trade_record = {
                    "entry_trade_id": entry_trade_id,
                    "order_id": str(context.get("order_id", "")),
                    "symbol": self.symbol,
                    "side": str(context.get("side", row.get("side", ""))),
                    "entry_type": str(context.get("entry_type", "aggressive")),
                    "momentum_100ms": float(context.get("momentum_100ms", 0.0)),
                    "last_50ms_price_move": float(context.get("last_50ms_price_move", 0.0)),
                    "spread": float(context.get("spread", 0.0)),
                    "bid_size": float(context.get("bid_size", 0.0)),
                    "ask_size": float(context.get("ask_size", 0.0)),
                    "imbalance": float(context.get("imbalance", 0.0)),
                    "entry_price": float(context.get("entry_price", 0.0)),
                    "mid_price": float(context.get("mid_price", 0.0)),
                    "pnl": float(row.get("total_pnl", 0.0)),
                    "mfe": float(row.get("mfe", 0.0) or 0.0),
                    "mae": float(row.get("mae", 0.0) or 0.0),
                    "fees": float(row.get("fees", 0.0) or 0.0),
                    "latency_ms": float(
                        context.get("latency_ms", row.get("entry_time_in_book_ms", 0.0)) or 0.0
                    ),
                    "adverse_flag": immediate_move < 0.0,
                    "immediate_move": immediate_move,
                    "timestamp": str(row.get("exit_ts", "")),
                }
                # Keep minimal logging/metrics; no post-fill analysis or adaptive thresholding.
                self._policy_runtime.virtual.apply_close(
                    net_pnl=float(row.get("total_pnl", 0.0) or 0.0),
                    fees=float(row.get("fees", 0.0) or 0.0),
                )

    def _forecast_dynamic_profit_target_for_side(
        self,
        *,
        latest: MarketData,
        now_mono: float,
        side_label: str,
    ) -> dict[str, float]:
        """
        Прогноз величины `dynamic_tp` (в тех же единицах, что PnL unrealized),
        чтобы осознанно фильтровать входы.
        """
        mark = float(latest.mid_price)
        vol_ratio = self._current_short_term_volatility()
        vol_abs = mark * vol_ratio * self._dynamic_profit_target_vol_multiplier

        mom_500 = abs(self._mid_delta_over_window(now_mono=now_mono, window_ms=500) or 0.0)
        mom_abs = mom_500 * self._dynamic_profit_target_momentum_weight

        bid_size = float(getattr(latest, "bid_size", 0.0) or 0.0)
        ask_size = float(getattr(latest, "ask_size", 0.0) or 0.0)
        flow_bias = 0.0
        total_size = bid_size + ask_size
        if total_size > 0.0:
            flow_bias = (bid_size - ask_size) / total_size

        trend_ticks = (self._mid_delta_over_window(now_mono=now_mono, window_ms=500) or 0.0) / self._tick_size
        trend_bias = max(-1.0, min(1.0, trend_ticks / 2.0))

        side = str(side_label).strip().upper()
        side_sign = 1.0 if side == "BUY" else -1.0

        alignment = side_sign * (
            self._dynamic_profit_target_flow_weight * flow_bias + self._dynamic_profit_target_trend_weight * trend_bias
        )
        alignment = max(-0.85, min(1.25, alignment))

        dyn_factor = max(0.25, 1.0 + alignment)
        dyn_tp_raw = (vol_abs + mom_abs) * dyn_factor
        dyn_tp = max(self._dynamic_profit_target_min, dyn_tp_raw)
        dyn_tp = min(self._dynamic_profit_target_max, dyn_tp)

        return {
            "mark": mark,
            "vol_ratio": float(vol_ratio),
            "vol_abs": float(vol_abs),
            "mom_abs": float(mom_abs),
            "flow_bias": float(flow_bias),
            "trend_bias": float(trend_bias),
            "alignment": float(alignment),
            "dyn_tp_raw": float(dyn_tp_raw),
            "dyn_tp": float(dyn_tp),
        }

    def _maybe_force_exit(self, data: MarketData) -> bool:
        qty = self._position_qty
        if abs(qty) <= 1e-9 or self._avg_price <= 0:
            return False
        if self._forced_exit_in_progress:
            return True
        avg_before = self._avg_price
        mark = float(data.mid_price)
        unrealized = (mark - avg_before) * qty
        tp_eff, sl_eff, hold_ms_eff = self._effective_exit_params()
        if self._position_policy_enabled:
            self._policy_runtime.update_excursions(unrealized)
        momentum_controls_exit = False
        if (
            not momentum_controls_exit
            and not self._position_policy_enabled
            and self._take_profit_per_trade <= 0
            and self._stop_loss_per_trade <= 0
        ):
            return False
        now_mono = time.monotonic()
        exit_reason: str | None = None
        policy_detail: dict[str, object] = {}

        if (
            self._immediate_filter_active
            and int(self._immediate_stop_ticks) > 0
            and self._immediate_ticks_seen < max(1, int(self._immediate_stop_ticks))
        ):
            self._immediate_ticks_seen += 1
            adverse_tick = (qty > 0.0 and mark <= avg_before) or (qty < 0.0 and mark >= avg_before)
            if adverse_tick:
                exit_reason = "IMMEDIATE_FAST_STOP"
                policy_detail = {
                    "fast_stop_triggered": True,
                    "reason": "first_tick_wrong_direction",
                    "source": "immediate_move_guard",
                    "ticks_seen": int(self._immediate_ticks_seen),
                    "ticks_limit": int(self._immediate_stop_ticks),
                    "entry_price": float(avg_before),
                    "mark": float(mark),
                    "unrealized": float(unrealized),
                    "adverse_tick": bool(adverse_tick),
                }
            if self._immediate_ticks_seen >= max(1, int(self._immediate_stop_ticks)):
                self._immediate_filter_active = False

        if (
            exit_reason is None
            and self._hard_max_loss_per_trade > 0.0
            and unrealized <= -float(self._hard_max_loss_per_trade)
        ):
            exit_reason = "HARD_MAX_LOSS"
            policy_detail = {
                "source": "hard_max_loss_guard",
                "max_loss_per_trade": float(self._hard_max_loss_per_trade),
                "unrealized": float(unrealized),
            }

        # Profit-following breakeven trail:
        # once price moves in our favor by offset ticks, keep stop at (best_price -/+ offset).
        trail_offset_abs = self._breakeven_trailing_offset_ticks * max(1e-9, self._tick_size)
        if trail_offset_abs > 0.0:
            if qty > 0.0:
                if self._position_peak_mark <= 0.0:
                    self._position_peak_mark = mark
                self._position_peak_mark = max(self._position_peak_mark, mark)
                if (self._position_peak_mark - avg_before) >= trail_offset_abs:
                    candidate = self._position_peak_mark - trail_offset_abs
                    self._position_trailing_stop_price = max(self._position_trailing_stop_price, candidate)
                    if mark <= self._position_trailing_stop_price:
                        exit_reason = "TRAIL_BE_5"
                        policy_detail = {
                            "source": "breakeven_trailing",
                            "peak_mark": self._position_peak_mark,
                            "trail_stop_price": self._position_trailing_stop_price,
                            "offset_ticks": self._breakeven_trailing_offset_ticks,
                        }
            else:
                if self._position_peak_mark <= 0.0:
                    self._position_peak_mark = mark
                self._position_peak_mark = min(self._position_peak_mark, mark)
                if (avg_before - self._position_peak_mark) >= trail_offset_abs:
                    candidate = self._position_peak_mark + trail_offset_abs
                    if self._position_trailing_stop_price <= 0.0:
                        self._position_trailing_stop_price = candidate
                    else:
                        self._position_trailing_stop_price = min(self._position_trailing_stop_price, candidate)
                    if mark >= self._position_trailing_stop_price:
                        exit_reason = "TRAIL_BE_5"
                        policy_detail = {
                            "source": "breakeven_trailing",
                            "peak_mark": self._position_peak_mark,
                            "trail_stop_price": self._position_trailing_stop_price,
                            "offset_ticks": self._breakeven_trailing_offset_ticks,
                        }

        if exit_reason is None and self._virtual_account_drawdown_hit(unrealized=unrealized):
            exit_reason = "VACCOUNT_MAX_DD"
            policy_detail = {
                "source": "virtual_account_dd",
                "virtual_equity": self._virtual_equity_current(unrealized=unrealized),
                "virtual_drawdown_abs": self._virtual_drawdown_abs(unrealized=unrealized),
                "virtual_dd_limit_abs": self._virtual_account_max_drawdown_abs,
                "unrealized": unrealized,
            }

        # Protective loss guard must stay active even when momentum exit module
        # controls discretionary exits, otherwise losses can drift until global DD halt.
        if exit_reason is None and sl_eff > 0.0 and unrealized <= -sl_eff:
            exit_reason = "SL_GUARD"
            policy_detail = {
                "source": "protective_stop_guard",
                "stop_loss_abs": float(sl_eff),
                "unrealized": float(unrealized),
            }
        if exit_reason is None and self._position_policy_enabled:
            exit_reason, policy_detail = self._policy_runtime.evaluate_exit(
                unrealized=unrealized,
                now_mono=now_mono,
                take_profit_abs=tp_eff,
                stop_loss_abs=sl_eff,
                max_hold_ms=hold_ms_eff,
                targets=self._adaptive_targets,
                policy_enabled=True,
                skip_stop_while_in_profit=True,
            )
        if exit_reason is None and not self._position_policy_enabled:
            dyn_tp = self._take_profit_per_trade
            if self._dynamic_profit_target_enabled:
                # Dynamic TP is fixed at position open and reused until flat.
                if self._position_fixed_dynamic_tp > 0.0:
                    dyn_tp = float(self._position_fixed_dynamic_tp)
                    policy_detail = dict(self._position_fixed_dynamic_tp_detail or {})
                else:
                    dyn_tp, policy_detail = self._compute_dynamic_tp_for_position(
                        mark=mark,
                        now_mono=now_mono,
                        qty=qty,
                        data=data,
                    )
                    self._position_fixed_dynamic_tp = float(dyn_tp)
                    self._position_fixed_dynamic_tp_detail = dict(policy_detail)
                policy_detail["dynamic_tp_fixed"] = True
            if dyn_tp > 0 and unrealized >= dyn_tp:
                exit_reason = "DYN_TP" if self._dynamic_profit_target_enabled else "TP"
            elif self._stop_loss_per_trade > 0 and unrealized <= -self._stop_loss_per_trade:
                exit_reason = "SL"
        if exit_reason is None:
            return False
        close_side = "2" if qty > 0 else "1"
        close_qty = abs(qty)
        # Force close as a marketable limit (aggressive, no market orders).
        close_price = float(data.bid) if close_side == "2" else float(data.ask)
        try:
            forced_id = self.gateway.send_order(
                OrderRequest(
                    symbol=self.symbol,
                    side=close_side,
                    qty=close_qty,
                    account="",
                    market=self.market,
                    lot_size=1,
                    price=close_price,
                    bypass_risk=True,
                )
            )
            self._forced_exit_in_progress = True
            self.logger.warning(
                "[FORCED_EXIT] symbol=%s exit_reason=%s qty=%.4f side=%s avg=%.4f mark=%.4f unrealized=%.4f take_profit=%.4f stop_loss=%.4f hold_ms_cap=%.0f policy_detail=%s cl_ord_id=%s",
                self.symbol,
                exit_reason,
                close_qty,
                close_side,
                avg_before,
                mark,
                unrealized,
                tp_eff,
                sl_eff,
                hold_ms_eff,
                policy_detail,
                forced_id,
            )
        except Exception as exc:
            self._forced_exit_in_progress = False
            self.logger.error(
                "[FORCED_EXIT][FAILED] symbol=%s exit_reason=%s qty=%.4f side=%s unrealized=%.4f take_profit=%.4f stop_loss=%.4f err=%s",
                self.symbol,
                exit_reason,
                close_qty,
                close_side,
                unrealized,
                tp_eff,
                sl_eff,
                exc,
            )
        return True

    def _apply_fill(self, *, side: str, qty: float, price: float) -> None:
        sign = 1.0 if str(side).strip().upper() in {"1", "BUY", "B"} else -1.0
        fill_qty = abs(float(qty))
        fill_px = float(price)
        if fill_qty <= 0:
            return
        if (
            self._position_qty == 0
            or (self._position_qty > 0 and sign > 0)
            or (self._position_qty < 0 and sign < 0)
        ):
            new_qty = self._position_qty + sign * fill_qty
            if abs(new_qty) > 1e-9:
                gross_notional = abs(self._position_qty) * self._avg_price + fill_qty * fill_px
                self._avg_price = gross_notional / abs(new_qty)
            self._position_qty = new_qty
            return

        closing_qty = min(abs(self._position_qty), fill_qty)
        self._position_qty += sign * fill_qty
        if abs(self._position_qty) <= 1e-9:
            self._position_qty = 0.0
            self._avg_price = 0.0
        elif closing_qty < fill_qty:
            self._avg_price = fill_px

    def _compute_dynamic_tp_for_position(
        self,
        *,
        mark: float,
        now_mono: float,
        qty: float,
        data: MarketData,
    ) -> tuple[float, dict[str, object]]:
        vol_ratio = self._current_short_term_volatility()
        vol_abs = mark * vol_ratio * self._dynamic_profit_target_vol_multiplier
        mom_500 = abs(self._mid_delta_over_window(now_mono=now_mono, window_ms=500) or 0.0)
        mom_abs = mom_500 * self._dynamic_profit_target_momentum_weight
        bid_size = float(getattr(data, "bid_size", 0.0) or 0.0)
        ask_size = float(getattr(data, "ask_size", 0.0) or 0.0)
        flow_bias = 0.0
        total_size = bid_size + ask_size
        if total_size > 0.0:
            flow_bias = (bid_size - ask_size) / total_size
        trend_ticks = (self._mid_delta_over_window(now_mono=now_mono, window_ms=500) or 0.0) / self._tick_size
        trend_bias = max(-1.0, min(1.0, trend_ticks / 2.0))
        side_sign = 1.0 if qty > 0 else -1.0
        alignment = side_sign * (
            self._dynamic_profit_target_flow_weight * flow_bias
            + self._dynamic_profit_target_trend_weight * trend_bias
        )
        alignment = max(-0.85, min(1.25, alignment))
        dyn_tp = (vol_abs + mom_abs) * max(0.25, 1.0 + alignment)
        dyn_tp = max(self._dynamic_profit_target_min, dyn_tp)
        dyn_tp = min(self._dynamic_profit_target_max, dyn_tp)
        detail = {
            "dynamic_tp": dyn_tp,
            "vol_abs": vol_abs,
            "mom_abs": mom_abs,
            "flow_bias": flow_bias,
            "trend_bias": trend_bias,
            "alignment": alignment,
            "source": "dynamic_profit_target",
        }
        return float(dyn_tp), detail

    def _virtual_realized_net(self) -> float:
        if not self._virtual_account_enabled:
            return 0.0
        # PositionPolicyRuntime.virtual.realized_net is updated on round-trips.
        try:
            return float(getattr(self._policy_runtime.virtual, "realized_net", 0.0) or 0.0)
        except Exception:
            return 0.0

    def _virtual_equity_current(self, *, unrealized: float = 0.0) -> float:
        if not self._virtual_account_enabled:
            return 0.0
        return float(self._virtual_account_start_balance + self._virtual_realized_net() + float(unrealized or 0.0))

    def _virtual_drawdown_abs(self, *, unrealized: float = 0.0) -> float:
        if not self._virtual_account_halt_enabled:
            return 0.0
        eq = self._virtual_equity_current(unrealized=unrealized)
        return max(0.0, float(self._virtual_account_start_balance - eq))

    def _virtual_account_drawdown_hit(self, *, unrealized: float = 0.0) -> bool:
        if not self._virtual_account_halt_enabled:
            return False
        if self._virtual_account_max_drawdown_abs <= 0.0:
            return False
        return self._virtual_drawdown_abs(unrealized=unrealized) >= self._virtual_account_max_drawdown_abs

    def _restore_virtual_account_state_from_economics(self) -> None:
        """
        Restore virtual realized PnL across restarts so DD halt
        does not reset just because the process was restarted.
        """
        if not self._virtual_account_enabled:
            return
        if self._economics_store is None:
            return
        try:
            # Prefer session-scoped restore (marker written by main.py on process start)
            # so old historical PnL does not immediately halt new runs.
            realized: float | None = None
            restore_source = "all_time"
            marker_path = Path(__file__).resolve().parent / "log" / "session_start_marker.txt"
            db_path = Path(str(getattr(self._economics_store, "_db_path", "")))
            if marker_path.exists() and db_path.exists():
                raw_marker = marker_path.read_text(encoding="utf-8", errors="ignore").strip()
                if raw_marker:
                    marker_dt = datetime.fromisoformat(raw_marker.replace("Z", "+00:00"))
                    if marker_dt.tzinfo is None:
                        marker_dt = marker_dt.replace(tzinfo=timezone.utc)
                    marker_dt = marker_dt.astimezone(timezone.utc)
                    marker_sql = marker_dt.strftime("%Y-%m-%d %H:%M:%S")
                    with sqlite3.connect(str(db_path)) as conn:
                        row = conn.execute(
                            """
                            SELECT COALESCE(SUM(net_pnl), 0.0)
                            FROM trade_economics
                            WHERE symbol = ? AND created_at >= ?
                            """,
                            (self.symbol, marker_sql),
                        ).fetchone()
                    realized = float(row[0] or 0.0) if row is not None else 0.0
                    restore_source = "session_marker"
            if realized is None:
                metrics = self._economics_store.get_metrics()
                realized = float(metrics.get("cumulative_pnl", 0.0) or 0.0)
            self._policy_runtime.virtual.realized_net = realized
            self.logger.info(
                "[MM][VACCOUNT_RESTORE] symbol=%s source=%s restored_realized=%.4f start_balance=%.2f dd_abs=%.4f dd_limit_abs=%.4f",
                self.symbol,
                restore_source,
                realized,
                self._virtual_account_start_balance,
                self._virtual_drawdown_abs(unrealized=0.0),
                self._virtual_account_max_drawdown_abs,
            )
        except Exception as exc:
            self.logger.warning("[MM][VACCOUNT_RESTORE][FAILED] symbol=%s err=%s", self.symbol, exc)

    def _current_short_term_volatility(self) -> float:
        if len(self._mid_prices) < 2:
            return 0.0
        first = float(self._mid_prices[0])
        last = float(self._mid_prices[-1])
        if first <= 0:
            return 0.0
        return abs(last - first) / first

    def _current_short_term_trend(self) -> float:
        if len(self._trend_mids) < 2:
            return 0.0
        first = float(self._trend_mids[0])
        last = float(self._trend_mids[-1])
        if first <= 0:
            return 0.0
        return (last - first) / first

    def _sync_aggressive_entry_price_for_side(self, decision: dict[str, object]) -> None:
        """Align aggressive entry_price with routed side (ask for BUY, bid for SELL)."""
        side_label = str(decision.get("side", "NONE")).strip().upper()
        bp = float(decision.get("bid_price", 0.0))
        ap = float(decision.get("ask_price", 0.0))
        if side_label == "BUY":
            decision["entry_price"] = ap
        elif side_label == "SELL":
            decision["entry_price"] = bp

    # Trend/secondary modules removed (single-layer decision).
    def evaluate_entry_signal(self, market_snapshot: dict[str, float]) -> str:
        """
        Single-layer momentum decision engine.
        Input: pure snapshot of features. Output: LONG / SHORT / NONE.
        No side effects.
        """
        spread = float(market_snapshot.get("spread", 0.0))
        price_move_ticks = float(market_snapshot.get("price_move_ticks", 0.0))
        velocity = float(market_snapshot.get("velocity", 0.0))
        delta = float(market_snapshot.get("delta", 0.0))
        imbalance = float(market_snapshot.get("imbalance", 0.0))
        move_thr = float(market_snapshot.get("move_threshold", 0.0))
        vel_thr = float(market_snapshot.get("velocity_threshold", 0.0))
        delta_thr = float(market_snapshot.get("delta_threshold", 0.0))
        imb_thr = float(market_snapshot.get("imbalance_threshold", 0.0))
        if spread > float(self._spread_threshold):
            return "NONE"
        if abs(price_move_ticks) < move_thr:
            return "NONE"
        if abs(velocity) < vel_thr:
            return "NONE"
        if abs(delta) < delta_thr:
            return "NONE"
        if abs(imbalance) < imb_thr:
            return "NONE"
        return "LONG" if price_move_ticks > 0.0 else "SHORT"

    def _evaluate_and_execute_entry(self, *, latest: MarketData, now_mono: float) -> None:
        if self._one_position_only and abs(self._position_qty) > 1e-9:
            return
        decision = self._evaluate_entry_signal(latest=latest, now_mono=now_mono)
        self._log_entry_decision(decision)
        self._persist_entry_decision(decision)
        if str(decision.get("decision", "SKIP")) != "EXECUTE":
            return
        # Inventory gate only (no confirmations / multi-layer logic).
        side = str(decision.get("side", "NONE"))
        if side == "BUY" and not self.position_manager.can_place_buy(self.symbol, self.lot_size):
            return
        if side == "SELL" and not self.position_manager.can_place_sell(self.symbol, self.lot_size):
            return
        self._send_aggressive_entry(decision=decision)

    def _maybe_refresh_weekly_trend(self, *, now_mono: float) -> None:
        if not self._weekly_trend_db_path:
            self._weekly_trend_state = "UNKNOWN"
            self._weekly_trend_pct = 0.0
            return
        if self._weekly_trend_last_refresh_mono > 0.0:
            if (now_mono - self._weekly_trend_last_refresh_mono) < self._weekly_trend_refresh_sec:
                return
        self._weekly_trend_last_refresh_mono = now_mono
        try:
            now_ms = int(time.time() * 1000)
            from_ms = now_ms - int(self._weekly_trend_days * 86400.0 * 1000.0)
            with sqlite3.connect(self._weekly_trend_db_path) as conn:
                first = conn.execute(
                    """
                    SELECT mid
                    FROM md_quote_snapshots
                    WHERE symbol=? AND ts_ms>=?
                    ORDER BY ts_ms ASC
                    LIMIT 1
                    """,
                    (self.symbol, from_ms),
                ).fetchone()
                last = conn.execute(
                    """
                    SELECT mid
                    FROM md_quote_snapshots
                    WHERE symbol=? AND ts_ms>=?
                    ORDER BY ts_ms DESC
                    LIMIT 1
                    """,
                    (self.symbol, from_ms),
                ).fetchone()
            if not first or not last:
                self._weekly_trend_state = "UNKNOWN"
                self._weekly_trend_pct = 0.0
                return
            first_mid = float(first[0] or 0.0)
            last_mid = float(last[0] or 0.0)
            if first_mid <= 0.0:
                self._weekly_trend_state = "UNKNOWN"
                self._weekly_trend_pct = 0.0
                return
            pct = ((last_mid - first_mid) / first_mid) * 100.0
            self._weekly_trend_pct = pct
            thr = self._weekly_trend_threshold_pct
            if pct >= thr:
                self._weekly_trend_state = "UP"
            elif pct <= -thr:
                self._weekly_trend_state = "DOWN"
            else:
                self._weekly_trend_state = "FLAT"
        except Exception:
            self._weekly_trend_state = "UNKNOWN"
            self._weekly_trend_pct = 0.0

    def _apply_weekly_trend_filter(self, decision: dict[str, object]) -> None:
        decision["weekly_trend_state"] = self._weekly_trend_state
        decision["weekly_trend_pct"] = float(self._weekly_trend_pct)
        if str(decision.get("decision", "SKIP")) != "EXECUTE":
            return
        side = str(decision.get("side", "NONE")).strip().upper()
        if side == "BUY" and self._weekly_trend_state == "DOWN":
            decision["decision"] = "SKIP"
            decision["reason"] = "weekly_trend_down_block_long"
        elif side == "SELL" and self._weekly_trend_state == "UP":
            decision["decision"] = "SKIP"
            decision["reason"] = "weekly_trend_up_block_short"

    def _maybe_refresh_volume_move_correlation(self, *, now_mono: float) -> None:
        if not self._volume_move_corr_enabled or not self._volume_move_corr_db_path:
            self._volume_move_corr_value = 0.0
            self._volume_move_corr_samples = 0
            return
        if self._volume_move_corr_last_refresh_mono > 0.0:
            if (now_mono - self._volume_move_corr_last_refresh_mono) < self._volume_move_corr_refresh_sec:
                return
        self._volume_move_corr_last_refresh_mono = now_mono
        try:
            now_ms = int(time.time() * 1000)
            from_ms = now_ms - int(self._volume_move_corr_lookback_days * 86400.0 * 1000.0)
            bucket_ms = self._volume_move_corr_bar_minutes * 60 * 1000
            with sqlite3.connect(self._volume_move_corr_db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT
                        (ts_ms / ?) * ? AS bucket_start_ms,
                        AVG(mid) AS bucket_mid,
                        AVG(spread) AS bucket_spread,
                        AVG(bid_size + ask_size) AS bucket_volume
                    FROM md_quote_snapshots
                    WHERE symbol=? AND ts_ms>=?
                    GROUP BY bucket_start_ms
                    ORDER BY bucket_start_ms ASC
                    """,
                    (bucket_ms, bucket_ms, self.symbol, from_ms),
                ).fetchall()
            # Shared 5m features for entry decisions:
            # - impulse in ticks between two latest 5m bars
            # - current-bar volume ratio vs trailing 5m baseline
            self._five_min_impulse_ticks = 0.0
            self._five_min_volume_ratio = 0.0
            self._five_min_side_hint = "NONE"
            if len(rows) >= 2:
                last_mid = float(rows[-1][1] or 0.0)
                prev_mid = float(rows[-2][1] or 0.0)
                if self._tick_size > 0.0 and last_mid > 0.0 and prev_mid > 0.0:
                    impulse_ticks = (last_mid - prev_mid) / self._tick_size
                    self._five_min_impulse_ticks = float(impulse_ticks)
                    if impulse_ticks > 0.0:
                        self._five_min_side_hint = "BUY"
                    elif impulse_ticks < 0.0:
                        self._five_min_side_hint = "SELL"
                baseline_count = min(12, max(0, len(rows) - 1))
                if baseline_count > 0:
                    baseline = [max(0.0, float(x[3] or 0.0)) for x in rows[-1 - baseline_count : -1]]
                    baseline_avg = (sum(baseline) / float(len(baseline))) if baseline else 0.0
                    curr_volume = max(0.0, float(rows[-1][3] or 0.0))
                    if baseline_avg > 1e-9:
                        self._five_min_volume_ratio = float(curr_volume / baseline_avg)
            if len(rows) < self._volume_move_corr_min_samples + 1:
                self._volume_move_corr_value = 0.0
                self._volume_move_corr_samples = max(0, len(rows) - 1)
                return
            xs: list[float] = []  # abs move
            ys: list[float] = []  # volume proxy
            prev_mid = float(rows[0][1] or 0.0)
            for _bucket_ms, mid, spread, volume in rows[1:]:
                cur_mid = float(mid or 0.0)
                if prev_mid <= 0.0 or cur_mid <= 0.0:
                    prev_mid = cur_mid
                    continue
                # Strong move proxy: absolute tick move between snapshots.
                abs_move = abs(cur_mid - prev_mid)
                # 5m bar volume proxy from L1 sizes (avg over bucket), de-noised by spread penalty.
                spread_penalty = max(1.0, float(spread or 0.0))
                vol_proxy = max(0.0, float(volume or 0.0)) / spread_penalty
                prev_mid = cur_mid
                xs.append(abs_move)
                ys.append(vol_proxy)
            n = min(len(xs), len(ys))
            if n < self._volume_move_corr_min_samples:
                self._volume_move_corr_value = 0.0
                self._volume_move_corr_samples = n
                return
            mean_x = sum(xs) / n
            mean_y = sum(ys) / n
            cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
            var_x = sum((x - mean_x) ** 2 for x in xs)
            var_y = sum((y - mean_y) ** 2 for y in ys)
            denom = math.sqrt(var_x * var_y)
            corr = (cov / denom) if denom > 1e-12 else 0.0
            self._volume_move_corr_value = float(corr)
            self._volume_move_corr_samples = int(n)
        except Exception:
            self._volume_move_corr_value = 0.0
            self._volume_move_corr_samples = 0

    def _apply_volume_move_correlation_gate(self, *, decision: dict[str, object], now_mono: float) -> None:
        if str(decision.get("decision", "SKIP")) != "EXECUTE":
            return
        self._maybe_refresh_volume_move_correlation(now_mono=now_mono)
        decision["volume_move_corr"] = float(self._volume_move_corr_value)
        decision["volume_move_corr_samples"] = int(self._volume_move_corr_samples)
        decision["volume_move_corr_threshold"] = float(self._volume_move_corr_threshold)
        if not self._volume_move_corr_enabled:
            return
        if self._volume_move_corr_samples < self._volume_move_corr_min_samples:
            decision["decision"] = "SKIP"
            decision["reason"] = "volume_move_corr_insufficient_samples"
            return
        if self._volume_move_corr_value < self._volume_move_corr_threshold:
            decision["decision"] = "SKIP"
            decision["reason"] = "volume_move_corr_below_threshold"
            return

    def _apply_five_min_volume_impulse_gate(self, *, decision: dict[str, object], now_mono: float) -> None:
        if str(decision.get("decision", "SKIP")) != "EXECUTE":
            return
        self._maybe_refresh_volume_move_correlation(now_mono=now_mono)
        decision["five_min_impulse_ticks"] = float(self._five_min_impulse_ticks)
        decision["five_min_volume_ratio"] = float(self._five_min_volume_ratio)
        decision["five_min_side_hint"] = str(self._five_min_side_hint)
        if not self._five_min_entry_gate_enabled:
            return
        side = str(decision.get("side", "NONE")).strip().upper()
        if side not in {"BUY", "SELL"}:
            decision["decision"] = "SKIP"
            decision["reason"] = "five_min_invalid_side"
            return
        if abs(self._five_min_impulse_ticks) < self._five_min_impulse_min_ticks:
            decision["decision"] = "SKIP"
            decision["reason"] = "five_min_impulse_below_threshold"
            return
        if self._five_min_volume_ratio < self._five_min_volume_ratio_min:
            decision["decision"] = "SKIP"
            decision["reason"] = "five_min_volume_ratio_below_threshold"
            return
        if self._five_min_side_hint in {"BUY", "SELL"} and side != self._five_min_side_hint:
            decision["decision"] = "SKIP"
            decision["reason"] = "five_min_impulse_side_mismatch"
            return

    def _effective_entry_edge_threshold(self, *, side: str, spread_tertile_idx: int) -> tuple[float, float]:
        # No learning/patch weighting in momentum-only mode.
        eff_thr = max(0.0, min(1.0, float(self._entry_score_threshold)))
        return float(eff_thr), 1.0

    def _apply_trend_continuation_fallback(self, *, decision: dict[str, object], now_mono: float) -> None:
        if not self._trend_continuation_enabled:
            return
        if str(decision.get("decision", "SKIP")) == "EXECUTE":
            return
        reason = str(decision.get("reason", ""))
        if reason not in {"no_upper_touch", "no_lower_touch"}:
            self._trend_continuation_candidate_side = ""
            self._trend_continuation_candidate_count = 0
            return
        mom100 = self._mid_delta_over_window(now_mono=now_mono, window_ms=100)
        mom500 = self._mid_delta_over_window(now_mono=now_mono, window_ms=500)
        if mom100 is None or mom500 is None:
            return
        move_min = self._trend_continuation_min_move_ticks * self._tick_size
        side = "NONE"
        module, _ = self._active_trend_module(now_mono=now_mono)
        if module == "SHORT" and mom100 <= -move_min and mom500 <= -move_min:
            side = "SELL"
        elif module == "LONG" and mom100 >= move_min and mom500 >= move_min:
            side = "BUY"
        if side == "NONE":
            self._trend_continuation_candidate_side = ""
            self._trend_continuation_candidate_count = 0
            return
        if self._trend_continuation_candidate_side == side:
            self._trend_continuation_candidate_count += 1
        else:
            self._trend_continuation_candidate_side = side
            self._trend_continuation_candidate_count = 1
        if self._trend_continuation_candidate_count < self._trend_continuation_confirm_updates:
            decision["decision"] = "SKIP"
            decision["reason"] = "trend_continuation_wait_confirmation"
            decision["side"] = side
            return
        decision["decision"] = "EXECUTE"
        decision["reason"] = "trend_continuation_short" if side == "SELL" else "trend_continuation_long"
        decision["side"] = side
        decision["entry_type"] = "aggressive"
        self._sync_aggressive_entry_price_for_side(decision)
        self._trend_continuation_candidate_side = ""
        self._trend_continuation_candidate_count = 0

    def _handle_open_position_signal(self, *, decision: dict[str, object], now_mono: float) -> bool:
        side = str(decision.get("side", "NONE")).strip().upper()
        if side not in {"BUY", "SELL"}:
            decision["decision"] = "SKIP"
            decision["reason"] = "open_position_non_directional_signal"
            return False
        current_side = "BUY" if self._position_qty > 0 else "SELL"
        if side == current_side:
            decision["decision"] = "SKIP"
            decision["reason"] = "one_position_only_already_in_direction"
            self._reversal_candidate_side = ""
            self._reversal_candidate_count = 0
            return False
        if not self._reversal_enabled:
            decision["decision"] = "SKIP"
            decision["reason"] = "reversal_disabled_position_open"
            return False
        if self._forced_exit_in_progress:
            decision["decision"] = "SKIP"
            decision["reason"] = "reversal_wait_forced_exit_in_progress"
            return False
        if self._last_reversal_monotonic > 0.0 and self._reversal_cooldown_sec > 0.0:
            since_rev = now_mono - self._last_reversal_monotonic
            if since_rev < self._reversal_cooldown_sec:
                decision["decision"] = "SKIP"
                decision["reason"] = "reversal_cooldown_active"
                return False
        if self._position_open_monotonic > 0.0 and self._reversal_min_hold_ms > 0.0:
            hold_ms = (now_mono - self._position_open_monotonic) * 1000.0
            if hold_ms < self._reversal_min_hold_ms:
                decision["decision"] = "SKIP"
                decision["reason"] = "reversal_min_hold_not_reached"
                return False
        trend = self._current_short_term_trend()
        min_trend = self._reversal_min_trend_strength
        if side == "BUY" and trend < min_trend:
            decision["decision"] = "SKIP"
            decision["reason"] = "reversal_trend_not_confirmed"
            return False
        if side == "SELL" and trend > -min_trend:
            decision["decision"] = "SKIP"
            decision["reason"] = "reversal_trend_not_confirmed"
            return False
        if self._reversal_candidate_side == side:
            self._reversal_candidate_count += 1
        else:
            self._reversal_candidate_side = side
            self._reversal_candidate_count = 1
        if self._reversal_candidate_count < self._reversal_confirmation_updates:
            decision["decision"] = "SKIP"
            decision["reason"] = "reversal_wait_confirmation"
            return False
        reverse_qty = abs(float(self._position_qty)) + float(self.lot_size)
        if reverse_qty <= 0.0:
            decision["decision"] = "SKIP"
            decision["reason"] = "reversal_invalid_qty"
            return False
        if side == "BUY" and not self.position_manager.can_place_buy(self.symbol, reverse_qty):
            decision["decision"] = "SKIP"
            decision["reason"] = "reversal_inventory_block_buy"
            return False
        if side == "SELL" and not self.position_manager.can_place_sell(self.symbol, reverse_qty):
            decision["decision"] = "SKIP"
            decision["reason"] = "reversal_inventory_block_sell"
            return False
        self._send_aggressive_entry(decision=decision, qty_override=reverse_qty, entry_type_override="reversal")
        self._last_reversal_monotonic = now_mono
        self._reversal_candidate_side = ""
        self._reversal_candidate_count = 0
        return True

    def _entry_filters_pass(self, *, side_label: str) -> bool:
        # Legacy passive quoting gate removed (momentum-only).
        return True

    def _mid_delta_over_window(self, *, now_mono: float, window_ms: int) -> float | None:
        if not self._signal_mid_history:
            return None
        target_mono = now_mono - (float(window_ms) / 1000.0)
        current_mid = float(self._signal_mid_history[-1][1])
        ref_mid: float | None = None
        for ts, mid in reversed(self._signal_mid_history):
            if ts <= target_mono:
                ref_mid = float(mid)
                break
        if ref_mid is None:
            return None
        return current_mid - ref_mid

    def _effective_signal_window_ms(self, base_window_ms: int) -> int:
        """
        Adapt signal windows to stream cadence.
        With sparse updates (~1s), fixed 50/100/500ms windows are often empty.
        """
        base_ms = max(1, int(base_window_ms))
        if len(self._signal_mid_history) < 2:
            return base_ms
        dt_sec = float(self._signal_mid_history[-1][0] - self._signal_mid_history[-2][0])
        if dt_sec <= 0.0:
            return base_ms
        scale = max(1.0, min(8.0, dt_sec / 0.1))
        return max(base_ms, int(round(base_ms * scale)))

    def _price_delta_over_window(
        self,
        *,
        now_mono: float,
        window_ms: int,
        side_label: str,
    ) -> float | None:
        side = str(side_label).strip().upper()
        history = self._signal_ask_history if side == "BUY" else self._signal_bid_history
        if not history:
            return None
        target_mono = now_mono - (float(window_ms) / 1000.0)
        current_px = float(history[-1][1])
        ref_px: float | None = None
        for ts, px in reversed(history):
            if ts <= target_mono:
                ref_px = float(px)
                break
        if ref_px is None:
            return None
        return current_px - ref_px

    def _recent_flow_bias(self) -> float:
        if not self._flow_bias_history:
            return 0.0
        vals = list(self._flow_bias_history)
        if not vals:
            return 0.0
        return float(sum(vals) / len(vals))

    def _avg_volume_before_current(self) -> float:
        vals = list(self._volume_total_history)
        if len(vals) <= 1:
            return 0.0
        baseline = vals[:-1]
        if not baseline:
            return 0.0
        return float(sum(baseline) / len(baseline))

    def _flow_bias_from_level1(self, latest: MarketData) -> float | None:
        bid_sz = float(getattr(latest, "bid_size", 0.0) or 0.0)
        ask_sz = float(getattr(latest, "ask_size", 0.0) or 0.0)
        total = bid_sz + ask_sz
        if total <= 0.0:
            return None
        return float((bid_sz - ask_sz) / total)

    @staticmethod
    def _clip_unit(value: float) -> float:
        return max(-1.0, min(1.0, float(value)))

    def _entry_edge_features(self, *, latest: MarketData, side: str, now_mono: float, entry_price: float) -> dict[str, float]:
        bid_size = float(getattr(latest, "bid_size", 0.0) or 0.0)
        ask_size = float(getattr(latest, "ask_size", 0.0) or 0.0)
        bid_volume_top3 = max(0.0, bid_size)
        ask_volume_top3 = max(0.0, ask_size)
        top_sum = bid_volume_top3 + ask_volume_top3
        bid_ask_imbalance = ((bid_volume_top3 - ask_volume_top3) / top_sum) if top_sum > 0.0 else 0.0

        flow = list(self._trade_flow_window)
        buy_volume = sum(x[1] for x in flow)
        sell_volume = sum(x[2] for x in flow)
        flow_total = buy_volume + sell_volume
        delta = buy_volume - sell_volume
        delta_ratio = (delta / flow_total) if flow_total > 0.0 else 0.0
        trade_intensity = 0.0
        if len(flow) >= 2:
            span = max(1e-6, flow[-1][0] - flow[0][0])
            trade_intensity = float(len(flow)) / span

        mids = [v for _, v in self._signal_mid_history]
        microtrend_3 = 0.0
        microtrend_10 = 0.0
        volatility_short = 0.0
        price_position_in_range = 0.5
        if len(mids) >= 2:
            microtrend_3 = float(mids[-1] - mids[-3]) if len(mids) >= 3 else float(mids[-1] - mids[0])
            microtrend_10 = float(mids[-1] - mids[-10]) if len(mids) >= 10 else float(mids[-1] - mids[0])
            win = mids[-20:] if len(mids) >= 20 else mids
            mn = min(win)
            mx = max(win)
            if mx > mn:
                price_position_in_range = (float(latest.mid_price) - mn) / (mx - mn)
            if len(win) >= 2:
                m = sum(win) / float(len(win))
                var = sum((x - m) ** 2 for x in win) / float(len(win))
                volatility_short = math.sqrt(max(0.0, var))

        spread = float(latest.spread)
        mid_price = float(latest.mid_price)
        entry_price_vs_mid = float(entry_price - mid_price) if mid_price > 0.0 else 0.0
        latency_ms = (
            (now_mono - self._last_entry_place_monotonic) * 1000.0
            if self._last_entry_place_monotonic > 0.0
            else 0.0
        )

        imbalance_n = self._clip_unit(bid_ask_imbalance)
        delta_n = self._clip_unit(delta_ratio)
        trend_scale = max(self._tick_size * 5.0, 1e-9)
        microtrend_n = self._clip_unit((0.4 * (microtrend_3 / trend_scale)) + (0.6 * (microtrend_10 / trend_scale)))
        spread_n = self._clip_unit(spread / max(self._tick_size, 1e-9))
        spread_quality_n = -spread_n
        pos_n = self._clip_unit((price_position_in_range * 2.0) - 1.0)
        side_sign = 1.0 if side == "BUY" else -1.0
        price_pos_n = self._clip_unit(pos_n * side_sign)

        return {
            "bid_volume_top3": bid_volume_top3,
            "ask_volume_top3": ask_volume_top3,
            "bid_ask_imbalance": bid_ask_imbalance,
            "best_bid_volume": bid_size,
            "best_ask_volume": ask_size,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "delta": delta,
            "delta_ratio": delta_ratio,
            "trade_intensity": trade_intensity,
            "microtrend_3": microtrend_3,
            "microtrend_10": microtrend_10,
            "volatility_short": volatility_short,
            "price_position_in_range": price_position_in_range,
            "spread": spread,
            "entry_price_vs_mid": entry_price_vs_mid,
            "latency_ms": latency_ms,
            "imbalance_n": imbalance_n,
            "delta_n": delta_n,
            "microtrend_n": microtrend_n,
            "spread_n": spread_quality_n,
            "price_pos_n": price_pos_n,
        }

    def _entry_edge_score(self, *, latest: MarketData, side: str, now_mono: float, entry_price: float) -> tuple[float, dict[str, float]]:
        feat = self._entry_edge_features(latest=latest, side=side, now_mono=now_mono, entry_price=entry_price)
        w_imbalance = 0.25
        w_delta = 0.25
        w_microtrend = 0.20
        w_spread = 0.15
        w_price_pos = 0.15
        score = (
            w_imbalance * feat["imbalance_n"]
            + w_delta * feat["delta_n"]
            + w_microtrend * feat["microtrend_n"]
            + w_spread * feat["spread_n"]
            + w_price_pos * feat["price_pos_n"]
        )
        return float(self._clip_unit(score)), feat

    def _evaluate_entry_signal(self, *, latest: MarketData, now_mono: float) -> dict[str, object]:
        """
        Active momentum entry (NOT market making).
        Decision is made BEFORE order placement, using:
        - price_move_ticks: short-term velocity in ticks
        - delta: aggressive buy volume - aggressive sell volume
        - imbalance: order book imbalance
        Hard filters first, then directional signal -> aggressive (marketable limit) order.
        """
        self._strategy_mode = "MOMENTUM_ACTIVE"

        spread = float(latest.spread)
        bid_size = float(getattr(latest, "bid_size", 0.0) or 0.0)
        ask_size = float(getattr(latest, "ask_size", 0.0) or 0.0)
        tot = bid_size + ask_size
        imbalance = ((bid_size - ask_size) / (tot + 1e-12)) if tot > 0.0 else 0.0
        mid_price = float(latest.mid_price)

        # Velocity: ticks over last N updates, normalized by elapsed ms.
        lookback_updates = 4
        price_move_ticks = 0.0
        time_window_ms = 0.0
        velocity_ticks_per_ms = 0.0
        if len(self._signal_mid_history) > lookback_updates:
            prev_mono, prev_px = self._signal_mid_history[-(lookback_updates + 1)]
            last_mono, _last_px = self._signal_mid_history[-1]
            time_window_ms = max(0.0, float(last_mono - prev_mono) * 1000.0)
            price_move_ticks = (mid_price - float(prev_px)) / max(1e-9, float(self._tick_size))
            velocity_ticks_per_ms = price_move_ticks / max(1e-9, time_window_ms)

        # Recent volatility: mean absolute mid move in ticks over last N updates.
        vol_window_updates = 12
        avg_move_ticks = 0.0
        if len(self._signal_mid_history) >= 2:
            mids = [float(px) for _t, px in list(self._signal_mid_history)[-vol_window_updates:]]
            if len(mids) >= 2:
                moves = [abs(mids[i] - mids[i - 1]) / max(1e-9, float(self._tick_size)) for i in range(1, len(mids))]
                avg_move_ticks = float(sum(moves) / float(len(moves))) if moves else 0.0

        # Adaptive thresholds based on recent volatility.
        dynamic_move_threshold = max(1.0, avg_move_ticks * 2.0)
        dynamic_velocity_threshold = (avg_move_ticks / max(1e-9, time_window_ms)) if time_window_ms > 0.0 else 0.0

        # Aggressive delta (flow): sum recent buy/sell "aggressive" volumes
        window_sec = 0.50
        buy_vol = 0.0
        sell_vol = 0.0
        for t_mono, b, s in list(self._trade_flow_window):
            if (now_mono - float(t_mono)) <= window_sec:
                buy_vol += float(b)
                sell_vol += float(s)
        delta = float(buy_vol - sell_vol)

        # Normalize delta by top-of-book volume.
        normalized_delta = delta / max(1e-9, float(tot))

        # Adaptive absolute delta threshold based on recent top-of-book volume.
        # Keep a floor to avoid entering on tiny book sizes.
        avg_volume = float(self._avg_volume_before_current())
        delta_threshold = max(1.0, avg_volume * 0.25)

        # Prevent late entries: if the move has already persisted for too many updates, skip.
        # Define "move start" by consecutive mid moves in the direction of the current move.
        ticks_since_move_start = 0
        if len(self._entry_direction_mids) >= 2:
            last = float(self._entry_direction_mids[-1])
            prev = float(self._entry_direction_mids[-2])
            d_last = last - prev
            direction = 1 if d_last > 0 else (-1 if d_last < 0 else 0)
            if direction != 0:
                streak = 0
                mids = list(self._entry_direction_mids)
                for i in range(len(mids) - 1, 0, -1):
                    d = float(mids[i]) - float(mids[i - 1])
                    if d == 0:
                        continue
                    if (d > 0 and direction > 0) or (d < 0 and direction < 0):
                        streak += 1
                    else:
                        break
                ticks_since_move_start = int(streak)

        # Microprice confirmation
        microprice = mid_price
        if tot > 0.0:
            microprice = (float(latest.bid) * ask_size + float(latest.ask) * bid_size) / max(1e-12, tot)

        w100 = self._effective_signal_window_ms(100)
        w50 = self._effective_signal_window_ms(50)
        momentum_100ms = self._mid_delta_over_window(now_mono=now_mono, window_ms=w100)
        momentum_50ms = self._mid_delta_over_window(now_mono=now_mono, window_ms=w50)

        # Single-layer decision: compute features once, then pure decision.
        snapshot = {
            "spread": float(spread),
            "price_move_ticks": float(price_move_ticks),
            "velocity": float(velocity_ticks_per_ms),
            "delta": float(delta),
            "imbalance": float(imbalance),
            "move_threshold": float(dynamic_move_threshold),
            "velocity_threshold": float(dynamic_velocity_threshold),
            "delta_threshold": float(delta_threshold),
            "imbalance_threshold": float(0.2),
        }
        signal = self.evaluate_entry_signal(snapshot)

        decision: dict[str, object] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": self.symbol,
            "side": "NONE",
            "entry_type": "aggressive",
            "bid_price": float(latest.bid),
            "ask_price": float(latest.ask),
            "spread": spread,
            "imbalance": float(imbalance),
            "delta": float(delta),
            "normalized_delta": float(normalized_delta),
            "price_move_ticks": float(price_move_ticks),
            "time_window_ms": float(time_window_ms),
            "velocity": float(velocity_ticks_per_ms),
            "avg_move_ticks": float(avg_move_ticks),
            "dynamic_move_threshold": float(dynamic_move_threshold),
            "dynamic_velocity_threshold": float(dynamic_velocity_threshold),
            "microprice": float(microprice),
            "mid_price": float(mid_price),
            "entry_price": 0.0,
            "decision": "SKIP",
            "reason": "",
            "strategy_mode": "MOMENTUM_ACTIVE",
        }
        if signal == "LONG":
            decision["side"] = "BUY"
            decision["decision"] = "EXECUTE"
            self._sync_aggressive_entry_price_for_side(decision)
        elif signal == "SHORT":
            decision["side"] = "SELL"
            decision["decision"] = "EXECUTE"
            self._sync_aggressive_entry_price_for_side(decision)
        return decision

    def _log_short_term_adverse_filter(self, *, signal: str, skipped: bool) -> None:
        payload = {
            "signal": str(signal),
            "skipped": bool(skipped),
            "skip_reason": "short_term_adverse" if skipped else "",
        }
        self.logger.info("[MM][PRETRADE_FILTER] %s", json.dumps(payload, ensure_ascii=True))

    def _send_aggressive_entry(
        self,
        *,
        decision: dict[str, object],
        qty_override: float | None = None,
        entry_type_override: str | None = None,
    ) -> None:
        side_label = str(decision.get("side", "NONE"))
        side = "1" if side_label == "BUY" else "2"
        qty = abs(float(qty_override if qty_override is not None else self.lot_size))
        if qty <= 0.0:
            return
        if float(decision.get("entry_price", 0.0) or 0.0) <= 0.0:
            self._sync_aggressive_entry_price_for_side(decision)
        entry_type = str(entry_type_override or "aggressive")
        entry_ts = datetime.now(timezone.utc)
        signal_ts_raw = str(decision.get("timestamp", ""))
        if signal_ts_raw:
            try:
                signal_ts = datetime.fromisoformat(signal_ts_raw)
            except ValueError as exc:
                self.logger.error(
                    "[MM][LOOKAHEAD_PARSE_FAIL] symbol=%s side=%s signal_timestamp=%s err=%s",
                    self.symbol,
                    side_label,
                    signal_ts_raw,
                    exc,
                )
            else:
                if signal_ts > entry_ts:
                    self.logger.error(
                        "[MM][LOOKAHEAD_VIOLATION] symbol=%s side=%s signal_timestamp=%s entry_timestamp=%s",
                        self.symbol,
                        side_label,
                        signal_ts.isoformat(),
                        entry_ts.isoformat(),
                    )
                assert signal_ts <= entry_ts, "signal_timestamp must be <= entry_timestamp"
        with self._adaptive_learning_lock:
            self._entry_context_fallback_by_side[side_label] = {
                "order_id": "",
                "symbol": self.symbol,
                "side": side_label,
                "entry_type": entry_type,
                "bid_price": float(decision.get("bid_price", 0.0)),
                "ask_price": float(decision.get("ask_price", 0.0)),
                "momentum_100ms": float(decision.get("momentum_100ms", 0.0)),
                "last_50ms_price_move": float(decision.get("last_50ms_price_move", 0.0)),
                "spread": float(decision.get("spread", 0.0)),
                "bid_size": float(decision.get("bid_size", 0.0)),
                "ask_size": float(decision.get("ask_size", 0.0)),
                "imbalance": float(decision.get("imbalance", 0.0)),
                "mid_price": float(decision.get("mid_price", 0.0)),
                "entry_price": float(decision.get("entry_price", 0.0)),
                "timestamp": str(decision.get("timestamp", "")),
                "entry_timestamp": entry_ts.isoformat(),
                "strategy_mode": "MOMENTUM_ACTIVE",
            }
        try:
            entry_px = float(decision.get("entry_price", 0.0) or 0.0)
            cl_ord_id = self.gateway.send_order(
                OrderRequest(
                    symbol=self.symbol,
                    side=side,
                    qty=qty,
                    account="",
                    market=self.market,
                    lot_size=1,
                    price=entry_px if entry_px > 0.0 else None,
                )
            )
            self._last_entry_place_monotonic = time.monotonic()
            with self._adaptive_learning_lock:
                self._entry_order_context[cl_ord_id] = {
                    "order_id": cl_ord_id,
                    "symbol": self.symbol,
                    "side": side_label,
                    "entry_type": entry_type,
                    "bid_price": float(decision.get("bid_price", 0.0)),
                    "ask_price": float(decision.get("ask_price", 0.0)),
                    "momentum_100ms": float(decision.get("momentum_100ms", 0.0)),
                    "last_50ms_price_move": float(decision.get("last_50ms_price_move", 0.0)),
                    "spread": float(decision.get("spread", 0.0)),
                    "bid_size": float(decision.get("bid_size", 0.0)),
                    "ask_size": float(decision.get("ask_size", 0.0)),
                    "imbalance": float(decision.get("imbalance", 0.0)),
                    "mid_price": float(decision.get("mid_price", 0.0)),
                    "entry_price": float(decision.get("entry_price", 0.0)),
                    "timestamp": str(decision.get("timestamp", "")),
                    "entry_timestamp": entry_ts.isoformat(),
                    "strategy_mode": "MOMENTUM_ACTIVE",
                }
            self.logger.info(
                "[MM][ENTRY_EXECUTE] symbol=%s side=%s cl_ord_id=%s reason=%s spread=%.4f bid_size=%.2f ask_size=%.2f imbalance=%.4f flow_bias=%.4f vol=%.2f avg_vol=%.2f spike=%s mom100=%.4f mom50=%.4f entry_price=%.4f mid_price=%.4f",
                self.symbol,
                side_label,
                cl_ord_id,
                str(decision.get("reason", "")),
                float(decision.get("spread", 0.0)),
                float(decision.get("bid_size", 0.0)),
                float(decision.get("ask_size", 0.0)),
                float(decision.get("imbalance", 0.0)),
                float(decision.get("flow_bias_signal", 0.0)),
                float(decision.get("total_volume", 0.0)),
                float(decision.get("avg_volume", 0.0)),
                bool(decision.get("volume_spike", False)),
                float(decision.get("momentum_100ms", 0.0)),
                float(decision.get("momentum_50ms", 0.0)),
                float(decision.get("entry_price", 0.0)),
                float(decision.get("mid_price", 0.0)),
            )
        except Exception as exc:
            self.logger.warning(
                "[MM][ENTRY_EXECUTE][FAILED] symbol=%s side=%s reason=%s err=%s",
                self.symbol,
                side_label,
                str(decision.get("reason", "")),
                exc,
            )

    def get_entry_order_context(self, cl_ord_id: str) -> dict[str, object] | None:
        with self._adaptive_learning_lock:
            ctx = self._entry_order_context.get(cl_ord_id)
            if ctx is None:
                return None
            return dict(ctx)

    def get_entry_trade_context(self, exec_id: str) -> dict[str, object] | None:
        with self._adaptive_learning_lock:
            ctx = self._entry_trade_context.get(exec_id)
            if ctx is None:
                return None
            return dict(ctx)

    def _effective_exit_params(self) -> tuple[float, float, float]:
        hold_ms = float(getattr(self._adaptive_targets, "MAX_HOLD_DURATION_MS", 3_000_000.0) or 3_000_000.0)
        return float(self._take_profit_per_trade), float(self._stop_loss_per_trade), hold_ms

    def _spread_tertile_index(self) -> int:
        if len(self._spread_history) < 10:
            return 1
        spreads = sorted(self._spread_history)
        n = len(spreads)
        t0 = spreads[n // 3]
        t1 = spreads[2 * n // 3]
        s = float(self._latest_spread)
        if s <= t0:
            return 0
        if s <= t1:
            return 1
        return 2

    def _tertile_win_rates_from_kpi(self, trades: list[dict[str, object]]) -> tuple[list[float], list[float]]:
        spreads = [float(t.get("spread", 0.0)) for t in trades]
        if len(spreads) < 3:
            return [0.5, 0.5, 0.5], [0.5, 0.5, 0.5]
        srt = sorted(spreads)
        n = len(srt)
        t0 = srt[n // 3]
        t1 = srt[2 * n // 3]
        labels: list[int] = []
        for s in spreads:
            if s <= t0:
                labels.append(0)
            elif s <= t1:
                labels.append(1)
            else:
                labels.append(2)
        buy_wr = [0.0, 0.0, 0.0]
        sell_wr = [0.0, 0.0, 0.0]
        for i in range(3):
            br = [t for t, lab in zip(trades, labels) if lab == i and str(t.get("side", "")).upper() == "BUY"]
            sr = [t for t, lab in zip(trades, labels) if lab == i and str(t.get("side", "")).upper() == "SELL"]
            if br:
                buy_wr[i] = sum(1 for x in br if float(x.get("pnl", 0.0)) > 0) / len(br)
            if sr:
                sell_wr[i] = sum(1 for x in sr if float(x.get("pnl", 0.0)) > 0) / len(sr)
        return buy_wr, sell_wr

    def _max_bid_ask_move_ticks(self) -> float:
        if len(self._bid_history) < 2 or len(self._ask_history) < 2:
            return 0.0
        bid_move = (max(self._bid_history) - min(self._bid_history)) / self._tick_size
        ask_move = (max(self._ask_history) - min(self._ask_history)) / self._tick_size
        return max(float(bid_move), float(ask_move))

    def _is_bid_ask_stable(self) -> bool:
        if len(self._bid_history) < self._entry_stability_window_ticks:
            return False
        return self._max_bid_ask_move_ticks() <= self._entry_max_bid_ask_move_ticks

    def _log_entry_decision(self, decision: dict[str, object]) -> None:
        self.logger.info(
            "[MM][ENTRY_DECISION] symbol=%s decision=%s spread=%.2f move_ticks=%.2f vel=%.6f delta=%.2f imb=%.3f",
            self.symbol,
            str(decision.get("decision", "SKIP")),
            float(decision.get("spread", 0.0)),
            float(decision.get("price_move_ticks", 0.0)),
            float(decision.get("velocity", 0.0)),
            float(decision.get("delta", 0.0)),
            float(decision.get("imbalance", 0.0)),
        )

    def _persist_entry_decision(self, decision: dict[str, object]) -> None:
        if self._entry_decision_sink is None:
            return
        flow_bias = float(decision.get("flow_bias_signal", 0.0) or 0.0)
        volume_spike = bool(decision.get("volume_spike", False))
        row = {
            "timestamp": str(decision.get("timestamp", datetime.now(timezone.utc).isoformat())),
            "symbol": self.symbol,
            "side": str(decision.get("side", "NONE")),
            "entry_score": float(abs(flow_bias)),
            "spread": float(decision.get("spread", 0.0)),
            "decision": str(decision.get("decision", "SKIP")),
            "reason": str(decision.get("reason", "")),
            "entry_type": str(decision.get("entry_type", "aggressive")),
            "bid_size": float(decision.get("bid_size", 0.0)),
            "ask_size": float(decision.get("ask_size", 0.0)),
            "imbalance": float(decision.get("imbalance", 0.0)),
            "last_50ms_price_move": float(decision.get("last_50ms_price_move", 0.0)),
            "price_move_ticks": float(decision.get("price_move_ticks", 0.0)),
            "velocity": float(decision.get("velocity", 0.0)),
            "avg_move_ticks": float(decision.get("avg_move_ticks", 0.0)),
            "dyn_move_thr": float(decision.get("dynamic_move_threshold", 0.0)),
            "dyn_vel_thr": float(decision.get("dynamic_velocity_threshold", 0.0)),
            "delta": float(decision.get("delta", 0.0)),
            "normalized_delta": float(decision.get("normalized_delta", 0.0)),
            "microprice": float(decision.get("microprice", 0.0)),
            "entry_price": float(decision.get("entry_price", 0.0)),
            "mid_price": float(decision.get("mid_price", 0.0)),
        }
        try:
            self._entry_decision_sink([row])
        except Exception as exc:
            self.logger.warning("[MM][ENTRY_DECISION][DB_FAIL] symbol=%s err=%s", self.symbol, exc)

    # Decision batching/confirmation removed (single-snapshot decision).


# Ensure the momentum-only implementation is exported even if legacy code
# gets appended/loaded below during iterative refactors.
BasicMarketMaker = _MomentumBasicMarketMaker

