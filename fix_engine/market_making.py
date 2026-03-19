from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
import math
import time
from threading import RLock
from typing import Callable

from execution_gateway import ExecutionGateway
from market_data.models import MarketData
from order_models import MarketType, OrderRequest
from position_manager import PositionManager


@dataclass
class _DecisionConfirmation:
    target_tick: int | None = None
    count: int = 0


@dataclass
class _SideState:
    cl_ord_id: str | None = None
    side: str | None = None
    price: float | None = None
    placed_monotonic: float = 0.0
    last_cancel_monotonic: float = 0.0
    adverse_started_monotonic: float = 0.0
    adverse_peak_ticks: float = 0.0
    replace_hysteresis_armed: bool = False
    replace_started_monotonic: float = 0.0
    replace_peak_ticks: float = 0.0
    last_would_cancel_tick: int | None = None


class BasicMarketMaker:
    """
    Deterministic MM:
    - quote buy at bid
    - quote sell at ask
    - cancel/replace on price changes
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
        cancel_analytics_sink: Callable[[list[dict[str, object]]], None] | None = None,
        entry_decision_sink: Callable[[list[dict[str, object]]], None] | None = None,
    ) -> None:
        self.symbol = symbol.upper()
        self.lot_size = float(lot_size)
        self.market = market
        self.gateway = gateway
        self.position_manager = position_manager
        self.logger = logger
        self.buy_state = _SideState()
        self.sell_state = _SideState()
        self.max_loss_per_trade = abs(float(max_loss_per_trade))
        self._position_qty = 0.0
        self._avg_price = 0.0
        self._forced_exit_in_progress = False
        self._volatility_window_ticks = max(2, int(volatility_window_ticks))
        self._max_short_term_volatility = max(0.0, float(max_short_term_volatility))
        self._cancel_on_high_volatility = bool(cancel_on_high_volatility)
        self._mid_prices: deque[float] = deque(maxlen=self._volatility_window_ticks)
        self._resting_order_timeout_sec = max(0.0, float(resting_order_timeout_sec))
        self._tick_size = max(1e-9, float(tick_size))
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
        self._decision_confirmation_updates = max(1, int(decision_confirmation_updates))
        self._min_decision_interval_sec = max(0.0, float(min_decision_interval_ms) / 1000.0)
        self._decision_batch_ticks = max(1, int(decision_batch_ticks))
        self._last_decision_monotonic = 0.0
        self._pending_decision_batch = 0
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
        self._entry_score_threshold = min(1.0, max(0.0, float(entry_score_threshold)))
        self._entry_score_spread_threshold = max(
            self._tick_size,
            float(entry_score_spread_threshold) if entry_score_spread_threshold > 0 else float(entry_min_spread),
        )
        self._entry_score_w_spread = max(0.0, float(entry_score_w_spread))
        self._entry_score_w_stability = max(0.0, float(entry_score_w_stability))
        self._entry_score_w_trend = max(0.0, float(entry_score_w_trend))
        self._entry_score_w_imbalance = max(0.0, float(entry_score_w_imbalance))
        self._entry_score_cooldown_penalty_max = min(1.0, max(0.0, float(entry_score_cooldown_penalty_max)))
        self._adaptive_entry_learning_enabled = bool(adaptive_entry_learning_enabled)
        self._adaptive_entry_learning_window = max(20, int(adaptive_entry_learning_window))
        self._adaptive_entry_learning_min_bin_trades = max(3, int(adaptive_entry_learning_min_bin_trades))
        self._adaptive_entry_learning_step_up = max(0.0, float(adaptive_entry_learning_step_up))
        self._adaptive_entry_learning_step_down = max(0.0, float(adaptive_entry_learning_step_down))
        self._adaptive_entry_learning_max_step_per_update = max(
            0.0, float(adaptive_entry_learning_max_step_per_update)
        )
        self._adaptive_entry_learning_threshold_min = min(
            1.0, max(0.0, float(adaptive_entry_learning_threshold_min))
        )
        self._adaptive_entry_learning_threshold_max = min(
            1.0, max(self._adaptive_entry_learning_threshold_min, float(adaptive_entry_learning_threshold_max))
        )
        self._adaptive_entry_learning_drift_alert = max(0.0, float(adaptive_entry_learning_drift_alert))
        self._adaptive_entry_learning_perf_alert_delta = max(0.0, float(adaptive_entry_learning_perf_alert_delta))
        self._adaptive_learning_lock = RLock()
        self._adaptive_score_bins: tuple[tuple[float, float], ...] = (
            (0.4, 0.5),
            (0.5, 0.6),
            (0.6, 0.7),
            (0.7, 0.8),
            (0.8, 0.9),
            (0.9, 1.01),
        )
        self._adaptive_threshold_by_bin: dict[str, float] = {
            self._score_bin_label(lo, hi): self._entry_score_threshold for lo, hi in self._adaptive_score_bins
        }
        self._adaptive_threshold_backup: dict[str, float] = dict(self._adaptive_threshold_by_bin)
        self._adaptive_learning_window_records: deque[dict[str, float | str | bool]] = deque(
            maxlen=self._adaptive_entry_learning_window
        )
        self._adaptive_order_context: dict[str, dict[str, float | str]] = {}
        self._adaptive_trade_context: dict[str, dict[str, float | str]] = {}
        self._adaptive_last_decision_by_side: dict[str, dict[str, float | str]] = {}
        self._adaptive_prev_window_avg_pnl = 0.0
        self._adaptive_updates = 0
        self._adaptive_last_score_pnl_corr = 0.0
        self._adaptive_last_score_adverse_corr = 0.0
        self._bid_history: deque[float] = deque(maxlen=self._entry_stability_window_ticks)
        self._ask_history: deque[float] = deque(maxlen=self._entry_stability_window_ticks)
        self._entry_direction_mids: deque[float] = deque(maxlen=self._entry_direction_window_ticks)
        self._last_fill_monotonic = 0.0
        self._last_entry_place_monotonic = 0.0
        self._buy_confirmation = _DecisionConfirmation()
        self._sell_confirmation = _DecisionConfirmation()
        self._cancel_impact_horizon_sec = max(0.05, float(cancel_impact_horizon_ms) / 1000.0)
        self._cancel_reason_summary_every = max(1, int(cancel_reason_summary_every))
        self._cancel_reason_counts: dict[str, int] = defaultdict(int)
        self._cancel_reason_missed_pnl: dict[str, float] = defaultdict(float)
        self._cancel_reason_impact_count: dict[str, int] = defaultdict(int)
        self._pending_cancel_impact: list[dict[str, object]] = []
        self._disable_price_move_cancel = bool(disable_price_move_cancel)
        self._cancel_analytics_sink = cancel_analytics_sink
        self._entry_decision_sink = entry_decision_sink
        self.logger.info(
            "[MM][ADAPTIVE] enabled=%s window=%s min_bin_trades=%s step_up=%s step_down=%s max_step=%s threshold_min=%s threshold_max=%s",
            self._adaptive_entry_learning_enabled,
            self._adaptive_entry_learning_window,
            self._adaptive_entry_learning_min_bin_trades,
            self._adaptive_entry_learning_step_up,
            self._adaptive_entry_learning_step_down,
            self._adaptive_entry_learning_max_step_per_update,
            self._adaptive_entry_learning_threshold_min,
            self._adaptive_entry_learning_threshold_max,
        )

    def on_market_data(self, data: MarketData) -> None:
        if data.symbol.upper() != self.symbol:
            return
        if data.bid <= 0 or data.ask <= 0 or data.ask < data.bid:
            return
        self._latest_data = data
        self._latest_mid_price = float(data.mid_price)
        self._latest_bid = float(data.bid)
        self._latest_ask = float(data.ask)
        self._latest_spread = float(data.ask) - float(data.bid)
        self._bid_history.append(float(data.bid))
        self._ask_history.append(float(data.ask))
        self._entry_direction_mids.append(float(data.mid_price))
        self._update_cancel_reason_impact(data)
        self._pending_decision_batch += 1

        if not self._should_run_decision():
            return

        latest = self._latest_data
        if latest is None:
            return
        self._last_decision_monotonic = time.monotonic()
        self._pending_decision_batch = 0

        self._cancel_stale_resting_orders()
        self._cancel_if_price_moved_away(latest)
        self._update_adverse_fill_feedback(latest)
        self._mid_prices.append(float(latest.mid_price))
        self._trend_mids.append(float(latest.mid_price))
        if self._maybe_force_exit(latest):
            return
        if self._forced_exit_in_progress:
            return
        trend_strength = self._current_short_term_trend()
        if self._trend_strength_threshold > 0 and abs(trend_strength) >= self._trend_strength_threshold:
            if self._cancel_on_strong_trend:
                self._cancel_side(self.buy_state, "RISK trend cancel buy exposure")
                self._cancel_side(self.sell_state, "RISK trend cancel sell exposure")
            self.logger.info(
                "[MM][TREND][SKIP] symbol=%s trend=%.6f threshold=%.6f window_ticks=%d cancel_existing=%s",
                self.symbol,
                trend_strength,
                self._trend_strength_threshold,
                self._trend_window_ticks,
                self._cancel_on_strong_trend,
            )
            return
        vol_ratio = self._current_short_term_volatility()
        if self._max_short_term_volatility > 0 and vol_ratio > self._max_short_term_volatility:
            if self._cancel_on_high_volatility:
                if self._disable_price_move_cancel:
                    self._log_would_cancel(
                        self.buy_state,
                        side_label="BUY",
                        reason="price_move volatility cancel buy exposure",
                        current_price=float(latest.mid_price),
                    )
                    self._log_would_cancel(
                        self.sell_state,
                        side_label="SELL",
                        reason="price_move volatility cancel sell exposure",
                        current_price=float(latest.mid_price),
                    )
                else:
                    self._cancel_side(self.buy_state, "VOLATILITY cancel buy exposure")
                    self._cancel_side(self.sell_state, "VOLATILITY cancel sell exposure")
            self.logger.warning(
                "[MM][VOLATILITY][SKIP] symbol=%s vol=%.6f threshold=%.6f window_ticks=%d cancel_existing=%s",
                self.symbol,
                vol_ratio,
                self._max_short_term_volatility,
                self._volatility_window_ticks,
                self._cancel_on_high_volatility,
            )
            return
        offset_px = self._current_defensive_offset_ticks() * self._tick_size
        target_bid = max(self._tick_size, float(latest.bid) - offset_px)
        target_ask = max(target_bid + self._tick_size, float(latest.ask) + offset_px)
        self._maintain_buy(target_bid)
        self._maintain_sell(target_ask)

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
                ctx = self._adaptive_order_context.get(cl_ord_id)
                if ctx is not None:
                    self._adaptive_trade_context[exec_id] = dict(ctx)
        self._register_fill_for_adverse_check(side=side, qty=last_qty, fill_price=last_px)
        self._last_fill_monotonic = time.monotonic()
        self._apply_fill(side=side, qty=last_qty, price=last_px)
        if abs(self._position_qty) <= 1e-9:
            self._forced_exit_in_progress = False

    def on_round_trip_outcomes(self, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        if not self._adaptive_entry_learning_enabled:
            return
        added = 0
        with self._adaptive_learning_lock:
            for row in rows:
                entry_trade_id = str(row.get("entry_trade_id", ""))
                if not entry_trade_id:
                    continue
                context = self._adaptive_trade_context.pop(entry_trade_id, None)
                if context is None:
                    continue
                entry_score = float(context.get("entry_score", 0.0))
                if entry_score <= 0:
                    continue
                pnl = float(row.get("total_pnl", 0.0))
                mae = float(row.get("mae", 0.0))
                mfe = float(row.get("mfe", 0.0))
                immediate_move = float(row.get("immediate_move", 0.0))
                record = {
                    "entry_trade_id": entry_trade_id,
                    "order_id": str(context.get("order_id", "")),
                    "symbol": self.symbol,
                    "side": str(row.get("side", "")),
                    "entry_score": entry_score,
                    "pnl": pnl,
                    "mae": mae,
                    "mfe": mfe,
                    "immediate_move": immediate_move,
                    "immediate_adverse": immediate_move < 0.0,
                    "fill_type": str(context.get("fill_type", "unknown")),
                    "timestamp": str(row.get("exit_ts", "")),
                    "entry_spread": float(context.get("entry_spread", 0.0)),
                    "entry_mid_price_change": float(context.get("entry_mid_price_change", 0.0)),
                    "entry_volatility": float(context.get("entry_volatility", 0.0)),
                    "entry_liquidity": float(context.get("entry_liquidity", 0.0)),
                }
                self._adaptive_learning_window_records.append(record)
                added += 1
            if added <= 0:
                return
            self._recompute_adaptive_thresholds_locked()

    def _maintain_buy(self, target_bid: float) -> None:
        if not self.position_manager.can_place_buy(self.symbol, self.lot_size):
            self._cancel_side(self.buy_state, "BUY blocked by inventory")
            self._reset_confirmation(self._buy_confirmation)
            return
        if self.buy_state.cl_ord_id and self.buy_state.price == target_bid:
            self._reset_confirmation(self._buy_confirmation)
            self._reset_replace_hysteresis(self.buy_state)
            return
        if not self._entry_filters_pass(side_label="BUY"):
            self._reset_confirmation(self._buy_confirmation)
            return
        if not self._confirm_decision(self._buy_confirmation, target_bid, "BUY"):
            return
        if self.buy_state.cl_ord_id:
            if not self._should_replace_for_price_move(self.buy_state, target_bid, side_label="BUY"):
                return
            if self._disable_price_move_cancel:
                self._log_would_cancel(
                    self.buy_state,
                    side_label="BUY",
                    reason="price_move replace on bid move",
                    current_price=target_bid,
                )
                return
            if not self._can_cancel_replace(self.buy_state):
                return
        self._cancel_side(self.buy_state, "BUY replace on bid move")
        self._place_side(self.buy_state, side="1", price=target_bid, label="BUY")

    def _maintain_sell(self, target_ask: float) -> None:
        if not self.position_manager.can_place_sell(self.symbol, self.lot_size):
            self._cancel_side(self.sell_state, "SELL blocked by inventory")
            self._reset_confirmation(self._sell_confirmation)
            return
        if self.sell_state.cl_ord_id and self.sell_state.price == target_ask:
            self._reset_confirmation(self._sell_confirmation)
            self._reset_replace_hysteresis(self.sell_state)
            return
        if not self._entry_filters_pass(side_label="SELL"):
            self._reset_confirmation(self._sell_confirmation)
            return
        if not self._confirm_decision(self._sell_confirmation, target_ask, "SELL"):
            return
        if self.sell_state.cl_ord_id:
            if not self._should_replace_for_price_move(self.sell_state, target_ask, side_label="SELL"):
                return
            if self._disable_price_move_cancel:
                self._log_would_cancel(
                    self.sell_state,
                    side_label="SELL",
                    reason="price_move replace on ask move",
                    current_price=target_ask,
                )
                return
            if not self._can_cancel_replace(self.sell_state):
                return
        self._cancel_side(self.sell_state, "SELL replace on ask move")
        self._place_side(self.sell_state, side="2", price=target_ask, label="SELL")

    def _cancel_side(self, state: _SideState, reason: str) -> None:
        if not state.cl_ord_id:
            return
        canceled_id = state.cl_ord_id
        canceled_side = state.side or ""
        canceled_px = state.price if state.price is not None else 0.0
        try:
            self.gateway.cancel_order(canceled_id, market=self.market)
            self._record_cancel_reason(
                reason=reason,
                side=canceled_side,
                order_id=canceled_id,
                cancel_price=float(canceled_px),
            )
            self.logger.info("[MM] cancel %s reason=%s", canceled_id, reason)
        except Exception as exc:
            self.logger.warning("[MM] cancel failed cl_ord_id=%s err=%s", canceled_id, exc)
        finally:
            with self._adaptive_learning_lock:
                self._adaptive_order_context.pop(canceled_id, None)
            state.last_cancel_monotonic = time.monotonic()
            state.cl_ord_id = None
            state.side = None
            state.price = None
            state.placed_monotonic = 0.0
            state.adverse_started_monotonic = 0.0
            state.adverse_peak_ticks = 0.0
            state.replace_hysteresis_armed = False
            state.replace_started_monotonic = 0.0
            state.replace_peak_ticks = 0.0
            state.last_would_cancel_tick = None
            if canceled_px > 0:
                self.logger.debug(
                    "[MM][CANCEL_CONTEXT] symbol=%s cl_ord_id=%s side=%s order_px=%.4f latest_mid=%.4f",
                    self.symbol,
                    canceled_id,
                    canceled_side,
                    float(canceled_px),
                    self._latest_mid_price,
                )

    def _place_side(self, state: _SideState, *, side: str, price: float, label: str) -> None:
        try:
            cl_ord_id = self.gateway.send_order(
                OrderRequest(
                    symbol=self.symbol,
                    side=side,
                    qty=self.lot_size,
                    account="",
                    price=price,
                    market=self.market,
                    lot_size=1,
                )
            )
            state.cl_ord_id = cl_ord_id
            state.side = side
            state.price = price
            state.placed_monotonic = time.monotonic()
            self._last_entry_place_monotonic = state.placed_monotonic
            state.adverse_started_monotonic = 0.0
            state.adverse_peak_ticks = 0.0
            state.replace_hysteresis_armed = False
            state.replace_started_monotonic = 0.0
            state.replace_peak_ticks = 0.0
            state.last_would_cancel_tick = None
            with self._adaptive_learning_lock:
                decision_ctx = dict(self._adaptive_last_decision_by_side.get(label, {}))
                decision_ctx["order_id"] = cl_ord_id
                decision_ctx["side_label"] = label
                decision_ctx["price_at_place"] = float(price)
                decision_ctx["fill_type"] = "passive" if price > 0 else "aggressive"
                prev_mid = float(decision_ctx.get("entry_mid", self._latest_mid_price))
                decision_ctx["entry_mid_price_change"] = float(self._latest_mid_price - prev_mid)
                self._adaptive_order_context[cl_ord_id] = decision_ctx
            self.logger.info("[MM] place %s %s qty=%s px=%s", label, self.symbol, self.lot_size, price)
        except Exception as exc:
            self.logger.warning("[MM] place rejected %s %s px=%s err=%s", label, self.symbol, price, exc)

    def _maybe_force_exit(self, data: MarketData) -> bool:
        if self.max_loss_per_trade <= 0:
            return False
        qty = self._position_qty
        if abs(qty) <= 1e-9 or self._avg_price <= 0:
            return False
        avg_before = self._avg_price
        mark = float(data.mid_price)
        unrealized = (mark - avg_before) * qty
        if unrealized > -self.max_loss_per_trade:
            return False
        self._cancel_side(self.buy_state, "FORCED_EXIT cancel buy exposure")
        self._cancel_side(self.sell_state, "FORCED_EXIT cancel sell exposure")
        close_side = "2" if qty > 0 else "1"
        close_qty = abs(qty)
        try:
            forced_id = self.gateway.send_order(
                OrderRequest(
                    symbol=self.symbol,
                    side=close_side,
                    qty=close_qty,
                    account="",
                    market=self.market,
                    bypass_risk=True,
                )
            )
            self._forced_exit_in_progress = True
            self.logger.warning(
                "[FORCED_EXIT] symbol=%s qty=%.4f side=%s avg=%.4f mark=%.4f unrealized=%.4f threshold=-%.4f cl_ord_id=%s",
                self.symbol,
                close_qty,
                close_side,
                avg_before,
                mark,
                unrealized,
                self.max_loss_per_trade,
                forced_id,
            )
        except Exception as exc:
            self._forced_exit_in_progress = False
            self.logger.error(
                "[FORCED_EXIT][FAILED] symbol=%s qty=%.4f side=%s unrealized=%.4f threshold=-%.4f err=%s",
                self.symbol,
                close_qty,
                close_side,
                unrealized,
                self.max_loss_per_trade,
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

    def _current_entry_micro_trend(self) -> tuple[str, float]:
        if len(self._entry_direction_mids) < 2:
            return "flat", 0.0
        first = float(self._entry_direction_mids[0])
        last = float(self._entry_direction_mids[-1])
        move_ticks = (last - first) / self._tick_size
        if abs(move_ticks) < self._entry_direction_min_move_ticks:
            return "flat", move_ticks
        if move_ticks > 0:
            return "upward", move_ticks
        return "downward", move_ticks

    def _entry_filters_pass(self, *, side_label: str) -> bool:
        now = time.monotonic()
        spread_score = self._entry_spread_score()
        stability_score = self._entry_stability_score()
        trend_score = self._entry_trend_score(side_label=side_label)
        imbalance_score = self._entry_imbalance_score(side_label=side_label)
        cooldown_penalty = self._entry_cooldown_penalty(now=now)
        interval_penalty = self._entry_interval_penalty(now=now)
        penalty = max(cooldown_penalty, interval_penalty)
        entry_score = self._entry_composite_score(
            spread_score=spread_score,
            stability_score=stability_score,
            trend_score=trend_score,
            imbalance_score=imbalance_score,
            penalty=penalty,
        )
        threshold_used = self._threshold_for_entry_score(entry_score)
        decision = "EXECUTE" if entry_score >= threshold_used else "SKIP"
        with self._adaptive_learning_lock:
            self._adaptive_last_decision_by_side[side_label] = {
                "entry_score": float(entry_score),
                "spread_score": float(spread_score),
                "stability_score": float(stability_score),
                "trend_score": float(trend_score),
                "imbalance_score": float(imbalance_score),
                "penalty": float(penalty),
                "threshold_used": float(threshold_used),
                "entry_spread": float(self._latest_spread),
                "entry_mid": float(self._latest_mid_price),
                "entry_volatility": float(self._current_short_term_volatility()),
                "entry_liquidity": float(getattr(self._latest_data, "volume", 0.0) if self._latest_data is not None else 0.0),
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        self._log_entry_decision(
            side_label=side_label,
            entry_score=entry_score,
            spread_score=spread_score,
            stability_score=stability_score,
            trend_score=trend_score,
            imbalance_score=imbalance_score,
            penalty=penalty,
            threshold_used=threshold_used,
            decision=decision,
        )
        self._persist_entry_decision(
            side_label=side_label,
            entry_score=entry_score,
            spread_score=spread_score,
            stability_score=stability_score,
            trend_score=trend_score,
            imbalance_score=imbalance_score,
            decision=decision,
        )
        return decision == "EXECUTE"

    @staticmethod
    def _score_bin_label(low: float, high: float) -> str:
        return f"{low:.1f}-{high:.1f}"

    def _score_bin_for_value(self, score: float) -> str:
        value = min(1.0, max(0.0, float(score)))
        for low, high in self._adaptive_score_bins:
            if low <= value < high:
                return self._score_bin_label(low, high)
        low, high = self._adaptive_score_bins[-1]
        return self._score_bin_label(low, high)

    def _threshold_for_entry_score(self, score: float) -> float:
        if not self._adaptive_entry_learning_enabled:
            return self._entry_score_threshold
        with self._adaptive_learning_lock:
            bin_key = self._score_bin_for_value(score)
            return float(self._adaptive_threshold_by_bin.get(bin_key, self._entry_score_threshold))

    def _recompute_adaptive_thresholds_locked(self) -> None:
        if not self._adaptive_learning_window_records:
            return
        previous_thresholds = dict(self._adaptive_threshold_by_bin)
        grouped: dict[str, list[dict[str, float | str | bool]]] = defaultdict(list)
        for row in self._adaptive_learning_window_records:
            score = float(row.get("entry_score", 0.0))
            grouped[self._score_bin_for_value(score)].append(row)

        changes: list[dict[str, float]] = []
        for low, high in self._adaptive_score_bins:
            bin_key = self._score_bin_label(low, high)
            records = grouped.get(bin_key, [])
            if len(records) < self._adaptive_entry_learning_min_bin_trades:
                continue
            avg_pnl = sum(float(r.get("pnl", 0.0)) for r in records) / len(records)
            winrate = sum(1 for r in records if float(r.get("pnl", 0.0)) > 0.0) / len(records)
            adverse_rate = (
                sum(1 for r in records if bool(r.get("immediate_adverse", False))) / len(records)
            )
            old_thr = float(self._adaptive_threshold_by_bin.get(bin_key, self._entry_score_threshold))
            new_thr = old_thr
            if avg_pnl < 0.0:
                step = min(self._adaptive_entry_learning_step_down, self._adaptive_entry_learning_max_step_per_update)
                new_thr = max(self._adaptive_entry_learning_threshold_min, old_thr - step)
            elif avg_pnl > 0.0:
                step = min(self._adaptive_entry_learning_step_up, self._adaptive_entry_learning_max_step_per_update)
                new_thr = min(self._adaptive_entry_learning_threshold_max, old_thr + step)
            if abs(new_thr - old_thr) > 1e-12:
                changes.append(
                    {
                        "bin": bin_key,
                        "old": old_thr,
                        "new": new_thr,
                        "avg_pnl": avg_pnl,
                        "winrate": winrate * 100.0,
                        "adverse_rate": adverse_rate * 100.0,
                    }
                )
                self._adaptive_threshold_by_bin[bin_key] = new_thr

        if changes:
            self._adaptive_threshold_backup = previous_thresholds
            self._adaptive_updates += 1
            self.logger.info(
                "[MM][ADAPTIVE][THRESHOLD_UPDATE] symbol=%s update_id=%s changes=%s",
                self.symbol,
                self._adaptive_updates,
                changes,
            )

        records = list(self._adaptive_learning_window_records)
        score_values = [float(r.get("entry_score", 0.0)) for r in records]
        pnl_values = [float(r.get("pnl", 0.0)) for r in records]
        adverse_values = [1.0 if bool(r.get("immediate_adverse", False)) else 0.0 for r in records]
        self._adaptive_last_score_pnl_corr = self._pearson_corr(score_values, pnl_values)
        self._adaptive_last_score_adverse_corr = self._pearson_corr(score_values, adverse_values)
        avg_window_pnl = sum(pnl_values) / len(pnl_values)
        if (self._adaptive_prev_window_avg_pnl - avg_window_pnl) > self._adaptive_entry_learning_perf_alert_delta:
            self.logger.warning(
                "[MM][ADAPTIVE][ALERT] symbol=%s event=performance_drop prev_avg_pnl=%.4f current_avg_pnl=%.4f delta=%.4f threshold=%.4f",
                self.symbol,
                self._adaptive_prev_window_avg_pnl,
                avg_window_pnl,
                self._adaptive_prev_window_avg_pnl - avg_window_pnl,
                self._adaptive_entry_learning_perf_alert_delta,
            )
        self._adaptive_prev_window_avg_pnl = avg_window_pnl
        for bin_key, threshold in self._adaptive_threshold_by_bin.items():
            drift = abs(float(threshold) - self._entry_score_threshold)
            if drift > self._adaptive_entry_learning_drift_alert:
                self.logger.warning(
                    "[MM][ADAPTIVE][ALERT] symbol=%s event=threshold_drift bin=%s base_threshold=%.4f current_threshold=%.4f drift=%.4f",
                    self.symbol,
                    bin_key,
                    self._entry_score_threshold,
                    threshold,
                    drift,
                )
        self.logger.info(
            "[MM][ADAPTIVE][WINDOW] symbol=%s size=%s avg_pnl=%.4f score_pnl_corr=%.4f score_adverse_corr=%.4f thresholds=%s",
            self.symbol,
            len(records),
            avg_window_pnl,
            self._adaptive_last_score_pnl_corr,
            self._adaptive_last_score_adverse_corr,
            self._adaptive_threshold_by_bin,
        )

    @staticmethod
    def _pearson_corr(xs: list[float], ys: list[float]) -> float:
        n = min(len(xs), len(ys))
        if n < 2:
            return 0.0
        x = xs[:n]
        y = ys[:n]
        mx = sum(x) / n
        my = sum(y) / n
        numerator = sum((a - mx) * (b - my) for a, b in zip(x, y))
        denom_x = sum((a - mx) ** 2 for a in x)
        denom_y = sum((b - my) ** 2 for b in y)
        denom = math.sqrt(denom_x * denom_y)
        if denom <= 1e-12:
            return 0.0
        return numerator / denom

    def rollback_adaptive_thresholds(self) -> None:
        if not self._adaptive_entry_learning_enabled:
            return
        with self._adaptive_learning_lock:
            self._adaptive_threshold_by_bin = dict(self._adaptive_threshold_backup)
            self.logger.warning(
                "[MM][ADAPTIVE][ROLLBACK] symbol=%s thresholds=%s",
                self.symbol,
                self._adaptive_threshold_by_bin,
            )

    def _entry_spread_score(self) -> float:
        threshold = max(self._tick_size, self._entry_score_spread_threshold)
        return min(max(self._latest_spread / threshold, 0.0), 1.0)

    def _entry_stability_score(self) -> float:
        if self._entry_max_bid_ask_move_ticks <= 0:
            return 1.0
        if len(self._bid_history) < self._entry_stability_window_ticks:
            return 0.5
        move_ticks = self._max_bid_ask_move_ticks()
        raw = 1.0 - (move_ticks / max(1e-9, self._entry_max_bid_ask_move_ticks))
        return min(max(raw, 0.0), 1.0)

    def _entry_trend_score(self, *, side_label: str) -> float:
        trend_dir, trend_move_ticks = self._current_entry_micro_trend()
        if trend_dir == "flat":
            base = 0.7
        elif (side_label == "BUY" and trend_dir == "upward") or (side_label == "SELL" and trend_dir == "downward"):
            base = 1.0
        else:
            base = 0.05
        if self._entry_anti_trend_threshold > 0:
            trend = self._current_short_term_trend()
            if (side_label == "BUY" and trend <= -self._entry_anti_trend_threshold) or (
                side_label == "SELL" and trend >= self._entry_anti_trend_threshold
            ):
                base = min(base, 0.1)
        if abs(trend_move_ticks) > self._entry_direction_min_move_ticks * 2.0 and base < 0.2:
            return 0.0
        return min(max(base, 0.0), 1.0)

    def _entry_imbalance_score(self, *, side_label: str) -> float:
        latest = self._latest_data
        if latest is None:
            return 0.5
        bid_size = getattr(latest, "bid_size", None)
        ask_size = getattr(latest, "ask_size", None)
        if bid_size is None or ask_size is None:
            return 0.5
        bid_sz = float(bid_size)
        ask_sz = float(ask_size)
        total = bid_sz + ask_sz
        if total <= 0:
            return 0.5
        imbalance = (bid_sz - ask_sz) / total
        if side_label == "BUY":
            score = (imbalance + 1.0) * 0.5
        else:
            score = (1.0 - imbalance) * 0.5
        return min(max(score, 0.0), 1.0)

    def _entry_cooldown_penalty(self, *, now: float) -> float:
        if self._trade_cooldown_sec <= 0 or self._last_fill_monotonic <= 0:
            return 0.0
        elapsed = now - self._last_fill_monotonic
        if elapsed >= self._trade_cooldown_sec:
            return 0.0
        remaining_ratio = (self._trade_cooldown_sec - elapsed) / self._trade_cooldown_sec
        return min(max(remaining_ratio * self._entry_score_cooldown_penalty_max, 0.0), 1.0)

    def _entry_interval_penalty(self, *, now: float) -> float:
        if self._entry_min_place_interval_sec <= 0 or self._last_entry_place_monotonic <= 0:
            return 0.0
        elapsed = now - self._last_entry_place_monotonic
        if elapsed >= self._entry_min_place_interval_sec:
            return 0.0
        return min(max((self._entry_min_place_interval_sec - elapsed) / self._entry_min_place_interval_sec, 0.0), 1.0)

    def _entry_composite_score(
        self,
        *,
        spread_score: float,
        stability_score: float,
        trend_score: float,
        imbalance_score: float,
        penalty: float,
    ) -> float:
        w_sum = (
            self._entry_score_w_spread
            + self._entry_score_w_stability
            + self._entry_score_w_trend
            + self._entry_score_w_imbalance
        )
        if w_sum <= 0:
            base = 0.0
        else:
            base = (
                self._entry_score_w_spread * spread_score
                + self._entry_score_w_stability * stability_score
                + self._entry_score_w_trend * trend_score
                + self._entry_score_w_imbalance * imbalance_score
            ) / w_sum
        return min(max(base - penalty, 0.0), 1.0)

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

    def _log_entry_decision(
        self,
        *,
        side_label: str,
        entry_score: float,
        spread_score: float,
        stability_score: float,
        trend_score: float,
        imbalance_score: float,
        penalty: float,
        threshold_used: float,
        decision: str,
    ) -> None:
        self.logger.info(
            "[MM][ENTRY_DECISION] symbol=%s side=%s decision=%s entry_score=%.4f threshold=%.4f spread_score=%.4f stability_score=%.4f trend_score=%.4f imbalance_score=%.4f penalty=%.4f",
            self.symbol,
            side_label,
            decision,
            entry_score,
            threshold_used,
            spread_score,
            stability_score,
            trend_score,
            imbalance_score,
            penalty,
        )

    def _persist_entry_decision(
        self,
        *,
        side_label: str,
        entry_score: float,
        spread_score: float,
        stability_score: float,
        trend_score: float,
        imbalance_score: float,
        decision: str,
    ) -> None:
        if self._entry_decision_sink is None:
            return
        row = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": self.symbol,
            "side": side_label,
            "entry_score": float(entry_score),
            "spread_score": float(spread_score),
            "stability_score": float(stability_score),
            "trend_score": float(trend_score),
            "imbalance_score": float(imbalance_score),
            "decision": decision,
        }
        try:
            self._entry_decision_sink([row])
        except Exception as exc:
            self.logger.warning("[MM][ENTRY_DECISION][DB_FAIL] symbol=%s err=%s", self.symbol, exc)

    def _cancel_stale_resting_orders(self) -> None:
        if self._resting_order_timeout_sec <= 0:
            return
        now_mono = time.monotonic()
        self._cancel_stale_side(self.buy_state, now_mono=now_mono)
        self._cancel_stale_side(self.sell_state, now_mono=now_mono)

    def _cancel_stale_side(self, state: _SideState, *, now_mono: float) -> None:
        if not state.cl_ord_id or state.placed_monotonic <= 0:
            return
        age = now_mono - state.placed_monotonic
        if age < self._resting_order_timeout_sec:
            return
        stale_id = state.cl_ord_id
        stale_px = state.price
        self._cancel_side(state, "STALE timeout")
        self.logger.warning(
            "[MM][STALE_CANCEL] symbol=%s cl_ord_id=%s age_sec=%.3f timeout_sec=%.3f order_px=%s",
            self.symbol,
            stale_id,
            age,
            self._resting_order_timeout_sec,
            stale_px,
        )

    def _cancel_if_price_moved_away(self, data: MarketData) -> None:
        if self._adverse_move_cancel_ticks <= 0:
            return
        self._cancel_side_on_adverse_move(
            self.buy_state,
            mark=float(data.mid_price),
            threshold_ticks=self._adverse_move_cancel_ticks,
            is_buy=True,
        )
        self._cancel_side_on_adverse_move(
            self.sell_state,
            mark=float(data.mid_price),
            threshold_ticks=self._adverse_move_cancel_ticks,
            is_buy=False,
        )

    def _cancel_side_on_adverse_move(
        self, state: _SideState, *, mark: float, threshold_ticks: int, is_buy: bool
    ) -> None:
        if not state.cl_ord_id or state.price is None or state.price <= 0:
            return
        now = time.monotonic()
        moved_ticks = self._adverse_ticks(state.price, mark, is_buy=is_buy)
        if is_buy:
            side_label = "BUY"
        else:
            side_label = "SELL"
        if moved_ticks <= 0:
            self._reset_fast_cancel_tracking(state)
            return
        state.adverse_peak_ticks = max(state.adverse_peak_ticks, moved_ticks)
        if moved_ticks >= threshold_ticks and state.adverse_started_monotonic <= 0:
            state.adverse_started_monotonic = now
            self.logger.info(
                "[MM][FAST_CANCEL][ARMED] symbol=%s side=%s cl_ord_id=%s moved_ticks=%.2f cancel_ticks=%d keep_ticks=%.2f persist_ms=%d",
                self.symbol,
                side_label,
                state.cl_ord_id,
                moved_ticks,
                threshold_ticks,
                self._fast_cancel_keep_ticks,
                int(self._fast_cancel_persist_sec * 1000),
            )
        if moved_ticks <= self._fast_cancel_keep_ticks:
            self._reset_fast_cancel_tracking(state)
            return
        if state.adverse_started_monotonic <= 0:
            return
        persisted_sec = now - state.adverse_started_monotonic
        if persisted_sec < self._fast_cancel_persist_sec:
            return
        if moved_ticks < threshold_ticks:
            return
        if not self._is_price_move_beyond_tolerance(state.price, mark):
            return
        if not self._can_cancel_replace(state):
            return
        moved_id = state.cl_ord_id
        order_px = state.price
        peak_ticks = state.adverse_peak_ticks
        if self._disable_price_move_cancel:
            self._log_would_cancel(
                state,
                side_label=side_label,
                reason=(
                    f"price_move fast_cancel moved_ticks={moved_ticks:.2f} "
                    f"peak_ticks={peak_ticks:.2f} cancel_ticks={threshold_ticks} "
                    f"keep_ticks={self._fast_cancel_keep_ticks:.2f} persist_ms={persisted_sec * 1000.0:.1f}"
                ),
                current_price=mark,
            )
            self._reset_fast_cancel_tracking(state)
            return
        reason = (
            f"{side_label} fast_cancel moved_ticks={moved_ticks:.2f} "
            f"peak_ticks={peak_ticks:.2f} "
            f"cancel_ticks={threshold_ticks} keep_ticks={self._fast_cancel_keep_ticks:.2f} "
            f"persist_ms={persisted_sec * 1000.0:.1f}"
        )
        self._cancel_side(state, reason)
        self.logger.info(
            "[MM][FAST_CANCEL] symbol=%s side=%s cl_ord_id=%s order_px=%.4f mark=%.4f moved_ticks=%.2f peak_ticks=%.2f cancel_ticks=%d keep_ticks=%.2f persisted_ms=%.1f reason=%s",
            self.symbol,
            side_label,
            moved_id,
            order_px,
            mark,
            moved_ticks,
            peak_ticks,
            threshold_ticks,
            self._fast_cancel_keep_ticks,
            persisted_sec * 1000.0,
            reason,
        )

    def _price_move_ticks(self, source_price: float, target_price: float) -> float:
        return abs(float(target_price) - float(source_price)) / self._tick_size

    def _adverse_ticks(self, order_price: float, mark: float, *, is_buy: bool) -> float:
        if is_buy:
            return max(0.0, (float(order_price) - float(mark)) / self._tick_size)
        return max(0.0, (float(mark) - float(order_price)) / self._tick_size)

    def _should_replace_for_price_move(self, state: _SideState, target_price: float, *, side_label: str) -> bool:
        if state.price is None:
            return True
        if not self._is_price_move_beyond_tolerance(state.price, target_price):
            if state.replace_hysteresis_armed:
                self.logger.debug(
                    "[MM][REPLACE_HYST][KEEP] symbol=%s side=%s reason=tolerance_gate order_px=%.4f target_px=%.4f",
                    self.symbol,
                    side_label,
                    state.price,
                    target_price,
                )
            self._reset_replace_hysteresis(state)
            return False
        moved_ticks = self._price_move_ticks(state.price, target_price)
        cancel_ticks = self._replace_cancel_threshold_ticks
        keep_ticks = self._replace_keep_threshold_ticks
        now = time.monotonic()
        if cancel_ticks <= 0:
            return True
        if state.replace_hysteresis_armed:
            if moved_ticks <= keep_ticks:
                self._reset_replace_hysteresis(state)
                self.logger.debug(
                    "[MM][REPLACE_HYST][KEEP] symbol=%s side=%s moved_ticks=%.2f keep_ticks=%.2f",
                    self.symbol,
                    side_label,
                    moved_ticks,
                    keep_ticks,
                )
                return False
            state.replace_peak_ticks = max(state.replace_peak_ticks, moved_ticks)
            if moved_ticks < cancel_ticks:
                return False
            if state.replace_started_monotonic <= 0:
                state.replace_started_monotonic = now
                return False
            persisted_sec = now - state.replace_started_monotonic
            if persisted_sec < self._replace_persist_sec:
                self.logger.debug(
                    "[MM][REPLACE_ADAPTIVE][WAIT] symbol=%s side=%s moved_ticks=%.2f persist_ms=%.1f need_ms=%.1f",
                    self.symbol,
                    side_label,
                    moved_ticks,
                    persisted_sec * 1000.0,
                    self._replace_persist_sec * 1000.0,
                )
                return False
            return True
        if moved_ticks >= cancel_ticks:
            state.replace_hysteresis_armed = True
            state.replace_started_monotonic = now
            state.replace_peak_ticks = moved_ticks
            self.logger.info(
                "[MM][REPLACE_ADAPTIVE][ARMED] symbol=%s side=%s moved_ticks=%.2f cancel_ticks=%.2f keep_ticks=%.2f persist_ms=%d",
                self.symbol,
                side_label,
                moved_ticks,
                cancel_ticks,
                keep_ticks,
                int(self._replace_persist_sec * 1000),
            )
            return False
        return False

    def _is_price_move_beyond_tolerance(self, source_price: float, target_price: float) -> bool:
        move_abs = abs(float(target_price) - float(source_price))
        # If percent tolerance is configured, it has priority over tick tolerance.
        if self._price_tolerance_pct > 0 and source_price > 0:
            return ((move_abs / float(source_price)) * 100.0) >= self._price_tolerance_pct
        if self._price_tolerance_ticks > 0:
            return (move_abs / self._tick_size) >= self._price_tolerance_ticks
        return True

    def _can_cancel_replace(self, state: _SideState) -> bool:
        now = time.monotonic()
        if state.placed_monotonic > 0 and (now - state.placed_monotonic) < self._min_order_lifetime_sec:
            return False
        if state.last_cancel_monotonic > 0 and (now - state.last_cancel_monotonic) < self._cancel_replace_cooldown_sec:
            return False
        return True

    def _register_fill_for_adverse_check(self, *, side: str, qty: float, fill_price: float) -> None:
        side_norm = str(side).strip().upper()
        self._pending_fill_checks.append(
            {
                "side": side_norm,
                "qty": abs(float(qty)),
                "fill_price": float(fill_price),
                "ts": time.monotonic(),
            }
        )

    def _update_adverse_fill_feedback(self, data: MarketData) -> None:
        if not self._pending_fill_checks:
            return
        now = time.monotonic()
        mid = float(data.mid_price)
        remaining: list[dict[str, float | str]] = []
        for row in self._pending_fill_checks:
            age = now - float(row["ts"])
            if age < self._post_fill_horizon_sec:
                remaining.append(row)
                continue
            side = str(row["side"])
            fill_px = float(row["fill_price"])
            is_buy = side in {"1", "BUY", "B"}
            adverse = (mid < fill_px) if is_buy else (mid > fill_px)
            self._adverse_fill_flags.append(1 if adverse else 0)
        self._pending_fill_checks = remaining

    def _current_adverse_fill_rate(self) -> float:
        if not self._adverse_fill_flags:
            return 0.0
        return float(sum(self._adverse_fill_flags)) / float(len(self._adverse_fill_flags))

    def _current_defensive_offset_ticks(self) -> int:
        if self._defensive_quote_offset_ticks <= 0:
            return 0
        adverse_rate = self._current_adverse_fill_rate()
        enabled = adverse_rate >= self._adverse_fill_rate_threshold and len(self._adverse_fill_flags) >= 5
        if enabled != self._last_defensive_mode:
            self._last_defensive_mode = enabled
            self.logger.info(
                "[MM][ADVERSE_FEEDBACK] symbol=%s defensive_mode=%s adverse_rate=%.3f threshold=%.3f sample=%d offset_ticks=%d",
                self.symbol,
                enabled,
                adverse_rate,
                self._adverse_fill_rate_threshold,
                len(self._adverse_fill_flags),
                self._defensive_quote_offset_ticks,
            )
        return self._defensive_quote_offset_ticks if enabled else 0

    def _should_run_decision(self) -> bool:
        if self._pending_decision_batch < self._decision_batch_ticks:
            return False
        if self._last_decision_monotonic <= 0:
            return True
        return (time.monotonic() - self._last_decision_monotonic) >= self._min_decision_interval_sec

    def _confirm_decision(self, state: _DecisionConfirmation, target_price: float, side_label: str) -> bool:
        target_tick = int(round(float(target_price) / self._tick_size))
        if state.target_tick == target_tick:
            state.count += 1
        else:
            state.target_tick = target_tick
            state.count = 1
        if state.count < self._decision_confirmation_updates:
            self.logger.debug(
                "[MM][INERTIA][WAIT] symbol=%s side=%s target_tick=%s count=%d need=%d",
                self.symbol,
                side_label,
                target_tick,
                state.count,
                self._decision_confirmation_updates,
            )
            return False
        return True

    @staticmethod
    def _reset_confirmation(state: _DecisionConfirmation) -> None:
        state.target_tick = None
        state.count = 0

    @staticmethod
    def _reset_fast_cancel_tracking(state: _SideState) -> None:
        state.adverse_started_monotonic = 0.0
        state.adverse_peak_ticks = 0.0

    @staticmethod
    def _reset_replace_hysteresis(state: _SideState) -> None:
        state.replace_hysteresis_armed = False
        state.replace_started_monotonic = 0.0
        state.replace_peak_ticks = 0.0

    def _classify_cancel_reason(self, reason: str) -> str:
        text = reason.lower()
        if "timeout" in text or "stale" in text:
            return "timeout"
        if "volatility" in text or "trend" in text:
            return "volatility"
        if "inventory" in text or "risk" in text or "forced_exit" in text:
            return "risk_limit"
        return "price_move"

    def _record_cancel_reason(self, *, reason: str, side: str, order_id: str, cancel_price: float) -> None:
        reason_tag = self._classify_cancel_reason(reason)
        self._cancel_reason_counts[reason_tag] += 1
        now_mono = time.monotonic()
        cancel_id = f"{order_id}:{int(now_mono * 1000)}"
        self._pending_cancel_impact.append(
            {
                "cancel_id": cancel_id,
                "order_id": order_id,
                "symbol": self.symbol,
                "reason": reason_tag,
                "side": str(side).strip().upper(),
                "cancel_price": float(cancel_price if cancel_price > 0 else self._latest_mid_price),
                "mid_at_cancel": float(self._latest_mid_price if self._latest_mid_price > 0 else cancel_price),
                "qty": float(self.lot_size),
                "created_mono": now_mono,
                "created_at": str(int(time.time())),
            }
        )
        total = sum(self._cancel_reason_counts.values())
        if total % self._cancel_reason_summary_every == 0:
            self._log_cancel_reason_summary(total_cancels=total)

    def _log_would_cancel(self, state: _SideState, *, side_label: str, reason: str, current_price: float) -> None:
        if not state.cl_ord_id or state.price is None:
            return
        target_tick = int(round(float(current_price) / self._tick_size))
        if state.last_would_cancel_tick == target_tick:
            return
        state.last_would_cancel_tick = target_tick
        self.logger.info(
            "[MM][WOULD_CANCEL][PRICE_MOVE] symbol=%s side=%s cl_ord_id=%s order_px=%.4f current_px=%.4f reason=%s",
            self.symbol,
            side_label,
            state.cl_ord_id,
            float(state.price),
            float(current_price),
            reason,
        )

    def _update_cancel_reason_impact(self, data: MarketData) -> None:
        if not self._pending_cancel_impact:
            return
        now = time.monotonic()
        mid = float(data.mid_price)
        pending: list[dict[str, object]] = []
        finalized_rows: list[dict[str, object]] = []
        for row in self._pending_cancel_impact:
            age = now - float(row["created_mono"])
            if age < self._cancel_impact_horizon_sec:
                pending.append(row)
                continue
            reason_tag = str(row["reason"])
            side = str(row["side"])
            qty = float(row["qty"])
            mid_at_cancel = float(row["mid_at_cancel"])
            side_sign = 1.0 if side in {"1", "BUY", "B"} else -1.0
            missed_pnl = (mid - mid_at_cancel) * qty * side_sign
            self._cancel_reason_missed_pnl[reason_tag] += missed_pnl
            self._cancel_reason_impact_count[reason_tag] += 1
            finalized_rows.append(
                {
                    "cancel_id": str(row.get("cancel_id", "")),
                    "order_id": str(row.get("order_id", "")),
                    "symbol": str(row.get("symbol", self.symbol)),
                    "side": side,
                    "cancel_reason": reason_tag,
                    "cancel_price": float(row.get("cancel_price", mid_at_cancel)),
                    "horizon_price": mid,
                    "missed_pnl": missed_pnl,
                    "horizon_ms": int(self._cancel_impact_horizon_sec * 1000),
                    "created_at": str(row.get("created_at", "")),
                }
            )
        self._pending_cancel_impact = pending
        if finalized_rows and self._cancel_analytics_sink is not None:
            try:
                self._cancel_analytics_sink(finalized_rows)
            except Exception as exc:
                self.logger.warning("[MM] cancel analytics sink failed err=%s", exc)

    def _log_cancel_reason_summary(self, *, total_cancels: int) -> None:
        ordered = ("price_move", "timeout", "risk_limit", "volatility")
        parts: list[str] = []
        for key in ordered:
            cnt = int(self._cancel_reason_counts.get(key, 0))
            total_missed = float(self._cancel_reason_missed_pnl.get(key, 0.0))
            samples = int(self._cancel_reason_impact_count.get(key, 0))
            avg_missed = (total_missed / samples) if samples > 0 else 0.0
            parts.append(f"{key}:count={cnt},missed_pnl={total_missed:.4f},avg={avg_missed:.6f},n={samples}")
        self.logger.info(
            "[MM][CANCEL_REASON_SUMMARY] symbol=%s total_cancels=%d impact_horizon_ms=%d %s",
            self.symbol,
            total_cancels,
            int(self._cancel_impact_horizon_sec * 1000),
            " | ".join(parts),
        )
