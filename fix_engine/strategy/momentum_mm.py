from __future__ import annotations

import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from statistics import mean

from fix_engine.execution_gateway import ExecutionGateway
from fix_engine.market_data.models import MarketData
from fix_engine.order_models import MarketType, OrderRequest
from fix_engine.position_manager import PositionManager
from fix_engine.risk_manager import RiskManager


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    return default if abs(d) <= 1e-12 else (n / d)


@dataclass
class SignalMetrics:
    signals_total: int = 0
    signals_skipped: int = 0
    signals_executed: int = 0
    skip_reason_counter: dict[str, int] = field(default_factory=dict)
    side_pass_counter: dict[str, int] = field(default_factory=dict)
    side_skip_counter: dict[str, int] = field(default_factory=dict)

    def bump_skip(self, reason: str) -> None:
        self.signals_skipped += 1
        self.skip_reason_counter[reason] = self.skip_reason_counter.get(reason, 0) + 1

    def bump_side(self, side: str, *, passed: bool) -> None:
        if passed:
            self.side_pass_counter[side] = self.side_pass_counter.get(side, 0) + 1
        else:
            self.side_skip_counter[side] = self.side_skip_counter.get(side, 0) + 1


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
        # Back-compat: `spread_threshold` used to be hard max spread in ticks.
        spread_threshold: float,
        move_threshold: float,
        velocity_threshold: float,
        delta_threshold: float,
        imbalance_threshold: float = 0.0,
        cooldown_ms: int,
        # New config (scoring / modes / dynamic spread / fallback)
        strategy_mode: str = "SAFE",  # SAFE / NORMAL / AGGRESSIVE
        max_spread: float | None = None,  # in ticks
        dynamic_spread_enabled: bool = False,
        dynamic_spread_window_ticks: int = 200,
        dynamic_spread_mult: float = 1.5,
        normalized_delta_threshold: float = 0.03,
        score_threshold: float = 0.72,
        w_spread: float = 0.30,
        w_delta: float = 0.70,
        penalty_wide_spread: float = 0.25,
        penalty_low_delta: float = 0.25,
        position_sizing_enabled: bool = True,
        sizing_risk_per_trade_abs: float = 8.0,
        sizing_stop_loss_ticks: float = 4.0,
        sizing_min_qty: float = 1.0,
        sizing_max_qty: float = 1.0,
        risk_manager: RiskManager | None = None,
        no_trade_fallback_minutes: float = 3.0,
        fallback_relax_step: float = 0.85,
        fallback_min_delta: float = 0.0,
        fallback_min_score: float = 0.10,
        fallback_max_spread: float = 8.0,
        metrics_log_every: int = 200,
        # Exit / closing logic (optional; defaults keep current behavior stable)
        exit_on_score_enabled: bool = False,
        exit_threshold: float | None = None,
        max_holding_sec: float = 0.0,
        max_exit_spread: float | None = None,
        force_time_exit_enabled: bool = False,
        force_time_exit_after_sec: float = 0.0,
        # Entry stabilizers (early entry gating, low-latency)
        flat_filter_window_sec: float = 10.0,  # 5..15s recommended
        flat_threshold_points: float = 5.0,  # 4..6 points for SR
        volatility_filter_window_sec: float = 3.0,
        volatility_max_range_points: float = 25.0,
        max_trades_per_min: int = 3,  # 2..3 recommended
        entry_cooldown_seconds: float = 20.0,  # 15..30 recommended
        kill_zone_n: int = 5,  # last N closed trades
        kill_zone_stop_threshold: int = 2,  # stop_loss count => pause
        kill_zone_pause_seconds: float = 180.0,  # 120..300 recommended
        kill_zone_pause_2_sec: float = 60.0,
        kill_zone_pause_3_sec: float = 120.0,
        kill_zone_consecutive_only: bool = True,  # consecutive stop losses only
        entry_skip_log_throttle_sec: float = 0.5,
        # Entry pattern filters (strict order-flow mode)
        strict_entry_enabled: bool = False,
        strict_spread_max_ticks: float = 1.0,  # spread<=1 only
        strong_imbalance_high_min: float = 0.75,  # HIGH tier
        strong_imbalance_med_min: float = 0.60,  # MEDIUM tier
        strict_delta_score_min: float = 0.70,
        strict_delta_score_med: float = 0.90,
        strict_min_trend_ticks: float = 0.50,
        delta_spike_threshold: float = 300.0,  # abs(delta)>thr => skip
        continuation_ticks: int = 3,  # require delta/imb to strengthen last N ticks
        continuation_small_drop: float = 25.0,  # allow small last-tick fading
        trend_align_score_boost: float = 0.05,  # bonus only (never gates)
        # Entry/exit quality filters (optional)
        min_entry_spread: float = 1.0,  # ticks
        max_entry_spread: float = 3.0,  # ticks
        entry_imbalance_threshold: float = 0.2,
        entry_imbalance_threshold_long: float | None = None,
        entry_imbalance_threshold_short: float | None = None,
        min_abs_imbalance: float = 0.1,
        trend_lookback_ticks: int = 8,
        min_trend_move_ticks: float = 1.0,
        # TP/SL/trailing (ticks)
        take_profit_ticks: float = 2.0,
        stop_loss_ticks: float = 1.0,
        stop_loss_min_hold_bars: int = 2,
        stop_loss_min_age_ms: float = 400.0,
        trailing_start_ticks: float = 2.0,
        trailing_gap_ticks: float = 1.0,
        exit_imbalance_reversal: float = 0.2,
        flow_w_delta: float = 0.45,
        flow_w_imbalance: float = 0.35,
        flow_w_trend: float = 0.20,
        flow_decay_exit_threshold: float = 0.40,
        flow_decay_hold_threshold: float = 0.80,
        flow_decay_tp_disable_pnl_ticks: float = 1.0,
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
        # `on_market_data()` can be called from multiple threads depending on the data provider.
        # Guard all stateful decision/execution logic to prevent double-sends (observed as 0-1ms duplicate entries).
        self._md_lock = threading.Lock()
        # Mode presets (can be overridden by explicit config below)
        mode = (strategy_mode or "SAFE").strip().upper()
        if mode not in {"SAFE", "NORMAL", "AGGRESSIVE"}:
            mode = "SAFE"
        self._mode = mode

        # Base thresholds (will be adapted by fallback)
        base_max_spread = float(max_spread) if max_spread is not None else float(spread_threshold)
        base_delta_thr = float(delta_threshold)
        base_score_thr = float(score_threshold)

        if self._mode == "AGGRESSIVE":
            base_max_spread = max(base_max_spread, 6.0)
            base_score_thr = min(base_score_thr, 0.45)
        elif self._mode == "NORMAL":
            base_max_spread = max(base_max_spread, 5.0)
            base_score_thr = min(base_score_thr, 0.50)
        else:  # SAFE
            base_max_spread = max(base_max_spread, 5.0)  # requested default
            base_score_thr = max(base_score_thr, 0.55)

        self._base_max_spread = max(0.1, float(base_max_spread))
        self._base_velocity_threshold = max(0.0, float(velocity_threshold))
        self._base_imbalance_threshold = max(0.0, float(imbalance_threshold))
        self._base_delta_threshold = max(0.0, float(base_delta_thr))
        self._base_score_threshold = max(0.0, float(base_score_thr))
        # Preserve configured baselines (fallback relaxes thresholds; we may reset after a trade).
        self._cfg_max_spread = float(self._base_max_spread)
        self._cfg_delta_threshold = float(self._base_delta_threshold)
        self._cfg_score_threshold = float(self._base_score_threshold)

        # Scoring weights
        wsum = max(1e-9, float(w_spread) + float(w_delta))
        self._w_spread = float(w_spread) / wsum
        self._w_delta = float(w_delta) / wsum

        # Dynamic spread
        self._dynamic_spread_enabled = bool(dynamic_spread_enabled)
        self._spread_hist: deque[float] = deque(maxlen=max(10, int(dynamic_spread_window_ticks)))
        self._dynamic_spread_mult = max(1.0, float(dynamic_spread_mult))

        # Fallback relaxation
        self._no_trade_fallback_sec = max(0.0, float(no_trade_fallback_minutes) * 60.0)
        self._fallback_relax_step = _clamp(float(fallback_relax_step), 0.50, 0.99)
        self._fallback_min_delta = max(0.0, float(fallback_min_delta))
        self._fallback_min_score = max(0.0, float(fallback_min_score))
        self._fallback_max_spread = max(self._base_max_spread, float(fallback_max_spread))
        self._last_fallback_adjust_mono = 0.0
        self._fallback_adjust_cooldown_sec = 30.0
        self._fallback_max_adjustments = 6
        self._fallback_adjustments_done = 0

        # Metrics
        self.metrics = SignalMetrics()
        self._metrics_log_every = max(10, int(metrics_log_every))

        # Penalties (subtractive, soft)
        self._penalty_wide_spread = _clamp(float(penalty_wide_spread), 0.0, 10.0)
        self._penalty_low_delta = _clamp(float(penalty_low_delta), 0.0, 10.0)

        # Position sizing (risk-based)
        self._position_sizing_enabled = bool(position_sizing_enabled)
        self._sizing_risk_per_trade_abs = max(0.0, float(sizing_risk_per_trade_abs))
        self._sizing_stop_loss_ticks = max(1e-9, float(sizing_stop_loss_ticks))
        self._sizing_min_qty = max(0.0, float(sizing_min_qty))
        self._sizing_max_qty = max(self._sizing_min_qty, float(sizing_max_qty))
        self._risk_manager = risk_manager

        self._cooldown_sec = max(0.0, float(cooldown_ms) / 1000.0)

        # 1) ANTI-FLAT filter: rolling high-low range for the last window.
        self._flat_filter_window_sec = max(0.0, float(flat_filter_window_sec))
        self._flat_threshold_points = max(0.0, float(flat_threshold_points))
        self._flat_max_deque: deque[tuple[float, float]] = deque()  # (mono_s, price), monotonic max
        self._flat_min_deque: deque[tuple[float, float]] = deque()  # (mono_s, price), monotonic min
        self._flat_last_range_points = 0.0
        self._flat_window_coverage_sec = 0.0

        # 1.5) ANTI-VOL filter: avoid entering during short-term high volatility.
        self._vol_filter_window_sec = max(0.0, float(volatility_filter_window_sec))
        self._vol_max_range_points = max(0.0, float(volatility_max_range_points))

        # 2) Entry frequency limitation (based on ENTRY_EXECUTE = actual entry fills).
        self._max_trades_per_min = max(0, int(max_trades_per_min))
        self._entry_cooldown_seconds = max(0.0, float(entry_cooldown_seconds))
        self._entry_exec_ts_60s: deque[float] = deque()  # entry fill timestamps (mono_s)
        self._last_entry_execute_mono = 0.0

        # 3) Kill-zone control after stop_loss streaks.
        self._kill_zone_n = max(1, int(kill_zone_n))
        self._kill_zone_stop_threshold = max(1, int(kill_zone_stop_threshold))
        self._kill_zone_pause_seconds = max(0.0, float(kill_zone_pause_seconds))
        self._kill_zone_pause_2_sec = max(0.0, float(kill_zone_pause_2_sec))
        self._kill_zone_pause_3_sec = max(0.0, float(kill_zone_pause_3_sec))
        self._kill_zone_consecutive_only = bool(kill_zone_consecutive_only)
        self._kill_last_trade_is_stop: deque[bool] = deque(maxlen=self._kill_zone_n)
        self._kill_pause_until_mono = 0.0
        self._kill_last_stop_series_count = 0

        # Broker-side rejects can otherwise spam logs and stall the MD loop (exceptions bubble up).
        # We apply a short entry pause after certain broker errors.
        self._order_reject_pause_until_mono = 0.0
        self._last_order_reject_log_mono = 0.0
        self._order_reject_pause_sec = 300.0

        # Logging throttles for entry skip reasons (avoid log storms).
        self._entry_skip_log_throttle_sec = max(0.0, float(entry_skip_log_throttle_sec))
        self._last_skip_log_mono_flat = 0.0
        self._last_skip_log_mono_vol = 0.0
        self._last_skip_log_mono_freq = 0.0
        self._last_skip_log_mono_pause = 0.0

        # Strict entry pattern filters (order-flow hardened mode)
        self._strict_entry_enabled = bool(strict_entry_enabled)
        self._strict_spread_max_ticks = max(0.0, float(strict_spread_max_ticks))
        self._strong_imb_high_min = _clamp(float(strong_imbalance_high_min), 0.0, 1.0)
        self._strong_imb_med_min = _clamp(float(strong_imbalance_med_min), 0.0, 1.0)
        self._strict_delta_score_min = _clamp(float(strict_delta_score_min), 0.0, 1.0)
        self._strict_delta_score_med = _clamp(float(strict_delta_score_med), 0.0, 1.0)
        self._strict_min_trend_ticks = max(0.0, float(strict_min_trend_ticks))
        if self._strong_imb_med_min > self._strong_imb_high_min:
            self._strong_imb_med_min = float(self._strong_imb_high_min)
        self._delta_spike_threshold = max(0.0, float(delta_spike_threshold))
        self._continuation_ticks = max(2, int(continuation_ticks))
        self._continuation_small_drop = max(0.0, float(continuation_small_drop))
        self._trend_align_score_boost = _clamp(float(trend_align_score_boost), 0.0, 2.0)
        self._delta_hist: deque[float] = deque(maxlen=max(4, self._continuation_ticks + 1))
        self._imb_hist: deque[float] = deque(maxlen=max(4, self._continuation_ticks + 1))

        self._one_position_only = bool(one_position_only)

        # Minimal position state
        self._position_qty = 0.0
        self._entry_price = 0.0
        self._entry_monotonic = 0.0
        self._entry_cl_ord_id = ""
        self._immediate_ticks_seen = 0
        self._entry_mid_price = 0.0
        self._entry_qty_abs = 0.0
        self._entry_sign = 0  # +1 long, -1 short
        self._best_favorable_ticks = 0.0
        self._worst_adverse_ticks = 0.0
        self._max_mid_since_entry = 0.0
        self._min_mid_since_entry = 0.0
        self._partial_exit_done = False
        self._breakeven_armed = False
        self._exit_in_flight = False
        self._exit_last_send_mono = 0.0
        self._exit_cl_ord_id = ""
        self._exit_reason = ""
        # Exit reversal anchors (captured from market snapshot at entry send).
        self._pending_entry_delta = 0.0
        self._entry_delta_open = 0.0
        self._pending_entry_imbalance = 0.0
        self._entry_imbalance_open = 0.0
        self._pending_entry_flow_strength = 1.0
        self._entry_flow_strength = 1.0
        # User tuning (SRM6): entry/exit tweaks
        self._imb_hard_gate = 0.35
        self._force_entry_delta_abs = 6.0
        self._force_entry_imb_abs = 0.40
        self._tp_scale_entry_delta_abs = 6.0
        self._tp_scale_mult = 1.5
        # SRM6 exit tuning: allow profit-taking from +2 ticks.
        self._tp_min_profit_ticks = 2.0
        self._dynamic_tp_min_profit_ticks = 2.0
        self._no_move_min_age_ms = 4500.0
        self._no_move_delta_abs_max = 1.0
        self._no_move_imb_abs_max = 0.10
        # Exit tuning: strong reversal filter (prevents churn on noise).
        self._strong_rev_threshold = 1.60
        self._rev_min_abs_delta = 3.0
        self._rev_min_abs_imb = 0.30
        # Exit tuning: improve winner PnL / reduce premature TP
        self._tp_min_hold_bars = 3
        self._tp_min_profit_ticks = max(float(self._tp_min_profit_ticks), 2.0)
        self._tp_strong_entry_delta_abs = 10.0
        self._tp_strong_entry_mult = 1.5
        self._tp_trailing_gap = 2.0
        # Exit tuning: soften SL when flow still supports position
        self._sl_flow_delta_min_abs = 3.0
        self._sl_flow_max_extra_mult = 2.0  # never ignore SL beyond 2x SL
        # Runtime state
        self._bars_in_position = 0
        self._tp_armed = False
        self._tp_max_pnl_ticks = 0.0
        self._tp_blocked_logged = False
        self._entry_expected_price = 0.0
        self._entry_slippage_ticks = 0.0
        # Profit-zone guard: once trade has seen >=2 ticks, disable no-movement exit.
        self._in_profit_zone = False

        self._warned_zero_delta_threshold = False

        # Minimal PnL / winrate stats (realized on close)
        self._realized_pnl = 0.0
        self._trades_closed = 0
        self._wins = 0
        self._consecutive_losses = 0
        self._recent_trade_pnl_ticks: deque[float] = deque(maxlen=20)
        self._size_risk_multiplier = 1.0
        self._unrealized_pnl_ticks_hist: deque[float] = deque(maxlen=10)
        # Risk / session controls (exit-only changes live in exit engine)

        # Minimal feature history (for debug/compat; strategy uses single-snapshot)
        self._mid_hist: deque[tuple[float, float]] = deque(maxlen=64)  # (mono_ms, mid)
        self._last_entry_place_monotonic = 0.0
        # Prevent immediate fallback relaxation right after startup.
        # Fallback is meant to apply only after `no_trade_fallback_minutes` of inactivity.
        self._last_signal_execute_mono = time.monotonic()

        _ = entry_decision_sink
        _ = economics_store

        # Exit thresholds (kept separate from entry thresholds)
        self._exit_on_score_enabled = bool(exit_on_score_enabled)
        base_exit_thr = float(exit_threshold) if exit_threshold is not None else (self._base_score_threshold * 0.80)
        self._exit_threshold = _clamp(float(base_exit_thr), 0.0, 2.0)
        self._max_holding_sec = max(0.0, float(max_holding_sec))
        self._max_exit_spread = float(max_exit_spread) if max_exit_spread is not None else None
        self._force_time_exit_enabled = bool(force_time_exit_enabled)
        self._force_time_exit_after_sec = max(0.0, float(force_time_exit_after_sec))

        # Entry filters
        self._min_entry_spread = max(0.0, float(min_entry_spread))
        self._max_entry_spread = max(self._min_entry_spread, float(max_entry_spread))
        _ = imbalance_threshold  # legacy arg retained for back-compat (ignored)
        base_imb = _clamp(float(entry_imbalance_threshold), 0.0, 1.0)
        self._imbalance_threshold_long = (
            _clamp(float(entry_imbalance_threshold_long), 0.0, 1.0)
            if entry_imbalance_threshold_long is not None
            else base_imb
        )
        self._imbalance_threshold_short = (
            _clamp(float(entry_imbalance_threshold_short), 0.0, 1.0)
            if entry_imbalance_threshold_short is not None
            else base_imb
        )
        self._min_abs_imbalance = _clamp(float(min_abs_imbalance), 0.0, 1.0)
        self._trend_lookback = max(2, int(trend_lookback_ticks))
        self._min_trend_move_ticks = max(0.0, float(min_trend_move_ticks))

        # TP/SL/trailing
        self._tp_ticks = max(0.0, float(take_profit_ticks))
        self._sl_ticks = max(0.0, float(stop_loss_ticks))
        self._sl_min_hold_bars = max(0, int(stop_loss_min_hold_bars))
        self._sl_min_age_ms = max(0.0, float(stop_loss_min_age_ms))
        self._trail_start = max(0.0, float(trailing_start_ticks))
        self._trail_gap = max(0.0, float(trailing_gap_ticks))
        self._exit_imbalance_reversal = _clamp(float(exit_imbalance_reversal), 0.0, 1.0)
        flow_w_sum = max(1e-9, float(flow_w_delta) + float(flow_w_imbalance) + float(flow_w_trend))
        self._flow_w_delta = max(0.0, float(flow_w_delta)) / flow_w_sum
        self._flow_w_imbalance = max(0.0, float(flow_w_imbalance)) / flow_w_sum
        self._flow_w_trend = max(0.0, float(flow_w_trend)) / flow_w_sum
        self._flow_decay_exit_threshold = _clamp(float(flow_decay_exit_threshold), 0.05, 1.0)
        self._flow_decay_hold_threshold = _clamp(float(flow_decay_hold_threshold), self._flow_decay_exit_threshold, 2.0)
        self._flow_decay_tp_disable_pnl_ticks = float(flow_decay_tp_disable_pnl_ticks)

    def on_execution_report(self, state: dict[str, object]) -> None:
        if str(state.get("symbol", "")).upper() != self.symbol:
            return
        # IMPORTANT: we must also react to terminal statuses for the exit order.
        # Otherwise `_exit_in_flight` can stay True forever after cancel/reject and block exits.
        cl_ord_id = str(state.get("cl_ord_id", "") or "")
        status_new = str(state.get("status_new", ""))
        if cl_ord_id and cl_ord_id == self._exit_cl_ord_id and status_new in {"CANCELED", "REJECTED"}:
            self.logger.warning(
                "[MM][EXIT_ORDER_TERMINAL] symbol=%s status=%s reason=%s cl_ord_id=%s",
                self.symbol,
                status_new,
                self._exit_reason,
                cl_ord_id,
            )
            self._exit_in_flight = False
            self._exit_last_send_mono = 0.0
            self._exit_cl_ord_id = ""
            self._exit_reason = ""
            return
        if status_new not in {"PARTIALLY_FILLED", "FILLED"}:
            return
        last_qty = float(state.get("last_qty", 0) or 0)
        if last_qty <= 0:
            return
        side = str(state.get("side", ""))
        last_px = state.get("last_px", None)
        avg_px = state.get("avg_px", None)
        fill_px = float(last_px or avg_px or 0.0)

        prev_qty = float(self._position_qty)
        if side == "1":
            self._position_qty = prev_qty + last_qty
        else:
            self._position_qty = prev_qty - last_qty

        if abs(self._entry_price) < 1e-12 and abs(self._position_qty) > 1e-12:
            self._entry_price = fill_px
            self._entry_monotonic = time.perf_counter()
            # Entry fill executed: update frequency/cooldown gates.
            self._last_entry_execute_mono = float(self._entry_monotonic)
            if self._max_trades_per_min > 0 or self._entry_cooldown_seconds > 0.0:
                self._entry_exec_ts_60s.append(self._last_entry_execute_mono)
                self._prune_entry_exec_ts_60s(now_mono=self._last_entry_execute_mono)
            self._entry_cl_ord_id = str(state.get("cl_ord_id", "") or "")
            self._immediate_ticks_seen = 0
            self._entry_mid_price = 0.0
            self._entry_qty_abs = abs(self._position_qty)
            self._entry_sign = 1 if self._position_qty > 0 else -1
            self._best_favorable_ticks = 0.0
            self._worst_adverse_ticks = 0.0
            self._max_mid_since_entry = float(fill_px)
            self._min_mid_since_entry = float(fill_px)
            self._partial_exit_done = False
            self._breakeven_armed = False
            self._entry_delta_open = float(self._pending_entry_delta)
            self._entry_imbalance_open = float(self._pending_entry_imbalance)
            self._entry_flow_strength = max(1e-6, float(self._pending_entry_flow_strength))
            self._unrealized_pnl_ticks_hist.clear()
            self._bars_in_position = 0
            self._tp_armed = False
            self._tp_max_pnl_ticks = 0.0
            self._tp_blocked_logged = False
            self._in_profit_zone = False
            if abs(self._tick_size) > 1e-12:
                self._entry_slippage_ticks = (float(fill_px) - float(self._entry_expected_price)) / float(self._tick_size) * float(self._entry_sign)
            else:
                self._entry_slippage_ticks = 0.0
            self._exit_in_flight = False
            self._exit_last_send_mono = 0.0
            self._exit_cl_ord_id = ""
            self._exit_reason = ""

        if abs(self._position_qty) < 1e-12 and abs(prev_qty) > 1e-12:
            self._position_qty = 0.0
            self._bars_in_position = 0
            self._tp_armed = False
            self._tp_max_pnl_ticks = 0.0
            self._tp_blocked_logged = False
            self._in_profit_zone = False
            # Realize PnL on close using last fill price.
            if abs(self._entry_price) > 1e-12 and self._entry_sign != 0:
                qty = max(0.0, float(self._entry_qty_abs))
                pnl = (fill_px - self._entry_price) * float(self._entry_sign) * qty
                pnl_ticks = pnl / self._tick_size if self._tick_size > 0 else pnl
                self._realized_pnl += pnl
                self._trades_closed += 1
                if pnl > 0:
                    self._wins += 1
                    self._consecutive_losses = 0
                elif pnl < 0:
                    self._consecutive_losses += 1
                winrate = (self._wins / self._trades_closed) if self._trades_closed > 0 else 0.0
                self._recent_trade_pnl_ticks.append(float(pnl_ticks))

                # De-risk after loss streaks (very conservative):
                # If 3 losses in a row, cut position size by 50% until a win occurs.
                if self._consecutive_losses >= 3:
                    self._size_risk_multiplier = 0.5
                elif pnl > 0:
                    self._size_risk_multiplier = 1.0

                # NOTE: do not apply entry gating here (exit-only changes).
                exit_side = "2" if prev_qty > 0 else "1"
                exit_reason = str(self._exit_reason or "")
                exit_cl_ord_id = str(self._exit_cl_ord_id or "")
                self.logger.info(
                    "[MM][TRADE_CLOSE] symbol=%s order_id=%s side=%s qty=%.4f exit_price=%.4f realized_pnl=%.4f exit_reason=%s exit_cl_ord_id=%s timestamp=%s realized_total=%.4f trades=%s winrate=%.2f%%",
                    self.symbol,
                    self._entry_cl_ord_id or str(state.get("cl_ord_id", "") or ""),
                    exit_side,
                    qty,
                    fill_px,
                    pnl,
                    exit_reason,
                    exit_cl_ord_id,
                    float(time.time()),
                    self._realized_pnl,
                    self._trades_closed,
                    100.0 * winrate,
                )
                if self._consecutive_losses >= 3:
                    self.logger.warning(
                        "[MM][RISK_DEGRADE] consecutive_losses=%s size_multiplier=%.2f",
                        self._consecutive_losses,
                        self._size_risk_multiplier,
                    )
                if self._risk_manager is not None:
                    self._risk_manager.on_trade_result(net_pnl=float(pnl))

                # 3) Kill-zone update on closed trade.
                # Exit engine emits reasons like "stop_loss" / "stop_loss_max_loss".
                if self._kill_zone_pause_seconds > 0.0:
                    exit_reason = str(self._exit_reason or "")
                    is_stop = exit_reason.startswith("stop_loss")
                    self._kill_last_trade_is_stop.append(is_stop)
                    stop_series_count = self._kill_zone_stop_series_count()
                    self._kill_last_stop_series_count = int(stop_series_count)
                    if stop_series_count >= self._kill_zone_stop_threshold:
                        # Adaptive pause: 2 stops -> short pause, 3+ stops -> longer pause.
                        pause_sec = 0.0
                        if stop_series_count >= 3:
                            pause_sec = float(self._kill_zone_pause_3_sec)
                        elif stop_series_count == 2:
                            pause_sec = float(self._kill_zone_pause_2_sec)
                        else:
                            pause_sec = float(self._kill_zone_pause_seconds)
                        if pause_sec > 0.0:
                            close_now_mono = time.perf_counter()
                            self._kill_pause_until_mono = float(close_now_mono + pause_sec)
                            self.logger.warning(
                                "[MM][KILL_ZONE] activated stop_series=%s/%s pause_sec=%.1f exit_reason=%s",
                                str(stop_series_count),
                                str(self._kill_zone_stop_threshold),
                                float(pause_sec),
                                exit_reason,
                            )
            # After any closed trade, reset fallback-relaxed entry thresholds back to configured.
            # This keeps fallback as a temporary "wake-up" mechanism, not a permanent mode change.
            self._base_delta_threshold = float(self._cfg_delta_threshold)
            self._base_score_threshold = float(self._cfg_score_threshold)
            self._base_max_spread = float(self._cfg_max_spread)
            self._fallback_adjustments_done = 0
            self._last_fallback_adjust_mono = 0.0
            self._entry_price = 0.0
            self._entry_monotonic = 0.0
            self._entry_cl_ord_id = ""
            self._immediate_ticks_seen = 0
            self._entry_mid_price = 0.0
            self._entry_qty_abs = 0.0
            self._entry_sign = 0
            self._pending_entry_flow_strength = 1.0
            self._entry_flow_strength = 1.0
            self._pending_entry_imbalance = 0.0
            self._entry_imbalance_open = 0.0
            self._best_favorable_ticks = 0.0
            self._exit_in_flight = False
            self._exit_last_send_mono = 0.0
            self._exit_cl_ord_id = ""
            self._exit_reason = ""
            self._in_profit_zone = False

    def on_market_data(self, data: MarketData) -> None:
        with self._md_lock:
            if data.symbol.upper() != self.symbol:
                return

            now_mono = time.perf_counter()
            now_ms = now_mono * 1000.0
            mid = float(data.mid_price)
            self._mid_hist.append((now_ms, mid))
            self._update_flat_range(now_mono=now_mono, mid_price=mid)
            # Keep micro order-flow history hot for entry pattern filters.
            try:
                self._delta_hist.append(float(getattr(data, "bid_size", 0.0) or 0.0) - float(getattr(data, "ask_size", 0.0) or 0.0))
                bid_sz = float(getattr(data, "bid_size", 0.0) or 0.0)
                ask_sz = float(getattr(data, "ask_size", 0.0) or 0.0)
                denom = bid_sz + ask_sz
                self._imb_hist.append(((bid_sz - ask_sz) / denom) if denom > 1e-12 else 0.0)
            except Exception:
                # Keep strategy resilient to missing fields.
                self._delta_hist.append(0.0)
                self._imb_hist.append(0.0)

            if abs(self._position_qty) > 1e-12:
                self._maybe_fast_invalidate(data=data, now_mono=now_mono)
                self._maybe_exit_position(data=data, now_mono=now_mono, now_ms=now_ms)
                return

            # 3) KILL ZONE control: stop opening new positions while pause is active.
            if self._kill_pause_until_mono > 0.0 and now_mono < self._kill_pause_until_mono:
                self.metrics.bump_skip("pause_kill_zone")
                if (now_mono - self._last_skip_log_mono_pause) >= self._entry_skip_log_throttle_sec:
                    self._last_skip_log_mono_pause = now_mono
                    remaining = float(self._kill_pause_until_mono - now_mono)
                    self.logger.info(
                        "[MM][ENTRY_SKIP] reason=pause_kill_zone remaining_sec=%.2f stop_series=%s/%s window_n=%s",
                        remaining,
                        str(self._kill_last_stop_series_count),
                        str(self._kill_zone_stop_threshold),
                        str(self._kill_zone_n),
                    )
                return

            # 1) ANTI-FLAt filter: early exit for low micro-volatility.
            if self._vol_filter_window_sec > 0.0 and self._vol_max_range_points > 0.0:
                rr = self._recent_range_points(now_ms=now_ms, window_sec=self._vol_filter_window_sec)
                if rr is not None:
                    rng, cov = rr
                    if cov >= float(self._vol_filter_window_sec) * 0.80 and float(rng) > float(self._vol_max_range_points):
                        self.metrics.bump_skip("volatility_too_high")
                        if (now_mono - self._last_skip_log_mono_vol) >= self._entry_skip_log_throttle_sec:
                            self._last_skip_log_mono_vol = now_mono
                            self.logger.info(
                                "[MM][ENTRY_SKIP] reason=volatility_too_high range=%.4f>max=%.4f window_sec=%.2f coverage_sec=%.2f",
                                float(rng),
                                float(self._vol_max_range_points),
                                float(self._vol_filter_window_sec),
                                float(cov),
                            )
                        return

            if self._flat_filter_window_sec > 0.0 and self._flat_threshold_points > 0.0:
                if self._flat_window_coverage_sec >= self._flat_filter_window_sec * 0.80:
                    if self._flat_last_range_points < self._flat_threshold_points:
                        self.metrics.bump_skip("flat_filter")
                        if (now_mono - self._last_skip_log_mono_flat) >= self._entry_skip_log_throttle_sec:
                            self._last_skip_log_mono_flat = now_mono
                            self.logger.info(
                                "[MM][ENTRY_SKIP] reason=flat_filter range=%.4f<thr=%.4f window_sec=%.1f coverage_sec=%.2f",
                                float(self._flat_last_range_points),
                                float(self._flat_threshold_points),
                                float(self._flat_filter_window_sec),
                                float(self._flat_window_coverage_sec),
                            )
                        return

            # 2) Entry frequency limitation (executed entries only).
            if self._max_trades_per_min > 0 or self._entry_cooldown_seconds > 0.0:
                self._prune_entry_exec_ts_60s(now_mono=now_mono)
                if self._max_trades_per_min > 0 and len(self._entry_exec_ts_60s) >= self._max_trades_per_min:
                    self.metrics.bump_skip("max_trades_per_min")
                    if (now_mono - self._last_skip_log_mono_freq) >= self._entry_skip_log_throttle_sec:
                        self._last_skip_log_mono_freq = now_mono
                        self.logger.info(
                            "[MM][ENTRY_SKIP] reason=max_trades_per_min trades_last_60s=%s max=%s",
                            str(len(self._entry_exec_ts_60s)),
                            str(self._max_trades_per_min),
                        )
                    return

                if self._entry_cooldown_seconds > 0.0 and self._last_entry_execute_mono > 0.0:
                    dt = float(now_mono - self._last_entry_execute_mono)
                    if dt < self._entry_cooldown_seconds:
                        self.metrics.bump_skip("entry_cooldown_exec")
                        if (now_mono - self._last_skip_log_mono_freq) >= self._entry_skip_log_throttle_sec:
                            self._last_skip_log_mono_freq = now_mono
                            remaining = float(self._entry_cooldown_seconds - dt)
                            self.logger.info(
                                "[MM][ENTRY_SKIP] reason=entry_cooldown_exec remaining_sec=%.2f cooldown_sec=%.2f",
                                remaining,
                                float(self._entry_cooldown_seconds),
                            )
                        return

            if self._cooldown_sec > 0.0 and self._last_entry_place_monotonic > 0.0:
                dt_send = float(now_mono - self._last_entry_place_monotonic)
                if dt_send < self._cooldown_sec:
                    self.metrics.bump_skip("cooldown_ms_send")
                    if (now_mono - self._last_skip_log_mono_freq) >= self._entry_skip_log_throttle_sec:
                        self._last_skip_log_mono_freq = now_mono
                        remaining = float(self._cooldown_sec - dt_send)
                        self.logger.info(
                            "[MM][ENTRY_SKIP] reason=cooldown_ms_send remaining_sec=%.2f cooldown_sec=%.2f",
                            remaining,
                            float(self._cooldown_sec),
                        )
                    return

            snapshot = self._compute_features(data=data, now_ms=now_ms)
            decision, score_row = self.evaluate_entry_signal(snapshot, now_mono=now_mono)
            self._log_decision(score_row)
            if decision == "NONE":
                return

            self._send_aggressive_entry(decision=decision, data=data, now_mono=now_mono)

    def _compute_features(self, *, data: MarketData, now_ms: float) -> dict[str, float]:
        spread_ticks = float(data.spread) / self._tick_size
        self._spread_hist.append(spread_ticks)
        bid_sz = float(getattr(data, "bid_size", 0.0) or 0.0)
        ask_sz = float(getattr(data, "ask_size", 0.0) or 0.0)
        delta = bid_sz - ask_sz
        denom = bid_sz + ask_sz
        imbalance = (delta / denom) if denom > 1e-12 else 0.0

        return {
            "spread": spread_ticks,
            "mid_price": float(data.mid_price),
            "delta": delta,
            "imbalance": float(imbalance),
        }

    def _trend_move_ticks(self, *, now_ms: float, mid: float) -> float:
        """
        Price confirmation: mid move in ticks over last N updates.
        Uses `_mid_hist` which stores (ms, mid).
        """
        if len(self._mid_hist) < self._trend_lookback:
            return 0.0
        ref_mid = float(self._mid_hist[-self._trend_lookback][1])
        return (float(mid) - ref_mid) / self._tick_size

    def _recent_range_points(self, *, now_ms: float, window_sec: float) -> tuple[float, float] | None:
        if window_sec <= 0.0 or len(self._mid_hist) < 2:
            return None
        cutoff = float(now_ms) - float(window_sec) * 1000.0
        pts = [float(px) for (ts, px) in self._mid_hist if float(ts) >= cutoff]
        if len(pts) < 2:
            return None
        t0 = min(float(ts) for (ts, _) in self._mid_hist if float(ts) >= cutoff)
        coverage_sec = max(0.0, (float(now_ms) - float(t0)) / 1000.0)
        return (max(pts) - min(pts), coverage_sec)

    def _update_flat_range(self, *, now_mono: float, mid_price: float) -> None:
        """
        Maintain rolling high-low range for ANTI-FLAt filter.
        Uses monotonic min/max deques for O(1) amortized updates.
        """
        if self._flat_filter_window_sec <= 0.0 or self._flat_threshold_points <= 0.0:
            return
        # NaN guard
        if not (mid_price == mid_price):
            return

        cutoff = now_mono - self._flat_filter_window_sec

        # Prune old candidates from both deques by timestamp.
        while self._flat_max_deque and self._flat_max_deque[0][0] < cutoff:
            self._flat_max_deque.popleft()
        while self._flat_min_deque and self._flat_min_deque[0][0] < cutoff:
            self._flat_min_deque.popleft()

        # Monotonic max deque (decreasing by price).
        while self._flat_max_deque and self._flat_max_deque[-1][1] <= mid_price:
            self._flat_max_deque.pop()
        self._flat_max_deque.append((now_mono, mid_price))

        # Monotonic min deque (increasing by price).
        while self._flat_min_deque and self._flat_min_deque[-1][1] >= mid_price:
            self._flat_min_deque.pop()
        self._flat_min_deque.append((now_mono, mid_price))

        if self._flat_max_deque and self._flat_min_deque:
            max_px = float(self._flat_max_deque[0][1])
            min_px = float(self._flat_min_deque[0][1])
            self._flat_last_range_points = max(0.0, max_px - min_px)
            earliest_ts = min(float(self._flat_max_deque[0][0]), float(self._flat_min_deque[0][0]))
            self._flat_window_coverage_sec = max(0.0, float(now_mono - earliest_ts))
        else:
            self._flat_last_range_points = 0.0
            self._flat_window_coverage_sec = 0.0

    def _prune_entry_exec_ts_60s(self, *, now_mono: float) -> None:
        cutoff = now_mono - 60.0
        while self._entry_exec_ts_60s and self._entry_exec_ts_60s[0] < cutoff:
            self._entry_exec_ts_60s.popleft()

    def _kill_zone_stop_series_count(self) -> int:
        """
        Count stop_loss streak:
        - consecutive only (default): from newest trades backwards until first non-stop
        - or occurrences within last N trades (if consecutive_only=False)
        """
        if self._kill_zone_consecutive_only:
            cnt = 0
            for flag in reversed(self._kill_last_trade_is_stop):
                if flag:
                    cnt += 1
                else:
                    break
            return cnt
        return sum(1 for x in self._kill_last_trade_is_stop if x)

    # NOTE: volatility-based sizing / entry cooldown intentionally not used here.

    def _compute_flow_strength(self, *, delta: float, imbalance: float, trend_ticks: float, pos_sign: int) -> float:
        # Direction-aware "signal life": only supportive flow contributes.
        delta_norm = _clamp(_safe_div(max(0.0, float(pos_sign) * float(delta)), max(self._base_delta_threshold, 1e-6)), 0.0, 1.5)
        imb_norm = _clamp(max(0.0, float(pos_sign) * float(imbalance)), 0.0, 1.0)
        trend_norm = _clamp(
            _safe_div(max(0.0, float(pos_sign) * float(trend_ticks)), max(self._min_trend_move_ticks * 2.0, 1e-6)),
            0.0,
            1.0,
        )
        return (
            self._flow_w_delta * float(delta_norm)
            + self._flow_w_imbalance * float(imb_norm)
            + self._flow_w_trend * float(trend_norm)
        )

    def _effective_max_spread(self) -> float:
        max_spread = float(self._base_max_spread)
        if self._dynamic_spread_enabled and len(self._spread_hist) >= 10:
            avg = sum(self._spread_hist) / float(len(self._spread_hist))
            dyn = max(0.1, avg * self._dynamic_spread_mult)
            # keep at least configured max_spread, but allow dyn to increase during wide markets
            max_spread = max(max_spread, dyn)
        return min(max_spread, float(self._fallback_max_spread))

    def _entry_spread_limit_dynamic(self) -> float:
        """
        Dynamic entry spread limit (in ticks):
        - baseline: configured self._max_entry_spread
        - dynamic: p90 of last ~100 observed spreads (from self._spread_hist)
        """
        base = float(self._max_entry_spread)
        hist = list(self._spread_hist)
        if len(hist) < 20:
            return base
        hist.sort()
        i = int(round(0.90 * (len(hist) - 1)))
        p90 = float(hist[max(0, min(len(hist) - 1, i))])
        # Keep within sane range to avoid runaway in broken feeds.
        return _clamp(p90, 0.5, max(base, 8.0))

    def _maybe_apply_fallback(self, *, now_mono: float) -> None:
        if self._no_trade_fallback_sec <= 0.0:
            return
        if self._last_signal_execute_mono > 0.0 and (now_mono - self._last_signal_execute_mono) < self._no_trade_fallback_sec:
            return
        if self._last_fallback_adjust_mono > 0.0 and (now_mono - self._last_fallback_adjust_mono) < self._fallback_adjust_cooldown_sec:
            return
        if self._fallback_adjustments_done >= self._fallback_max_adjustments:
            return
        # Stop if we're already at bounds.
        if (
            self._base_delta_threshold <= self._fallback_min_delta + 1e-12
            and self._base_score_threshold <= self._fallback_min_score + 1e-12
            and self._base_max_spread >= self._fallback_max_spread - 1e-12
        ):
            return

        old_delta = self._base_delta_threshold
        old_score = self._base_score_threshold
        old_spread = self._base_max_spread

        next_step = int(self._fallback_adjustments_done) + 1
        cfg_delta_floor = max(self._fallback_min_delta, float(self._cfg_delta_threshold) * 0.50) if next_step >= 6 else self._fallback_min_delta
        self._base_delta_threshold = max(cfg_delta_floor, self._base_delta_threshold * self._fallback_relax_step)
        self._base_score_threshold = max(self._fallback_min_score, self._base_score_threshold * self._fallback_relax_step)
        self._base_max_spread = min(self._fallback_max_spread, self._base_max_spread / self._fallback_relax_step)

        self._last_fallback_adjust_mono = now_mono
        self._fallback_adjustments_done += 1
        self.logger.warning(
            "[MM][FALLBACK] mode=%s no_trades_sec=%.1f step=%s/%s delta_thr %.3f->%.3f score_thr %.3f->%.3f max_spread %.2f->%.2f",
            self._mode,
            float(now_mono - (self._last_signal_execute_mono or now_mono)),
            self._fallback_adjustments_done,
            self._fallback_max_adjustments,
            old_delta,
            self._base_delta_threshold,
            old_score,
            self._base_score_threshold,
            old_spread,
            self._base_max_spread,
        )

    def evaluate_entry_signal(self, market_snapshot: dict[str, float], *, now_mono: float) -> tuple[str, dict[str, float | str]]:
        self.metrics.signals_total += 1
        self._maybe_apply_fallback(now_mono=now_mono)

        # NOTE: do not gate entries here (exit-only changes).

        spread = float(market_snapshot.get("spread", 0.0))
        delta = float(market_snapshot.get("delta", 0.0))
        mid_price = float(market_snapshot.get("mid_price", 0.0))
        imbalance = float(market_snapshot.get("imbalance", 0.0))

        max_spread = self._effective_max_spread()
        delta_thr_raw = self._base_delta_threshold
        delta_thr = float(delta_thr_raw) if delta_thr_raw is not None else 7.0
        if delta_thr <= 0.0:
            if not self._warned_zero_delta_threshold:
                self._warned_zero_delta_threshold = True
                self.logger.warning(
                    "[MM][CONFIG] delta_threshold<=0 (%.6f). Using eps=1e-6 for scoring; set delta_threshold>=1 in config.",
                    delta_thr,
                )
            delta_thr = 1e-6
        score_thr_raw = self._base_score_threshold
        score_thr = float(score_thr_raw) if score_thr_raw is not None else 0.45

        # Trend used both for direction confirm and (softly) for scoring.
        trend_ticks = self._trend_move_ticks(now_ms=(now_mono * 1000.0), mid=mid_price)

        # Component scores (0..1)
        spread_score = _clamp(1.0 - (spread / max_spread), 0.0, 1.0)
        delta_thr_eff = delta_thr * 0.86 if self._mode == "AGGRESSIVE" else delta_thr
        delta_score = min(1.0, abs(delta) / max(delta_thr_eff, 1e-6))
        # Trend score: 0 if no confirmed trend; ramps up to 1 as trend grows.
        trend_score = _clamp(_safe_div(abs(trend_ticks), max(self._min_trend_move_ticks * 2.0, 1e-6)), 0.0, 1.0)
        imbalance_score = _clamp(abs(imbalance), 0.0, 1.0)

        # Strict entry mode: hardened order-flow patterns (skip noise/exhaustion).
        if self._strict_entry_enabled:
            # Adapted strict filters (per prompt):
            # - imbalance: abs(imb)>=0.7 OR abs(imb)>=0.45 and delta_score>0.9
            # - spread: spread<=3
            # - delta_score gate: delta_score>=0.7
            # Other strict guards (delta spike, direction/continuation) are kept.
            if self._delta_spike_threshold > 0.0 and abs(delta) > self._delta_spike_threshold:
                decision = "NONE"
                reason = "delta_spike_exhaustion"
            else:
                abs_imb = abs(float(imbalance))
                ok_spread = float(spread) <= float(self._strict_spread_max_ticks)
                # imbalance gate
                if abs_imb >= float(self._strong_imb_high_min):
                    imbalance_ok = True
                elif abs_imb >= float(self._strong_imb_med_min) and float(delta_score) > float(self._strict_delta_score_med):
                    imbalance_ok = True
                else:
                    imbalance_ok = False

                if not ok_spread:
                    decision = "NONE"
                    reason = "spread_too_wide"
                elif float(delta_score) < float(self._strict_delta_score_min):
                    decision = "NONE"
                    reason = "delta_not_confirmed"
                elif not imbalance_ok:
                    decision = "NONE"
                    reason = "imbalance_too_weak"
                else:
                    # Candidate direction: take imbalance sign (more stable than delta sign in micro-noise)
                    cand_sign = _sign(imbalance)
                    if cand_sign == 0:
                        cand_sign = _sign(delta)
                    if cand_sign == 0 or (_sign(delta) != 0 and _sign(delta) != cand_sign):
                        decision = "NONE"
                        reason = "direction_mismatch"
                    else:
                        # If there is no meaningful local trend, strict entries tend to be mean-reverting noise.
                        # Require at least some trend impulse unless imbalance is very strong.
                        if abs(float(trend_ticks)) < float(self._strict_min_trend_ticks) and abs(float(imbalance)) < (
                            float(self._strong_imb_high_min) + 0.10
                        ):
                            decision = "NONE"
                            reason = "trend_too_weak"
                        # Trend mismatch kill-switch: if we have a clear local trend and it's opposite
                        # to the intended direction, skip (this prevents "strong imbalance into a falling trend").
                        elif _sign(trend_ticks) != 0 and _sign(trend_ticks) != int(cand_sign):
                            decision = "NONE"
                            reason = "trend_mismatch"
                        else:
                            # Continuation: require N last ticks to support direction without large drop.
                            n = max(2, int(self._continuation_ticks))
                            cont_ok = False
                            if len(self._delta_hist) >= n:
                                seq = [float(cand_sign) * float(self._delta_hist[-k]) for k in range(n, 0, -1)]
                                # Must not flip sign, and last should not fade materially vs first.
                                if min(seq) >= 0.0 and seq[-1] >= (seq[0] - float(self._continuation_small_drop)):
                                    cont_ok = True

                            if not cont_ok:
                                decision = "NONE"
                                reason = "weak_continuation"
                            else:
                                decision = "LONG" if cand_sign > 0 else "SHORT"
                                reason = "strict_flow_continuation"

            # Lightweight quality score for strict mode (kept for logging/audit only).
            score_strict = (
                0.60 * float(delta_score)
                + 0.40 * abs(float(imbalance))
                - 0.10 * float(spread)
            )
            if _sign(trend_ticks) != 0 and _sign(trend_ticks) == _sign(imbalance):
                score_strict += float(self._trend_align_score_boost)
            row = {
                "symbol": self.symbol,
                "mode": self._mode,
                "decision": decision,
                "reason": reason,
                "score": float(score_strict),
                "score_thr": float(score_thr),
                "cfg_score_thr": float(self._cfg_score_threshold),
                "spread": float(spread),
                "max_spread": float(max_spread),
                "mid": float(mid_price),
                "delta": float(delta),
                "delta_thr": float(delta_thr),
                "cfg_delta_thr": float(self._cfg_delta_threshold),
                "delta_score": float(delta_score),
                "imbalance": float(imbalance),
                "trend_ticks": float(trend_ticks),
                "fallback_step": str(self._fallback_adjustments_done),
                "fallback_max": str(self._fallback_max_adjustments),
                "need_delta": max(0.0, float(delta_thr) - abs(float(delta))),
                "need_spread": max(0.0, float(spread) - float(self._strict_spread_max_ticks)),
            }
            return decision, row

        # Score composition: keep legacy spread/delta weights, but let strong delta/imbalance partially compensate
        # lack of confirmed trend. Trend weight is intentionally small.
        w_trend = 0.06
        w_imb = 0.10
        w_res = max(1e-9, 1.0 - w_trend - w_imb)
        # Slightly increase delta contribution vs spread inside residual (conservative nudge).
        delta_boost = 1.20
        spread_boost = 1.00
        w_spread_eff = w_res * (self._w_spread * spread_boost)
        w_delta_eff = w_res * (self._w_delta * delta_boost)
        norm = max(1e-9, w_spread_eff + w_delta_eff)
        w_spread_eff /= norm
        w_delta_eff /= norm
        w_spread_eff *= w_res
        w_delta_eff *= w_res

        raw_score = (
            w_spread_eff * spread_score
            + w_delta_eff * delta_score
            + w_imb * imbalance_score
            + w_trend * trend_score
        )

        score = float(raw_score)

        # Subtractive penalties (soft)
        if spread > max_spread:
            score -= self._penalty_wide_spread
        if abs(delta) < delta_thr:
            score -= self._penalty_low_delta

        score = _clamp(score, -2.0, 2.0)

        decision = "NONE"
        reason = "score_below_threshold"
        # Noise filters (spread/imbalance) + confirmation by recent mid move.
        # We ignore spread==0 (crossed/locked) and very wide spreads.
        imb = float(imbalance)
        strong_momentum = abs(delta) >= (delta_thr_eff * 1.3) and abs(imb) >= 0.60 and score >= (score_thr * 0.70)
        # STRONG SIGNAL BOOST (SRM6): explicit force-entry path
        force_entry = abs(delta) >= float(self._force_entry_delta_abs) and abs(imb) >= float(self._force_entry_imb_abs)
        override = bool(strong_momentum or force_entry)
        if override:
            self.logger.debug(
                "[MM][OVERRIDE] symbol=%s delta=%.2f delta_thr=%.2f imb=%.3f score=%.3f score_thr=%.3f spread=%.2f",
                self.symbol,
                float(delta),
                float(delta_thr),
                float(imb),
                float(score),
                float(score_thr),
                float(spread),
            )

        # Temporary debug hook: should_trigger vs actual override.
        if abs(delta) > 100.0 and abs(imb) > 0.8:
            self.logger.debug(
                "[MM][OVERRIDE_CHECK] symbol=%s should_trigger=%s delta=%.2f delta_thr=%.2f imb=%.3f spread=%.2f score=%.3f score_thr=%.3f override=%s",
                self.symbol,
                bool(strong_momentum),
                float(delta),
                float(delta_thr),
                float(imb),
                float(spread),
                float(score),
                float(score_thr),
                bool(override),
            )
        # Debug the score gate: ensures we compare exactly what we log.
        if abs(delta) > 100.0 or abs(imbalance) > 0.8 or self._fallback_adjustments_done >= 6:
            self.logger.debug(
                "[MM][SCORE_CHECK] symbol=%s raw_score=%.6f final_score=%.6f score_thr=%.6f used_score=final override=%s spread=%.2f max_spread=%.2f delta=%.2f delta_thr=%.2f imb=%.3f trend=%.2f fb=%s/%s",
                self.symbol,
                float(raw_score),
                float(score),
                float(score_thr),
                bool(override),
                float(spread),
                float(max_spread),
                float(delta),
                float(delta_thr),
                float(imbalance),
                float(trend_ticks),
                str(self._fallback_adjustments_done),
                str(self._fallback_max_adjustments),
            )

        # Spread gate: strong momentum override can bypass spread skips.
        # Hard entry quality gates (new stricter mode):
        # - score >= 0.85
        # - abs_imbalance >= 0.70
        # Hard score gate should stay stricter than score_thr, but not reject
        # near-strong impulses (e.g. score ~0.84 in AGGRESSIVE mode).
        hard_score_floor = 0.80 if self._mode == "AGGRESSIVE" else 0.85
        score_gate = float(score) >= hard_score_floor
        # FIX IMBALANCE HARD GATE (SRM6)
        # Allow entry if abs(imb) >= IMB_HARD_GATE OR strong delta impulse.
        imb_gate = abs(imb) >= float(self._imb_hard_gate) or abs(delta) >= 5.0

        # Dynamic spread limit: p90(last spreads) (fallback to config early in session).
        dyn_spread_limit = self._entry_spread_limit_dynamic()

        spread_ok = False
        # Treat spread==0 as a valid tight market (some feeds report locked books as 0 ticks).
        # We still keep `min_entry_spread` for filtering out invalid / missing values (<0 or tiny).
        if spread < 0.0 or spread < self._min_entry_spread:
            spread_ok = bool(override and spread >= 0.0)
        elif spread <= 1.0:
            spread_ok = True
        elif spread <= 2.0:
            cfg_delta_thr = float(self._cfg_delta_threshold)
            # Allow spread=2 on strong impulse even with moderate imbalance.
            spread_ok = (
                (abs(imbalance) >= 0.60 and abs(delta) >= max(cfg_delta_thr, 1e-12))
                or (abs(delta) >= max(cfg_delta_thr * 1.20, 6.0) and score >= 0.80)
            )
        elif self._mode == "AGGRESSIVE" and spread <= 3.0:
            spread_ok = abs(imbalance) >= 0.75 and abs(delta) >= (delta_thr_eff * 1.5)
        elif self._mode == "AGGRESSIVE" and spread <= 6.0:
            # Wider-spread entries allowed only on strong impulse (user request: allow 3..6).
            spread_ok = abs(imbalance) >= 0.80 and abs(delta) >= (delta_thr_eff * 1.7) and score >= (score_thr * 0.95)
        elif spread > 6.0:
            spread_ok = False
        elif spread > dyn_spread_limit:
            spread_ok = bool(override)
        else:
            spread_ok = True

        if not spread_ok:
            decision = "NONE"
            reason = "bad_spread_for_entry"
        elif abs(imb) < self._min_abs_imbalance:
            decision = "NONE"
            reason = "imbalance_too_small"
        elif not imb_gate and not override:
            decision = "NONE"
            reason = "imbalance_below_hard_gate"
        elif not score_gate and not override:
            decision = "NONE"
            reason = "score_below_hard_gate"
        elif score < score_thr and not override:
            decision = "NONE"
            reason = "score_below_threshold"
        else:
            # Debug the score gate: ensures we compare exactly what we log.
            if score < score_thr and override:
                self.logger.info(
                    "[MM][SCORE_OVERRIDE] symbol=%s strong_momentum=True delta=%.2f delta_thr=%.2f imb=%.3f score=%.3f score_thr=%.3f spread=%.2f",
                    self.symbol,
                    float(delta),
                    float(delta_thr),
                    float(imbalance),
                    float(score),
                    float(score_thr),
                    float(spread),
                )
            # Direction: imbalance + delta must align, and mid must have moved in that direction.
            dir_imb = _sign(imbalance)
            dir_delta = _sign(delta)
            dir_trend = _sign(trend_ticks) if abs(trend_ticks) >= self._min_trend_move_ticks else 0
            strong_delta = abs(delta) >= (delta_thr_eff * 1.5)
            # Conservative anti-miss path: allow trendless entry only for very strong impulse.
            # This specifically targets frequent direction_not_confirmed with high-quality signal.
            trendless_impulse_ok = (
                abs(delta) >= (delta_thr_eff * 1.8)
                and abs(imbalance) >= 0.80
                and spread <= 3.0
                and score >= (score_thr * 0.85)
            )
            direction_relax_ok = abs(imbalance) >= 0.70 and score >= (score_thr * 0.80)

            if dir_imb > 0 and imbalance >= self._imbalance_threshold_long and dir_delta > 0 and (dir_trend > 0 or strong_delta or direction_relax_ok):
                decision = "LONG"
                reason = "score_pass_confirmed"
            elif dir_imb < 0 and imbalance <= -self._imbalance_threshold_short and dir_delta < 0 and (dir_trend < 0 or strong_delta or direction_relax_ok):
                decision = "SHORT"
                reason = "score_pass_confirmed"
            elif trendless_impulse_ok and dir_imb > 0 and dir_delta > 0:
                decision = "LONG"
                reason = "trendless_impulse_confirmed"
            elif trendless_impulse_ok and dir_imb < 0 and dir_delta < 0:
                decision = "SHORT"
                reason = "trendless_impulse_confirmed"
            else:
                decision = "NONE"
                reason = "direction_not_confirmed"

        if decision == "NONE":
            self.metrics.bump_skip(reason)
            candidate_side = "BUY" if delta > 0 else ("SELL" if delta < 0 else "FLAT")
            if candidate_side in {"BUY", "SELL"}:
                self.metrics.bump_side(candidate_side, passed=False)
            # Near-entry logging: signal almost passed the score gate, but got filtered.
            if score >= (float(score_thr) * 0.80):
                self.logger.info(
                    "[MM][NEAR_ENTRY] symbol=%s reason=%s score=%.3f/%.3f spread=%.2f imb=%.3f delta=%.2f trend=%.2f",
                    self.symbol,
                    reason,
                    float(score),
                    float(score_thr),
                    float(spread),
                    float(imbalance),
                    float(delta),
                    float(trend_ticks),
                )
        elif decision in {"LONG", "SHORT"}:
            self.metrics.bump_side("BUY" if decision == "LONG" else "SELL", passed=True)
        row: dict[str, float | str] = {
            "symbol": self.symbol,
            "mode": self._mode,
            "decision": decision,
            "reason": reason,
            "score": float(score),
            "score_thr": float(score_thr),
            "cfg_score_thr": float(self._cfg_score_threshold),
            "spread": float(spread),
            "max_spread": float(max_spread),
            "entry_spread_limit_dynamic": float(dyn_spread_limit),
            "mid": float(mid_price),
            "delta": float(delta),
            "delta_score": float(delta_score),
            "imbalance": float(imbalance),
            "trend_ticks": float(trend_ticks),
            "trend_score": float(trend_score),
            "imb_score": float(imbalance_score),
            "override_score": bool(override),
            # deficits ("what missing")
            "need_spread": float(max(0.0, spread - dyn_spread_limit)),
            "need_delta": float(max(0.0, delta_thr_eff - abs(delta))),
            "delta_thr": float(delta_thr_eff),
            "cfg_delta_thr": float(self._cfg_delta_threshold),
            "fallback_step": str(self._fallback_adjustments_done),
            "fallback_max": str(self._fallback_max_adjustments),
        }
        return decision, row

    def _compute_score_components(self, market_snapshot: dict[str, float]) -> tuple[float, float, float, float, float]:
        """
        Pure scoring helper (no metrics, no fallback side effects).
        Returns: (score, delta_score, spread, max_spread, mid_price)
        """
        spread = float(market_snapshot.get("spread", 0.0))
        delta = float(market_snapshot.get("delta", 0.0))
        mid_price = float(market_snapshot.get("mid_price", 0.0))

        max_spread = self._effective_max_spread()
        delta_thr = float(self._base_delta_threshold)
        if delta_thr <= 0.0:
            delta_thr = 1e-6

        spread_score = _clamp(1.0 - (spread / max_spread), 0.0, 1.0)
        delta_score = min(1.0, abs(delta) / max(delta_thr, 1e-6))
        score = self._w_spread * spread_score + self._w_delta * delta_score

        if spread > max_spread:
            score -= self._penalty_wide_spread
        if abs(delta) < delta_thr:
            score -= self._penalty_low_delta
        score = _clamp(score, -2.0, 2.0)
        return float(score), float(delta_score), float(spread), float(max_spread), float(mid_price)

    def _log_decision(self, row: dict[str, float | str]) -> None:
        self.logger.info(
            "[MM][ENTRY_DECISION] symbol=%s mode=%s decision=%s reason=%s score=%.3f/%.3f(c%.3f) spread=%.2f/%.2f mid=%.4f delta=%.2f/%.2f(c%.2f) delta_score=%.3f imb=%.3f trend=%.2f fb=%s/%s need(delta=%.2f spread=%.2f)",
            str(row.get("symbol", "")),
            str(row.get("mode", "")),
            str(row.get("decision", "")),
            str(row.get("reason", "")),
            float(row.get("score", 0.0)),
            float(row.get("score_thr", 0.0)),
            float(row.get("cfg_score_thr", 0.0)),
            float(row.get("spread", 0.0)),
            float(row.get("max_spread", 0.0)),
            float(row.get("mid", 0.0)),
            float(row.get("delta", 0.0)),
            float(row.get("delta_thr", 0.0)),
            float(row.get("cfg_delta_thr", 0.0)),
            float(row.get("delta_score", 0.0)),
            float(row.get("imbalance", 0.0)),
            float(row.get("trend_ticks", 0.0)),
            str(row.get("fallback_step", "")),
            str(row.get("fallback_max", "")),
            float(row.get("need_delta", 0.0)),
            float(row.get("need_spread", 0.0)),
        )
        if self.metrics.signals_total % self._metrics_log_every == 0:
            top = sorted(self.metrics.skip_reason_counter.items(), key=lambda kv: kv[1], reverse=True)[:5]
            self.logger.info(
                "[MM][SIGNALS] total=%s skipped=%s executed=%s top_skips=%s side_pass=%s side_skip=%s",
                self.metrics.signals_total,
                self.metrics.signals_skipped,
                self.metrics.signals_executed,
                top,
                dict(self.metrics.side_pass_counter),
                dict(self.metrics.side_skip_counter),
            )

    def _entry_qty(self) -> float:
        # Default: configured lot size (legacy behavior).
        base = max(0.0, float(self.lot_size)) * float(self._size_risk_multiplier)
        if not self._position_sizing_enabled:
            return base
        if self._sizing_risk_per_trade_abs <= 0.0:
            return base
        # Very simple risk model: worst-case loss per 1 qty ~= stop_loss_ticks * tick_size.
        risk_per_unit = self._sizing_stop_loss_ticks * self._tick_size
        if risk_per_unit <= 0:
            return base
        raw_qty = math.floor(self._sizing_risk_per_trade_abs / risk_per_unit)
        qty = float(raw_qty)
        qty = _clamp(qty, self._sizing_min_qty, self._sizing_max_qty)
        # If clamp makes it 0, fall back to base.
        return qty if qty > 0 else base

    def _adaptive_sl_tp_ticks(self) -> tuple[float, float]:
        """
        Adaptive SL/TP based on recent realized outcomes (in ticks).
        - stop_loss: at least avg loss magnitude of last ~20 closed trades (clamped)
        - take_profit: keep configured TP as baseline (dynamic TP handled in exit)
        """
        window = list(self._recent_trade_pnl_ticks)[-20:]
        losses = [abs(x) for x in window if x < 0]
        if len(losses) < 5:
            return float(self._sl_ticks), float(self._tp_ticks)
        avg_loss = float(mean(losses))
        sl = _clamp(max(float(self._sl_ticks), avg_loss), 1.0, 12.0)
        tp = float(self._tp_ticks)
        return float(sl), float(tp)

    def _send_aggressive_entry(self, *, decision: str, data: MarketData, now_mono: float) -> None:
        if self._one_position_only and abs(self._position_qty) > 1e-12:
            return
        if self._order_reject_pause_until_mono > 0.0 and now_mono < self._order_reject_pause_until_mono:
            return

        # Capture delta snapshot for reversal-based exits (best-effort; fills are near-immediate in sim).
        try:
            now_ms = now_mono * 1000.0
            snapshot = self._compute_features(data=data, now_ms=now_ms)
            self._pending_entry_delta = float(snapshot.get("delta", 0.0))
            self._pending_entry_imbalance = float(snapshot.get("imbalance", 0.0))
            entry_imb = float(snapshot.get("imbalance", 0.0))
            entry_trend = self._trend_move_ticks(now_ms=now_ms, mid=float(snapshot.get("mid_price", 0.0)))
            entry_sign = 1 if decision == "LONG" else -1
            self._pending_entry_flow_strength = max(
                1e-6,
                float(
                    self._compute_flow_strength(
                        delta=float(self._pending_entry_delta),
                        imbalance=entry_imb,
                        trend_ticks=entry_trend,
                        pos_sign=entry_sign,
                    )
                ),
            )
        except Exception:
            self._pending_entry_delta = 0.0
            self._pending_entry_imbalance = 0.0
            self._pending_entry_flow_strength = 1.0

        side = "1" if decision == "LONG" else "2"
        price = float(data.ask) if side == "1" else float(data.bid)
        self._entry_expected_price = float(price)
        qty = self._entry_qty()
        if qty <= 0:
            return
        req = OrderRequest(
            symbol=self.symbol,
            side=side,
            qty=qty,
            account="",
            price=price,
            market=self.market,
            lot_size=1,
            bypass_risk=False,
        )
        try:
            cl_ord_id = self.gateway.send_order(req)
        except Exception as exc:
            # Do not let broker errors bubble to market-data feed (it will log "[TBANK][MD] on_raw_market_data error").
            msg = str(exc)
            # Known T-Invest reject: 30051 "Account margin status is disabled" (FORTS/margin not enabled).
            is_margin_disabled = ("30051" in msg) or ("margin status is disabled" in msg.lower())
            if is_margin_disabled:
                self._order_reject_pause_until_mono = float(now_mono + self._order_reject_pause_sec)
            # Throttle error log to avoid storms.
            if (now_mono - self._last_order_reject_log_mono) >= 1.0:
                self._last_order_reject_log_mono = float(now_mono)
                pause_left = max(0.0, float(self._order_reject_pause_until_mono - now_mono))
                self.logger.error(
                    "[MM][ENTRY_REJECT] symbol=%s side=%s qty=%s px=%.4f market=%s err=%s pause_left_sec=%.1f",
                    self.symbol,
                    side,
                    qty,
                    price,
                    self.market.value,
                    msg,
                    pause_left,
                )
            return
        self._last_entry_place_monotonic = now_mono
        self._last_signal_execute_mono = now_mono
        self.metrics.signals_executed += 1
        self.logger.info(
            "[MM][ENTRY_EXECUTE] symbol=%s side=%s qty=%s px=%.4f cl_ord_id=%s",
            self.symbol,
            side,
            qty,
            price,
            cl_ord_id,
        )

    def _maybe_fast_invalidate(self, *, data: MarketData, now_mono: float) -> None:
        if self._entry_monotonic <= 0.0 or self._immediate_ticks_seen >= 1:
            return

        mid = float(data.mid_price)
        if self._entry_mid_price <= 0.0:
            self._entry_mid_price = mid
            self._immediate_ticks_seen = 1
            return

        adverse = (self._entry_mid_price - mid) / self._tick_size if self._position_qty > 0 else (mid - self._entry_mid_price) / self._tick_size
        self._immediate_ticks_seen = 1
        if adverse >= 1.0:
            self._force_exit(data=data, now_mono=now_mono, reason="fast_invalidation")

    def _force_exit(self, *, data: MarketData, now_mono: float, reason: str) -> None:
        # Kept for fast-invalidation; delegates to unified exit sender.
        self._send_exit_order(data=data, now_mono=now_mono, reason=reason, bypass_risk=True, qty_override=None)

    def _maybe_exit_position(self, *, data: MarketData, now_mono: float, now_ms: float) -> None:
        if abs(self._position_qty) <= 1e-12:
            return
        # Avoid sending duplicate exit orders while an exit order is still open.
        # A short throttle alone is not enough: exits can remain open for seconds and
        # repeated sends may over-close / flip the position.
        if self._exit_in_flight:
            try:
                clid = str(self._exit_cl_ord_id or "")
                if clid:
                    order = self.gateway.order_manager.get_order(clid)
                    if order is not None and order.status not in {"FILLED", "CANCELED", "REJECTED"}:
                        return
            except Exception:
                # If we cannot introspect order state, fall back to a conservative throttle.
                pass
            if (now_mono - self._exit_last_send_mono) < 1.0:
                return

        snapshot = self._compute_features(data=data, now_ms=now_ms)
        score, delta_score, spread, _max_spread, mid = self._compute_score_components(snapshot)
        holding = (now_mono - self._entry_monotonic) if self._entry_monotonic > 0.0 else 0.0
        imbalance = float(snapshot.get("imbalance", 0.0))
        delta = float(snapshot.get("delta", 0.0))
        trend_ticks = self._trend_move_ticks(now_ms=now_ms, mid=float(mid))
        flow_strength_now = float(
            self._compute_flow_strength(
                delta=float(delta),
                imbalance=float(imbalance),
                trend_ticks=float(trend_ticks),
                pos_sign=int(self._entry_sign or (1 if self._position_qty > 0 else -1)),
            )
        )
        flow_decay = _safe_div(float(flow_strength_now), max(float(self._entry_flow_strength), 1e-6), default=1.0)

        # Unrealized in ticks based on mid (mark)
        if abs(self._entry_price) > 1e-12 and self._entry_sign != 0:
            pnl_ticks = ((mid - self._entry_price) / self._tick_size) * float(self._entry_sign)
        else:
            pnl_ticks = 0.0
        self._best_favorable_ticks = max(float(self._best_favorable_ticks), float(pnl_ticks))
        self._worst_adverse_ticks = min(float(self._worst_adverse_ticks), float(pnl_ticks))
        self._unrealized_pnl_ticks_hist.append(float(pnl_ticks))
        self._bars_in_position += 1
        if self._tp_armed:
            self._tp_max_pnl_ticks = max(float(self._tp_max_pnl_ticks), float(pnl_ticks))
        if self._max_mid_since_entry <= 0.0 and self._min_mid_since_entry <= 0.0:
            self._max_mid_since_entry = float(mid)
            self._min_mid_since_entry = float(mid)
        else:
            self._max_mid_since_entry = max(float(self._max_mid_since_entry), float(mid))
            self._min_mid_since_entry = min(float(self._min_mid_since_entry), float(mid))

        age_ms = float(max(0.0, now_mono - float(self._entry_monotonic))) * 1000.0
        pos_sign = 1 if self._position_qty > 0 else -1
        decision = self.exit_decision(
            now_mono=now_mono,
            age_ms=age_ms,
            bars_in_position=int(self._bars_in_position),
            pos_sign=int(pos_sign),
            pnl_ticks=float(pnl_ticks),
            max_pnl_since_entry=float(self._best_favorable_ticks),
            delta=float(delta),
            imbalance=float(imbalance),
            flow_decay=float(flow_decay),
        )
        if decision is None:
            return
        reason, qty_override = decision

        self.logger.debug(
            "[MM][EXIT_DEBUG] symbol=%s reason=%s pnl_ticks=%.2f max_pnl=%.2f delta=%.2f imb=%.3f flow_decay=%.3f bars=%s",
            self.symbol,
            reason,
            float(pnl_ticks),
            float(self._best_favorable_ticks),
            float(delta),
            float(imbalance),
            float(flow_decay),
            self._bars_in_position,
        )
        self._send_exit_order(data=data, now_mono=now_mono, reason=reason, bypass_risk=True, qty_override=qty_override)

    def _send_exit_order(
        self,
        *,
        data: MarketData,
        now_mono: float,
        reason: str,
        bypass_risk: bool,
        qty_override: float | None,
    ) -> None:
        qty = float(qty_override) if qty_override is not None else abs(self._position_qty)
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
            bypass_risk=bool(bypass_risk),
        )
        # IMPORTANT: in simulation, `send_order()` can synchronously emit execution reports
        # (including immediate fills). Set exit state BEFORE sending so `on_execution_report`
        # sees a consistent `_exit_reason` / `_exit_in_flight`.
        self._exit_reason = str(reason or "")

        cl_ord_id = self.gateway.send_order(req)
        self._last_entry_place_monotonic = now_mono
        self._exit_in_flight = True
        self._exit_last_send_mono = now_mono
        self._exit_cl_ord_id = str(cl_ord_id or "")
        self.logger.info(
            "[MM][EXIT] symbol=%s reason=%s side=%s qty=%.4f px=%.4f cl_ord_id=%s",
            self.symbol,
            reason,
            side,
            qty,
            price,
            cl_ord_id,
        )

    def exit_decision(
        self,
        *,
        now_mono: float,
        age_ms: float,
        bars_in_position: int,
        pos_sign: int,
        pnl_ticks: float,
        max_pnl_since_entry: float,
        delta: float,
        imbalance: float,
        flow_decay: float,
    ) -> tuple[str, float | None] | None:
        """
        Strict, fee-aware exit engine (entry logic is untouched).

        Returns:
          - None -> hold
          - (reason, qty_override) -> exit now (qty_override None == close full position)
        """
        # Parameters (recommended for config)
        sl = max(0.0, float(self._sl_ticks))
        min_hold_bars = 3
        min_profit_ticks = 2.0
        no_trailing_before_pnl = 2.0
        reversal_enable_from_max_pnl = 5.0
        trailing_enable_from_max_pnl = 2.0
        max_loss_mult = 1.5

        # 1) Hard stop policy (no delay, no softening)
        max_loss = float(sl) * float(max_loss_mult)
        if max_loss > 0.0 and float(pnl_ticks) <= -float(max_loss):
            return "stop_loss_max_loss", None
        # Soft SL: avoid instant stop-outs on micro-noise right after entry.
        # Allow SL once position has aged a bit (bars or time), but keep max-loss immediate above.
        allow_soft_sl = bool(
            (int(bars_in_position) >= int(self._sl_min_hold_bars))
            or (float(age_ms) >= float(self._sl_min_age_ms))
        )
        if sl > 0.0 and float(pnl_ticks) <= -float(sl) and allow_soft_sl:
            return "stop_loss", None

        # 2) Minimum hold
        if int(bars_in_position) < int(min_hold_bars):
            return None

        # 3) Ban micro-profit exits (except stops above)
        if float(pnl_ticks) < float(min_profit_ticks):
            return None

        # 4) Ban early trailing / reversal while pnl < 3
        if float(pnl_ticks) < float(no_trailing_before_pnl):
            return None

        # 5) Strong reversal allowed only after trade had enough profit
        #    and while current pnl is not tiny.
        if float(max_pnl_since_entry) >= float(reversal_enable_from_max_pnl):
            strong_reversal = bool(
                (_sign(delta) == -int(pos_sign))
                and (_sign(imbalance) == -int(pos_sign))
                and (abs(float(delta)) >= float(self._rev_min_abs_delta))
                and (abs(float(imbalance)) >= float(self._rev_min_abs_imb))
                and (abs(float(delta)) >= float(self._strong_rev_threshold) * max(abs(float(self._entry_delta_open)), 1e-9))
            )
            if strong_reversal:
                return "strong_reversal_exit", None

        # 6) Trailing profit: start earlier, become more aggressive later.
        # - if max_pnl > 2: trailing enabled
        # - if max_pnl > 4: aggressive trailing
        max_pnl = float(max_pnl_since_entry)
        if max_pnl >= float(trailing_enable_from_max_pnl):
            gap = 1.0 if max_pnl >= 4.0 else 2.0
            lock = max_pnl - gap
            if float(pnl_ticks) <= float(lock):
                return ("profit_trailing_aggressive" if gap <= 1.0 else "profit_trailing"), None

        _ = now_mono
        _ = age_ms
        _ = flow_decay
        return None

