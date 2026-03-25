from __future__ import annotations

import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field

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


@dataclass
class SignalMetrics:
    signals_total: int = 0
    signals_skipped: int = 0
    signals_executed: int = 0
    skip_reason_counter: dict[str, int] = field(default_factory=dict)

    def bump_skip(self, reason: str) -> None:
        self.signals_skipped += 1
        self.skip_reason_counter[reason] = self.skip_reason_counter.get(reason, 0) + 1


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
        trailing_start_ticks: float = 2.0,
        trailing_gap_ticks: float = 1.0,
        exit_imbalance_reversal: float = 0.2,
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
        self._fallback_max_adjustments = 12
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
        self._exit_in_flight = False
        self._exit_last_send_mono = 0.0
        self._exit_cl_ord_id = ""
        self._exit_reason = ""

        self._warned_zero_delta_threshold = False

        # Minimal PnL / winrate stats (realized on close)
        self._realized_pnl = 0.0
        self._trades_closed = 0
        self._wins = 0

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
        self._trail_start = max(0.0, float(trailing_start_ticks))
        self._trail_gap = max(0.0, float(trailing_gap_ticks))
        self._exit_imbalance_reversal = _clamp(float(exit_imbalance_reversal), 0.0, 1.0)

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

        prev_qty = float(self._position_qty)
        if side == "1":
            self._position_qty = prev_qty + last_qty
        else:
            self._position_qty = prev_qty - last_qty

        if abs(self._entry_price) < 1e-12 and abs(self._position_qty) > 1e-12:
            self._entry_price = fill_px
            self._entry_monotonic = time.perf_counter()
            self._entry_cl_ord_id = str(state.get("cl_ord_id", "") or "")
            self._immediate_ticks_seen = 0
            self._entry_mid_price = 0.0
            self._entry_qty_abs = abs(self._position_qty)
            self._entry_sign = 1 if self._position_qty > 0 else -1
            self._best_favorable_ticks = 0.0
            self._exit_in_flight = False
            self._exit_last_send_mono = 0.0
            self._exit_cl_ord_id = ""
            self._exit_reason = ""

        if abs(self._position_qty) < 1e-12 and abs(prev_qty) > 1e-12:
            self._position_qty = 0.0
            # Realize PnL on close using last fill price.
            if abs(self._entry_price) > 1e-12 and self._entry_sign != 0:
                qty = max(0.0, float(self._entry_qty_abs))
                pnl = (fill_px - self._entry_price) * float(self._entry_sign) * qty
                self._realized_pnl += pnl
                self._trades_closed += 1
                if pnl > 0:
                    self._wins += 1
                winrate = (self._wins / self._trades_closed) if self._trades_closed > 0 else 0.0
                exit_side = "2" if prev_qty > 0 else "1"
                self.logger.info(
                    "[MM][TRADE_CLOSE] symbol=%s order_id=%s side=%s qty=%.4f exit_price=%.4f realized_pnl=%.4f timestamp=%s realized_total=%.4f trades=%s winrate=%.2f%%",
                    self.symbol,
                    self._entry_cl_ord_id or str(state.get("cl_ord_id", "") or ""),
                    exit_side,
                    qty,
                    fill_px,
                    pnl,
                    float(time.time()),
                    self._realized_pnl,
                    self._trades_closed,
                    100.0 * winrate,
                )
                if self._risk_manager is not None:
                    self._risk_manager.on_trade_result(net_pnl=float(pnl))
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
            self._best_favorable_ticks = 0.0
            self._exit_in_flight = False
            self._exit_last_send_mono = 0.0
            self._exit_cl_ord_id = ""
            self._exit_reason = ""

    def on_market_data(self, data: MarketData) -> None:
        if data.symbol.upper() != self.symbol:
            return

        now_mono = time.perf_counter()
        now_ms = now_mono * 1000.0
        mid = float(data.mid_price)
        self._mid_hist.append((now_ms, mid))

        if abs(self._position_qty) > 1e-12:
            self._maybe_fast_invalidate(data=data, now_mono=now_mono)
            self._maybe_exit_position(data=data, now_mono=now_mono, now_ms=now_ms)
            return

        if self._cooldown_sec > 0.0 and self._last_entry_place_monotonic > 0.0:
            if (now_mono - self._last_entry_place_monotonic) < self._cooldown_sec:
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

    def _effective_max_spread(self) -> float:
        max_spread = float(self._base_max_spread)
        if self._dynamic_spread_enabled and len(self._spread_hist) >= 10:
            avg = sum(self._spread_hist) / float(len(self._spread_hist))
            dyn = max(0.1, avg * self._dynamic_spread_mult)
            # keep at least configured max_spread, but allow dyn to increase during wide markets
            max_spread = max(max_spread, dyn)
        return min(max_spread, float(self._fallback_max_spread))

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

        self._base_delta_threshold = max(self._fallback_min_delta, self._base_delta_threshold * self._fallback_relax_step)
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

        spread = float(market_snapshot.get("spread", 0.0))
        delta = float(market_snapshot.get("delta", 0.0))
        mid_price = float(market_snapshot.get("mid_price", 0.0))
        imbalance = float(market_snapshot.get("imbalance", 0.0))

        max_spread = self._effective_max_spread()
        delta_thr = float(self._base_delta_threshold)
        if delta_thr <= 0.0:
            if not self._warned_zero_delta_threshold:
                self._warned_zero_delta_threshold = True
                self.logger.warning(
                    "[MM][CONFIG] delta_threshold<=0 (%.6f). Using eps=1e-6 for scoring; set delta_threshold>=1 in config.",
                    delta_thr,
                )
            delta_thr = 1e-6
        score_thr = float(self._base_score_threshold)

        # Component scores (0..1)
        spread_score = _clamp(1.0 - (spread / max_spread), 0.0, 1.0)
        delta_score = min(1.0, abs(delta) / max(delta_thr, 1e-6))

        score = self._w_spread * spread_score + self._w_delta * delta_score

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
        trend_ticks = self._trend_move_ticks(now_ms=(now_mono * 1000.0), mid=mid_price)
        if spread <= 0.0 or spread < self._min_entry_spread or spread > self._max_entry_spread:
            decision = "NONE"
            reason = "bad_spread_for_entry"
        elif abs(imbalance) < self._min_abs_imbalance:
            decision = "NONE"
            reason = "imbalance_too_small"
        elif score < score_thr:
            decision = "NONE"
            reason = "score_below_threshold"
        else:
            # Direction: imbalance + delta must align, and mid must have moved in that direction.
            dir_imb = _sign(imbalance)
            dir_delta = _sign(delta)
            dir_trend = _sign(trend_ticks) if abs(trend_ticks) >= self._min_trend_move_ticks else 0

            if dir_imb > 0 and imbalance >= self._imbalance_threshold_long and dir_delta > 0 and dir_trend > 0:
                decision = "LONG"
                reason = "score_pass_confirmed"
            elif dir_imb < 0 and imbalance <= -self._imbalance_threshold_short and dir_delta < 0 and dir_trend < 0:
                decision = "SHORT"
                reason = "score_pass_confirmed"
            else:
                decision = "NONE"
                reason = "direction_not_confirmed"

        if decision == "NONE":
            self.metrics.bump_skip(reason)
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
            "mid": float(mid_price),
            "delta": float(delta),
            "delta_score": float(delta_score),
            "imbalance": float(imbalance),
            "trend_ticks": float(trend_ticks),
            # deficits ("what missing")
            "need_spread": float(max(0.0, spread - max_spread)),
            "need_delta": float(max(0.0, delta_thr - abs(delta))),
            "delta_thr": float(delta_thr),
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
                "[MM][SIGNALS] total=%s skipped=%s executed=%s top_skips=%s",
                self.metrics.signals_total,
                self.metrics.signals_skipped,
                self.metrics.signals_executed,
                top,
            )

    def _entry_qty(self) -> float:
        # Default: configured lot size (legacy behavior).
        base = max(0.0, float(self.lot_size))
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

    def _send_aggressive_entry(self, *, decision: str, data: MarketData, now_mono: float) -> None:
        if self._one_position_only and abs(self._position_qty) > 1e-12:
            return

        side = "1" if decision == "LONG" else "2"
        price = float(data.ask) if side == "1" else float(data.bid)
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
        cl_ord_id = self.gateway.send_order(req)
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
        self._send_exit_order(data=data, now_mono=now_mono, reason=reason, bypass_risk=True)

    def _maybe_exit_position(self, *, data: MarketData, now_mono: float, now_ms: float) -> None:
        if abs(self._position_qty) <= 1e-12:
            return
        if self._exit_in_flight and (now_mono - self._exit_last_send_mono) < 0.25:
            return

        snapshot = self._compute_features(data=data, now_ms=now_ms)
        score, delta_score, spread, _max_spread, mid = self._compute_score_components(snapshot)
        holding = (now_mono - self._entry_monotonic) if self._entry_monotonic > 0.0 else 0.0
        imbalance = float(snapshot.get("imbalance", 0.0))

        # Unrealized in ticks based on mid (mark)
        if abs(self._entry_price) > 1e-12 and self._entry_sign != 0:
            pnl_ticks = ((mid - self._entry_price) / self._tick_size) * float(self._entry_sign)
        else:
            pnl_ticks = 0.0
        self._best_favorable_ticks = max(float(self._best_favorable_ticks), float(pnl_ticks))

        reason = ""
        # Hard exits: TP/SL
        if not reason and self._tp_ticks > 0.0 and pnl_ticks >= self._tp_ticks:
            reason = "take_profit"
        if not reason and self._sl_ticks > 0.0 and pnl_ticks <= -self._sl_ticks:
            reason = "stop_loss"
        # Trailing: once in profit >= start, exit on retrace >= gap
        if (
            not reason
            and self._trail_start > 0.0
            and self._trail_gap > 0.0
            and float(self._best_favorable_ticks) >= self._trail_start
        ):
            retrace = float(self._best_favorable_ticks) - float(pnl_ticks)
            if retrace >= self._trail_gap:
                reason = "trailing_stop"
        # Exit on strong reversal of imbalance
        if not reason and self._exit_imbalance_reversal > 0.0:
            if self._position_qty > 0 and imbalance <= -self._exit_imbalance_reversal:
                reason = "imbalance_reversal"
            elif self._position_qty < 0 and imbalance >= self._exit_imbalance_reversal:
                reason = "imbalance_reversal"
        # Optional: score-based exit disabled by default (caused tiny churn losses)
        if not reason and self._exit_on_score_enabled and score < self._exit_threshold:
            reason = "score_below_exit_threshold"
        if not reason and self._max_holding_sec > 0.0 and holding >= self._max_holding_sec:
            reason = "max_holding_time"
        if not reason and self._max_exit_spread is not None and spread > float(self._max_exit_spread):
            reason = "spread_too_wide"
        if not reason and self._force_time_exit_enabled and self._force_time_exit_after_sec > 0.0 and holding >= self._force_time_exit_after_sec:
            reason = "force_time_exit"

        if not reason:
            return

        self.logger.debug(
            "[MM][EXIT_DEBUG] symbol=%s reason=%s score=%.3f exit_thr=%.3f delta_score=%.3f spread=%.2f mid=%.4f holding_sec=%.3f pos_qty=%.4f pnl_ticks=%.2f best_ticks=%.2f imb=%.3f",
            self.symbol,
            reason,
            float(score),
            float(self._exit_threshold),
            float(delta_score),
            float(spread),
            float(mid),
            float(holding),
            float(self._position_qty),
            float(pnl_ticks),
            float(self._best_favorable_ticks),
            float(imbalance),
        )
        self._send_exit_order(data=data, now_mono=now_mono, reason=reason, bypass_risk=True)

    def _send_exit_order(self, *, data: MarketData, now_mono: float, reason: str, bypass_risk: bool) -> None:
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
            bypass_risk=bool(bypass_risk),
        )
        cl_ord_id = self.gateway.send_order(req)
        self._last_entry_place_monotonic = now_mono
        self._exit_in_flight = True
        self._exit_last_send_mono = now_mono
        self._exit_cl_ord_id = str(cl_ord_id or "")
        self._exit_reason = str(reason or "")
        self.logger.info(
            "[MM][EXIT] symbol=%s reason=%s side=%s qty=%.4f px=%.4f cl_ord_id=%s",
            self.symbol,
            reason,
            side,
            qty,
            price,
            cl_ord_id,
        )

