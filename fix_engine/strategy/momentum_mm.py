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
        imbalance_threshold: float,
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
            base_score_thr = min(base_score_thr, 0.40)
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
        self._immediate_ticks_seen = 0
        self._entry_mid_price = 0.0
        self._entry_qty_abs = 0.0
        self._entry_sign = 0  # +1 long, -1 short

        self._warned_zero_delta_threshold = False

        # Minimal PnL / winrate stats (realized on close)
        self._realized_pnl = 0.0
        self._trades_closed = 0
        self._wins = 0

        # Minimal feature history (for debug/compat; strategy uses single-snapshot)
        self._mid_hist: deque[tuple[float, float]] = deque(maxlen=64)  # (mono_ms, mid)
        self._last_entry_place_monotonic = 0.0
        self._last_signal_execute_mono = 0.0

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

        prev_qty = float(self._position_qty)
        if side == "1":
            self._position_qty = prev_qty + last_qty
        else:
            self._position_qty = prev_qty - last_qty

        if abs(self._entry_price) < 1e-12 and abs(self._position_qty) > 1e-12:
            self._entry_price = fill_px
            self._entry_monotonic = time.perf_counter()
            self._immediate_ticks_seen = 0
            self._entry_mid_price = 0.0
            self._entry_qty_abs = abs(self._position_qty)
            self._entry_sign = 1 if self._position_qty > 0 else -1

        if abs(self._position_qty) < 1e-12 and abs(prev_qty) > 1e-12:
            self._position_qty = 0.0
            # Realize PnL on close using last fill price.
            if abs(self._entry_price) > 1e-12 and self._entry_sign != 0:
                pnl = (fill_px - self._entry_price) * float(self._entry_sign) * max(1.0, float(self._entry_qty_abs))
                self._realized_pnl += pnl
                self._trades_closed += 1
                if pnl > 0:
                    self._wins += 1
                winrate = (self._wins / self._trades_closed) if self._trades_closed > 0 else 0.0
                self.logger.info(
                    "[MM][TRADE_CLOSE] symbol=%s pnl=%.4f realized_total=%.4f trades=%s winrate=%.2f%% exit_px=%.4f entry_px=%.4f",
                    self.symbol,
                    pnl,
                    self._realized_pnl,
                    self._trades_closed,
                    100.0 * winrate,
                    fill_px,
                    self._entry_price,
                )
                if self._risk_manager is not None:
                    self._risk_manager.on_trade_result(net_pnl=float(pnl))
            self._entry_price = 0.0
            self._entry_monotonic = 0.0
            self._immediate_ticks_seen = 0
            self._entry_mid_price = 0.0
            self._entry_qty_abs = 0.0
            self._entry_sign = 0

    def on_market_data(self, data: MarketData) -> None:
        if data.symbol.upper() != self.symbol:
            return

        now_mono = time.perf_counter()
        now_ms = now_mono * 1000.0
        mid = float(data.mid_price)
        self._mid_hist.append((now_ms, mid))

        if abs(self._position_qty) > 1e-12:
            self._maybe_fast_invalidate(data=data, now_mono=now_mono)
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

        return {
            "spread": spread_ticks,
            "mid_price": float(data.mid_price),
            "delta": delta,
        }

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
        if score >= score_thr:
            dir_delta = _sign(delta)
            if dir_delta > 0:
                decision = "LONG"
                reason = "score_pass"
            elif dir_delta < 0:
                decision = "SHORT"
                reason = "score_pass"
            else:
                decision = "NONE"
                reason = "direction_undefined"

        if decision == "NONE":
            self.metrics.bump_skip(reason)
        row: dict[str, float | str] = {
            "symbol": self.symbol,
            "mode": self._mode,
            "decision": decision,
            "reason": reason,
            "score": float(score),
            "score_thr": float(score_thr),
            "spread": float(spread),
            "max_spread": float(max_spread),
            "mid": float(mid_price),
            "delta": float(delta),
            "delta_score": float(delta_score),
            # deficits ("what missing")
            "need_spread": float(max(0.0, spread - max_spread)),
            "need_delta": float(max(0.0, delta_thr - abs(delta))),
            "delta_thr": float(delta_thr),
        }
        return decision, row

    def _log_decision(self, row: dict[str, float | str]) -> None:
        self.logger.info(
            "[MM][ENTRY_DECISION] symbol=%s mode=%s decision=%s reason=%s score=%.3f/%.3f spread=%.2f/%.2f mid=%.4f delta=%.2f/%.2f delta_score=%.3f need(delta=%.2f spread=%.2f)",
            str(row.get("symbol", "")),
            str(row.get("mode", "")),
            str(row.get("decision", "")),
            str(row.get("reason", "")),
            float(row.get("score", 0.0)),
            float(row.get("score_thr", 0.0)),
            float(row.get("spread", 0.0)),
            float(row.get("max_spread", 0.0)),
            float(row.get("mid", 0.0)),
            float(row.get("delta", 0.0)),
            float(row.get("delta_thr", 0.0)),
            float(row.get("delta_score", 0.0)),
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

