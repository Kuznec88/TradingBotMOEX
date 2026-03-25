from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field

from fix_engine.execution_gateway import ExecutionGateway
from fix_engine.market_data.models import MarketData
from fix_engine.order_models import MarketType, OrderRequest
from fix_engine.position_manager import PositionManager


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
        w_move: float = 0.40,
        w_ndelta: float = 0.30,
        no_trade_fallback_minutes: float = 3.0,
        fallback_relax_step: float = 0.85,
        fallback_min_move_ticks: float = 0.75,
        fallback_min_ndelta: float = 0.015,
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
        base_move_thr = float(move_threshold)
        base_ndelta_thr = float(normalized_delta_threshold)
        base_score_thr = float(score_threshold)

        if self._mode == "AGGRESSIVE":
            base_max_spread = max(base_max_spread, 6.0)
            base_move_thr = min(base_move_thr, 1.0) if base_move_thr > 0 else 1.0
            base_ndelta_thr = min(base_ndelta_thr, 0.02)
            base_score_thr = min(base_score_thr, 0.55)
        elif self._mode == "NORMAL":
            base_max_spread = max(base_max_spread, 5.0)
            base_move_thr = min(base_move_thr, 1.5) if base_move_thr > 0 else 1.5
            base_ndelta_thr = min(base_ndelta_thr, 0.03)
            base_score_thr = min(base_score_thr, 0.65)
        else:  # SAFE
            base_max_spread = max(base_max_spread, 5.0)  # requested default
            base_move_thr = max(base_move_thr, 2.0) if base_move_thr > 0 else 2.0
            base_ndelta_thr = max(base_ndelta_thr, 0.03)
            base_score_thr = max(base_score_thr, 0.72)

        self._base_max_spread = max(0.1, float(base_max_spread))
        self._base_move_threshold = max(0.0, float(base_move_thr))
        self._base_velocity_threshold = max(0.0, float(velocity_threshold))
        self._base_delta_threshold = max(0.0, float(delta_threshold))
        self._base_imbalance_threshold = max(0.0, float(imbalance_threshold))
        self._base_normalized_delta_threshold = max(0.0, float(base_ndelta_thr))
        self._base_score_threshold = max(0.0, float(base_score_thr))

        # Scoring weights
        wsum = max(1e-9, float(w_spread) + float(w_move) + float(w_ndelta))
        self._w_spread = float(w_spread) / wsum
        self._w_move = float(w_move) / wsum
        self._w_ndelta = float(w_ndelta) / wsum

        # Dynamic spread
        self._dynamic_spread_enabled = bool(dynamic_spread_enabled)
        self._spread_hist: deque[float] = deque(maxlen=max(10, int(dynamic_spread_window_ticks)))
        self._dynamic_spread_mult = max(1.0, float(dynamic_spread_mult))

        # Fallback relaxation
        self._no_trade_fallback_sec = max(0.0, float(no_trade_fallback_minutes) * 60.0)
        self._fallback_relax_step = _clamp(float(fallback_relax_step), 0.50, 0.99)
        self._fallback_min_move_ticks = max(0.0, float(fallback_min_move_ticks))
        self._fallback_min_ndelta = max(0.0, float(fallback_min_ndelta))
        self._fallback_max_spread = max(self._base_max_spread, float(fallback_max_spread))
        self._last_fallback_adjust_mono = 0.0
        self._fallback_adjust_cooldown_sec = 30.0

        # Metrics
        self.metrics = SignalMetrics()
        self._metrics_log_every = max(10, int(metrics_log_every))

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

        if side == "1":
            self._position_qty += last_qty
        else:
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
        velocity = move_ticks / dt_ms

        bid_sz = float(getattr(data, "bid_size", 0.0) or 0.0)
        ask_sz = float(getattr(data, "ask_size", 0.0) or 0.0)
        delta = bid_sz - ask_sz
        denom = bid_sz + ask_sz
        imbalance = (delta / denom) if denom > 1e-12 else 0.0
        normalized_delta = (delta / denom) if denom > 1e-12 else 0.0

        return {
            "spread": spread_ticks,
            "price_move_ticks": move_ticks,
            "velocity": velocity,
            "delta": delta,
            "imbalance": imbalance,
            "normalized_delta": normalized_delta,
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

        old_move = self._base_move_threshold
        old_nd = self._base_normalized_delta_threshold
        old_score = self._base_score_threshold
        old_spread = self._base_max_spread

        self._base_move_threshold = max(self._fallback_min_move_ticks, self._base_move_threshold * self._fallback_relax_step)
        self._base_normalized_delta_threshold = max(self._fallback_min_ndelta, self._base_normalized_delta_threshold * self._fallback_relax_step)
        self._base_score_threshold = max(0.30, self._base_score_threshold * self._fallback_relax_step)
        self._base_max_spread = min(self._fallback_max_spread, self._base_max_spread / self._fallback_relax_step)

        self._last_fallback_adjust_mono = now_mono
        self.logger.warning(
            "[MM][FALLBACK] mode=%s no_trades_sec=%.1f move_thr %.3f->%.3f ndelta_thr %.4f->%.4f score_thr %.3f->%.3f max_spread %.2f->%.2f",
            self._mode,
            float(now_mono - (self._last_signal_execute_mono or now_mono)),
            old_move,
            self._base_move_threshold,
            old_nd,
            self._base_normalized_delta_threshold,
            old_score,
            self._base_score_threshold,
            old_spread,
            self._base_max_spread,
        )

    def evaluate_entry_signal(self, market_snapshot: dict[str, float], *, now_mono: float) -> tuple[str, dict[str, float | str]]:
        self.metrics.signals_total += 1
        self._maybe_apply_fallback(now_mono=now_mono)

        spread = float(market_snapshot.get("spread", 0.0))
        price_move_ticks = float(market_snapshot.get("price_move_ticks", 0.0))
        velocity = float(market_snapshot.get("velocity", 0.0))
        delta = float(market_snapshot.get("delta", 0.0))
        imbalance = float(market_snapshot.get("imbalance", 0.0))
        normalized_delta = float(market_snapshot.get("normalized_delta", 0.0))

        max_spread = self._effective_max_spread()
        move_thr = float(self._base_move_threshold)
        vel_thr = float(self._base_velocity_threshold)
        delta_thr = float(self._base_delta_threshold)
        imb_thr = float(self._base_imbalance_threshold)
        ndelta_thr = float(self._base_normalized_delta_threshold)
        score_thr = float(self._base_score_threshold)

        # Component scores (0..1)
        spread_score = _clamp(1.0 - (spread / max_spread), 0.0, 1.0)
        move_score = _clamp(abs(price_move_ticks) / max(move_thr, 1e-9), 0.0, 1.0)
        ndelta_score = _clamp(abs(normalized_delta) / max(ndelta_thr, 1e-9), 0.0, 1.0)

        score = self._w_spread * spread_score + self._w_move * move_score + self._w_ndelta * ndelta_score

        # Optional soft gates (penalties, not hard skips)
        if vel_thr > 0.0 and abs(velocity) < vel_thr:
            score *= 0.75
        if delta_thr > 0.0 and abs(delta) < delta_thr:
            score *= 0.80
        if imb_thr > 0.0 and abs(imbalance) < imb_thr:
            score *= 0.85

        # Direction confirmation bonus/penalty
        dir_move = _sign(price_move_ticks)
        dir_flow = _sign(normalized_delta)
        dir_imb = _sign(imbalance)
        if dir_move != 0 and dir_flow != 0 and dir_move == dir_flow:
            score *= 1.10
        elif dir_move != 0 and dir_flow != 0 and dir_move != dir_flow:
            score *= 0.85
        if dir_move != 0 and dir_imb != 0 and dir_move != dir_imb:
            score *= 0.92

        score = _clamp(score, 0.0, 2.0)

        decision = "NONE"
        reason = "score_below_threshold"
        if score >= score_thr:
            if dir_move > 0:
                decision = "LONG"
                reason = "score_pass"
            elif dir_move < 0:
                decision = "SHORT"
                reason = "score_pass"
            else:
                # If move is 0 but score passed via ndelta, use flow direction.
                if dir_flow > 0:
                    decision = "LONG"
                    reason = "score_pass_flow_dir"
                elif dir_flow < 0:
                    decision = "SHORT"
                    reason = "score_pass_flow_dir"
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
            "move_ticks": float(price_move_ticks),
            "move_thr": float(move_thr),
            "vel": float(velocity),
            "ndelta": float(normalized_delta),
            "ndelta_thr": float(ndelta_thr),
            "delta": float(delta),
            "imb": float(imbalance),
            # deficits ("what missing")
            "need_spread": float(max(0.0, spread - max_spread)),
            "need_move": float(max(0.0, move_thr - abs(price_move_ticks))),
            "need_ndelta": float(max(0.0, ndelta_thr - abs(normalized_delta))),
        }
        return decision, row

    def _log_decision(self, row: dict[str, float | str]) -> None:
        # Compact but informative: include what was missing to execute.
        self.logger.info(
            "[MM][ENTRY_DECISION] symbol=%s mode=%s decision=%s reason=%s score=%.3f/%.3f spread=%.2f/%.2f move=%.2f/%.2f ndelta=%.3f/%.3f need(move=%.2f ndelta=%.3f spread=%.2f) delta=%.2f imb=%.3f vel=%.6f",
            str(row.get("symbol", "")),
            str(row.get("mode", "")),
            str(row.get("decision", "")),
            str(row.get("reason", "")),
            float(row.get("score", 0.0)),
            float(row.get("score_thr", 0.0)),
            float(row.get("spread", 0.0)),
            float(row.get("max_spread", 0.0)),
            float(row.get("move_ticks", 0.0)),
            float(row.get("move_thr", 0.0)),
            float(row.get("ndelta", 0.0)),
            float(row.get("ndelta_thr", 0.0)),
            float(row.get("need_move", 0.0)),
            float(row.get("need_ndelta", 0.0)),
            float(row.get("need_spread", 0.0)),
            float(row.get("delta", 0.0)),
            float(row.get("imb", 0.0)),
            float(row.get("vel", 0.0)),
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
        self._last_signal_execute_mono = now_mono
        self.metrics.signals_executed += 1
        self.logger.info(
            "[MM][ENTRY_EXECUTE] symbol=%s side=%s qty=%s px=%.4f cl_ord_id=%s",
            self.symbol,
            side,
            self.lot_size,
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

