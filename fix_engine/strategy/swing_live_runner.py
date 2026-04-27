"""
Живой раннер свинг-стратегии: свечи 5m/15m с T-Invest API, FSM ретеста (swing_breakout),
вход лимитом на уровне, выход intrabar по TP/SL/трейлингу.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import pandas as pd

from fix_engine.data.models import MarketData
from fix_engine.order_models import MarketType, OrderRequest
from fix_engine.strategy.swing_breakout import (
    RetestEntryIntent,
    SwingBreakoutParams,
    SwingRetestFSMState,
    SwingTradeGovernor,
    append_swing_trade_csv,
    compute_indicators,
    compute_position_qty_rub,
    generate_signals,
    intent_to_entry_meta,
    retest_fsm_step,
    _fee_rub,
)


def _ensure_grpc_ca_bundle(base_dir: Path) -> None:
    try:
        from fix_engine.data.preflight import prepare_invest_api_tls_trust_store

        prepare_invest_api_tls_trust_store(base_dir)
    except Exception:
        pass


def _q_float(q: object | None) -> float:
    if q is None:
        return 0.0
    u = float(getattr(q, "units", 0) or 0)
    n = float(getattr(q, "nano", 0) or 0)
    return u + n / 1_000_000_000.0


class SwingLiveRunner:
    def __init__(
        self,
        *,
        base_dir: Path,
        gateway: object,
        logger: logging.Logger,
        symbol: str,
        market: MarketType,
        lot_size: float,
        tick_size: float,
        params: SwingBreakoutParams,
        candle_interval: str,
        poll_interval_sec: float,
        get_latest: Callable[[str], MarketData | None],
        instrument_id: str,
        token: str,
        host: str,
    ) -> None:
        self._base_dir = base_dir
        self.gateway = gateway
        self.logger = logger
        self.symbol = symbol.upper()
        self.market = market
        self.lot_size = float(lot_size)
        self.tick_size = float(tick_size)
        self.params = params
        self.candle_interval = candle_interval.strip().lower()
        self.poll_interval_sec = max(5.0, float(poll_interval_sec))
        self.get_latest = get_latest
        self.instrument_id = instrument_id
        self.token = token
        self.host = host

        self._lock = threading.RLock()
        self._position_qty = 0.0
        self._entry_price = 0.0
        self._exit_in_flight = False
        self._exit_cl_ord_id = ""
        self._last_closed_bar_ts: pd.Timestamp | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        self._use_retest = bool(getattr(params, "use_retest_fsm", True))
        # Legacy rub-based exits (если не retest или до первого fill)
        self._tp_pts = params.take_profit_rub / max(params.rub_per_point, 1e-9)
        self._sl_pts = params.stop_loss_rub / max(params.rub_per_point, 1e-9)
        self._max_entry_spread_ticks = float(getattr(params, "max_entry_spread_ticks", 2.0))
        self._chop_window_bars = max(1, int(getattr(params, "chop_window_bars", 5)))
        self._chop_max_range_ticks = float(getattr(params, "chop_max_range_ticks", 5.0))
        self._trail_act_pts = (
            params.trailing_activation_rub / max(params.rub_per_point, 1e-9)
            if params.trailing_activation_rub > 0
            else 0.0
        )
        self._trail_gap_pts = (
            params.trailing_gap_rub / max(params.rub_per_point, 1e-9)
            if params.trailing_gap_rub > 0
            else 0.0
        )
        self._best_extreme_price: float = 0.0
        self._trail_stop_level: float | None = None

        # Вход: только закрытые бары; при retest — всегда без подмешивания котировки в OHLC.
        self._signal_on_close_only = bool(getattr(params, "signal_on_close_only", True)) or self._use_retest
        self._last_poll_signal = 0
        self._entry_retry = False

        self._fsm_state = SwingRetestFSMState()
        self._pending_limit_cl_ord_id: str = ""
        self._pending_intent: RetestEntryIntent | None = None
        self._pending_bars_waited: int = 0
        self._pending_bar_snap_ts: pd.Timestamp | None = None

        self._struct_sl: float = 0.0
        self._struct_tp: float = 0.0
        self._struct_risk: float = 0.0
        self._struct_trail_arm: float = 0.0
        self._struct_trail_gap: float = 0.0

        self._partial_exit_done: bool = False
        self._bars_in_position: int = 0
        self._best_tp_progress: float = 0.0
        self._initial_position_qty: float = 0.0

        self._trade_governor = SwingTradeGovernor(params)
        self._open_entry_meta: dict[str, object] | None = None
        self._last_exit_reason: str = ""

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._poll_loop, name="SwingLivePoll", daemon=True)
        self._thread.start()
        mode = "retest_fsm+bar_close" if self._use_retest else (
            "bar_close_only" if self._signal_on_close_only else "intrabar_forming"
        )
        self.logger.info(
            "[SWING] started poll interval=%.1fs interval=%s symbol=%s n=%s k=%s mode=%s",
            self.poll_interval_sec,
            self.candle_interval,
            self.symbol,
            self.params.n_levels,
            self.params.k_volume,
            mode,
        )

    def stop(self) -> None:
        self._stop.set()
        th = self._thread
        if th is not None and th.is_alive():
            th.join(timeout=max(float(self.poll_interval_sec), 5.0) + 30.0)

    def on_market_data(self, data: MarketData) -> None:
        if str(data.symbol).upper() != self.symbol:
            return
        partial_qty_out: float | None = None
        reason = ""
        with self._lock:
            qty = float(self._position_qty)
            if abs(qty) < 1e-12 or self._exit_in_flight:
                return
            entry = float(self._entry_price)
            bid = float(data.bid)
            ask = float(data.ask)
            side_long = qty > 0
            p = self.params

            if (
                self._use_retest
                and self._struct_risk > 0
                and not self._partial_exit_done
                and 0.0 < p.partial_exit_fraction < 1.0
                and p.partial_exit_r_multiple > 0
            ):
                rmul = p.partial_exit_r_multiple
                part_qty = qty * p.partial_exit_fraction
                if part_qty > 1e-9:
                    if side_long and bid >= entry + rmul * self._struct_risk:
                        self.logger.info(
                            "[SWING][EXIT] reason=partial_tp symbol=%s qty=%.6f bid=%.4f",
                            self.symbol,
                            part_qty,
                            bid,
                        )
                        partial_qty_out = part_qty
                    elif (not side_long) and ask <= entry - rmul * self._struct_risk:
                        self.logger.info(
                            "[SWING][EXIT] reason=partial_tp symbol=%s qty=%.6f ask=%.4f",
                            self.symbol,
                            part_qty,
                            ask,
                        )
                        partial_qty_out = part_qty

            hit_sl = False
            hit_tp = False
            if partial_qty_out is None and self._use_retest and self._struct_risk > 0:
                if side_long:
                    self._best_extreme_price = max(self._best_extreme_price, bid) if self._best_extreme_price > 0 else bid
                    sl_level = float(self._struct_sl)
                    tp_level = float(self._struct_tp)
                    if self._struct_trail_arm > 0 and self._struct_trail_gap > 0:
                        if (self._best_extreme_price - entry) >= self._struct_trail_arm:
                            cand = self._best_extreme_price - self._struct_trail_gap
                            self._trail_stop_level = (
                                cand if self._trail_stop_level is None else max(self._trail_stop_level, cand)
                            )
                            sl_level = max(sl_level, self._trail_stop_level)
                    hit_sl = bid <= sl_level
                    hit_tp = bid >= tp_level
                else:
                    if self._best_extreme_price <= 0:
                        self._best_extreme_price = ask
                    else:
                        self._best_extreme_price = min(self._best_extreme_price, ask)
                    sl_level = float(self._struct_sl)
                    tp_level = float(self._struct_tp)
                    if self._struct_trail_arm > 0 and self._struct_trail_gap > 0:
                        if (entry - self._best_extreme_price) >= self._struct_trail_arm:
                            cand = self._best_extreme_price + self._struct_trail_gap
                            self._trail_stop_level = (
                                cand if self._trail_stop_level is None else min(self._trail_stop_level, cand)
                            )
                            sl_level = min(sl_level, self._trail_stop_level)
                    hit_sl = ask >= sl_level
                    hit_tp = ask <= tp_level
            elif partial_qty_out is None:
                if side_long:
                    self._best_extreme_price = max(self._best_extreme_price, bid) if self._best_extreme_price > 0 else bid
                    sl_level = entry - self._sl_pts
                    tp_level = entry + self._tp_pts
                    if self._trail_act_pts > 0 and self._trail_gap_pts > 0:
                        if (self._best_extreme_price - entry) >= self._trail_act_pts:
                            cand = self._best_extreme_price - self._trail_gap_pts
                            self._trail_stop_level = (
                                cand if self._trail_stop_level is None else max(self._trail_stop_level, cand)
                            )
                            sl_level = max(sl_level, self._trail_stop_level)
                    hit_sl = bid <= sl_level
                    hit_tp = bid >= tp_level
                else:
                    if self._best_extreme_price <= 0:
                        self._best_extreme_price = ask
                    else:
                        self._best_extreme_price = min(self._best_extreme_price, ask)
                    sl_level = entry + self._sl_pts
                    tp_level = entry - self._tp_pts
                    if self._trail_act_pts > 0 and self._trail_gap_pts > 0:
                        if (entry - self._best_extreme_price) >= self._trail_act_pts:
                            cand = self._best_extreme_price + self._trail_gap_pts
                            self._trail_stop_level = (
                                cand if self._trail_stop_level is None else min(self._trail_stop_level, cand)
                            )
                            sl_level = min(sl_level, self._trail_stop_level)
                    hit_sl = ask >= sl_level
                    hit_tp = ask <= tp_level

            if partial_qty_out is None:
                if hit_sl and hit_tp:
                    reason = "stop_loss" if self.params.intrabar_priority == "stop_first" else "take_profit"
                elif hit_sl:
                    reason = "stop_loss"
                elif hit_tp:
                    reason = "take_profit"

        if partial_qty_out is not None:
            self._send_exit(data, "partial_tp", qty_override=partial_qty_out)
            return
        if reason:
            self._send_exit(data, reason)

    def on_execution_report(self, state: dict[str, object]) -> None:
        if str(state.get("symbol", "")).upper() != self.symbol:
            return
        cl_ord_id = str(state.get("cl_ord_id", "") or "")
        status_new = str(state.get("status_new", ""))
        if cl_ord_id and cl_ord_id == self._exit_cl_ord_id and status_new in {"CANCELED", "REJECTED"}:
            with self._lock:
                self._exit_in_flight = False
                self._exit_cl_ord_id = ""
            return
        if (
            cl_ord_id
            and cl_ord_id == self._pending_limit_cl_ord_id
            and status_new in {"CANCELED", "REJECTED"}
        ):
            with self._lock:
                self._clear_pending_entry("order_canceled_or_rejected", invoke_cancel=False)
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

        with self._lock:
            prev = float(self._position_qty)
            ep_snap = float(self._entry_price)
            if side == "1":
                self._position_qty = prev + last_qty
            else:
                self._position_qty = prev - last_qty

            if abs(self._position_qty) < 1e-12 and abs(prev) > 1e-12:
                q_close = abs(prev)
                was_long = prev > 0
                if self._use_retest and fill_px > 0 and ep_snap > 0 and q_close > 1e-12:
                    p = self.params
                    if was_long:
                        gross = (fill_px - ep_snap) * q_close * p.rub_per_point
                    else:
                        gross = (ep_snap - fill_px) * q_close * p.rub_per_point
                    fees = _fee_rub(ep_snap, q_close, p.commission_rate, p.notional_scale) + _fee_rub(
                        fill_px, q_close, p.commission_rate, p.notional_scale
                    )
                    net = gross - fees
                    fg = abs(gross) if abs(gross) > 1e-12 else 1e-12
                    self._trade_governor.on_closed_trade_pnl(net, is_partial=False)
                    log_csv = (p.trades_log_csv_path or "").strip()
                    if log_csv and self._open_entry_meta is not None:
                        om = dict(self._open_entry_meta)
                        om["commission_to_gross"] = fees / fg
                        append_swing_trade_csv(
                            log_csv,
                            {
                                "entry_price": ep_snap,
                                "exit_price": fill_px,
                                "pnl_net_rub": net,
                                "pnl_gross_rub": gross,
                                "fees_rub": fees,
                                "commission_to_gross": om.get("commission_to_gross", ""),
                                "exit_reason": self._last_exit_reason or "unknown",
                                "side": "LONG" if was_long else "SHORT",
                                "entry_reason": om.get("entry_reason", ""),
                                "atr_slope": om.get("atr_slope", ""),
                                "impulse_strength_ratio": om.get("impulse_strength_ratio", ""),
                                "retest_bars_to_touch": om.get("retest_bars_to_touch", ""),
                                "rr_multiple": om.get("rr_multiple", ""),
                                "range_atr_ratio": om.get("range_atr_ratio", ""),
                            },
                        )
                self._open_entry_meta = None
                self._position_qty = 0.0
                self._entry_price = 0.0
                self._exit_in_flight = False
                self._exit_cl_ord_id = ""
                self._last_exit_reason = ""
                self._best_extreme_price = 0.0
                self._trail_stop_level = None
                self._struct_sl = 0.0
                self._struct_tp = 0.0
                self._struct_risk = 0.0
                self._struct_trail_arm = 0.0
                self._struct_trail_gap = 0.0
                self._partial_exit_done = False
                self._bars_in_position = 0
                self._best_tp_progress = 0.0
                self._initial_position_qty = 0.0
                if self._use_retest:
                    self._fsm_state = SwingRetestFSMState()
            elif (
                abs(prev) > 1e-12
                and abs(self._position_qty) > 1e-12
                and abs(self._position_qty) < abs(prev) - 1e-9
            ):
                q_part = last_qty
                was_long = prev > 0
                if self._use_retest and fill_px > 0 and ep_snap > 0 and q_part > 1e-12:
                    p = self.params
                    if was_long:
                        gross = (fill_px - ep_snap) * q_part * p.rub_per_point
                    else:
                        gross = (ep_snap - fill_px) * q_part * p.rub_per_point
                    fees = _fee_rub(ep_snap, q_part, p.commission_rate, p.notional_scale) + _fee_rub(
                        fill_px, q_part, p.commission_rate, p.notional_scale
                    )
                    net = gross - fees
                    self._trade_governor.on_closed_trade_pnl(net, is_partial=True)
                self._partial_exit_done = True
                if self.params.partial_remainder_mode == "tp":
                    self._struct_trail_arm = 0.0
                    self._struct_trail_gap = 0.0
            elif abs(prev) < 1e-12 and abs(self._position_qty) > 1e-12 and fill_px > 0:
                self._entry_price = fill_px
                self._best_extreme_price = 0.0
                self._trail_stop_level = None
                self._partial_exit_done = False
                self._bars_in_position = 0
                self._best_tp_progress = 0.0
                self._initial_position_qty = abs(float(self._position_qty))
                if self._use_retest and self._pending_intent is not None:
                    it = self._pending_intent
                    self._struct_sl = float(it.stop_price)
                    self._struct_tp = float(it.take_profit_price)
                    self._struct_risk = float(it.risk_per_unit)
                    p = self.params
                    if p.trail_after_r_multiple > 0 and p.trail_gap_r_multiple > 0:
                        self._struct_trail_arm = p.trail_after_r_multiple * self._struct_risk
                        self._struct_trail_gap = p.trail_gap_r_multiple * self._struct_risk
                    else:
                        self._struct_trail_arm = 0.0
                        self._struct_trail_gap = 0.0
                    self._open_entry_meta = intent_to_entry_meta(it, self.params)
                    self._trade_governor.note_entry_opened()
                    self._pending_intent = None
                    self._pending_limit_cl_ord_id = ""
                    self._pending_bars_waited = 0
                    self._pending_bar_snap_ts = None
                    self.logger.info(
                        "[SWING][ENTRY_EXECUTED] symbol=%s side=%s fill_px=%.4f sl=%.4f tp=%.4f risk=%.4f cl_ord_id=%s",
                        self.symbol,
                        side,
                        fill_px,
                        self._struct_sl,
                        self._struct_tp,
                        self._struct_risk,
                        cl_ord_id,
                    )

            if cl_ord_id and cl_ord_id == self._exit_cl_ord_id and status_new == "FILLED":
                self._exit_in_flight = False
                self._exit_cl_ord_id = ""

    def _try_post_entry_time_stop(self, last_row: pd.Series) -> None:
        p = self.params
        if p.post_entry_time_stop_bars <= 0:
            return
        bars = 0
        prog = 0.0
        with self._lock:
            if abs(float(self._position_qty)) < 1e-12 or self._exit_in_flight:
                return
            if self._bars_in_position < p.post_entry_time_stop_bars:
                return
            if self._best_tp_progress >= p.post_entry_time_stop_min_tp_progress:
                return
            bars = self._bars_in_position
            prog = self._best_tp_progress
        md = self.get_latest(self.symbol)
        if md is None:
            return
        self.logger.info(
            "[SWING][EXIT] reason=time_stop symbol=%s bars=%d best_progress=%.4f min=%.4f",
            self.symbol,
            bars,
            prog,
            p.post_entry_time_stop_min_tp_progress,
        )
        self._send_exit(md, "time_stop")

    def _fetch_candles_df(self) -> tuple[pd.DataFrame, bool]:
        """OHLCV из API. Второй элемент: True если последняя строка — незакрытая свеча."""
        _ensure_grpc_ca_bundle(self._base_dir)
        from t_tech.invest import Client
        from t_tech.invest.schemas import CandleInterval

        interval_map = {
            "5m": CandleInterval.CANDLE_INTERVAL_5_MIN,
            "15m": CandleInterval.CANDLE_INTERVAL_15_MIN,
        }
        if self.candle_interval not in interval_map:
            raise ValueError(f"SwingCandleInterval must be 5m or 15m, got {self.candle_interval!r}")
        ci = interval_map[self.candle_interval]

        to_dt = datetime.now(timezone.utc)
        from_dt = to_dt - timedelta(days=14)
        rows: list[dict[str, object]] = []
        forming_included = False
        with Client(self.token, target=self.host) as client:
            r = client.market_data.get_candles(
                instrument_id=self.instrument_id,
                from_=from_dt,
                to=to_dt,
                interval=ci,
                limit=2400,
            )
            clist = list(getattr(r, "candles", []) or [])
            for j, c in enumerate(clist):
                is_last = j == len(clist) - 1
                complete = bool(getattr(c, "is_complete", True))
                if self._signal_on_close_only:
                    if not complete:
                        continue
                else:
                    if not complete and not is_last:
                        continue
                if not complete and is_last:
                    forming_included = True
                t = getattr(c, "time", None)
                ts = pd.Timestamp(t) if t is not None else pd.Timestamp.utcnow()
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                else:
                    ts = ts.tz_convert("UTC")
                rows.append(
                    {
                        "time_utc": ts,
                        "open": _q_float(getattr(c, "open", None)),
                        "high": _q_float(getattr(c, "high", None)),
                        "low": _q_float(getattr(c, "low", None)),
                        "close": _q_float(getattr(c, "close", None)),
                        "volume": int(getattr(c, "volume", 0) or 0),
                    }
                )
        if not rows:
            return pd.DataFrame(), False
        df = pd.DataFrame(rows)
        df = df.sort_values("time_utc").drop_duplicates(subset=["time_utc"], keep="last")
        df = df.set_index("time_utc")
        return df, forming_included

    def _apply_live_quote_to_last_candle(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        md = self.get_latest(self.symbol)
        if md is None:
            return
        idx = df.index[-1]
        h = float(df.loc[idx, "high"])
        l_ = float(df.loc[idx, "low"])
        bid = float(md.bid)
        ask = float(md.ask)
        lp = float(md.last_price) if md.last_price else (bid + ask) / 2.0
        c = lp
        nh = max(h, c, bid, ask)
        nl = min(l_, c, bid, ask)
        df.loc[idx, "close"] = c
        df.loc[idx, "high"] = nh
        df.loc[idx, "low"] = nl

    def _chop_filter_legacy(self, dfi: pd.DataFrame) -> tuple[bool, float]:
        if self._chop_max_range_ticks <= 0.0 or self._chop_window_bars <= 0:
            return False, 0.0
        if "high" not in dfi.columns or "low" not in dfi.columns:
            return False, 0.0
        window = min(int(self._chop_window_bars), len(dfi))
        if window < 2:
            return False, 0.0
        recent = dfi.iloc[-window:]
        hi = float(recent["high"].max())
        lo = float(recent["low"].min())
        range_price = hi - lo
        range_ticks = range_price / max(float(self.tick_size), 1e-12)
        if range_ticks < self._chop_max_range_ticks:
            return True, range_ticks
        return False, range_ticks

    def _clear_pending_entry(self, reason: str, *, invoke_cancel: bool = True) -> None:
        cid = self._pending_limit_cl_ord_id
        if invoke_cancel and cid:
            try:
                cancel = getattr(self.gateway, "cancel_order", None)
                if callable(cancel):
                    cancel(cid, self.market)
            except Exception:
                self.logger.exception("[SWING] cancel pending limit failed cl_ord_id=%s", cid)
        if cid or self._pending_intent is not None:
            self.logger.info(
                "[SWING][ENTRY_SKIPPED] reason=%s symbol=%s cl_ord_id=%s",
                reason,
                self.symbol,
                cid,
            )
        self._pending_limit_cl_ord_id = ""
        self._pending_intent = None
        self._pending_bars_waited = 0
        self._pending_bar_snap_ts = None

    def _entry_order_qty(self, intent: RetestEntryIntent) -> float:
        return compute_position_qty_rub(
            self.params,
            float(intent.risk_per_unit),
            fallback_qty=float(self.lot_size),
        )

    def _place_limit_entry(self, intent: RetestEntryIntent) -> bool:
        md = self.get_latest(self.symbol)
        if md is None:
            self.logger.warning("[SWING][ENTRY_SKIPPED] reason=no_market_data symbol=%s", self.symbol)
            return False
        spread_ticks = float(md.spread) / max(float(self.tick_size), 1e-12)
        if self._max_entry_spread_ticks > 0 and spread_ticks > self._max_entry_spread_ticks:
            self.logger.info(
                "[SWING][ENTRY_SKIPPED] reason=spread_too_wide spread_ticks=%.2f max=%.2f symbol=%s",
                spread_ticks,
                self._max_entry_spread_ticks,
                self.symbol,
            )
            return False
        side = "1" if intent.side == 1 else "2"
        qty = self._entry_order_qty(intent)
        if intent.use_market_entry:
            price = float(md.ask) if side == "1" else float(md.bid)
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
            log_tag = "ENTRY_MARKET"
        else:
            price = float(intent.limit_price)
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
            log_tag = "ENTRY_LIMIT"
        try:
            cl_ord_id = self.gateway.send_order(req)
            self._pending_limit_cl_ord_id = str(cl_ord_id or "")
            self._pending_intent = intent
            self._pending_bars_waited = 0
            self.logger.info(
                "[SWING][%s] symbol=%s side=%s qty=%s px=%.4f sl=%.4f tp=%.4f cl_ord_id=%s",
                log_tag,
                self.symbol,
                side,
                qty,
                price,
                intent.stop_price,
                intent.take_profit_price,
                self._pending_limit_cl_ord_id,
            )
            return True
        except Exception as exc:
            self.logger.error("[SWING][ENTRY_SKIPPED] reason=gateway_reject err=%s symbol=%s", exc, self.symbol)
            return False

    def _poll_loop(self) -> None:
        k_vol = float(self.params.k_volume)
        while not self._stop.is_set():
            try:
                df, forming = self._fetch_candles_df()
                if not self._signal_on_close_only and forming and not self._use_retest:
                    self._apply_live_quote_to_last_candle(df)

                need = (
                    max(
                        self.params.ma_period,
                        self.params.n_levels,
                        self.params.atr_period,
                        self.params.atr_slope_lag + self.params.atr_period,
                        self.params.impulse_range_lookback + 2,
                    )
                    + 5
                )
                if len(df) < need:
                    self.logger.info(
                        "[SWING][POLL] phase=warmup candles=%d need=%d symbol=%s interval=%s",
                        len(df),
                        need,
                        self.symbol,
                        self.candle_interval,
                    )
                    time.sleep(self.poll_interval_sec)
                    continue

                df_work = df.reset_index()
                dfi = compute_indicators(
                    df_work,
                    n=self.params.n_levels,
                    ma_period=self.params.ma_period,
                    params=self.params,
                )
                if not self._use_retest:
                    dfi["signal"] = generate_signals(dfi, k_volume=k_vol, params=self.params)
                dfi = dfi.set_index(df_work["time_utc"])
                last_ts = dfi.index[-1]
                last_row = dfi.iloc[-1]
                prev_row = dfi.iloc[-2] if len(dfi) > 1 else None

                with self._lock:
                    pos_qty = float(self._position_qty)
                    flat = abs(pos_qty) < 1e-12

                if not self._signal_on_close_only and not self._use_retest:
                    sig = int(dfi.iloc[-1]["signal"])
                    self._poll_loop_legacy_intrabar(dfi, last_ts, sig, forming, pos_qty, flat, k_vol)
                    time.sleep(self.poll_interval_sec)
                    continue

                with self._lock:
                    closed_ts = self._last_closed_bar_ts

                if closed_ts is not None and last_ts <= closed_ts:
                    time.sleep(self.poll_interval_sec)
                    continue

                limit_timeout = False
                with self._lock:
                    if self._use_retest and flat and self._pending_limit_cl_ord_id:
                        pit = self._pending_intent
                        skip_to = pit is not None and bool(getattr(pit, "use_market_entry", False))
                        if not skip_to:
                            snap = self._pending_bar_snap_ts
                            if snap is None:
                                self._pending_bar_snap_ts = pd.Timestamp(last_ts)
                            elif pd.Timestamp(last_ts) > snap:
                                self._pending_bars_waited += 1
                                self._pending_bar_snap_ts = pd.Timestamp(last_ts)
                            to_bars = max(1, int(self.params.limit_order_timeout_bars))
                            if self._pending_bars_waited >= to_bars:
                                limit_timeout = True
                    self._last_closed_bar_ts = pd.Timestamp(last_ts)
                if limit_timeout:
                    self._clear_pending_entry("limit_timeout")
                if self._use_retest:
                    self._trade_governor.on_new_bar(pd.Timestamp(last_ts))

                with self._lock:
                    if self._use_retest and abs(float(self._position_qty)) > 1e-12:
                        self._bars_in_position += 1
                        hi_b = float(last_row["high"])
                        lo_b = float(last_row["low"])
                        ep = float(self._entry_price)
                        tpv = float(self._struct_tp)
                        if float(self._position_qty) > 0:
                            den = tpv - ep
                            if den > 1e-9:
                                self._best_tp_progress = max(self._best_tp_progress, (hi_b - ep) / den)
                        else:
                            den = ep - tpv
                            if den > 1e-9:
                                self._best_tp_progress = max(self._best_tp_progress, (ep - lo_b) / den)
                if self._use_retest:
                    self._try_post_entry_time_stop(last_row)

                if self._use_retest:
                    with self._lock:
                        can_fsm = flat and (not self._pending_limit_cl_ord_id)
                    intent: RetestEntryIntent | None = None
                    log_events: list[str] = []
                    if can_fsm:
                        new_state, intent, log_events = retest_fsm_step(
                            self._fsm_state,
                            row=last_row,
                            prev_row=prev_row,
                            params=self.params,
                            bar_time=pd.Timestamp(last_ts),
                            governor=self._trade_governor,
                        )
                        self._fsm_state = new_state
                        for ev in log_events:
                            if ev.startswith("breakout"):
                                self.logger.info(
                                    "[SWING][BREAKOUT_DETECTED] event=%s close=%.4f symbol=%s",
                                    ev,
                                    float(last_row["close"]),
                                    self.symbol,
                                )
                            elif ev.startswith("retest"):
                                self.logger.info(
                                    "[SWING][RETEST_DETECTED] event=%s level=%.4f close=%.4f symbol=%s",
                                    ev,
                                    float(self._fsm_state.level) if self._fsm_state.direction else 0.0,
                                    float(last_row["close"]),
                                    self.symbol,
                                )
                            elif ev == "skip_commission":
                                self.logger.info(
                                    "[SWING][ENTRY_SKIPPED] reason=profit_lt_commission_x symbol=%s",
                                    self.symbol,
                                )
                            elif ev in {"invalidate_long", "invalidate_short"}:
                                self.logger.info(
                                    "[SWING][FSM] event=%s symbol=%s",
                                    ev,
                                    self.symbol,
                                )
                            elif ev.startswith("breakout_rejected:"):
                                self.logger.info("[SWING][%s] symbol=%s", ev.upper().replace(":", "_"), self.symbol)
                            elif ev.startswith("retest_rejected:"):
                                self.logger.info("[SWING][%s] symbol=%s", ev.upper().replace(":", "_"), self.symbol)
                            elif ev.startswith("entry_skipped:"):
                                self.logger.info("[SWING][%s] symbol=%s", ev.upper().replace(":", "_"), self.symbol)
                            elif ev == "fsm_recover_invalid_breakout":
                                self.logger.info("[SWING][FSM_RECOVER] invalid_breakout symbol=%s", self.symbol)
                    if intent is not None and flat:
                        with self._lock:
                            already = bool(self._pending_limit_cl_ord_id)
                        if not already:
                            ok = self._place_limit_entry(intent)
                            if ok:
                                with self._lock:
                                    self._pending_bar_snap_ts = pd.Timestamp(last_ts)
                                    self._pending_bars_waited = 0
                            else:
                                self._fsm_state = SwingRetestFSMState()
                else:
                    sig = int(dfi.iloc[-1]["signal"])
                    close = float(last_row["close"])
                    high_n_v = last_row.get("high_N")
                    low_n_v = last_row.get("low_N")
                    ma_v = last_row.get("ma")
                    av_v = last_row.get("avg_volume")
                    vol = float(last_row["volume"])
                    high_n = float(high_n_v) if pd.notna(high_n_v) else float("nan")
                    low_n = float(low_n_v) if pd.notna(low_n_v) else float("nan")
                    ma = float(ma_v) if pd.notna(ma_v) else float("nan")
                    av = float(av_v) if pd.notna(av_v) else float("nan")
                    vol_ok = bool(vol > av * k_vol) if math.isfinite(av) and av > 0 else False
                    above_hn = bool(close > high_n) if math.isfinite(high_n) else False
                    below_ln = bool(close < low_n) if math.isfinite(low_n) else False
                    above_ma = bool(close > ma) if math.isfinite(ma) else False
                    below_ma = bool(close < ma) if math.isfinite(ma) else False
                    vol_ratio = (vol / av) if math.isfinite(av) and av > 0 else 0.0

                    with self._lock:
                        pos_qty = float(self._position_qty)
                        flat = abs(pos_qty) < 1e-12

                    if sig != 0 and flat:
                        chop_skipped, range_ticks = self._chop_filter_legacy(dfi.reset_index())
                        if chop_skipped:
                            self.logger.info(
                                "[SWING][ENTRY_SKIPPED] reason=chop_filter range_ticks=%.2f symbol=%s",
                                range_ticks,
                                self.symbol,
                            )
                        else:
                            self._enter_market(sig)
                    elif not flat:
                        self.logger.info(
                            "[SWING][POLL] phase=new_bar last_ts=%s sig=%d pos=%.6f symbol=%s",
                            last_ts.isoformat(),
                            sig,
                            pos_qty,
                            self.symbol,
                        )
                    else:
                        self.logger.info(
                            "[SWING][POLL] phase=new_bar last_ts=%s sig=0 close=%.4f gates above_hN=%s vol_ok=%s "
                            "above_ma=%s symbol=%s",
                            last_ts.isoformat(),
                            close,
                            above_hn,
                            vol_ok,
                            above_ma,
                            self.symbol,
                        )
                    self._last_poll_signal = sig
                    self._entry_retry = False

            except Exception:
                self.logger.exception("[SWING] poll error")
            time.sleep(self.poll_interval_sec)

    def _poll_loop_legacy_intrabar(
        self,
        dfi: pd.DataFrame,
        last_ts: pd.Timestamp,
        sig: int,
        forming: bool,
        pos_qty: float,
        flat: bool,
        k_vol: float,
    ) -> None:
        row = dfi.iloc[-1]
        close = float(row["close"])
        high_n_v = row.get("high_N")
        low_n_v = row.get("low_N")
        ma_v = row.get("ma")
        av_v = row.get("avg_volume")
        vol = float(row["volume"])
        high_n = float(high_n_v) if pd.notna(high_n_v) else float("nan")
        low_n = float(low_n_v) if pd.notna(low_n_v) else float("nan")
        ma = float(ma_v) if pd.notna(ma_v) else float("nan")
        av = float(av_v) if pd.notna(av_v) else float("nan")
        vol_ok = bool(vol > av * k_vol) if math.isfinite(av) and av > 0 else False
        above_hn = bool(close > high_n) if math.isfinite(high_n) else False
        below_ln = bool(close < low_n) if math.isfinite(low_n) else False
        above_ma = bool(close > ma) if math.isfinite(ma) else False
        below_ma = bool(close < ma) if math.isfinite(ma) else False
        vol_ratio = (vol / av) if math.isfinite(av) and av > 0 else 0.0
        prev_sig = self._last_poll_signal
        if flat and sig != 0:
            try_enter = (prev_sig != sig) or self._entry_retry
            if try_enter:
                chop_skipped, range_ticks = self._chop_filter_legacy(dfi.reset_index())
                if chop_skipped:
                    self.logger.info(
                        "[SWING][ENTRY_SKIPPED] reason=chop_filter range_ticks=%.2f symbol=%s",
                        range_ticks,
                        self.symbol,
                    )
                    self._entry_retry = True
                else:
                    ok = self._enter_market(sig)
                    self._entry_retry = not ok
        else:
            self._entry_retry = False
        if flat and sig == 0:
            self.logger.info(
                "[SWING][POLL] phase=intrabar bar_ts=%s forming=%s sig=0 prev_sig=%d close=%.4f symbol=%s",
                last_ts.isoformat(),
                forming,
                prev_sig,
                close,
                self.symbol,
            )
        elif not flat:
            self.logger.info(
                "[SWING][POLL] phase=intrabar bar_ts=%s sig=%d pos=%.6f symbol=%s",
                last_ts.isoformat(),
                sig,
                pos_qty,
                self.symbol,
            )
        self._last_poll_signal = sig

    def _enter_market(self, sig: int) -> bool:
        md = self.get_latest(self.symbol)
        if md is None:
            self.logger.warning("[SWING][ENTRY_SKIPPED] reason=no_market_data symbol=%s", self.symbol)
            return False
        spread_ticks = float(md.spread) / max(float(self.tick_size), 1e-12)
        if self._max_entry_spread_ticks > 0 and spread_ticks > self._max_entry_spread_ticks:
            self.logger.info(
                "[SWING][ENTRY_SKIPPED] reason=spread_too_wide spread_ticks=%.2f symbol=%s",
                spread_ticks,
                self.symbol,
            )
            return False
        side = "1" if sig == 1 else "2"
        price = float(md.ask) if side == "1" else float(md.bid)
        qty = self.lot_size
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
            self.logger.info(
                "[SWING][ENTRY] symbol=%s side=%s qty=%s px=%.4f cl_ord_id=%s",
                self.symbol,
                side,
                qty,
                price,
                cl_ord_id,
            )
            return True
        except Exception as exc:
            self.logger.error("[SWING][ENTRY_REJECT] symbol=%s err=%s", self.symbol, exc)
            return False

    def _send_exit(self, data: MarketData, reason: str, qty_override: float | None = None) -> None:
        with self._lock:
            qty = abs(float(self._position_qty))
            if qty < 1e-12 or self._exit_in_flight:
                return
            if qty_override is not None and qty_override > 0:
                qty = min(qty, float(qty_override))
            side = "2" if self._position_qty > 0 else "1"
            self._exit_in_flight = True

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
        try:
            cl_ord_id = self.gateway.send_order(req)
            with self._lock:
                self._exit_cl_ord_id = str(cl_ord_id or "")
                self._last_exit_reason = reason
            self.logger.info(
                "[SWING][EXIT] reason=%s symbol=%s side=%s qty=%s px=%.4f cl_ord_id=%s",
                reason,
                self.symbol,
                side,
                qty,
                price,
                cl_ord_id,
            )
        except Exception as exc:
            with self._lock:
                self._exit_in_flight = False
                self._exit_cl_ord_id = ""
            self.logger.error("[SWING][EXIT_REJECT] symbol=%s err=%s", self.symbol, exc)
