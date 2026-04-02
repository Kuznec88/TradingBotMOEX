"""
Живой раннер свинг-стратегии: свечи 5m/15m с T-Invest API, сигнал как в swing_breakout,
вход/выход через ExecutionGateway.

MM (momentum_mm) не используется — только этот модуль.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import pandas as pd

from fix_engine.market_data.models import MarketData
from fix_engine.order_models import MarketType, OrderRequest
from fix_engine.strategy.swing_breakout import (
    SwingBreakoutParams,
    compute_indicators,
    generate_signals,
)


def _ensure_grpc_ca_bundle(base_dir: Path) -> None:
    try:
        cert = (base_dir / "certs" / "russian_trusted_chain.pem").resolve()
        if cert.exists():
            os.environ.setdefault("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", str(cert))
            os.environ.setdefault("SSL_CERT_FILE", str(cert))
            os.environ.setdefault("REQUESTS_CA_BUNDLE", str(cert))
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

        self._tp_pts = params.take_profit_rub / max(params.rub_per_point, 1e-9)
        self._sl_pts = params.stop_loss_rub / max(params.rub_per_point, 1e-9)
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

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._poll_loop, name="SwingLivePoll", daemon=True)
        self._thread.start()
        self.logger.info(
            "[SWING] started poll interval=%.1fs interval=%s symbol=%s n=%s k=%s ma=%s tp_rub=%s sl_rub=%s",
            self.poll_interval_sec,
            self.candle_interval,
            self.symbol,
            self.params.n_levels,
            self.params.k_volume,
            self.params.ma_period,
            self.params.take_profit_rub,
            self.params.stop_loss_rub,
        )

    def stop(self) -> None:
        self._stop.set()
        th = self._thread
        if th is not None and th.is_alive():
            # Дождаться выхода из цикла (может задержаться на gRPC get_candles).
            th.join(timeout=max(float(self.poll_interval_sec), 5.0) + 30.0)

    def on_market_data(self, data: MarketData) -> None:
        if str(data.symbol).upper() != self.symbol:
            return
        with self._lock:
            qty = float(self._position_qty)
            if abs(qty) < 1e-12 or self._exit_in_flight:
                return
            entry = float(self._entry_price)
            mid = float(data.mid_price)
            side_long = qty > 0

            if side_long:
                self._best_extreme_price = max(self._best_extreme_price, mid) if self._best_extreme_price > 0 else mid
                sl_level = entry - self._sl_pts
                tp_level = entry + self._tp_pts
                if self._trail_act_pts > 0 and self._trail_gap_pts > 0:
                    if (self._best_extreme_price - entry) >= self._trail_act_pts:
                        cand = self._best_extreme_price - self._trail_gap_pts
                        self._trail_stop_level = (
                            cand if self._trail_stop_level is None else max(self._trail_stop_level, cand)
                        )
                        sl_level = max(sl_level, self._trail_stop_level)
                hit_sl = mid <= sl_level
                hit_tp = mid >= tp_level
            else:
                if self._best_extreme_price <= 0:
                    self._best_extreme_price = mid
                else:
                    self._best_extreme_price = min(self._best_extreme_price, mid)
                sl_level = entry + self._sl_pts
                tp_level = entry - self._tp_pts
                if self._trail_act_pts > 0 and self._trail_gap_pts > 0:
                    if (entry - self._best_extreme_price) >= self._trail_act_pts:
                        cand = self._best_extreme_price + self._trail_gap_pts
                        self._trail_stop_level = (
                            cand if self._trail_stop_level is None else min(self._trail_stop_level, cand)
                        )
                        sl_level = min(sl_level, self._trail_stop_level)
                hit_sl = mid >= sl_level
                hit_tp = mid <= tp_level

            reason = ""
            if hit_sl and hit_tp:
                reason = "stop_loss" if self.params.intrabar_priority == "stop_first" else "take_profit"
            elif hit_sl:
                reason = "stop_loss"
            elif hit_tp:
                reason = "take_profit"
            if not reason:
                return

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
            if side == "1":
                self._position_qty = prev + last_qty
            else:
                self._position_qty = prev - last_qty

            if abs(self._position_qty) < 1e-12 and abs(prev) > 1e-12:
                self._position_qty = 0.0
                self._entry_price = 0.0
                self._exit_in_flight = False
                self._exit_cl_ord_id = ""
                self._best_extreme_price = 0.0
                self._trail_stop_level = None
            elif abs(prev) < 1e-12 and abs(self._position_qty) > 1e-12 and fill_px > 0:
                self._entry_price = fill_px
                self._best_extreme_price = 0.0
                self._trail_stop_level = None

            if cl_ord_id and cl_ord_id == self._exit_cl_ord_id and status_new == "FILLED":
                self._exit_in_flight = False
                self._exit_cl_ord_id = ""

    def _fetch_candles_df(self) -> pd.DataFrame:
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
        with Client(self.token, target=self.host) as client:
            r = client.market_data.get_candles(
                instrument_id=self.instrument_id,
                from_=from_dt,
                to=to_dt,
                interval=ci,
                limit=2400,
            )
            for c in r.candles:
                if not bool(getattr(c, "is_complete", True)):
                    continue
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
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df = df.sort_values("time_utc").drop_duplicates(subset=["time_utc"], keep="last")
        df = df.set_index("time_utc")
        return df

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                df = self._fetch_candles_df()
                need = max(self.params.ma_period, self.params.n_levels) + 3
                if len(df) < need:
                    time.sleep(self.poll_interval_sec)
                    continue

                df_work = df.reset_index()
                dfi = compute_indicators(
                    df_work,
                    n=self.params.n_levels,
                    ma_period=self.params.ma_period,
                )
                dfi["signal"] = generate_signals(dfi, k_volume=self.params.k_volume)
                dfi = dfi.set_index(df_work["time_utc"])
                last_ts = dfi.index[-1]
                with self._lock:
                    closed_ts = self._last_closed_bar_ts
                    flat = abs(self._position_qty) < 1e-12

                if closed_ts is not None and last_ts <= closed_ts:
                    time.sleep(self.poll_interval_sec)
                    continue

                # Новая закрытая свеча (последняя строка — самая свежая полная).
                sig = int(dfi.iloc[-1]["signal"])

                with self._lock:
                    self._last_closed_bar_ts = pd.Timestamp(last_ts)

                if sig != 0 and flat:
                    self._enter(sig)
            except Exception:
                self.logger.exception("[SWING] poll error")
            time.sleep(self.poll_interval_sec)

    def _enter(self, sig: int) -> None:
        md = self.get_latest(self.symbol)
        if md is None:
            self.logger.warning("[SWING] skip entry: no market data for %s", self.symbol)
            return
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
        except Exception as exc:
            self.logger.error("[SWING][ENTRY_REJECT] symbol=%s side=%s err=%s", self.symbol, side, exc)

    def _send_exit(self, data: MarketData, reason: str) -> None:
        with self._lock:
            qty = abs(float(self._position_qty))
            if qty < 1e-12 or self._exit_in_flight:
                return
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
