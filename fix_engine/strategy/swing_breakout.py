"""
Свинг-стратегия: пробой N-свечного диапазона + подтверждение объёмом + фильтр MA.

Расчёт на pandas DataFrame с колонками: open, high, low, close, volume
(опционально time_utc / timestamp для логов).

Бэктест: покомпонентно — индикаторы → сигналы → симуляция позиции (TP/SL в рублях,
комиссия % на вход/выход, проскальзывание в тиках).

Живой раннер: агрегируйте тики/стрим в OHLCV нужного ТФ и вызывайте те же функции;
подключение к ExecutionGateway — отдельный шаг (см. momentum_mm.BasicMarketMaker).
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Параметры
# ---------------------------------------------------------------------------


@dataclass
class SwingBreakoutParams:
    """Параметры стратегии и симуляции исполнения."""

    n_levels: int = 20
    k_volume: float = 1.5
    ma_period: int = 50
    take_profit_rub: float = 200.0
    stop_loss_rub: float = 100.0
    # Сколько рублей даёт движение цены на 1 пункт при позиции 1 лот (подставьте под ваш инструмент).
    rub_per_point: float = 1.0
    tick_size: float = 1.0
    slippage_ticks: int = 1
    # Комиссия доля от условного оборота (0.04% = 0.0004) на каждую сторону (вход и выход отдельно).
    commission_rate: float = 0.0004
    # Множитель к price*qty для базы комиссии (если брокер считает от номинала — подстройте).
    notional_scale: float = 1.0
    # Трейлинг: после прибыли >= activation (руб) тянуть стоп на gap_rub ниже/выше экстремума.
    trailing_activation_rub: float = 0.0
    trailing_gap_rub: float = 50.0
    # При одновременном касании TP и SL на одной свече: что считать первым.
    intrabar_priority: Literal["stop_first", "tp_first"] = "stop_first"


@dataclass
class TradeRecord:
    side: Literal["LONG", "SHORT"]
    entry_bar: int
    exit_bar: int
    entry_price: float
    exit_price: float
    pnl_gross_rub: float
    fees_rub: float
    pnl_net_rub: float
    exit_reason: str
    meta: dict[str, Any] = field(default_factory=dict)

    def to_log_line(self) -> str:
        return json.dumps({**asdict(self), "meta": self.meta}, ensure_ascii=False)


@dataclass
class BacktestResult:
    trades: list[TradeRecord]
    total_pnl_net_rub: float
    n_trades: int
    winrate: float | None
    max_drawdown_rub: float

    def summary(self) -> dict[str, Any]:
        return {
            "total_pnl_net_rub": self.total_pnl_net_rub,
            "n_trades": self.n_trades,
            "winrate": self.winrate,
            "max_drawdown_rub": self.max_drawdown_rub,
        }


# ---------------------------------------------------------------------------
# Индикаторы
# ---------------------------------------------------------------------------


def compute_indicators(
    df: pd.DataFrame,
    *,
    n: int,
    ma_period: int,
) -> pd.DataFrame:
    """
    Добавляет колонки:
      high_N, low_N — экстремумы за **предыдущие** N свечей (без lookahead).
      avg_volume — средний объём за предыдущие N.
      ma — SMA(close, ma_period) по **текущему** закрытию (на той же свече).
    """
    out = df.copy()
    high = out["high"].astype(float)
    low = out["low"].astype(float)
    vol = out["volume"].astype(float)
    close = out["close"].astype(float)

    # Уровни из прошлого окна (сдвиг на 1 бар: на баре i используем данные до i-1).
    roll_high = high.rolling(n, min_periods=n).max().shift(1)
    roll_low = low.rolling(n, min_periods=n).min().shift(1)
    roll_vol_mean = vol.rolling(n, min_periods=n).mean().shift(1)

    out["high_N"] = roll_high
    out["low_N"] = roll_low
    out["avg_volume"] = roll_vol_mean
    out["ma"] = close.rolling(ma_period, min_periods=ma_period).mean()
    return out


# ---------------------------------------------------------------------------
# Сигналы
# ---------------------------------------------------------------------------


def generate_signals(
    df: pd.DataFrame,
    *,
    k_volume: float,
) -> pd.Series:
    """
    Возвращает Series int8: 1 = LONG, -1 = SHORT, 0 = нет сигнала на закрытии свечи.
    Сигнал на **закрытии** текущей свечи при пробое уровня и объёме.
    """
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    vol = df["volume"].astype(float)
    hN = df["high_N"]
    lN = df["low_N"]
    av = df["avg_volume"]
    ma = df["ma"]

    vol_ok = vol > (av * k_volume)
    long_cond = (close > hN) & vol_ok & (close > ma)
    short_cond = (close < lN) & vol_ok & (close < ma)

    arr = np.zeros(len(df), dtype=np.int8)
    lc = long_cond.to_numpy()
    sc = short_cond.to_numpy()
    arr[lc] = 1
    arr[sc & ~lc] = -1
    return pd.Series(arr, index=df.index, dtype=np.int8)


# ---------------------------------------------------------------------------
# Исполнение / позиция (вспомогательные)
# ---------------------------------------------------------------------------


def _slip_price(price: float, side: Literal["buy", "sell"], tick: float, n_ticks: int) -> float:
    delta = tick * max(0, n_ticks)
    if side == "buy":
        return price + delta
    return price - delta


def _fee_rub(price: float, qty: float, rate: float, scale: float) -> float:
    return abs(price * qty * scale) * rate


def _points_from_rub(rub: float, rub_per_point: float) -> float:
    if rub_per_point <= 0:
        raise ValueError("rub_per_point must be > 0")
    return rub / rub_per_point


# ---------------------------------------------------------------------------
# Бэктест
# ---------------------------------------------------------------------------


def run_backtest(
    ohlcv: pd.DataFrame,
    params: SwingBreakoutParams | None = None,
    *,
    qty: float = 1.0,
    log_trades: bool = True,
) -> BacktestResult:
    """
    Проход по свечам:
    - Сигнал на close бара i → вход по **open** следующего бара со слиппеджем.
    - В позиции: проверка intrabar high/low на TP/SL в пунктах (из рублей через rub_per_point).
    - Опциональный трейлинг в рублях от экстремума в нашу сторону.
    """
    p = params or SwingBreakoutParams()
    df = ohlcv.copy()
    required = {"open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing columns: {missing}")

    df = compute_indicators(df, n=p.n_levels, ma_period=p.ma_period)
    df["signal"] = generate_signals(df, k_volume=p.k_volume)

    trades: list[TradeRecord] = []
    equity = 0.0
    peak = 0.0
    max_dd = 0.0

    pos: Literal["flat", "long", "short"] = "flat"
    entry_i = -1
    entry_price = 0.0
    entry_fee = 0.0
    tp_pts = _points_from_rub(p.take_profit_rub, p.rub_per_point)
    sl_pts = _points_from_rub(p.stop_loss_rub, p.rub_per_point)
    trail_gap_pts = _points_from_rub(p.trailing_gap_rub, p.rub_per_point) if p.trailing_gap_rub > 0 else 0.0
    trail_act_pts = _points_from_rub(p.trailing_activation_rub, p.rub_per_point) if p.trailing_activation_rub > 0 else 0.0

    best_favorable_price: float = 0.0  # long: highest high since entry; short: lowest low since entry
    trail_stop_price: float | None = None

    n = len(df)
    for i in range(n - 1):
        row = df.iloc[i]
        hi = float(row["high"])
        lo = float(row["low"])
        op = float(row["open"])
        cl = float(row["close"])

        if pos != "flat":
            exit_price: float | None = None
            reason = ""

            if pos == "long":
                tp_level = entry_price + tp_pts
                sl_level = entry_price - sl_pts
                # трейлинг
                if trail_act_pts > 0 and trail_gap_pts > 0:
                    if hi > best_favorable_price:
                        best_favorable_price = hi
                    if best_favorable_price - entry_price >= trail_act_pts:
                        cand = best_favorable_price - trail_gap_pts
                        trail_stop_price = cand if trail_stop_price is None else max(trail_stop_price, cand)
                        sl_level = max(sl_level, trail_stop_price)

                hit_tp = hi >= tp_level
                hit_sl = lo <= sl_level
                if hit_tp and hit_sl:
                    if p.intrabar_priority == "stop_first":
                        exit_price = _slip_price(sl_level, "sell", p.tick_size, p.slippage_ticks)
                        reason = "stop_loss"
                    else:
                        exit_price = _slip_price(tp_level, "sell", p.tick_size, p.slippage_ticks)
                        reason = "take_profit"
                elif hit_sl:
                    exit_price = _slip_price(sl_level, "sell", p.tick_size, p.slippage_ticks)
                    reason = "stop_loss"
                elif hit_tp:
                    exit_price = _slip_price(tp_level, "sell", p.tick_size, p.slippage_ticks)
                    reason = "take_profit"

            else:  # short
                tp_level = entry_price - tp_pts
                sl_level = entry_price + sl_pts
                if trail_act_pts > 0 and trail_gap_pts > 0:
                    if lo < best_favorable_price:
                        best_favorable_price = lo
                    if entry_price - best_favorable_price >= trail_act_pts:
                        cand = best_favorable_price + trail_gap_pts
                        trail_stop_price = cand if trail_stop_price is None else min(trail_stop_price, cand)
                        sl_level = min(sl_level, trail_stop_price)

                hit_tp = lo <= tp_level
                hit_sl = hi >= sl_level
                if hit_tp and hit_sl:
                    if p.intrabar_priority == "stop_first":
                        exit_price = _slip_price(sl_level, "buy", p.tick_size, p.slippage_ticks)
                        reason = "stop_loss"
                    else:
                        exit_price = _slip_price(tp_level, "buy", p.tick_size, p.slippage_ticks)
                        reason = "take_profit"
                elif hit_sl:
                    exit_price = _slip_price(sl_level, "buy", p.tick_size, p.slippage_ticks)
                    reason = "stop_loss"
                elif hit_tp:
                    exit_price = _slip_price(tp_level, "buy", p.tick_size, p.slippage_ticks)
                    reason = "take_profit"

            if exit_price is not None:
                exit_fee = _fee_rub(exit_price, qty, p.commission_rate, p.notional_scale)
                if pos == "long":
                    gross = (exit_price - entry_price) * qty * p.rub_per_point
                else:
                    gross = (entry_price - exit_price) * qty * p.rub_per_point
                fees = entry_fee + exit_fee
                net = gross - fees
                tr = TradeRecord(
                    side="LONG" if pos == "long" else "SHORT",
                    entry_bar=entry_i,
                    exit_bar=i,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    pnl_gross_rub=gross,
                    fees_rub=fees,
                    pnl_net_rub=net,
                    exit_reason=reason,
                    meta={"bar_time": str(df.index[i]) if hasattr(df.index[i], "__str__") else i},
                )
                trades.append(tr)
                if log_trades:
                    log.info("SWING_TRADE %s", tr.to_log_line())
                equity += net
                peak = max(peak, equity)
                max_dd = max(max_dd, peak - equity)
                pos = "flat"
                entry_i = -1
                trail_stop_price = None
                best_favorable_price = 0.0

        if pos == "flat" and i + 1 < n:
            sig = int(df.iloc[i]["signal"])
            if sig == 0:
                continue
            nxt = df.iloc[i + 1]
            nxt_open = float(nxt["open"])
            if sig == 1:
                entry_price = _slip_price(nxt_open, "buy", p.tick_size, p.slippage_ticks)
                pos = "long"
            else:
                entry_price = _slip_price(nxt_open, "sell", p.tick_size, p.slippage_ticks)
                pos = "short"
            entry_i = i + 1
            entry_fee = _fee_rub(entry_price, qty, p.commission_rate, p.notional_scale)
            best_favorable_price = float(nxt["high"]) if pos == "long" else float(nxt["low"])
            trail_stop_price = None

    wins = sum(1 for t in trades if t.pnl_net_rub > 0)
    losses = sum(1 for t in trades if t.pnl_net_rub < 0)
    n_tr = len(trades)
    wr = (wins / n_tr) if n_tr else None
    total_net = sum(t.pnl_net_rub for t in trades)

    return BacktestResult(
        trades=trades,
        total_pnl_net_rub=total_net,
        n_trades=n_tr,
        winrate=wr,
        max_drawdown_rub=max_dd,
    )


def load_ohlcv_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "time_utc" in df.columns:
        df["time_utc"] = pd.to_datetime(df["time_utc"], utc=True)
        df = df.set_index("time_utc")
    return df
