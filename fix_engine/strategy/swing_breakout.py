"""
Свинг-стратегия: пробой N-свечного диапазона + ретест уровня + подтверждение закрытием,
фильтры объёма/ATR/старшего ТФ, TP/SL от структуры (RR), комиссии.

Расчёт на pandas DataFrame: open, high, low, close, volume
(индекс — время или колонка time_utc).

Живой раннер: только закрытые свечи для входа; intrabar — только выход (TP/SL/трейлинг).
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date, time as dt_time
from pathlib import Path
from typing import Any, Callable, Literal
from zoneinfo import ZoneInfo

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
    max_entry_spread_ticks: float = 2.0
    # Устаревший chop по range (тикам); при use_retest_fsm используется ATR (см. atr_min_*).
    chop_window_bars: int = 5
    chop_max_range_ticks: float = 5.0
    signal_on_close_only: bool = True
    take_profit_rub: float = 200.0
    stop_loss_rub: float = 100.0
    rub_per_point: float = 1.0
    tick_size: float = 1.0
    slippage_ticks: int = 1
    commission_rate: float = 0.0004
    notional_scale: float = 1.0
    trailing_activation_rub: float = 0.0
    trailing_gap_rub: float = 50.0
    intrabar_priority: Literal["stop_first", "tp_first"] = "stop_first"

    # --- Retest FSM + структурный риск (основной режим) ---
    use_retest_fsm: bool = True
    atr_period: int = 14
    # Не входить, если ATR слишком низкий (шум/пила). 0 = выкл.
    atr_min_ticks: float = 0.0
    atr_min_price: float = 0.0
    retest_touch_epsilon_ticks: float = 0.5
    retest_max_penetration_ticks: float = 4.0
    # Объём: спайк на пробое; при False — только мягкий фильтр (не блокирует без спайка).
    volume_spike_required: bool = True
    volume_directional: bool = True
    # HTF тренд: правило pandas resample, напр. "1h", "4h". Пусто — без HTF (всегда разрешён тренд).
    htf_resample_rule: str = "1h"
    # Второй старший ТФ (например 4h). Пустая строка — только primary.
    htf_secondary_resample_rule: str = "4h"
    # Вес primary при смешивании score (остальное — secondary): 0.55 ≈ приоритет 1h чуть выше 4h.
    htf_dual_primary_weight: float = 0.55
    # True: оба ТФ должны дать entry_ok (мало сделок на 5m/15m). False: OR + смешанный score.
    htf_dual_require_both: bool = False
    # Согласованность движения цены и объёма на HTF (rolling corr returns vs Δvolume).
    htf_use_volume_correlation: bool = True
    htf_pv_corr_window: int = 12
    # Насколько усиливаем htf_score при сильной PV-согласованности (0 = выкл.)
    htf_pv_score_influence: float = 0.15
    htf_sma_period: int = 100
    htf_hh_hl_lag: int = 3
    risk_reward_multiple: float = 2.0
    stop_buffer_ticks: float = 2.0
    # Трейлинг после +1R в цене (0 = выкл., использовать rub-трейлинг из legacy).
    trail_after_r_multiple: float = 1.0
    trail_gap_r_multiple: float = 0.35
    commission_min_profit_mult: float = 2.0
    # Лимитка: сколько баров ждать исполнения (бэктест и логика раннера).
    limit_order_timeout_bars: int = 4

    # --- Сила пробоя (0 = выкл. impulse_k) ---
    impulse_k: float = 0.35
    impulse_close_top_range_frac: float = 0.22
    impulse_body_vs_shadow: bool = True

    # --- Ретест: глубина / окно / уход цены ---
    retest_max_depth_ticks: float = 10.0
    retest_max_bars: int = 16
    retest_max_runaway_ticks: float = 120.0

    # --- Ложный пробой ---
    false_breakout_max_bars: int = 4

    # --- HTF: наклон SMA и перекупленность (0 = выкл. порог) ---
    htf_slope_lag: int = 5
    htf_min_slope_norm: float = 0.03
    htf_overextended_atr_mult: float = 2.2
    htf_atr_period: int = 14

    # --- Сессия MOEX (Europe/Moscow), пустые часы = без фильтра ---
    swing_session_tz: str = "Europe/Moscow"
    swing_trade_start_hhmm: str = "10:00"
    swing_trade_end_hhmm: str = "18:45"
    swing_lunch_skip: bool = True
    swing_lunch_start_hhmm: str = "12:45"
    swing_lunch_end_hhmm: str = "13:45"

    # --- Адаптивный стоп: sl_distance = max(buffer_ticks*tick, adaptive_stop_atr_mult*ATR) от уровня ---
    adaptive_stop_atr_mult: float = 1.15

    # --- Частичный выход на 1R (0 = выкл., типично 0.5) ---
    partial_exit_fraction: float = 0.0
    partial_exit_r_multiple: float = 1.0
    # "tp" — остаток только до TP; "trail" — после частичного включить трейлинг с 1R
    partial_remainder_mode: Literal["tp", "trail"] = "trail"

    # --- Time stop после входа ---
    post_entry_time_stop_bars: int = 0
    post_entry_time_stop_min_tp_progress: float = 0.12

    # --- Расширение волатильности ATR: atr / atr.shift(lag); 0 = выкл. ---
    atr_slope_lag: int = 5
    min_atr_expansion: float = 1.08
    # --- Импульс по range vs max(range) за lookback (без текущего бара); 0 = выкл. ---
    impulse_range_lookback: int = 10
    impulse_strength: float = 1.35
    # --- Ретест: отклонить, если первое касание позже N баров после пробоя (медленный дрейф) ---
    max_retest_bars_fast: int = 6
    # --- Режим: range_N / ATR; ниже порога — боковик / шум, не торгуем ---
    min_range_atr_ratio: float = 2.5
    # --- Лимиты частоты и просадки по серии ---
    max_trades_per_day: int = 5
    max_loss_streak: int = 3
    loss_cooldown_bars: int = 30
    # --- Гибридный вход: сильный импульс → рыночная заявка ---
    use_market_on_strong_breakout: bool = True
    market_impulse_mult: float = 1.12
    # --- Размер позиции от риска (₽ на сделку); 0 = фикс. qty из вызывающего кода ---
    risk_per_trade_rub: float = 0.0
    sizing_min_qty: float = 1.0
    sizing_max_qty: float = 100.0
    # --- CSV сделок (пусто = не писать) ---
    trades_log_csv_path: str = ""

    # --- HTF: hard = как раньше (htf_entry_*_ok); soft = порог по htf_score_* (колонки из compute_indicators) ---
    htf_gate_mode: Literal["hard", "soft"] = "hard"
    htf_score_min: float = 0.35
    # Первое касание уровня ретеста считается не раньше N баров после пробоя (late-retest сетапы).
    min_retest_bars_for_touch: int = 1
    # После +N*R нереализованной прибыли подтянуть стоп к безубытку (0 = выкл.)
    break_even_r_multiple: float = 0.0


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
        wins = [t for t in self.trades if t.pnl_net_rub > 0]
        losses = [t for t in self.trades if t.pnl_net_rub < 0]
        avg_win = sum(t.pnl_net_rub for t in wins) / len(wins) if wins else None
        avg_loss = sum(t.pnl_net_rub for t in losses) / len(losses) if losses else None
        expectancy = (self.total_pnl_net_rub / self.n_trades) if self.n_trades else None
        gross_profit = sum(t.pnl_net_rub for t in wins)
        gross_loss = abs(sum(t.pnl_net_rub for t in losses))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 1e-12 else None
        return {
            "total_pnl_net_rub": self.total_pnl_net_rub,
            "n_trades": self.n_trades,
            "winrate": self.winrate,
            "max_drawdown_rub": self.max_drawdown_rub,
            "avg_win_rub": avg_win,
            "avg_loss_rub": avg_loss,
            "expectancy_rub": expectancy,
            "profit_factor": profit_factor,
        }


@dataclass
class SwingTradeGovernor:
    """
    Дневной лимит сделок и пауза после серии убытков (в барах стратегии).
    Вызывайте on_new_bar на каждом новом закрытом баре; allow_new_entry в FSM.
    """

    params: SwingBreakoutParams
    trades_today: int = 0
    _session_day: date | None = None
    loss_streak: int = 0
    cooldown_bars_left: int = 0

    def on_new_bar(self, bar_time: pd.Timestamp | None) -> None:
        if self.cooldown_bars_left > 0:
            self.cooldown_bars_left -= 1
        if bar_time is None:
            return
        try:
            tz = ZoneInfo(self.params.swing_session_tz.strip() or "Europe/Moscow")
            ts = pd.Timestamp(bar_time)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            d = ts.tz_convert(tz).date()
        except Exception:
            d = pd.Timestamp(bar_time).date()
        if self._session_day != d:
            self._session_day = d
            self.trades_today = 0

    def allow_new_entry(self) -> bool:
        if self.cooldown_bars_left > 0:
            return False
        mx = int(self.params.max_trades_per_day)
        if mx > 0 and self.trades_today >= mx:
            return False
        return True

    def note_entry_opened(self) -> None:
        self.trades_today += 1

    def on_closed_trade_pnl(self, pnl_net_rub: float, *, is_partial: bool = False) -> None:
        if is_partial:
            return
        if pnl_net_rub < -1e-9:
            self.loss_streak += 1
            mls = int(self.params.max_loss_streak)
            if mls > 0 and self.loss_streak >= mls:
                self.cooldown_bars_left = max(self.cooldown_bars_left, int(self.params.loss_cooldown_bars))
                self.loss_streak = 0
        else:
            self.loss_streak = 0


def append_swing_trade_csv(path: Path | str, row: dict[str, Any]) -> None:
    """Добавить строку в trades_log.csv; при новых колонках переписывает файл с объединённым заголовком."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists() or p.stat().st_size == 0:
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(row.keys()))
            w.writeheader()
            w.writerow(row)
        return
    with p.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        prev_fields = list(reader.fieldnames or [])
        old_rows = list(reader)
    keys = list(dict.fromkeys(prev_fields + list(row.keys())))
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in old_rows:
            w.writerow({k: r.get(k, "") for k in keys})
        w.writerow({k: row.get(k, "") for k in keys})


def compute_position_qty_rub(
    p: SwingBreakoutParams,
    risk_per_unit: float,
    *,
    fallback_qty: float,
) -> float:
    """Контракты/лоты: risk_rub / (stop_distance * rub_per_point). 0 risk_per_trade_rub → fallback_qty."""
    if p.risk_per_trade_rub <= 0 or risk_per_unit <= 1e-12 or p.rub_per_point <= 1e-12:
        return float(fallback_qty)
    raw = p.risk_per_trade_rub / (risk_per_unit * p.rub_per_point)
    return float(max(p.sizing_min_qty, min(p.sizing_max_qty, raw)))


@dataclass
class SwingRetestFSMState:
    """Состояние ретест-FSM (синхронно live и бэктест)."""

    phase: Literal[
        "waiting_breakout",
        "waiting_retest",
        "waiting_confirm",
        "invalid_breakout",
    ] = "waiting_breakout"
    direction: int = 0
    level: float = 0.0
    retest_bars: int = 0
    extreme_since_breakout: float = 0.0
    breakout_strong: bool = False


@dataclass(frozen=True)
class RetestEntryIntent:
    side: int  # 1 long, -1 short
    limit_price: float
    stop_price: float
    take_profit_price: float
    risk_per_unit: float
    use_market_entry: bool = False
    entry_reason: str = "retest_limit_confirm"
    diag_atr_slope: float = float("nan")
    diag_range_impulse_ratio: float = float("nan")
    diag_retest_bars_to_touch: int = 0
    diag_range_atr_ratio: float = float("nan")
    diag_rr_multiple: float = float("nan")
    size_mult: float = 1.0
    signal_score: float = 1.0
    setup_tag: str = ""


def classify_setup_tag(it: RetestEntryIntent, params: SwingBreakoutParams) -> str:
    """Тип сетапа для логов/метрик (совместимо с signal_scoring.classify_setup)."""
    t = (it.setup_tag or "").strip()
    if t in ("continuation", "pullback", "retest_fast", "retest_late"):
        return t
    er = (it.entry_reason or "").lower()
    if "continuation" in er:
        return "continuation"
    if "pullback" in er or "ema" in er:
        return "pullback"
    rb = int(it.diag_retest_bars_to_touch)
    if rb <= 2:
        return "retest_fast"
    if rb >= 3:
        return "retest_late"
    return "retest_fast"


def intent_to_entry_meta(
    it: RetestEntryIntent,
    params: SwingBreakoutParams | None = None,
    *,
    row: pd.Series | None = None,
) -> dict[str, Any]:
    """Поля для TradeRecord.meta и trades_log.csv. При row — факторы score и сырые диагностики."""
    tag = (it.setup_tag or "").strip()
    if not tag and params is not None:
        tag = classify_setup_tag(it, params)
    base: dict[str, Any] = {
        "entry_reason": it.entry_reason,
        "impulse_strength_ratio": it.diag_range_impulse_ratio,
        "atr_slope": it.diag_atr_slope,
        "retest_bars_to_touch": it.diag_retest_bars_to_touch,
        "rr_multiple": it.diag_rr_multiple,
        "range_atr_ratio": it.diag_range_atr_ratio,
        "use_market_entry": it.use_market_entry,
        "size_mult": it.size_mult,
        "signal_score": it.signal_score,
        "setup_tag": tag,
    }
    if row is not None and params is not None:
        from fix_engine.strategy.signal_scoring import entry_meta_signal_fields

        extra = entry_meta_signal_fields(it, row, params)
        base.update(extra)
    return base


def _enrich_entry_meta_execution(
    meta: dict[str, Any],
    *,
    intent: RetestEntryIntent,
    initial_qty: float,
    entry_i: int,
    df: pd.DataFrame,
    params: SwingBreakoutParams,
) -> None:
    """Риск в рублях и время входа для research (без lookahead — только данные на момент входа)."""
    rp = float(params.rub_per_point)
    rpu = float(intent.risk_per_unit)
    q = float(initial_qty)
    meta["risk_rub"] = rpu * q * rp
    meta["entry_qty"] = q
    meta["entry_bar"] = int(entry_i)
    try:
        meta["entry_timestamp"] = str(df.index[entry_i])
    except Exception:
        meta["entry_timestamp"] = ""


# ---------------------------------------------------------------------------
# Индикаторы
# ---------------------------------------------------------------------------


def _atr_wilder(df: pd.DataFrame, period: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0 / max(1, period), adjust=False, min_periods=period).mean()


def _round_to_tick(price: float, tick: float) -> float:
    t = max(float(tick), 1e-12)
    return round(price / t) * t


def _parse_hhmm(s: str) -> tuple[int, int] | None:
    t = (s or "").strip()
    if not t:
        return None
    parts = t.replace(".", ":").split(":")
    try:
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return h % 24, m % 60
    except (ValueError, IndexError):
        return None


def _session_allows(bar_time: pd.Timestamp | None, p: SwingBreakoutParams) -> bool:
    if bar_time is None:
        return True
    hm_start = _parse_hhmm(p.swing_trade_start_hhmm)
    hm_end = _parse_hhmm(p.swing_trade_end_hhmm)
    if hm_start is None or hm_end is None:
        return True
    try:
        tz = ZoneInfo(p.swing_session_tz.strip() or "Europe/Moscow")
    except Exception:
        tz = ZoneInfo("Europe/Moscow")
    local = bar_time.tz_convert(tz) if bar_time.tzinfo else bar_time.tz_localize("UTC").tz_convert(tz)
    now_t = local.time()
    start_t = dt_time(hm_start[0], hm_start[1])
    end_t = dt_time(hm_end[0], hm_end[1])
    if start_t <= end_t:
        if not (start_t <= now_t <= end_t):
            return False
    else:
        if not (now_t >= start_t or now_t <= end_t):
            return False
    if p.swing_lunch_skip:
        ls = _parse_hhmm(p.swing_lunch_start_hhmm)
        le = _parse_hhmm(p.swing_lunch_end_hhmm)
        if ls is not None and le is not None:
            lunch_a = dt_time(ls[0], ls[1])
            lunch_b = dt_time(le[0], le[1])
            if lunch_a <= lunch_b:
                if lunch_a <= now_t <= lunch_b:
                    return False
            else:
                if now_t >= lunch_a or now_t <= lunch_b:
                    return False
    return True


def _impulse_long_ok(row: pd.Series, atr_v: float, p: SwingBreakoutParams) -> tuple[bool, str | None]:
    # Range vs max(range) за предыдущие бары (колонка из compute_indicators).
    if p.impulse_strength > 0:
        prev_max = row.get("impulse_prev_range_max")
        prev_max_f = float(prev_max) if pd.notna(prev_max) else float("nan")
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
        rng = h - l
        if not (np.isfinite(prev_max_f) and prev_max_f > 1e-12 and rng > prev_max_f * p.impulse_strength):
            return False, "weak_impulse_range"
    if p.impulse_k <= 0:
        return True, None
    if not np.isfinite(atr_v) or atr_v <= 0:
        return False, "weak_impulse"
    o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
    if c <= o:
        return False, "weak_impulse"
    body = c - o
    if body <= p.impulse_k * atr_v:
        return False, "weak_impulse"
    rng = h - l
    if rng <= 1e-12:
        return False, "weak_impulse"
    upper = h - max(o, c)
    lower = min(o, c) - l
    if p.impulse_body_vs_shadow and body <= max(upper, lower) + 1e-12:
        return False, "weak_impulse"
    top_frac = (h - c) / rng
    if top_frac > p.impulse_close_top_range_frac:
        return False, "weak_impulse"
    return True, None


def _impulse_short_ok(row: pd.Series, atr_v: float, p: SwingBreakoutParams) -> tuple[bool, str | None]:
    if p.impulse_strength > 0:
        prev_max = row.get("impulse_prev_range_max")
        prev_max_f = float(prev_max) if pd.notna(prev_max) else float("nan")
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
        rng = h - l
        if not (np.isfinite(prev_max_f) and prev_max_f > 1e-12 and rng > prev_max_f * p.impulse_strength):
            return False, "weak_impulse_range"
    if p.impulse_k <= 0:
        return True, None
    if not np.isfinite(atr_v) or atr_v <= 0:
        return False, "weak_impulse"
    o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
    if c >= o:
        return False, "weak_impulse"
    body = o - c
    if body <= p.impulse_k * atr_v:
        return False, "weak_impulse"
    rng = h - l
    if rng <= 1e-12:
        return False, "weak_impulse"
    lower = min(o, c) - l
    upper = h - max(o, c)
    if p.impulse_body_vs_shadow and body <= max(upper, lower) + 1e-12:
        return False, "weak_impulse"
    bot_frac = (c - l) / rng
    if bot_frac > p.impulse_close_top_range_frac:
        return False, "weak_impulse"
    return True, None


def _breakout_strong_market(row: pd.Series, p: SwingBreakoutParams) -> bool:
    """Сильный импульс для гибридного рыночного входа: range >= prev_max * impulse_strength * market_impulse_mult."""
    if not p.use_market_on_strong_breakout or p.impulse_strength <= 0:
        return False
    prev_max = row.get("impulse_prev_range_max")
    prev_max_f = float(prev_max) if pd.notna(prev_max) else float("nan")
    h, l_ = float(row["high"]), float(row["low"])
    rng = h - l_
    thr = p.impulse_strength * p.market_impulse_mult
    return bool(np.isfinite(prev_max_f) and prev_max_f > 1e-12 and rng >= prev_max_f * thr)


def _adaptive_stop_distance(atr_v: float, tick: float, p: SwingBreakoutParams) -> float:
    buf = p.stop_buffer_ticks * tick
    if p.adaptive_stop_atr_mult > 0 and np.isfinite(atr_v) and atr_v > 0:
        return max(buf, p.adaptive_stop_atr_mult * atr_v)
    return buf


def _htf_bullish_series(htf: pd.DataFrame, sma_period: int, hh_hl_lag: int) -> pd.Series:
    """По каждому закрытому HTF-бару: bullish = close>SMA или HH/HL vs лаг."""
    if htf.empty:
        return pd.Series(dtype=bool)
    c = htf["close"].astype(float)
    h = htf["high"].astype(float)
    l = htf["low"].astype(float)
    sma = c.rolling(sma_period, min_periods=sma_period).mean()
    lag = max(1, int(hh_hl_lag))
    hh_hl = (h > h.shift(lag)) & (l > l.shift(lag))
    bull = (c > sma) | hh_hl
    return bull.fillna(False)


def _htf_bearish_series(htf: pd.DataFrame, sma_period: int, hh_hl_lag: int) -> pd.Series:
    c = htf["close"].astype(float)
    h = htf["high"].astype(float)
    l = htf["low"].astype(float)
    sma = c.rolling(sma_period, min_periods=sma_period).mean()
    lag = max(1, int(hh_hl_lag))
    ll_lh = (h < h.shift(lag)) & (l < l.shift(lag))
    bear = (c < sma) | ll_lh
    return bear.fillna(False)


def _merge_htf_flags_to_rows(df: pd.DataFrame, htf_bull: pd.Series, htf_bear: pd.Series, ts: pd.Series) -> tuple[pd.Series, pd.Series]:
    """ffill последнего известного HTF-состояния на метки времени LTF."""
    if htf_bull.empty:
        b = pd.Series(True, index=df.index)
        be = pd.Series(True, index=df.index)
        return b, be
    # htf_bull index = HTF close times
    htf_idx = htf_bull.index
    bull_pts = pd.Series(htf_bull.values, index=pd.to_datetime(htf_idx, utc=True))
    bear_pts = pd.Series(htf_bear.values, index=pd.to_datetime(htf_idx, utc=True))
    bull_pts = bull_pts.sort_index()
    bear_pts = bear_pts.sort_index()
    ts_u = pd.to_datetime(ts, utc=True)
    # reindex: asof backward
    out_b = []
    out_be = []
    for t in ts_u:
        try:
            sub = bull_pts.loc[:t]
            out_b.append(bool(sub.iloc[-1]) if len(sub) else True)
        except Exception:
            out_b.append(True)
        try:
            sub2 = bear_pts.loc[:t]
            out_be.append(bool(sub2.iloc[-1]) if len(sub2) else True)
        except Exception:
            out_be.append(True)
    return pd.Series(out_b, index=df.index), pd.Series(out_be, index=df.index)


def _merge_htf_float_to_rows(df: pd.DataFrame, htf_vals: pd.Series, ts: pd.Series, *, default: float = 0.5) -> pd.Series:
    """Последнее известное HTF-значение (asof backward) на метки LTF."""
    if htf_vals.empty or len(htf_vals.dropna()) == 0:
        return pd.Series(float(default), index=df.index)
    pts = pd.Series(htf_vals.values, index=pd.to_datetime(htf_vals.index, utc=True)).sort_index()
    ts_u = pd.to_datetime(ts, utc=True)
    out: list[float] = []
    for t in ts_u:
        try:
            sub = pts.loc[:t]
            out.append(float(sub.iloc[-1]) if len(sub) else default)
        except Exception:
            out.append(default)
    return pd.Series(out, index=df.index)


def _htf_price_volume_quality(ohlc: pd.DataFrame, window: int) -> pd.Series:
    """
    0..1: rolling Pearson corr(close.pct_change(), volume.pct_change()) на HTF, затем (corr+1)/2.
    Без lookahead: использует только закрытые HTF-бары в окне.
    """
    c = ohlc["close"].astype(float)
    v = ohlc["volume"].astype(float).replace(0, np.nan)
    ret = c.pct_change()
    vchg = v.pct_change()
    w = max(4, int(window))
    corr = ret.rolling(w, min_periods=max(3, w // 3)).corr(vchg)
    q = ((corr.fillna(0.0) + 1.0) / 2.0).clip(0.0, 1.0)
    return q


def _htf_entry_flags_from_ohlc(ohlc: pd.DataFrame, p: SwingBreakoutParams) -> tuple[pd.Series, pd.Series, pd.Series]:
    """entry_long_ok, entry_short_ok, pv_quality на индексе HTF ohlc."""
    c_htf = ohlc["close"].astype(float)
    sma_htf = c_htf.rolling(p.htf_sma_period, min_periods=p.htf_sma_period).mean()
    atr_htf = _atr_wilder(ohlc, max(2, p.htf_atr_period))
    lag_sl = max(1, int(p.htf_slope_lag))
    slope_raw = sma_htf - sma_htf.shift(lag_sl)
    slope_norm = slope_raw / atr_htf.replace(0, np.nan)
    h_tf = ohlc["high"].astype(float)
    l_tf = ohlc["low"].astype(float)
    lag_hh = max(1, int(p.htf_hh_hl_lag))
    hh_hl = (h_tf > h_tf.shift(lag_hh)) & (l_tf > l_tf.shift(lag_hh))
    ll_lh = (h_tf < h_tf.shift(lag_hh)) & (l_tf < l_tf.shift(lag_hh))
    trend_long = (c_htf > sma_htf) | hh_hl
    trend_short = (c_htf < sma_htf) | ll_lh
    min_sl = float(p.htf_min_slope_norm)
    if min_sl > 0:
        slope_ok_l = slope_norm >= min_sl
        slope_ok_s = (-slope_norm) >= min_sl
    else:
        slope_ok_l = pd.Series(True, index=ohlc.index)
        slope_ok_s = pd.Series(True, index=ohlc.index)
    om = float(p.htf_overextended_atr_mult)
    if om > 0:
        over_l = (c_htf - sma_htf) > om * atr_htf
        over_s = (sma_htf - c_htf) > om * atr_htf
    else:
        over_l = pd.Series(False, index=ohlc.index)
        over_s = pd.Series(False, index=ohlc.index)
    entry_l = trend_long & slope_ok_l.fillna(False) & ~over_l.fillna(False)
    entry_s = trend_short & slope_ok_s.fillna(False) & ~over_s.fillna(False)
    pv = _htf_price_volume_quality(ohlc, int(p.htf_pv_corr_window))
    return entry_l.fillna(False), entry_s.fillna(False), pv


def compute_indicators(
    df: pd.DataFrame,
    *,
    n: int,
    ma_period: int,
    params: SwingBreakoutParams | None = None,
) -> pd.DataFrame:
    """
    high_N, low_N — экстремумы за предыдущие N баров (без lookahead).
    avg_volume, ma, atr; при заданном htf_resample_rule — htf_bull / htf_bear (ffill на бар).
    """
    p = params or SwingBreakoutParams()
    out = df.copy()
    high = out["high"].astype(float)
    low = out["low"].astype(float)
    vol = out["volume"].astype(float)
    close = out["close"].astype(float)

    roll_high = high.rolling(n, min_periods=n).max().shift(1)
    roll_low = low.rolling(n, min_periods=n).min().shift(1)
    roll_vol_mean = vol.rolling(n, min_periods=n).mean().shift(1)

    out["high_N"] = roll_high
    out["low_N"] = roll_low
    out["avg_volume"] = roll_vol_mean
    out["ma"] = close.rolling(ma_period, min_periods=ma_period).mean()
    out["atr"] = _atr_wilder(out, p.atr_period)
    # --- Edge filters: расширение ATR, режим range/ATR, база для импульса по range ---
    hl_rng = high - low
    lag_as = max(1, int(p.atr_slope_lag))
    atr_ser = out["atr"].astype(float)
    out["atr_slope"] = atr_ser / atr_ser.shift(lag_as).replace(0, np.nan)
    out["range_N"] = (roll_high - roll_low).astype(float)
    out["range_atr_ratio"] = out["range_N"] / atr_ser.replace(0, np.nan)
    lb_ir = max(2, int(p.impulse_range_lookback))
    out["impulse_prev_range_max"] = hl_rng.rolling(lb_ir, min_periods=lb_ir).max().shift(1)

    out["htf_bull"] = True
    out["htf_bear"] = True
    rule = (p.htf_resample_rule or "").strip()
    if rule:
        tmp = out.copy()
        if "time_utc" in tmp.columns:
            tmp["_ts"] = pd.to_datetime(tmp["time_utc"], utc=True)
        elif isinstance(tmp.index, pd.DatetimeIndex):
            tmp["_ts"] = pd.to_datetime(tmp.index, utc=True)
        else:
            tmp = None  # type: ignore[assignment]
        if tmp is not None and "_ts" in tmp.columns:
            ts_col = tmp["_ts"]
            tmp = tmp.set_index("_ts").sort_index()
            ohlc = tmp.resample(rule).agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            ).dropna(how="any")
            if len(ohlc) >= max(p.htf_sma_period, p.htf_hh_hl_lag, p.htf_slope_lag) + 2:
                hb = _htf_bullish_series(ohlc, p.htf_sma_period, p.htf_hh_hl_lag)
                hbe = _htf_bearish_series(ohlc, p.htf_sma_period, p.htf_hh_hl_lag)
                ts_u = pd.to_datetime(ts_col, utc=True)
                out["htf_bull"], out["htf_bear"] = _merge_htf_flags_to_rows(out, hb, hbe, ts_u)

                entry_l1, entry_s1, pv1 = _htf_entry_flags_from_ohlc(ohlc, p)
                el1, es1 = _merge_htf_flags_to_rows(out, entry_l1.fillna(False), entry_s1.fillna(False), ts_u)
                pv1_ltf = _merge_htf_float_to_rows(out, pv1, ts_u, default=0.5)

                rule2 = (p.htf_secondary_resample_rule or "").strip()
                w1 = float(p.htf_dual_primary_weight)
                w2 = max(0.0, min(1.0, 1.0 - w1))
                if rule2 and rule2 != rule:
                    ohlc2 = tmp.resample(rule2).agg(
                        {
                            "open": "first",
                            "high": "max",
                            "low": "min",
                            "close": "last",
                            "volume": "sum",
                        }
                    ).dropna(how="any")
                    if len(ohlc2) >= max(p.htf_sma_period, p.htf_hh_hl_lag, p.htf_slope_lag) + 2:
                        entry_l2, entry_s2, pv2 = _htf_entry_flags_from_ohlc(ohlc2, p)
                        el2, es2 = _merge_htf_flags_to_rows(out, entry_l2.fillna(False), entry_s2.fillna(False), ts_u)
                        pv2_ltf = _merge_htf_float_to_rows(out, pv2, ts_u, default=0.5)
                        if p.htf_dual_require_both:
                            el = el1 & el2
                            es = es1 & es2
                        else:
                            el = el1 | el2
                            es = es1 | es2
                        pv_blend = (w1 * pv1_ltf + w2 * pv2_ltf).clip(0.0, 1.0)
                    else:
                        el, es = el1, es1
                        pv_blend = pv1_ltf
                else:
                    el, es = el1, es1
                    pv_blend = pv1_ltf

                out["htf_entry_long_ok"] = el
                out["htf_entry_short_ok"] = es
                out["htf_pv_quality"] = pv_blend
                infl = float(p.htf_pv_score_influence)
                base_long = 0.25 + 0.75 * el.astype(float)
                base_short = 0.25 + 0.75 * es.astype(float)
                if p.htf_use_volume_correlation and infl > 0:
                    fac = (1.0 - infl) + infl * pv_blend
                    out["htf_score_long"] = (base_long * fac).clip(0.25, 1.0).astype(float)
                    out["htf_score_short"] = (base_short * fac).clip(0.25, 1.0).astype(float)
                else:
                    out["htf_score_long"] = base_long.astype(float)
                    out["htf_score_short"] = base_short.astype(float)
    if "htf_entry_long_ok" not in out.columns:
        out["htf_entry_long_ok"] = True
        out["htf_entry_short_ok"] = True
    if "htf_score_long" not in out.columns:
        hel = out["htf_entry_long_ok"].astype(bool)
        hes = out["htf_entry_short_ok"].astype(bool)
        out["htf_score_long"] = (0.25 + 0.75 * hel.astype(float)).astype(float)
        out["htf_score_short"] = (0.25 + 0.75 * hes.astype(float)).astype(float)
    if "htf_pv_quality" not in out.columns:
        out["htf_pv_quality"] = pd.Series(1.0, index=out.index)
    return out


# ---------------------------------------------------------------------------
# FSM шаг (один закрытый бар)
# ---------------------------------------------------------------------------


def retest_fsm_step(
    state: SwingRetestFSMState,
    *,
    row: pd.Series,
    params: SwingBreakoutParams,
    prev_row: pd.Series | None = None,
    bar_time: pd.Timestamp | None = None,
    governor: SwingTradeGovernor | None = None,
) -> tuple[SwingRetestFSMState, RetestEntryIntent | None, list[str]]:
    """
    Обновление FSM на закрытии бара. Возвращает новое состояние, опционально намерение входа, лог-события.
    governor: дневной лимит сделок и cooldown после серии убытков (on_new_bar вызывает снаружи каждый бар).
    """
    p = params
    tick = max(float(p.tick_size), 1e-12)
    events: list[str] = []

    def atr_ok(atr_v: float) -> bool:
        if p.atr_min_price > 0 and atr_v < p.atr_min_price:
            return False
        if p.atr_min_ticks > 0 and atr_v < p.atr_min_ticks * tick:
            return False
        return True

    close = float(row["close"])
    hi = float(row["high"])
    lo = float(row["low"])
    op = float(row["open"])
    vol = float(row["volume"])
    hN = row.get("high_N")
    lN = row.get("low_N")
    av = row.get("avg_volume")
    atr_v = float(row.get("atr", float("nan")))
    hb = bool(row.get("htf_bull", True))
    hbe = bool(row.get("htf_bear", True))
    htf_long_ok = bool(row.get("htf_entry_long_ok", True))
    htf_short_ok = bool(row.get("htf_entry_short_ok", True))

    if not (np.isfinite(close) and np.isfinite(hi) and np.isfinite(lo)):
        return state, None, events

    if not np.isfinite(atr_v):
        atr_v = 0.0

    av_f = float(av) if pd.notna(av) else float("nan")
    vol_spike = bool(np.isfinite(av_f) and av_f > 0 and vol > av_f * p.k_volume)
    vol_soft = vol_spike or (not p.volume_spike_required)

    eps = p.retest_touch_epsilon_ticks * tick
    pen = p.retest_max_penetration_ticks * tick
    depth_lim = p.retest_max_depth_ticks * tick
    runaway_lim = p.retest_max_runaway_ticks * tick
    max_rb = max(1, int(p.retest_max_bars))
    fb_bars = max(1, int(p.false_breakout_max_bars))

    def commission_allows(entry_px: float, tp_px: float, risk: float) -> bool:
        if risk <= 0 or p.commission_min_profit_mult <= 0:
            return True
        gross = abs(tp_px - entry_px) * p.rub_per_point
        fees = _fee_rub(entry_px, 1.0, p.commission_rate, p.notional_scale) + _fee_rub(
            tp_px, 1.0, p.commission_rate, p.notional_scale
        )
        return gross >= p.commission_min_profit_mult * fees

    def _diag_float(name: str) -> float:
        v = row.get(name)
        return float(v) if v is not None and pd.notna(v) else float("nan")

    def build_long_intent(level: float, *, touch_bar_nb: int) -> tuple[RetestEntryIntent | None, list[str]]:
        lim = _round_to_tick(level, tick)
        dist = _adaptive_stop_distance(atr_v, tick, p)
        sl = lim - dist
        risk = lim - sl
        ev: list[str] = []
        if risk <= 0:
            return None, ev + ["skip_bad_risk"]
        tp = lim + p.risk_reward_multiple * risk
        tp = _round_to_tick(tp, tick)
        if not commission_allows(lim, tp, risk):
            ev.append("skip_commission")
            return None, ev
        prev_max = _diag_float("impulse_prev_range_max")
        rng_bar = hi - lo
        ratio_imp = (rng_bar / prev_max) if (np.isfinite(prev_max) and prev_max > 1e-12) else float("nan")
        rr_m = abs(tp - lim) / max(risk, 1e-12)
        use_mkt = bool(st.breakout_strong)
        reason = "strong_breakout_market" if use_mkt else "retest_limit_confirm"
        sm = float(row.get("htf_score_long", 1.0))
        rm = row.get("regime_size_mult")
        if rm is not None and pd.notna(rm):
            sm *= float(rm)
        sm = max(0.2, min(1.5, sm))
        return (
            RetestEntryIntent(
                side=1,
                limit_price=lim,
                stop_price=sl,
                take_profit_price=tp,
                risk_per_unit=risk,
                use_market_entry=use_mkt,
                entry_reason=reason,
                diag_atr_slope=_diag_float("atr_slope"),
                diag_range_impulse_ratio=ratio_imp,
                diag_retest_bars_to_touch=int(touch_bar_nb),
                diag_range_atr_ratio=_diag_float("range_atr_ratio"),
                diag_rr_multiple=rr_m,
                size_mult=sm,
            ),
            ev,
        )

    def build_short_intent(level: float, *, touch_bar_nb: int) -> tuple[RetestEntryIntent | None, list[str]]:
        lim = _round_to_tick(level, tick)
        dist = _adaptive_stop_distance(atr_v, tick, p)
        sl = lim + dist
        risk = sl - lim
        ev: list[str] = []
        if risk <= 0:
            return None, ev + ["skip_bad_risk"]
        tp = lim - p.risk_reward_multiple * risk
        tp = _round_to_tick(tp, tick)
        if not commission_allows(lim, tp, risk):
            ev.append("skip_commission")
            return None, ev
        prev_max = _diag_float("impulse_prev_range_max")
        rng_bar = hi - lo
        ratio_imp = (rng_bar / prev_max) if (np.isfinite(prev_max) and prev_max > 1e-12) else float("nan")
        rr_m = abs(lim - tp) / max(risk, 1e-12)
        use_mkt = bool(st.breakout_strong)
        reason = "strong_breakout_market" if use_mkt else "retest_limit_confirm"
        sm = float(row.get("htf_score_short", 1.0))
        rm = row.get("regime_size_mult")
        if rm is not None and pd.notna(rm):
            sm *= float(rm)
        sm = max(0.2, min(1.5, sm))
        return (
            RetestEntryIntent(
                side=-1,
                limit_price=lim,
                stop_price=sl,
                take_profit_price=tp,
                risk_per_unit=risk,
                use_market_entry=use_mkt,
                entry_reason=reason,
                diag_atr_slope=_diag_float("atr_slope"),
                diag_range_impulse_ratio=ratio_imp,
                diag_retest_bars_to_touch=int(touch_bar_nb),
                diag_range_atr_ratio=_diag_float("range_atr_ratio"),
                diag_rr_multiple=rr_m,
                size_mult=sm,
            ),
            ev,
        )

    def try_long_breakout() -> SwingRetestFSMState | None:
        if governor is not None and not governor.allow_new_entry():
            return None
        if not _session_allows(bar_time, p):
            return None
        if not atr_ok(atr_v):
            return None
        if not (np.isfinite(float(hN)) and hb):
            return None
        if p.htf_gate_mode == "soft":
            if float(row.get("htf_score_long", 0.0)) < float(p.htf_score_min):
                events.append("entry_skipped:htf_soft_low")
                return None
        elif not htf_long_ok:
            events.append("entry_skipped:overextended_trend")
            return None
        # Боковик: узкий N-барный диапазон относительно ATR.
        if p.min_range_atr_ratio > 0:
            rar = row.get("range_atr_ratio")
            if pd.isna(rar) or not np.isfinite(float(rar)) or float(rar) < p.min_range_atr_ratio:
                events.append("breakout_rejected:flat_regime")
                return None
        # Волатильность не расширяется — не ловим шумовой пробой.
        if p.min_atr_expansion > 0:
            slop = row.get("atr_slope")
            if pd.isna(slop) or not np.isfinite(float(slop)) or float(slop) < p.min_atr_expansion:
                events.append("breakout_rejected:atr_not_expanding")
                return None
        level = float(hN)
        if close <= level:
            return None
        if p.volume_spike_required and not vol_spike:
            return None
        if not vol_soft:
            return None
        if p.volume_directional and close <= op:
            return None
        ok_i, _why = _impulse_long_ok(row, atr_v, p)
        if not ok_i:
            events.append("breakout_rejected:weak_impulse")
            return None
        events.append("breakout_long")
        strong = _breakout_strong_market(row, p)
        return SwingRetestFSMState(
            phase="waiting_retest",
            direction=1,
            level=level,
            retest_bars=0,
            extreme_since_breakout=hi,
            breakout_strong=strong,
        )

    def try_short_breakout() -> SwingRetestFSMState | None:
        if governor is not None and not governor.allow_new_entry():
            return None
        if not _session_allows(bar_time, p):
            return None
        if not atr_ok(atr_v):
            return None
        if not (np.isfinite(float(lN)) and hbe):
            return None
        if p.htf_gate_mode == "soft":
            if float(row.get("htf_score_short", 0.0)) < float(p.htf_score_min):
                events.append("entry_skipped:htf_soft_low")
                return None
        elif not htf_short_ok:
            events.append("entry_skipped:overextended_trend")
            return None
        if p.min_range_atr_ratio > 0:
            rar = row.get("range_atr_ratio")
            if pd.isna(rar) or not np.isfinite(float(rar)) or float(rar) < p.min_range_atr_ratio:
                events.append("breakout_rejected:flat_regime")
                return None
        if p.min_atr_expansion > 0:
            slop = row.get("atr_slope")
            if pd.isna(slop) or not np.isfinite(float(slop)) or float(slop) < p.min_atr_expansion:
                events.append("breakout_rejected:atr_not_expanding")
                return None
        level = float(lN)
        if close >= level:
            return None
        if p.volume_spike_required and not vol_spike:
            return None
        if not vol_soft:
            return None
        if p.volume_directional and close >= op:
            return None
        ok_i, _why = _impulse_short_ok(row, atr_v, p)
        if not ok_i:
            events.append("breakout_rejected:weak_impulse")
            return None
        events.append("breakout_short")
        strong = _breakout_strong_market(row, p)
        return SwingRetestFSMState(
            phase="waiting_retest",
            direction=-1,
            level=level,
            retest_bars=0,
            extreme_since_breakout=lo,
            breakout_strong=strong,
        )

    def long_invalid(level: float) -> bool:
        return close < level - pen

    def long_touch(level: float) -> bool:
        if nb < int(p.min_retest_bars_for_touch):
            return False
        if lo < level - depth_lim:
            return False
        return lo <= level + eps and close >= level - pen

    def long_confirm(level: float) -> bool:
        return close > level

    def short_invalid(level: float) -> bool:
        return close > level + pen

    def short_touch(level: float) -> bool:
        if nb < int(p.min_retest_bars_for_touch):
            return False
        if hi > level + depth_lim:
            return False
        return hi >= level - eps and close <= level + pen

    def short_confirm(level: float) -> bool:
        return close < level

    st = state

    if st.phase == "invalid_breakout":
        return SwingRetestFSMState(), None, events + ["fsm_recover_invalid_breakout"]

    if st.phase == "waiting_breakout":
        ns = try_long_breakout()
        if ns is not None:
            return ns, None, events
        ns2 = try_short_breakout()
        if ns2 is not None:
            return ns2, None, events
        return st, None, events

    level = float(st.level)
    d = st.direction
    nb = st.retest_bars + 1

    if d == 1:
        ex = max(st.extreme_since_breakout, hi) if st.extreme_since_breakout > 0 else hi
        if nb > max_rb:
            events.append("retest_rejected:timeout_bars")
            return SwingRetestFSMState(), None, events
        if ex - level > runaway_lim:
            events.append("retest_rejected:runaway")
            return SwingRetestFSMState(), None, events
        if nb <= fb_bars and close < level:
            events.append("breakout_rejected:false_breakout")
            return SwingRetestFSMState(phase="invalid_breakout", direction=0, level=0.0), None, events
        if lo < level - depth_lim and (st.phase == "waiting_retest" or st.phase == "waiting_confirm"):
            events.append("retest_rejected:too_deep")
            return SwingRetestFSMState(), None, events

        if long_invalid(level):
            events.append("invalidate_long")
            return SwingRetestFSMState(), None, events

        base = SwingRetestFSMState(
            phase=st.phase,
            direction=1,
            level=level,
            retest_bars=nb,
            extreme_since_breakout=ex,
            breakout_strong=st.breakout_strong,
        )

        if st.phase == "waiting_retest":
            if long_touch(level):
                mf = int(p.max_retest_bars_fast)
                if mf > 0 and nb > mf:
                    events.append("retest_rejected:slow_retest")
                    return SwingRetestFSMState(), None, events
                events.append("retest_long")
                if long_confirm(level):
                    intent, ev_extra = build_long_intent(level, touch_bar_nb=nb)
                    events.extend(ev_extra)
                    if intent is None:
                        return SwingRetestFSMState(), None, events
                    events.append("ready_long")
                    return SwingRetestFSMState(), intent, events
                return (
                    SwingRetestFSMState(
                        phase="waiting_confirm",
                        direction=1,
                        level=level,
                        retest_bars=nb,
                        extreme_since_breakout=ex,
                        breakout_strong=st.breakout_strong,
                    ),
                    None,
                    events,
                )
            return base, None, events

        if st.phase == "waiting_confirm":
            if long_confirm(level):
                intent, ev_extra = build_long_intent(level, touch_bar_nb=st.retest_bars)
                events.extend(ev_extra)
                if intent is None:
                    return SwingRetestFSMState(), None, events
                events.append("ready_long")
                return SwingRetestFSMState(), intent, events
            return base, None, events

    if d == -1:
        ex = min(st.extreme_since_breakout, lo)
        if nb > max_rb:
            events.append("retest_rejected:timeout_bars")
            return SwingRetestFSMState(), None, events
        if level - ex > runaway_lim:
            events.append("retest_rejected:runaway")
            return SwingRetestFSMState(), None, events
        if nb <= fb_bars and close > level:
            events.append("breakout_rejected:false_breakout")
            return SwingRetestFSMState(phase="invalid_breakout", direction=0, level=0.0), None, events
        if hi > level + depth_lim and (st.phase == "waiting_retest" or st.phase == "waiting_confirm"):
            events.append("retest_rejected:too_deep")
            return SwingRetestFSMState(), None, events

        if short_invalid(level):
            events.append("invalidate_short")
            return SwingRetestFSMState(), None, events

        base = SwingRetestFSMState(
            phase=st.phase,
            direction=-1,
            level=level,
            retest_bars=nb,
            extreme_since_breakout=ex,
            breakout_strong=st.breakout_strong,
        )

        if st.phase == "waiting_retest":
            if short_touch(level):
                mf = int(p.max_retest_bars_fast)
                if mf > 0 and nb > mf:
                    events.append("retest_rejected:slow_retest")
                    return SwingRetestFSMState(), None, events
                events.append("retest_short")
                if short_confirm(level):
                    intent, ev_extra = build_short_intent(level, touch_bar_nb=nb)
                    events.extend(ev_extra)
                    if intent is None:
                        return SwingRetestFSMState(), None, events
                    events.append("ready_short")
                    return SwingRetestFSMState(), intent, events
                return (
                    SwingRetestFSMState(
                        phase="waiting_confirm",
                        direction=-1,
                        level=level,
                        retest_bars=nb,
                        extreme_since_breakout=ex,
                        breakout_strong=st.breakout_strong,
                    ),
                    None,
                    events,
                )
            return base, None, events

        if st.phase == "waiting_confirm":
            if short_confirm(level):
                intent, ev_extra = build_short_intent(level, touch_bar_nb=st.retest_bars)
                events.extend(ev_extra)
                if intent is None:
                    return SwingRetestFSMState(), None, events
                events.append("ready_short")
                return SwingRetestFSMState(), intent, events
            return base, None, events

    return SwingRetestFSMState(), None, events


# ---------------------------------------------------------------------------
# Сигналы
# ---------------------------------------------------------------------------


def generate_signals(
    df: pd.DataFrame,
    *,
    k_volume: float,
    params: SwingBreakoutParams | None = None,
) -> pd.Series:
    """
    int8: 1 LONG, -1 SHORT, 0 — на закрытии бара.
    При use_retest_fsm — сигнал на баре подтверждения ретеста (как в FSM).
    Иначе — классический пробой + объём + MA.
    """
    p = params or SwingBreakoutParams()
    if p.use_retest_fsm:
        return _generate_retest_signals(df, p)
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    vol = df["volume"].astype(float)
    hN = df["high_N"]
    lN = df["low_N"]
    av = df["avg_volume"]
    ma = df["ma"]
    kv = float(k_volume)

    vol_ok = vol > (av * kv)
    long_cond = (close > hN) & vol_ok & (close > ma)
    short_cond = (close < lN) & vol_ok & (close < ma)

    arr = np.zeros(len(df), dtype=np.int8)
    lc = long_cond.to_numpy()
    sc = short_cond.to_numpy()
    arr[lc] = 1
    arr[sc & ~lc] = -1
    return pd.Series(arr, index=df.index, dtype=np.int8)


def _bar_timestamp(df: pd.DataFrame, i: int, row: pd.Series) -> pd.Timestamp | None:
    if isinstance(df.index, pd.DatetimeIndex):
        return pd.Timestamp(df.index[i])
    t = row.get("time_utc")
    if t is not None:
        return pd.Timestamp(t)
    nm = getattr(row, "name", None)
    return pd.Timestamp(nm) if isinstance(nm, pd.Timestamp) else None


def _generate_retest_signals(df: pd.DataFrame, p: SwingBreakoutParams) -> pd.Series:
    arr = np.zeros(len(df), dtype=np.int8)
    st = SwingRetestFSMState()
    prev: pd.Series | None = None
    for i in range(len(df)):
        row = df.iloc[i]
        st, intent, _ev = retest_fsm_step(
            st,
            row=row,
            prev_row=prev,
            params=p,
            bar_time=_bar_timestamp(df, i, row),
        )
        if intent is not None:
            arr[i] = intent.side
        prev = row
    return pd.Series(arr, index=df.index, dtype=np.int8)


# ---------------------------------------------------------------------------
# Исполнение / позиция
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


def _run_backtest_retest(
    df: pd.DataFrame,
    p: SwingBreakoutParams,
    *,
    qty: float,
    log_trades: bool,
    extra_intent_fn: Callable[..., RetestEntryIntent | None] | None = None,
    intent_filter_fn: Callable[..., RetestEntryIntent | None] | None = None,
) -> BacktestResult:
    """Бэктест: FSM на закрытии, лимит, intrabar выход, опционально partial @1R и time-stop."""
    trades: list[TradeRecord] = []
    equity = 0.0
    peak = 0.0
    max_dd = 0.0

    governor = SwingTradeGovernor(p)
    st = SwingRetestFSMState()
    prev: pd.Series | None = None
    open_entry_meta: dict[str, Any] | None = None

    pos: Literal["flat", "long", "short"] = "flat"
    entry_i = -1
    entry_price = 0.0
    entry_fee_total = 0.0
    initial_qty = 0.0
    working_qty = 0.0
    stop_level = 0.0
    tp_level = 0.0
    risk = 0.0
    best_fav = 0.0
    trail_stop: float | None = None
    partial_done = False
    bars_in_trade = 0
    best_tp_progress = 0.0

    pending: tuple[int, RetestEntryIntent, int] | None = None
    be_armed = False

    n = len(df)

    def trail_arm_pts() -> float:
        if partial_done and p.partial_remainder_mode == "tp":
            return 0.0
        return p.trail_after_r_multiple * risk if p.trail_after_r_multiple > 0 else 0.0

    def trail_gap_pts() -> float:
        if partial_done and p.partial_remainder_mode == "tp":
            return 0.0
        return p.trail_gap_r_multiple * risk if p.trail_gap_r_multiple > 0 else 0.0

    def _append_trade(
        *,
        exit_px: float,
        exit_qty: float,
        reason: str,
        bar_idx: int,
        entry_fee_alloc: float,
    ) -> None:
        nonlocal equity, peak, max_dd, open_entry_meta
        xf = _fee_rub(exit_px, exit_qty, p.commission_rate, p.notional_scale)
        if pos == "long":
            gross = (exit_px - entry_price) * exit_qty * p.rub_per_point
        else:
            gross = (entry_price - exit_px) * exit_qty * p.rub_per_point
        fees = entry_fee_alloc + xf
        net = gross - fees
        meta: dict[str, Any] = {
            "bar_time": str(df.index[bar_idx]) if hasattr(df.index[bar_idx], "__str__") else bar_idx,
            "qty": exit_qty,
        }
        if open_entry_meta:
            meta = {**open_entry_meta, **meta}
        fg = abs(gross) if abs(gross) > 1e-12 else 1e-12
        meta["commission_to_gross"] = fees / fg
        tr = TradeRecord(
            side="LONG" if pos == "long" else "SHORT",
            entry_bar=entry_i,
            exit_bar=bar_idx,
            entry_price=entry_price,
            exit_price=exit_px,
            pnl_gross_rub=gross,
            fees_rub=fees,
            pnl_net_rub=net,
            exit_reason=reason,
            meta=meta,
        )
        trades.append(tr)
        if log_trades:
            log.info("SWING_TRADE %s", tr.to_log_line())
        equity += net
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
        governor.on_closed_trade_pnl(net, is_partial=(reason == "partial_tp"))
        log_path = (p.trades_log_csv_path or "").strip()
        if log_path:
            csv_row = {
                "entry_bar": tr.entry_bar,
                "exit_bar": tr.exit_bar,
                "side": tr.side,
                "entry_price": tr.entry_price,
                "exit_price": tr.exit_price,
                "pnl_net_rub": tr.pnl_net_rub,
                "pnl_gross_rub": tr.pnl_gross_rub,
                "fees_rub": tr.fees_rub,
                "commission_to_gross": meta.get("commission_to_gross", ""),
                "exit_reason": tr.exit_reason,
                "entry_reason": meta.get("entry_reason", ""),
                "atr_slope": meta.get("atr_slope", ""),
                "impulse_strength_ratio": meta.get("impulse_strength_ratio", ""),
                "retest_bars_to_touch": meta.get("retest_bars_to_touch", ""),
                "rr_multiple": meta.get("rr_multiple", ""),
                "range_atr_ratio": meta.get("range_atr_ratio", ""),
            }
            append_swing_trade_csv(log_path, csv_row)

    for i in range(n):
        row = df.iloc[i]
        hi = float(row["high"])
        lo = float(row["low"])
        cl = float(row["close"])
        filled_this_bar = False
        bar_t = _bar_timestamp(df, i, row)
        governor.on_new_bar(bar_t)

        if pos != "flat":
            bars_in_trade += 1
            if pos == "long":
                denom = tp_level - entry_price
                if denom > 1e-12:
                    best_tp_progress = max(best_tp_progress, (hi - entry_price) / denom)
            else:
                denom = entry_price - tp_level
                if denom > 1e-12:
                    best_tp_progress = max(best_tp_progress, (entry_price - lo) / denom)

            if (
                p.post_entry_time_stop_bars > 0
                and bars_in_trade >= p.post_entry_time_stop_bars
                and best_tp_progress < p.post_entry_time_stop_min_tp_progress
            ):
                ex_px = _slip_price(cl, "sell" if pos == "long" else "buy", p.tick_size, p.slippage_ticks)
                alloc = entry_fee_total * (working_qty / max(initial_qty, 1e-12))
                _append_trade(
                    exit_px=ex_px,
                    exit_qty=working_qty,
                    reason="time_stop",
                    bar_idx=i,
                    entry_fee_alloc=alloc,
                )
                pos = "flat"
                entry_i = -1
                trail_stop = None
                best_fav = 0.0
                partial_done = False
                be_armed = False
                bars_in_trade = 0
                best_tp_progress = 0.0
                st = SwingRetestFSMState()
                open_entry_meta = None
            else:
                exit_price: float | None = None
                reason = ""

                if (
                    not partial_done
                    and 0.0 < p.partial_exit_fraction < 1.0
                    and p.partial_exit_r_multiple > 0
                ):
                    rmul = p.partial_exit_r_multiple
                    part_qty = working_qty * p.partial_exit_fraction
                    if pos == "long" and part_qty > 1e-12 and hi >= entry_price + rmul * risk:
                        px = _slip_price(entry_price + rmul * risk, "sell", p.tick_size, p.slippage_ticks)
                        ef_alloc = entry_fee_total * (part_qty / max(initial_qty, 1e-12))
                        _append_trade(exit_px=px, exit_qty=part_qty, reason="partial_tp", bar_idx=i, entry_fee_alloc=ef_alloc)
                        working_qty -= part_qty
                        entry_fee_total -= ef_alloc
                        partial_done = True
                    elif pos == "short" and part_qty > 1e-12 and lo <= entry_price - rmul * risk:
                        px = _slip_price(entry_price - rmul * risk, "buy", p.tick_size, p.slippage_ticks)
                        ef_alloc = entry_fee_total * (part_qty / max(initial_qty, 1e-12))
                        _append_trade(exit_px=px, exit_qty=part_qty, reason="partial_tp", bar_idx=i, entry_fee_alloc=ef_alloc)
                        working_qty -= part_qty
                        entry_fee_total -= ef_alloc
                        partial_done = True

                if pos != "flat" and working_qty < 1e-9:
                    pos = "flat"
                    entry_i = -1
                    trail_stop = None
                    best_fav = 0.0
                    partial_done = False
                    be_armed = False
                    bars_in_trade = 0
                    best_tp_progress = 0.0
                    st = SwingRetestFSMState()
                    open_entry_meta = None
                elif pos != "flat":
                    if pos == "long":
                        sl_l = stop_level
                        tp_l = tp_level
                        if p.break_even_r_multiple > 0 and not be_armed and risk > 1e-12:
                            if hi >= entry_price + p.break_even_r_multiple * risk:
                                sl_l = max(sl_l, entry_price)
                                stop_level = max(stop_level, entry_price)
                                be_armed = True
                        t_arm = trail_arm_pts()
                        t_gap = trail_gap_pts()
                        if t_arm > 0 and t_gap > 0:
                            if hi > best_fav:
                                best_fav = hi
                            if best_fav - entry_price >= t_arm:
                                cand = best_fav - t_gap
                                trail_stop = cand if trail_stop is None else max(trail_stop, cand)
                                sl_l = max(sl_l, trail_stop)

                        hit_tp = hi >= tp_l
                        hit_sl = lo <= sl_l
                        if hit_tp and hit_sl:
                            if p.intrabar_priority == "stop_first":
                                exit_price = _slip_price(sl_l, "sell", p.tick_size, p.slippage_ticks)
                                reason = "stop_loss"
                            else:
                                exit_price = _slip_price(tp_l, "sell", p.tick_size, p.slippage_ticks)
                                reason = "take_profit"
                        elif hit_sl:
                            exit_price = _slip_price(sl_l, "sell", p.tick_size, p.slippage_ticks)
                            reason = "stop_loss"
                        elif hit_tp:
                            exit_price = _slip_price(tp_l, "sell", p.tick_size, p.slippage_ticks)
                            reason = "take_profit"
                    else:
                        sl_l = stop_level
                        tp_l = tp_level
                        if p.break_even_r_multiple > 0 and not be_armed and risk > 1e-12:
                            if lo <= entry_price - p.break_even_r_multiple * risk:
                                sl_l = min(sl_l, entry_price)
                                stop_level = min(stop_level, entry_price)
                                be_armed = True
                        t_arm = trail_arm_pts()
                        t_gap = trail_gap_pts()
                        if t_arm > 0 and t_gap > 0:
                            if best_fav <= 0:
                                best_fav = lo
                            else:
                                best_fav = min(best_fav, lo)
                            if entry_price - best_fav >= t_arm:
                                cand = best_fav + t_gap
                                trail_stop = cand if trail_stop is None else min(trail_stop, cand)
                                sl_l = min(sl_l, trail_stop)

                        hit_tp = lo <= tp_l
                        hit_sl = hi >= sl_l
                        if hit_tp and hit_sl:
                            if p.intrabar_priority == "stop_first":
                                exit_price = _slip_price(sl_l, "buy", p.tick_size, p.slippage_ticks)
                                reason = "stop_loss"
                            else:
                                exit_price = _slip_price(tp_l, "buy", p.tick_size, p.slippage_ticks)
                                reason = "take_profit"
                        elif hit_sl:
                            exit_price = _slip_price(sl_l, "buy", p.tick_size, p.slippage_ticks)
                            reason = "stop_loss"
                        elif hit_tp:
                            exit_price = _slip_price(tp_l, "buy", p.tick_size, p.slippage_ticks)
                            reason = "take_profit"

                    if exit_price is not None:
                        alloc = entry_fee_total
                        _append_trade(
                            exit_px=exit_price,
                            exit_qty=working_qty,
                            reason=reason,
                            bar_idx=i,
                            entry_fee_alloc=alloc,
                        )
                        pos = "flat"
                        entry_i = -1
                        trail_stop = None
                        best_fav = 0.0
                        partial_done = False
                        be_armed = False
                        bars_in_trade = 0
                        best_tp_progress = 0.0
                        entry_fee_total = 0.0
                        st = SwingRetestFSMState()
                        open_entry_meta = None

        if pos == "flat" and pending is not None:
            _start_bar, intent, waited = pending
            lim = intent.limit_price
            filled = False
            if intent.side == 1 and lo <= lim:
                entry_price = _slip_price(lim, "buy", p.tick_size, 0)
                filled = True
            elif intent.side == -1 and hi >= lim:
                entry_price = _slip_price(lim, "sell", p.tick_size, 0)
                filled = True

            if filled:
                pos = "long" if intent.side == 1 else "short"
                entry_i = i
                q_fill = compute_position_qty_rub(p, intent.risk_per_unit, fallback_qty=qty) * float(
                    getattr(intent, "size_mult", 1.0)
                )
                initial_qty = float(q_fill)
                working_qty = float(q_fill)
                entry_fee_total = _fee_rub(entry_price, initial_qty, p.commission_rate, p.notional_scale)
                stop_level = intent.stop_price
                tp_level = intent.take_profit_price
                risk = intent.risk_per_unit
                best_fav = hi if pos == "long" else lo
                trail_stop = None
                partial_done = False
                be_armed = False
                bars_in_trade = 0
                best_tp_progress = 0.0
                pending = None
                filled_this_bar = True
                st = SwingRetestFSMState()
                open_entry_meta = intent_to_entry_meta(intent, p, row=row)
                _enrich_entry_meta_execution(
                    open_entry_meta,
                    intent=intent,
                    initial_qty=initial_qty,
                    entry_i=i,
                    df=df,
                    params=p,
                )
                governor.note_entry_opened()
            else:
                waited += 1
                if waited >= p.limit_order_timeout_bars:
                    pending = None
                else:
                    pending = (_start_bar, intent, waited)

        if pos == "flat" and pending is None and not filled_this_bar:
            st, intent, _ = retest_fsm_step(
                st, row=row, prev_row=prev, params=p, bar_time=bar_t, governor=governor
            )
            if intent is None and extra_intent_fn is not None:
                intent = extra_intent_fn(
                    i=i,
                    row=row,
                    df=df,
                    params=p,
                    governor=governor,
                    bar_time=bar_t,
                    main_fsm_state=st,
                )
            if intent is not None and intent_filter_fn is not None:
                intent = intent_filter_fn(
                    intent=intent,
                    i=i,
                    row=row,
                    df=df,
                    params=p,
                    prev_row=prev,
                )
            if intent is not None:
                q_fill = compute_position_qty_rub(p, intent.risk_per_unit, fallback_qty=qty) * float(
                    getattr(intent, "size_mult", 1.0)
                )
                if intent.use_market_entry:
                    side_sl: Literal["buy", "sell"] = "buy" if intent.side == 1 else "sell"
                    entry_price = _slip_price(cl, side_sl, p.tick_size, p.slippage_ticks)
                    pos = "long" if intent.side == 1 else "short"
                    entry_i = i
                    initial_qty = float(q_fill)
                    working_qty = float(q_fill)
                    entry_fee_total = _fee_rub(entry_price, initial_qty, p.commission_rate, p.notional_scale)
                    stop_level = intent.stop_price
                    tp_level = intent.take_profit_price
                    risk = intent.risk_per_unit
                    best_fav = hi if pos == "long" else lo
                    trail_stop = None
                    partial_done = False
                    be_armed = False
                    bars_in_trade = 0
                    best_tp_progress = 0.0
                    filled_this_bar = True
                    st = SwingRetestFSMState()
                    open_entry_meta = intent_to_entry_meta(intent, p, row=row)
                    _enrich_entry_meta_execution(
                        open_entry_meta,
                        intent=intent,
                        initial_qty=initial_qty,
                        entry_i=i,
                        df=df,
                        params=p,
                    )
                    governor.note_entry_opened()
                else:
                    pending = (i, intent, 0)

        prev = row

    wins = sum(1 for t in trades if t.pnl_net_rub > 0)
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


def run_backtest(
    ohlcv: pd.DataFrame,
    params: SwingBreakoutParams | None = None,
    *,
    qty: float = 1.0,
    log_trades: bool = True,
    extra_intent_fn: Callable[..., RetestEntryIntent | None] | None = None,
    intent_filter_fn: Callable[..., RetestEntryIntent | None] | None = None,
    pre_indicators: bool = False,
) -> BacktestResult:
    p = params or SwingBreakoutParams()
    df = ohlcv.copy()
    required = {"open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing columns: {missing}")

    if not pre_indicators:
        df = compute_indicators(df, n=p.n_levels, ma_period=p.ma_period, params=p)

    if p.use_retest_fsm:
        return _run_backtest_retest(
            df,
            p,
            qty=qty,
            log_trades=log_trades,
            extra_intent_fn=extra_intent_fn,
            intent_filter_fn=intent_filter_fn,
        )

    df["signal"] = generate_signals(df, k_volume=p.k_volume, params=p)

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

    best_favorable_price: float = 0.0
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

            else:
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
