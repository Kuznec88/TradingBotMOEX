"""
Unified signal_score (0..1) для входов breakout/retest/continuation/pullback.

Факторы (каждая 0..1), итог — взвешенная сумма:
- impulse_strength — range / prev_max (diag_range_impulse_ratio)
- atr_expansion — наклон ATR (atr_slope)
- retest_speed — быстрее касание уровня — выше
- htf_score — колонка htf_score_long / short
- ema_distance — штраф за «перегрев» от EMA/VWAP (anti-chase)
- candle_structure — тело / range в сторону входа

Режим legacy: combine_impulse=True — один комбинированный импульс, как в ранней версии.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, replace
from typing import Any

import pandas as pd

from fix_engine.strategy.swing_breakout import RetestEntryIntent, SwingBreakoutParams, classify_setup_tag


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def classify_setup(intent: RetestEntryIntent, params: SwingBreakoutParams) -> str:
    return classify_setup_tag(intent, params)


def candle_body_score(row: pd.Series, side: int) -> float:
    o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
    rng = h - l
    if rng <= 1e-12:
        return 0.5
    body = abs(c - o)
    frac = body / rng
    if side == 1 and c > o:
        return _clamp01(0.5 + 0.5 * frac)
    if side == -1 and c < o:
        return _clamp01(0.5 + 0.5 * frac)
    return _clamp01(0.3 * frac)


def impulse_strength_score(intent: RetestEntryIntent) -> float:
    ri = intent.diag_range_impulse_ratio
    if ri is not None and math.isfinite(float(ri)):
        return _clamp01(float(ri) / 2.0)
    return 0.5


def atr_expansion_score(row: pd.Series) -> float:
    sl = row.get("atr_slope")
    if sl is not None and pd.notna(sl) and math.isfinite(float(sl)):
        return _clamp01((float(sl) - 0.9) / 0.35)
    return 0.5


def heat_score_side(row: pd.Series, side: int) -> float:
    """Штраф за «перегрев» в сторону входа относительно EMA/VWAP (score: меньше перегрева — выше)."""
    cl = float(row["close"])
    atr = float(row.get("atr", 0) or 0)
    if not math.isfinite(atr) or atr <= 0:
        return 0.7
    ema = row.get("ema_pullback")
    vw = row.get("vwap_approx")
    ref = float(ema) if ema is not None and pd.notna(ema) else cl
    if vw is not None and pd.notna(vw):
        ref = 0.6 * ref + 0.4 * float(vw)
    dist = (cl - ref) / max(atr, 1e-12)
    if side == 1:
        return _clamp01(1.0 - min(1.0, max(0.0, dist) / 2.5))
    return _clamp01(1.0 - min(1.0, max(0.0, -dist) / 2.5))


def ema_dist_atr_signed(row: pd.Series, side: int) -> float:
    """Signed distance close−ref in ATR units (для биннинга / research)."""
    cl = float(row["close"])
    atr = float(row.get("atr", 0) or 0)
    if not math.isfinite(atr) or atr <= 0:
        return float("nan")
    ema = row.get("ema_pullback")
    vw = row.get("vwap_approx")
    ref = float(ema) if ema is not None and pd.notna(ema) else cl
    if vw is not None and pd.notna(vw):
        ref = 0.6 * ref + 0.4 * float(vw)
    dist = (cl - ref) / max(atr, 1e-12)
    return float(dist) if side == 1 else float(-dist)


def candle_body_frac(row: pd.Series) -> float:
    o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
    rng = h - l
    if rng <= 1e-12:
        return 0.0
    return abs(c - o) / rng


def retest_speed_score(intent: RetestEntryIntent, params: SwingBreakoutParams) -> float:
    rb = max(0, int(intent.diag_retest_bars_to_touch))
    mx = max(2, int(params.max_retest_bars_fast))
    if rb == 0:
        return 0.55
    return _clamp01(1.0 - (rb - 1) / float(mx + 2))


def impulse_combo_score(intent: RetestEntryIntent, row: pd.Series) -> float:
    """Legacy: 0.55 * impulse_strength + 0.45 * atr_expansion (как раньше)."""
    a = impulse_strength_score(intent)
    b = atr_expansion_score(row)
    return _clamp01(0.55 * a + 0.45 * b)


def compute_signal_components(
    *,
    intent: RetestEntryIntent,
    row: pd.Series,
    params: SwingBreakoutParams,
) -> dict[str, float]:
    side = int(intent.side)
    return {
        "impulse_strength": impulse_strength_score(intent),
        "atr_expansion": atr_expansion_score(row),
        "retest_speed": retest_speed_score(intent, params),
        "htf_score": _clamp01(float(row.get("htf_score_long" if side == 1 else "htf_score_short", 1.0))),
        "ema_distance": heat_score_side(row, side),
        "candle_structure": candle_body_score(row, side),
    }


def raw_signal_diagnostics(
    intent: RetestEntryIntent,
    row: pd.Series,
    params: SwingBreakoutParams,
) -> dict[str, float]:
    side = int(intent.side)
    raw_htf = float(row.get("htf_score_long" if side == 1 else "htf_score_short", float("nan")))
    return {
        "htf_score_raw": raw_htf,
        "ema_dist_atr_signed": ema_dist_atr_signed(row, side),
        "candle_body_frac": candle_body_frac(row),
    }


def entry_meta_signal_fields(
    intent: RetestEntryIntent,
    row: pd.Series,
    params: SwingBreakoutParams,
    weights: SignalScoreWeights | None = None,
) -> dict[str, Any]:
    """Поля для merge в TradeRecord.meta: score, факторы 0..1, сырые диагностики."""
    score, _ = compute_unified_signal_score(
        intent=intent, row=row, params=params, weights=weights, apply_context_quality=True
    )
    comps = compute_signal_components(intent=intent, row=row, params=params)
    raw = raw_signal_diagnostics(intent, row, params)
    out: dict[str, Any] = {"signal_score": score}
    out.update({f"score_{k}": float(v) for k, v in comps.items()})
    out.update(raw)
    return out


@dataclass
class SignalScoreWeights:
    """Веса факторов (сумма активных = 1.0)."""

    combine_impulse: bool = False
    # legacy single block (если combine_impulse)
    w_impulse: float = 0.22
    # split: усилены импульс и скорость ретеста (по сравнению с проигрышными сделками на выборке)
    w_impulse_strength: float = 0.14
    w_atr_expansion: float = 0.10
    w_retest: float = 0.24
    w_htf: float = 0.16
    w_heat: float = 0.18
    w_body: float = 0.18

    def weight_vector(self) -> dict[str, float]:
        if self.combine_impulse:
            return {
                "impulse_combo": self.w_impulse,
                "retest_speed": self.w_retest,
                "htf_score": self.w_htf,
                "ema_distance": self.w_heat,
                "candle_structure": self.w_body,
            }
        return {
            "impulse_strength": self.w_impulse_strength,
            "atr_expansion": self.w_atr_expansion,
            "retest_speed": self.w_retest,
            "htf_score": self.w_htf,
            "ema_distance": self.w_heat,
            "candle_structure": self.w_body,
        }


def renormalize_weights_drop(
    w: SignalScoreWeights,
    drop: frozenset[str],
) -> SignalScoreWeights:
    """Ablation: обнулить факторы из drop и перенормировать оставшиеся (combine_impulse не трогаем)."""
    if w.combine_impulse:
        raise ValueError("renormalize_weights_drop requires combine_impulse=False")
    vec = {
        "impulse_strength": w.w_impulse_strength,
        "atr_expansion": w.w_atr_expansion,
        "retest_speed": w.w_retest,
        "htf_score": w.w_htf,
        "ema_distance": w.w_heat,
        "candle_structure": w.w_body,
    }
    for k in drop:
        vec[k] = 0.0
    s = sum(vec.values())
    if s <= 1e-12:
        return replace(w)
    fac = 1.0 / s
    return replace(
        w,
        w_impulse_strength=vec["impulse_strength"] * fac,
        w_atr_expansion=vec["atr_expansion"] * fac,
        w_retest=vec["retest_speed"] * fac,
        w_htf=vec["htf_score"] * fac,
        w_heat=vec["ema_distance"] * fac,
        w_body=vec["candle_structure"] * fac,
    )


@dataclass
class SignalScorePolicy:
    """Порог и опциональный top-N за календарный день (по Europe/Moscow из бара)."""

    min_score: float = 0.52
    use_top_n_per_day: bool = False
    top_n_per_day: int = 3
    weights: SignalScoreWeights | None = None
    # Мягкий множитель по alpha_* (если колонки есть на баре входа; иначе 1.0)
    apply_context_quality: bool = True
    # Жёсткий отсев «плохой ленты» (None = только мягкий множитель к score)
    hard_reject_context_below: float | None = None


def entry_context_quality(row: pd.Series) -> float:
    """
    Множитель [0.84, 1.0] по контексту сессии/режима (только прошлое + текущий бар).
    Без колонок quant — возвращает 1.0.
    """
    m = 1.0
    sb = row.get("alpha_session_bucket")
    if sb is not None and pd.notna(sb):
        sb = float(sb)
        # Выше bucket — ближе к вечеру в нормировке v1 — слегка штрафуем
        m *= 0.92 + 0.08 * (1.0 - sb)
    comp = row.get("alpha_compression")
    if comp is not None and pd.notna(comp):
        c = float(comp)
        if c > 0.72:
            m *= 0.96
    vr = row.get("alpha_v2_vol_regime")
    if vr is not None and pd.notna(vr):
        m *= 0.98 + 0.04 * float(vr)
    return max(0.78, min(1.0, m))


def compute_unified_signal_score(
    *,
    intent: RetestEntryIntent,
    row: pd.Series,
    params: SwingBreakoutParams,
    weights: SignalScoreWeights | None = None,
    apply_context_quality: bool = True,
) -> tuple[float, dict[str, float]]:
    w = weights or SignalScoreWeights()
    comps = compute_signal_components(intent=intent, row=row, params=params)
    if w.combine_impulse:
        c_imp = impulse_combo_score(intent, row)
        total = (
            w.w_impulse * c_imp
            + w.w_retest * comps["retest_speed"]
            + w.w_htf * comps["htf_score"]
            + w.w_heat * comps["ema_distance"]
            + w.w_body * comps["candle_structure"]
        )
        out_comps = {**comps, "impulse_combo": c_imp}
    else:
        total = (
            w.w_impulse_strength * comps["impulse_strength"]
            + w.w_atr_expansion * comps["atr_expansion"]
            + w.w_retest * comps["retest_speed"]
            + w.w_htf * comps["htf_score"]
            + w.w_heat * comps["ema_distance"]
            + w.w_body * comps["candle_structure"]
        )
        out_comps = dict(comps)
    cq = entry_context_quality(row) if apply_context_quality else 1.0
    total = _clamp01(total * cq)
    out_comps["context_quality"] = float(cq)
    return total, out_comps


class DailyTopNScores:
    """
    Онлайн-держим min-heap из N лучших score за календарный день (по дате бара).
    Это жадное приближение: первые сделки дня могут занять слоты до появления более сильных сигналов позже.
    Для строгого «top-N за весь день с оглядкой назад» нужен отдельный двухпроходный режим.
    """

    def __init__(self, n: int) -> None:
        self.n = max(1, int(n))
        self._day: Any = None
        self._heap: list[float] = []

    def reset_if_new_day(self, day_key: Any) -> None:
        if day_key != self._day:
            self._day = day_key
            self._heap = []

    def qualifies(self, score: float) -> bool:
        if len(self._heap) < self.n:
            heapq.heappush(self._heap, score)
            return True
        if score > self._heap[0]:
            heapq.heapreplace(self._heap, score)
            return True
        return False


def build_intent_filter_fn(
    policy: SignalScorePolicy,
    *,
    bar_day_key_fn: Any = None,
) -> Any:
    """
    Возвращает intent_filter_fn для run_backtest.
    Отбрасывает intent при score < min_score; опционально top-N/день поверх порога.
    """
    w = policy.weights or SignalScoreWeights()
    top = DailyTopNScores(policy.top_n_per_day) if policy.use_top_n_per_day else None

    def _day_from_row(i: int, row: pd.Series, df: pd.DataFrame) -> Any:
        if bar_day_key_fn is not None:
            return bar_day_key_fn(i, row, df)
        try:
            ts = df.index[i]
            return str(pd.Timestamp(ts).date())
        except Exception:
            return i // 96

    def intent_filter_fn(
        *,
        intent: RetestEntryIntent,
        i: int,
        row: pd.Series,
        df: pd.DataFrame,
        params: SwingBreakoutParams,
        prev_row: pd.Series | None = None,
        **__: Any,
    ) -> RetestEntryIntent | None:
        use_ctx = bool(policy.apply_context_quality)
        q = entry_context_quality(row) if use_ctx else 1.0
        thr = policy.hard_reject_context_below
        if thr is not None and q < float(thr):
            return None
        score, _comps = compute_unified_signal_score(
            intent=intent,
            row=row,
            params=params,
            weights=w,
            apply_context_quality=use_ctx,
        )
        if score < float(policy.min_score):
            return None
        if top is not None:
            dk = _day_from_row(i, row, df)
            top.reset_if_new_day(dk)
            if not top.qualifies(score):
                return None
        tag = classify_setup(intent, params)
        sm = max(0.15, min(1.5, float(intent.size_mult) * score))
        return replace(
            intent,
            signal_score=score,
            setup_tag=tag,
            size_mult=sm,
        )

    return intent_filter_fn


def priority_weight(setup_tag: str) -> int:
    """Выше число = выше приоритет (для разрешения конфликтов)."""
    return {
        "retest_fast": 100,
        "retest_late": 80,
        "continuation": 50,
        "pullback": 30,
    }.get(setup_tag, 40)
