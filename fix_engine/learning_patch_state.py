"""
Состояние патча самообучения (динамические target_roi, max_adverse, max_hold, веса по тертилям спреда).
Сохранение в JSON + опционально в SQLite через economics_store.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fix_engine.adaptive_learning_targets import AdaptiveLearningTargets


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class LearningPatchState:
    """Параметры выхода и веса входа; подмешиваются к AdaptiveLearningTargets."""

    target_roi_abs: float = 0.05
    max_adverse_abs: float = 0.02
    max_hold_duration_ms: float = 3_000_000.0
    buy_weight_tertile: list[float] = field(default_factory=lambda: [1.0, 1.0, 1.0])
    sell_weight_tertile: list[float] = field(default_factory=lambda: [1.0, 1.0, 1.0])
    version: int = 1

    def __post_init__(self) -> None:
        self.buy_weight_tertile = _norm3(self.buy_weight_tertile)
        self.sell_weight_tertile = _norm3(self.sell_weight_tertile)

    def weight_for_side_tertile(self, *, side: str, tertile: int) -> float:
        idx = max(0, min(2, int(tertile)))
        s = str(side).strip().upper()
        if s in ("BUY", "1", "LONG"):
            return float(self.buy_weight_tertile[idx])
        return float(self.sell_weight_tertile[idx])


def _norm3(xs: list[float]) -> list[float]:
    out = [float(x) for x in xs[:3]]
    while len(out) < 3:
        out.append(1.0)
    return out[:3]


def align_patch_state_to_targets(
    state: LearningPatchState,
    targets: AdaptiveLearningTargets,
) -> LearningPatchState:
    """
    Подтянуть динамический патч к нижним границам из adaptive_learning_targets.json
    (целевые TP/SL/удержание не ниже/не выше заданных в файле для ключевых метрик).
    """
    return LearningPatchState(
        target_roi_abs=max(state.target_roi_abs, float(targets.TARGET_ROI_ABS)),
        max_adverse_abs=max(state.max_adverse_abs, float(targets.MAX_ADVERSE_ABS)),
        max_hold_duration_ms=min(state.max_hold_duration_ms, float(targets.MAX_HOLD_DURATION_MS)),
        buy_weight_tertile=list(state.buy_weight_tertile),
        sell_weight_tertile=list(state.sell_weight_tertile),
        version=state.version,
    )


def load_learning_patch_state(path: Path | None) -> LearningPatchState:
    if path is None or not path.is_file():
        return LearningPatchState()
    raw = json.loads(path.read_text(encoding="utf-8"))
    return LearningPatchState(
        target_roi_abs=float(raw.get("target_roi_abs", 0.05)),
        max_adverse_abs=float(raw.get("max_adverse_abs", 0.02)),
        max_hold_duration_ms=float(raw.get("max_hold_duration_ms", 3_000_000.0)),
        buy_weight_tertile=list(raw.get("buy_weight_tertile", [1.0, 1.0, 1.0])),
        sell_weight_tertile=list(raw.get("sell_weight_tertile", [1.0, 1.0, 1.0])),
        version=int(raw.get("version", 1)),
    )


def save_learning_patch_state(path: Path, state: LearningPatchState, *, effects: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {
        "updated_ts": _utc_now_iso(),
        "target_roi_abs": state.target_roi_abs,
        "max_adverse_abs": state.max_adverse_abs,
        "max_hold_duration_ms": state.max_hold_duration_ms,
        "buy_weight_tertile": list(state.buy_weight_tertile),
        "sell_weight_tertile": list(state.sell_weight_tertile),
        "version": state.version,
    }
    if effects:
        payload["last_learning_effects"] = effects
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def merge_exit_params(
    *,
    base_take_profit: float,
    base_stop_loss: float,
    targets: AdaptiveLearningTargets,
    patch: LearningPatchState,
) -> tuple[float, float, float]:
    """
    Эффективные TP/SL (деньги на позицию) и max_hold_ms.
    TP/SL — max из базы MM, целей адаптации и патча; удержание — min(верхние капы).
    """
    tp = max(
        float(base_take_profit),
        float(getattr(targets, "TARGET_ROI_ABS", 0.0) or 0.0),
        float(patch.target_roi_abs),
    )
    sl = max(
        float(base_stop_loss),
        float(getattr(targets, "MAX_ADVERSE_ABS", 0.0) or 0.0),
        float(patch.max_adverse_abs),
    )
    cap_targets = float(getattr(targets, "MAX_HOLD_DURATION_MS", 3_000_000.0))
    hold_ms = min(cap_targets, float(patch.max_hold_duration_ms))
    return tp, sl, hold_ms


def apply_learning_adjustment(
    *,
    state: LearningPatchState,
    targets: AdaptiveLearningTargets,
    win_rate: float,
    avg_pnl: float,
    adverse_rate: float,
    avg_mfe: float,
    avg_mae: float,
    avg_latency_ms: float,
    avg_spread: float,
    spread_tertile_win_rates_buy: list[float],
    spread_tertile_win_rates_sell: list[float],
) -> tuple[LearningPatchState, dict[str, Any]]:
    """
    Эвристика: подстроить выходы и веса под метрики (без внешних зависимостей).
    """
    effects: dict[str, Any] = {"inputs": {}, "deltas": {}}
    new = LearningPatchState(
        target_roi_abs=state.target_roi_abs,
        max_adverse_abs=state.max_adverse_abs,
        max_hold_duration_ms=state.max_hold_duration_ms,
        buy_weight_tertile=list(state.buy_weight_tertile),
        sell_weight_tertile=list(state.sell_weight_tertile),
        version=state.version + 1,
    )

    effects["inputs"] = {
        "win_rate": win_rate,
        "avg_pnl": avg_pnl,
        "adverse_rate": adverse_rate,
        "avg_mfe": avg_mfe,
        "avg_mae": avg_mae,
        "avg_latency_ms": avg_latency_ms,
        "avg_spread": avg_spread,
    }
    effects["targets_ref"] = {k: float(v) for k, v in targets.to_dict().items()}

    # Ориентиры из adaptive_learning_targets.json (не хардкод)
    tw = float(targets.TARGET_WIN_RATE)
    tap = float(targets.TARGET_AVG_PNL)
    mar = float(targets.MAX_ADVERSE_RATE)
    tmfe = float(targets.TARGET_MFE)
    tmae = float(targets.TARGET_MAE)
    troi = float(targets.TARGET_ROI_ABS)
    madv = float(targets.MAX_ADVERSE_ABS)
    cap_hold = float(targets.MAX_HOLD_DURATION_MS)
    lat_ref = float(targets.LEARNING_LATENCY_REF_MS)
    spread_ref = float(targets.LEARNING_SPREAD_REF)
    wr_floor = float(targets.LEARNING_WIN_RATE_FLOOR_RATIO)

    # База патча не ниже целей из JSON
    new.target_roi_abs = max(new.target_roi_abs, troi)
    new.max_adverse_abs = max(new.max_adverse_abs, madv)
    new.max_hold_duration_ms = min(new.max_hold_duration_ms, cap_hold)

    # Средний MFE ниже целевого — мягко поднимаем take-profit к TARGET_MFE (не выше tmfe)
    if avg_mfe < tmfe * 0.9 and win_rate < tw:
        delta = min(0.02, max(1e-6, (tmfe - avg_mfe) * 0.05))
        new.target_roi_abs = min(tmfe, new.target_roi_abs + delta)
        effects["deltas"]["target_roi_abs_toward_TARGET_MFE"] = delta
    elif avg_mfe > troi * 1.1 and win_rate < tw:
        delta = min(0.02, new.target_roi_abs * 0.05)
        new.target_roi_abs = min(tmfe, new.target_roi_abs + delta)
        effects["deltas"]["target_roi_abs"] = delta
    elif avg_pnl < tap and adverse_rate <= mar:
        delta = min(0.015, max(1e-6, (tap - avg_pnl) * 0.1))
        new.target_roi_abs = min(tmfe, new.target_roi_abs + delta)
        effects["deltas"]["target_roi_abs_vs_TARGET_AVG_PNL"] = delta
    elif win_rate < tw * wr_floor and adverse_rate > mar:
        delta = min(0.01, new.max_adverse_abs * 0.1)
        new.max_adverse_abs = max(madv, new.max_adverse_abs - delta)
        effects["deltas"]["max_adverse_abs"] = -delta

    if avg_mae > tmae * 1.1:
        delta = min(0.01, new.max_adverse_abs * 0.08)
        new.max_adverse_abs = max(madv, new.max_adverse_abs - delta)
        effects["deltas"]["max_adverse_abs_vs_TARGET_MAE"] = -delta

    # Удержание: отталкиваемся от MAX_HOLD_DURATION_MS и порогов из JSON
    lat_factor = 1.0 - min(0.25, max(0.0, (avg_latency_ms - lat_ref) / max(lat_ref, 1.0)) * 0.15)
    spr_factor = 1.0 - min(0.15, max(0.0, (avg_spread - spread_ref) / max(spread_ref, 1e-9)) * 0.1)
    new.max_hold_duration_ms = cap_hold * lat_factor * spr_factor
    effects["deltas"]["max_hold_duration_ms_factors"] = {
        "latency": lat_factor,
        "spread": spr_factor,
        "cap_ms": cap_hold,
    }

    # Веса по тертилям: сравнение с TARGET_WIN_RATE из файла
    mid_wr = tw * 0.9
    for i in range(3):
        bwr = spread_tertile_win_rates_buy[i] if i < len(spread_tertile_win_rates_buy) else 0.5
        swr = spread_tertile_win_rates_sell[i] if i < len(spread_tertile_win_rates_sell) else 0.5
        # выше вес => строже порог (умножаем t1 на вес в MM)
        if bwr < mid_wr:
            new.buy_weight_tertile[i] = min(1.5, new.buy_weight_tertile[i] * 1.05)
        elif bwr > tw:
            new.buy_weight_tertile[i] = max(0.7, new.buy_weight_tertile[i] * 0.98)
        if swr < mid_wr:
            new.sell_weight_tertile[i] = min(1.5, new.sell_weight_tertile[i] * 1.05)
        elif swr > tw:
            new.sell_weight_tertile[i] = max(0.7, new.sell_weight_tertile[i] * 0.98)
    for i in range(3):
        new.buy_weight_tertile[i] = min(1.6, max(0.55, new.buy_weight_tertile[i]))
        new.sell_weight_tertile[i] = min(1.6, max(0.55, new.sell_weight_tertile[i]))

    effects["new_state"] = asdict(new)
    return new, effects
