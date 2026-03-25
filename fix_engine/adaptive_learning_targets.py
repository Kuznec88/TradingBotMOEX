"""
Целевые показатели для блока самообучения (KPI vs targets).
Загрузка из JSON или значения по умолчанию.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any


DEFAULT_TARGETS_JSON = """
{
  "TARGET_WIN_RATE": 0.6,
  "TARGET_AVG_PNL": 0.002,
  "MAX_ADVERSE_RATE": 0.2,
  "TARGET_MFE": 0.05,
  "TARGET_MAE": 0.02,
  "TARGET_ROI_ABS": 0.05,
  "MAX_ADVERSE_ABS": 0.02,
  "MAX_HOLD_DURATION_MS": 3000000,
  "TRAILING_STOP_PCT": 0.0,
  "LEARNING_LATENCY_REF_MS": 80.0,
  "LEARNING_SPREAD_REF": 0.03,
  "LEARNING_WIN_RATE_FLOOR_RATIO": 0.85
}
"""


@dataclass(frozen=True)
class AdaptiveLearningTargets:
    """Ключевые метрики и пороги — задаются в adaptive_learning_targets.json (источник истины)."""

    TARGET_WIN_RATE: float = 0.6
    TARGET_AVG_PNL: float = 0.002
    MAX_ADVERSE_RATE: float = 0.2
    TARGET_MFE: float = 0.05
    TARGET_MAE: float = 0.02
    TARGET_ROI_ABS: float = 0.05
    MAX_ADVERSE_ABS: float = 0.02
    MAX_HOLD_DURATION_MS: float = 3_000_000.0
    TRAILING_STOP_PCT: float = 0.0
    LEARNING_LATENCY_REF_MS: float = 80.0
    LEARNING_SPREAD_REF: float = 0.03
    LEARNING_WIN_RATE_FLOOR_RATIO: float = 0.85

    def to_dict(self) -> dict[str, float]:
        return {f.name: float(getattr(self, f.name)) for f in fields(self)}


def load_adaptive_learning_targets(path: Path | None) -> AdaptiveLearningTargets:
    if path is None or not path.exists():
        return AdaptiveLearningTargets()
    raw = json.loads(path.read_text(encoding="utf-8"))
    kw: dict[str, Any] = {}
    for f in fields(AdaptiveLearningTargets):
        if f.name in raw:
            kw[f.name] = float(raw[f.name])
    base = AdaptiveLearningTargets(**kw) if kw else AdaptiveLearningTargets()
    # Совместимость: если в JSON только TARGET_MFE/TARGET_MAE — дублируем в ROI/ADVERSE при отсутствии ключей
    patch: dict[str, float] = {}
    if "TARGET_ROI_ABS" not in raw and "TARGET_MFE" in raw:
        patch["TARGET_ROI_ABS"] = float(raw["TARGET_MFE"])
    if "MAX_ADVERSE_ABS" not in raw and "TARGET_MAE" in raw:
        patch["MAX_ADVERSE_ABS"] = float(raw["TARGET_MAE"])
    if not patch:
        return base
    merged = {f.name: float(getattr(base, f.name)) for f in fields(AdaptiveLearningTargets)}
    merged.update(patch)
    return AdaptiveLearningTargets(**merged)


def gap_vs_targets(
    *,
    win_rate: float,
    avg_pnl: float,
    adverse_rate: float,
    avg_mfe: float,
    avg_mae: float,
    targets: AdaptiveLearningTargets,
) -> dict[str, float]:
    """Signed gaps: positive = metric below target (bad for win/avg/mfe), etc."""
    return {
        "win_rate_gap": float(targets.TARGET_WIN_RATE - win_rate),
        "avg_pnl_gap": float(targets.TARGET_AVG_PNL - avg_pnl),
        "adverse_gap": float(adverse_rate - targets.MAX_ADVERSE_RATE),
        "mfe_gap": float(targets.TARGET_MFE - avg_mfe),
        "mae_gap": float(avg_mae - targets.TARGET_MAE),
    }
