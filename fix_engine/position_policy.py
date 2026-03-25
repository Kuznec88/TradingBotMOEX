"""
Удержание позиции, точки выхода (TP/SL/max hold) и виртуальный баланс с учётом комиссий.
Единицы: PnL / MFE / MAE в валюте инструмента (как в round_trip_analytics).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from adaptive_learning_targets import AdaptiveLearningTargets


@dataclass
class OpenPositionSnapshot:
    entry_price: float
    entry_wall_time: datetime
    entry_mono: float
    qty: float
    direction: str
    spread_at_entry: float
    latency_ms: float
    entry_exec_id: str = ""


@dataclass
class VirtualBalanceBook:
    """Виртуальный баланс: сумма net_pnl по сделкам (net уже после комиссий в пайплайне) + учёт fees отдельно."""

    realized_net: float = 0.0
    cumulative_fees: float = 0.0
    trade_count: int = 0

    def apply_close(self, *, net_pnl: float, fees: float) -> None:
        self.realized_net += float(net_pnl)
        self.cumulative_fees += float(fees)
        self.trade_count += 1

    def to_dict(self) -> dict[str, float | int]:
        return {
            "realized_net": self.realized_net,
            "cumulative_fees": self.cumulative_fees,
            "trade_count": self.trade_count,
        }


@dataclass
class PositionPolicyRuntime:
    """Состояние для одного символа MM (длинная позиция >0, короткая <0)."""

    snapshot: OpenPositionSnapshot | None = None
    peak_unrealized: float = 0.0
    trough_unrealized: float = 0.0
    virtual: VirtualBalanceBook = field(default_factory=VirtualBalanceBook)

    def on_position_open(
        self,
        *,
        entry_price: float,
        qty: float,
        direction_label: str,
        spread_at_entry: float,
        latency_ms: float,
        entry_mono: float,
        entry_exec_id: str = "",
    ) -> None:
        self.snapshot = OpenPositionSnapshot(
            entry_price=float(entry_price),
            entry_wall_time=datetime.now(timezone.utc),
            entry_mono=float(entry_mono),
            qty=float(qty),
            direction=str(direction_label),
            spread_at_entry=float(spread_at_entry),
            latency_ms=float(latency_ms),
            entry_exec_id=str(entry_exec_id),
        )
        self.peak_unrealized = 0.0
        self.trough_unrealized = 0.0

    def on_position_flat(self) -> None:
        self.snapshot = None
        self.peak_unrealized = 0.0
        self.trough_unrealized = 0.0

    def update_excursions(self, unrealized: float) -> None:
        if unrealized > self.peak_unrealized:
            self.peak_unrealized = unrealized
        if unrealized < self.trough_unrealized:
            self.trough_unrealized = unrealized

    def evaluate_exit(
        self,
        *,
        unrealized: float,
        now_mono: float,
        take_profit_abs: float,
        stop_loss_abs: float,
        max_hold_ms: float,
        targets: AdaptiveLearningTargets,
        policy_enabled: bool,
        skip_stop_while_in_profit: bool = True,
    ) -> tuple[str | None, dict[str, Any]]:
        """
        Возвращает (reason_code | None, detail).
        take_profit_abs / stop_loss_abs — в деньгах на позицию (как сейчас в MM).
        max_hold_ms — эффективный лимит удержания (мс).
        Если skip_stop_while_in_profit: стоп не срабатывает при unrealized > 0 (удержание в плюсе до TP/таймаута).
        """
        if not policy_enabled or self.snapshot is None:
            return None, {}
        sn = self.snapshot
        hold_ms = (now_mono - sn.entry_mono) * 1000.0
        detail: dict[str, Any] = {
            "hold_ms": hold_ms,
            "unrealized": unrealized,
            "peak_unrealized": self.peak_unrealized,
            "trough_unrealized": self.trough_unrealized,
            "spread_at_entry": sn.spread_at_entry,
            "latency_ms": sn.latency_ms,
        }

        if hold_ms >= max(0.0, float(max_hold_ms)):
            return "MAX_HOLD", detail

        tp = max(
            take_profit_abs,
            float(getattr(targets, "TARGET_ROI_ABS", 0.0)),
            float(targets.TARGET_MFE),
        )
        sl = max(
            stop_loss_abs,
            float(getattr(targets, "MAX_ADVERSE_ABS", 0.0)),
            float(targets.TARGET_MAE),
        )

        if tp > 0 and unrealized >= tp:
            return "TP_POLICY", detail
        trail = float(getattr(targets, "TRAILING_STOP_PCT", 0.0) or 0.0)
        if trail > 0 and self.peak_unrealized > 1e-12 and unrealized > 0:
            giveback = (self.peak_unrealized - unrealized) / self.peak_unrealized
            if giveback >= trail:
                return "TRAIL_STOP", detail
        if sl > 0:
            underwater = unrealized <= 0.0
            if skip_stop_while_in_profit and not underwater:
                return None, detail
            if unrealized <= -sl:
                return "SL_POLICY", detail

        return None, detail
