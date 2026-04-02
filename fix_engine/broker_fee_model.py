"""Модель брокерских комиссий (Т-Банк «Инвестор» / типичный тариф).

- Акции/облигации/ETF из базового списка: доля от суммы сделки (bps в конфиге).
- Фьючерсы (базовый список): пошаговая ставка от дневного оборота в рублях (п. 2.1 тарифа).
  Оборот накапливается по московским календарным дням; комиссия за сделку — маржинальная
  (как интеграл по кускам оборота).

Минимум 0,01 в валюте сделки (руб.) на ногу — по правилам округления тарифа.
"""

from __future__ import annotations

import threading
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from fix_engine.order_models import MarketType

if TYPE_CHECKING:
    pass

MOSCOW = ZoneInfo("Europe/Moscow")
MONEY_Q = Decimal("0.01")


def _d(x: str | float | int | Decimal) -> Decimal:
    return Decimal(str(x))


def _moscow_date(ts: datetime) -> date:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(MOSCOW).date()


def cumulative_forts_fee_rub(
    turnover_rub: Decimal,
    *,
    tier1_end: Decimal,
    tier2_end: Decimal,
    rate1: Decimal,
    rate2: Decimal,
    rate3: Decimal,
) -> Decimal:
    """Накопленная комиссия «с нуля» до оборота turnover_rub (руб. за день)."""
    t = max(Decimal("0"), turnover_rub)
    t1 = tier1_end
    t2 = tier2_end
    fee = Decimal("0")
    # [0, t1]
    seg = min(t, t1)
    if seg > 0:
        fee += seg * rate1
    if t <= t1:
        return fee.quantize(MONEY_Q, rounding=ROUND_HALF_UP)
    # (t1, t2]
    seg = min(t, t2) - t1
    if seg > 0:
        fee += seg * rate2
    if t <= t2:
        return fee.quantize(MONEY_Q, rounding=ROUND_HALF_UP)
    # (t2, +inf)
    seg = t - t2
    if seg > 0:
        fee += seg * rate3
    return fee.quantize(MONEY_Q, rounding=ROUND_HALF_UP)


class BrokerFeeCalculator:
    """
    Расчёт комиссии за одну ногу (одно исполнение) с учётом дневного оборота по FORTS.
    """

    def __init__(
        self,
        *,
        equities_bps: Decimal = Decimal("4"),  # 0.04% — типично для базового списка
        forts_flat_bps: Decimal = Decimal("0"),
        fixed_equities: Decimal = Decimal("0"),
        fixed_forts: Decimal = Decimal("0"),
        forts_tiered: bool = False,
        forts_tier1_end_rub: Decimal = Decimal("12000000"),
        forts_tier2_end_rub: Decimal = Decimal("17000000"),
        # 0.025%, 0.02%, 0.015% — п. 2.1.1–2.1.3 (фьючерсы базового списка)
        forts_tier1_rate: Decimal = Decimal("0.00025"),
        forts_tier2_rate: Decimal = Decimal("0.00020"),
        forts_tier3_rate: Decimal = Decimal("0.00015"),
        min_fee_rub: Decimal = Decimal("0.01"),
    ) -> None:
        self._equities_bps = max(Decimal("0"), equities_bps)
        self._forts_flat_bps = max(Decimal("0"), forts_flat_bps)
        self._fixed_equities = max(Decimal("0"), fixed_equities)
        self._fixed_forts = max(Decimal("0"), fixed_forts)
        self._forts_tiered = bool(forts_tiered)
        self._t1 = max(Decimal("0"), forts_tier1_end_rub)
        self._t2 = max(self._t1, forts_tier2_end_rub)
        self._r1 = max(Decimal("0"), forts_tier1_rate)
        self._r2 = max(Decimal("0"), forts_tier2_rate)
        self._r3 = max(Decimal("0"), forts_tier3_rate)
        self._min_fee = max(Decimal("0"), min_fee_rub)

        self._lock = threading.Lock()
        self._forts_day: date | None = None
        self._forts_turnover_rub = Decimal("0")

    def reset_forts_daily_turnover_for_tests(self) -> None:
        with self._lock:
            self._forts_day = None
            self._forts_turnover_rub = Decimal("0")

    def fee_for_fill(self, market: MarketType, notional_rub: Decimal, fill_ts: datetime | None = None) -> Decimal:
        """Комиссия за одну ногу (пропорциональная + фикс за рынок), не ниже min_fee_rub."""
        ts = fill_ts or datetime.now(timezone.utc)
        n = max(Decimal("0"), notional_rub.quantize(MONEY_Q, rounding=ROUND_HALF_UP))
        prop: Decimal
        fixed: Decimal
        if market == MarketType.EQUITIES:
            prop = (n * self._equities_bps / Decimal("10000")).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
            fixed = self._fixed_equities
        elif market == MarketType.FORTS:
            if self._forts_tiered:
                prop = self._forts_marginal_fee(n, ts)
            else:
                prop = (n * self._forts_flat_bps / Decimal("10000")).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
            fixed = self._fixed_forts
        else:
            prop = Decimal("0")
            fixed = Decimal("0")
        total = (prop + fixed).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        if total > 0 and total < self._min_fee:
            total = self._min_fee
        return total

    def _forts_marginal_fee(self, notional_rub: Decimal, fill_ts: datetime) -> Decimal:
        n = notional_rub
        if n <= 0:
            return Decimal("0")
        day = _moscow_date(fill_ts)
        with self._lock:
            if self._forts_day != day:
                self._forts_day = day
                self._forts_turnover_rub = Decimal("0")
            base = self._forts_turnover_rub
            after = base + n
            f0 = cumulative_forts_fee_rub(
                base,
                tier1_end=self._t1,
                tier2_end=self._t2,
                rate1=self._r1,
                rate2=self._r2,
                rate3=self._r3,
            )
            f1 = cumulative_forts_fee_rub(
                after,
                tier1_end=self._t1,
                tier2_end=self._t2,
                rate1=self._r1,
                rate2=self._r2,
                rate3=self._r3,
            )
            leg = (f1 - f0).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
            self._forts_turnover_rub = after
            return leg
