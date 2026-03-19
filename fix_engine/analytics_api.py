from __future__ import annotations

from economics_store import EconomicsStore


class TradingAnalyticsAPI:
    def __init__(self, store: EconomicsStore) -> None:
        self._store = store

    def get_metrics(self) -> dict[str, float]:
        return self._store.get_metrics()

    def top_losing_trades(self, limit: int = 10) -> list[dict[str, float | str]]:
        return self._store.top_losing_trades(limit=limit)

    def top_profitable_trades(self, limit: int = 10) -> list[dict[str, float | str]]:
        return self._store.top_profitable_trades(limit=limit)

    def worst_slippage_trades(self, limit: int = 10) -> list[dict[str, float | str]]:
        return self._store.worst_slippage_trades(limit=limit)

    def pnl_by_component(self) -> dict[str, float]:
        return self._store.pnl_by_component()

    def pnl_distribution(self) -> list[dict[str, float | str]]:
        return self._store.pnl_distribution()

    def pnl_by_time_of_day(self) -> list[dict[str, float | int]]:
        return self._store.pnl_by_time_of_day()

    def pnl_by_volatility_regime(self) -> list[dict[str, float | int | str]]:
        return self._store.pnl_by_volatility_regime()

    def pnl_by_spread_size(self) -> list[dict[str, float | int | str]]:
        return self._store.pnl_by_spread_size()

    def adverse_fill_stats(self) -> dict[str, float]:
        return self._store.adverse_fill_stats()

    def fill_quality_stats(self) -> dict[str, float]:
        return self._store.fill_quality_stats()

    def trade_outcome_analysis(self) -> dict[str, object]:
        return self._store.trade_outcome_analysis()

    def missed_pnl_by_cancel_reason(self) -> list[dict[str, float | int | str]]:
        return self._store.missed_pnl_by_cancel_reason()

    def entry_decisions_by_side(self) -> list[dict[str, float | int | str]]:
        return self._store.entry_decisions_by_side()

    def analytics_missing_fills(self, limit: int = 500) -> list[dict[str, object]]:
        return self._store.analytics_missing_fills(limit=limit)

    def entry_score_pnl_correlation(self) -> dict[str, float]:
        return self._store.entry_score_pnl_correlation()
