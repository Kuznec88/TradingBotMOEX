from __future__ import annotations

from economics_store import EconomicsStore


class TradingAnalyticsAPI:
    def __init__(self, store: EconomicsStore) -> None:
        self._store = store

    def get_metrics(self) -> dict[str, float]:
        return self._store.get_metrics()
