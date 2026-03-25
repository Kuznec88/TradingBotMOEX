from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import RLock

from fix_engine.economics_store import EconomicsStore
from fix_engine.market_data.models import MarketData


class FillAdverseSelectionTracker:
    def __init__(self, store: EconomicsStore) -> None:
        self._store = store
        self._lock = RLock()
        self._pending: dict[str, dict[str, object]] = {}
        self._horizons = {
            "px_10ms": timedelta(milliseconds=10),
            "px_100ms": timedelta(milliseconds=100),
            "px_500ms": timedelta(milliseconds=500),
            "px_1s": timedelta(seconds=1),
        }

    def register_fill(
        self,
        *,
        trade_id: str,
        side: str,
        qty: float,
        fill_price: float,
        symbol: str,
        fill_ts: datetime,
    ) -> None:
        with self._lock:
            self._pending[trade_id] = {
                "trade_id": trade_id,
                "side": side,
                "qty": float(qty),
                "fill_price": float(fill_price),
                "symbol": symbol.upper(),
                "fill_ts": fill_ts,
                "px_10ms": None,
                "px_100ms": None,
                "px_500ms": None,
                "px_1s": None,
            }

    def on_market_data(self, data: MarketData) -> None:
        now_ts = data.timestamp if data.timestamp.tzinfo else data.timestamp.replace(tzinfo=timezone.utc)
        finished: list[str] = []
        with self._lock:
            for trade_id, row in self._pending.items():
                if row["symbol"] != data.symbol.upper():
                    continue
                fill_ts = row["fill_ts"]
                for key, horizon in self._horizons.items():
                    if row[key] is not None:
                        continue
                    if now_ts >= fill_ts + horizon:
                        row[key] = float(data.mid_price)
                if (
                    row["px_10ms"] is not None
                    and row["px_100ms"] is not None
                    and row["px_500ms"] is not None
                    and row["px_1s"] is not None
                ):
                    adverse_pnl, adverse_fill = self._compute_adverse(row)
                    self._store.update_adverse_selection(
                        trade_id=trade_id,
                        px_10ms=float(row["px_10ms"]),
                        px_100ms=float(row["px_100ms"]),
                        px_500ms=float(row["px_500ms"]),
                        px_1s=float(row["px_1s"]),
                        adverse_pnl=adverse_pnl,
                        adverse_fill=adverse_fill,
                    )
                    finished.append(trade_id)
            for trade_id in finished:
                self._pending.pop(trade_id, None)

    @staticmethod
    def _compute_adverse(row: dict[str, object]) -> tuple[float, bool]:
        side = str(row["side"])
        qty = float(row["qty"])
        fill = float(row["fill_price"])
        px_1s = float(row["px_1s"])
        if side == "1":
            adverse_pnl = (px_1s - fill) * qty
        else:
            adverse_pnl = (fill - px_1s) * qty
        return adverse_pnl, adverse_pnl < 0.0

