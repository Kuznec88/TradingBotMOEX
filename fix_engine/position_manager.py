from __future__ import annotations

from order_manager import OrderManager


class PositionManager:
    """Simple inventory controls on top of OrderManager positions."""

    def __init__(self, order_manager: OrderManager, max_abs_inventory_per_symbol: float) -> None:
        self._order_manager = order_manager
        self._max_abs_inventory = float(max_abs_inventory_per_symbol)

    def get_inventory(self, symbol: str) -> float:
        return float(self._order_manager.get_position(symbol.upper()))

    def can_place_buy(self, symbol: str, qty: float) -> bool:
        if self._max_abs_inventory <= 0:
            return True
        inv = self.get_inventory(symbol)
        return (inv + float(qty)) <= self._max_abs_inventory

    def can_place_sell(self, symbol: str, qty: float) -> bool:
        if self._max_abs_inventory <= 0:
            return True
        inv = self.get_inventory(symbol)
        return (inv - float(qty)) >= -self._max_abs_inventory
