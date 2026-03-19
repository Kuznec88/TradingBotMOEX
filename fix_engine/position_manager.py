from __future__ import annotations

from dataclasses import dataclass

from order_manager import OrderManager


@dataclass(frozen=True)
class InventoryDecision:
    allowed: bool
    reason: str = ""


class PositionManager:
    """Simple inventory controls on top of OrderManager positions."""

    def __init__(
        self,
        order_manager: OrderManager,
        max_abs_inventory_per_symbol: float,
        soft_abs_inventory_per_symbol: float = 0.0,
    ) -> None:
        self._order_manager = order_manager
        self._max_abs_inventory = float(max_abs_inventory_per_symbol)
        self._soft_abs_inventory = max(0.0, float(soft_abs_inventory_per_symbol))
        if self._max_abs_inventory > 0 and self._soft_abs_inventory > self._max_abs_inventory:
            self._soft_abs_inventory = self._max_abs_inventory

    def get_inventory(self, symbol: str) -> float:
        return float(self._order_manager.get_position(symbol.upper()))

    def pre_check_order(self, symbol: str, side: str | int, qty: float) -> InventoryDecision:
        symbol_u = symbol.upper()
        qty_f = abs(float(qty))
        if qty_f <= 0:
            return InventoryDecision(allowed=False, reason="qty_must_be_positive")

        inv = self.get_inventory(symbol_u)
        projected = self._projected_inventory(inv, side, qty_f)
        reducing = abs(projected) < abs(inv) if abs(inv) > 1e-9 else False

        if self._max_abs_inventory > 0:
            if abs(inv) >= self._max_abs_inventory and not reducing:
                return InventoryDecision(
                    allowed=False,
                    reason=(
                        f"hard_limit_reducing_only symbol={symbol_u} inv={inv:.4f} "
                        f"limit={self._max_abs_inventory:.4f} projected={projected:.4f}"
                    ),
                )
            if abs(projected) > self._max_abs_inventory and not reducing:
                return InventoryDecision(
                    allowed=False,
                    reason=(
                        f"hard_limit_breached symbol={symbol_u} inv={inv:.4f} "
                        f"limit={self._max_abs_inventory:.4f} projected={projected:.4f}"
                    ),
                )

        if self._soft_abs_inventory > 0:
            if abs(inv) >= self._soft_abs_inventory and not reducing:
                return InventoryDecision(
                    allowed=False,
                    reason=(
                        f"soft_limit_reducing_only symbol={symbol_u} inv={inv:.4f} "
                        f"soft={self._soft_abs_inventory:.4f} projected={projected:.4f}"
                    ),
                )
            if abs(projected) > self._soft_abs_inventory and not reducing:
                return InventoryDecision(
                    allowed=False,
                    reason=(
                        f"soft_limit_breached symbol={symbol_u} inv={inv:.4f} "
                        f"soft={self._soft_abs_inventory:.4f} projected={projected:.4f}"
                    ),
                )

        return InventoryDecision(allowed=True)

    def can_place_buy(self, symbol: str, qty: float) -> bool:
        return self.pre_check_order(symbol=symbol, side="1", qty=qty).allowed

    def can_place_sell(self, symbol: str, qty: float) -> bool:
        return self.pre_check_order(symbol=symbol, side="2", qty=qty).allowed

    @staticmethod
    def _projected_inventory(current_inventory: float, side: str | int, qty: float) -> float:
        side_s = str(side).strip().upper()
        is_buy = side_s in {"1", "BUY", "B"}
        sign = 1.0 if is_buy else -1.0
        return float(current_inventory) + sign * float(qty)
