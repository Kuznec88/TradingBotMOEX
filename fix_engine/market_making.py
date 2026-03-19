from __future__ import annotations

import logging
from dataclasses import dataclass

from execution_gateway import ExecutionGateway
from market_data.models import MarketData
from order_models import MarketType, OrderRequest
from position_manager import PositionManager


@dataclass
class _SideState:
    cl_ord_id: str | None = None
    price: float | None = None


class BasicMarketMaker:
    """
    Deterministic MM:
    - quote buy at bid
    - quote sell at ask
    - cancel/replace on price changes
    """

    def __init__(
        self,
        *,
        symbol: str,
        lot_size: float,
        market: MarketType,
        gateway: ExecutionGateway,
        position_manager: PositionManager,
        logger: logging.Logger,
    ) -> None:
        self.symbol = symbol.upper()
        self.lot_size = float(lot_size)
        self.market = market
        self.gateway = gateway
        self.position_manager = position_manager
        self.logger = logger
        self.buy_state = _SideState()
        self.sell_state = _SideState()

    def on_market_data(self, data: MarketData) -> None:
        if data.symbol.upper() != self.symbol:
            return
        if data.bid <= 0 or data.ask <= 0 or data.ask < data.bid:
            return

        self._maintain_buy(float(data.bid))
        self._maintain_sell(float(data.ask))

    def _maintain_buy(self, target_bid: float) -> None:
        if not self.position_manager.can_place_buy(self.symbol, self.lot_size):
            self._cancel_side(self.buy_state, "BUY blocked by inventory")
            return
        if self.buy_state.cl_ord_id and self.buy_state.price == target_bid:
            return
        self._cancel_side(self.buy_state, "BUY replace on bid move")
        self._place_side(self.buy_state, side="1", price=target_bid, label="BUY")

    def _maintain_sell(self, target_ask: float) -> None:
        if not self.position_manager.can_place_sell(self.symbol, self.lot_size):
            self._cancel_side(self.sell_state, "SELL blocked by inventory")
            return
        if self.sell_state.cl_ord_id and self.sell_state.price == target_ask:
            return
        self._cancel_side(self.sell_state, "SELL replace on ask move")
        self._place_side(self.sell_state, side="2", price=target_ask, label="SELL")

    def _cancel_side(self, state: _SideState, reason: str) -> None:
        if not state.cl_ord_id:
            return
        try:
            self.gateway.cancel_order(state.cl_ord_id, market=self.market)
            self.logger.info("[MM] cancel %s reason=%s", state.cl_ord_id, reason)
        except Exception as exc:
            self.logger.warning("[MM] cancel failed cl_ord_id=%s err=%s", state.cl_ord_id, exc)
        finally:
            state.cl_ord_id = None
            state.price = None

    def _place_side(self, state: _SideState, *, side: str, price: float, label: str) -> None:
        try:
            cl_ord_id = self.gateway.send_order(
                OrderRequest(
                    symbol=self.symbol,
                    side=side,
                    qty=self.lot_size,
                    price=price,
                    market=self.market,
                    lot_size=1,
                )
            )
            state.cl_ord_id = cl_ord_id
            state.price = price
            self.logger.info("[MM] place %s %s qty=%s px=%s", label, self.symbol, self.lot_size, price)
        except Exception as exc:
            self.logger.warning("[MM] place rejected %s %s px=%s err=%s", label, self.symbol, price, exc)
