from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP, getcontext
from typing import Dict, List

from order_models import MarketType

getcontext().prec = 28
QTY_Q = Decimal("0.0001")
MONEY_Q = Decimal("0.01")


def _d(value: str | float | int) -> Decimal:
    return Decimal(str(value))


@dataclass
class _Lot:
    qty: Decimal
    price: Decimal


@dataclass
class _PositionState:
    lots: List[_Lot] = field(default_factory=list)
    side: int = 0  # 1 long, -1 short, 0 flat


class UnitEconomicsCalculator:
    """Deterministic FIFO realized PnL calculator per market/symbol."""

    def __init__(self, fee_bps_by_market: Dict[MarketType, Decimal]) -> None:
        self._fee_bps_by_market = fee_bps_by_market
        self._positions: Dict[tuple[MarketType, str], _PositionState] = {}

    def process_fill(
        self,
        *,
        market: MarketType,
        symbol: str,
        side: str,
        qty: Decimal,
        price: Decimal,
        cl_ord_id: str,
        exec_id: str,
    ) -> dict[str, str] | None:
        qty = qty.quantize(QTY_Q, rounding=ROUND_HALF_UP)
        price = price.quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        if qty <= 0 or price <= 0:
            return None

        key = (market, symbol)
        state = self._positions.setdefault(key, _PositionState())
        fill_sign = 1 if side == "1" else -1

        # Fees: per-trade notional * bps (market-specific).
        notional = (qty * price).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        fee_bps = self._fee_bps_by_market.get(market, Decimal("0"))
        fees = (notional * fee_bps / Decimal("10000")).quantize(MONEY_Q, rounding=ROUND_HALF_UP)

        gross = Decimal("0.00")
        remaining = qty

        # If incoming fill closes opposite inventory, realize PnL.
        while remaining > 0 and state.side != 0 and state.side != fill_sign and state.lots:
            lot = state.lots[0]
            matched = min(remaining, lot.qty).quantize(QTY_Q, rounding=ROUND_HALF_UP)
            if state.side == 1 and fill_sign == -1:
                # Closing long with sell.
                gross += (price - lot.price) * matched
            elif state.side == -1 and fill_sign == 1:
                # Closing short with buy.
                gross += (lot.price - price) * matched

            lot.qty = (lot.qty - matched).quantize(QTY_Q, rounding=ROUND_HALF_UP)
            remaining = (remaining - matched).quantize(QTY_Q, rounding=ROUND_HALF_UP)
            if lot.qty <= 0:
                state.lots.pop(0)

        # Remaining quantity opens/increases position on fill side.
        if remaining > 0:
            state.lots.append(_Lot(qty=remaining, price=price))
            state.side = fill_sign
        elif not state.lots:
            state.side = 0

        gross = gross.quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        net = (gross - fees).quantize(MONEY_Q, rounding=ROUND_HALF_UP)

        return {
            "market": market.value,
            "symbol": symbol,
            "cl_ord_id": cl_ord_id,
            "exec_id": exec_id,
            "side": side,
            "qty": str(qty),
            "price": str(price),
            "gross_pnl": str(gross),
            "fees": str(fees),
            "net_pnl": str(net),
        }
