from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP, getcontext
from datetime import datetime, timezone
from typing import Dict, List

from fix_engine.order_models import MarketType

getcontext().prec = 28
QTY_Q = Decimal("0.0001")
MONEY_Q = Decimal("0.01")


def _d(value: str | float | int) -> Decimal:
    return Decimal(str(value))


@dataclass
class _Lot:
    qty: Decimal
    price: Decimal
    expected_price: Decimal
    mid_price: Decimal
    bid: Decimal
    ask: Decimal
    entry_ts: datetime
    cl_ord_id: str
    exec_id: str
    entry_slippage_pnl: Decimal = Decimal("0.00")
    min_mid: Decimal = Decimal("0.00")
    max_mid: Decimal = Decimal("0.00")
    adverse_pnl: Decimal = Decimal("0.00")
    entry_time_in_book_ms: Decimal = Decimal("0.00")


@dataclass
class _PositionState:
    lots: List[_Lot] = field(default_factory=list)
    side: int = 0  # 1 long, -1 short, 0 flat


class UnitEconomicsCalculator:
    """Deterministic FIFO realized PnL calculator per market/symbol."""

    def __init__(
        self,
        fee_bps_by_market: Dict[MarketType, Decimal],
        fixed_fee_by_market: Dict[MarketType, Decimal] | None = None,
    ) -> None:
        self._fee_bps_by_market = fee_bps_by_market
        self._fixed_fee_by_market = fixed_fee_by_market or {}
        self._positions: Dict[tuple[MarketType, str], _PositionState] = {}
        self._round_trip_seq = 0

    def on_market_data(self, *, market: MarketType, symbol: str, mid_price: Decimal) -> None:
        key = (market, symbol)
        state = self._positions.get(key)
        if state is None:
            return
        mid = mid_price.quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        for lot in state.lots:
            if lot.min_mid == Decimal("0.00") or mid < lot.min_mid:
                lot.min_mid = mid
            if lot.max_mid == Decimal("0.00") or mid > lot.max_mid:
                lot.max_mid = mid

    def process_fill(
        self,
        *,
        market: MarketType,
        symbol: str,
        side: str,
        qty: Decimal,
        price: Decimal,
        expected_price: Decimal | None = None,
        bid: Decimal | None = None,
        ask: Decimal | None = None,
        mid_price: Decimal | None = None,
        fill_ts: datetime | None = None,
        cl_ord_id: str,
        exec_id: str,
        time_in_book_ms: float = 0.0,
    ) -> dict[str, object] | None:
        qty = qty.quantize(QTY_Q, rounding=ROUND_HALF_UP)
        price = price.quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        if qty <= 0 or price <= 0:
            return None
        fill_ts = fill_ts or datetime.now(timezone.utc)

        key = (market, symbol)
        state = self._positions.setdefault(key, _PositionState())
        fill_sign = 1 if side == "1" else -1
        expected = (expected_price or price).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        bid_px = (bid or price).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        ask_px = (ask or price).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        mid_px = (mid_price or ((bid_px + ask_px) / Decimal("2"))).quantize(MONEY_Q, rounding=ROUND_HALF_UP)

        # Fees: per-trade notional * bps (market-specific).
        notional = (qty * price).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        fee_bps = self._fee_bps_by_market.get(market, Decimal("0"))
        proportional_fees = (notional * fee_bps / Decimal("10000")).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        fixed_fee = self._fixed_fee_by_market.get(market, Decimal("0")).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        fees = (proportional_fees + fixed_fee).quantize(MONEY_Q, rounding=ROUND_HALF_UP)

        gross = Decimal("0.00")
        spread_pnl = Decimal("0.00")
        slippage_pnl = Decimal("0.00")
        holding_pnl = Decimal("0.00")
        adverse_pnl = Decimal("0.00")
        closed_qty = Decimal("0.0000")
        remaining = qty
        trade_analytics_rows: list[dict[str, str]] = []
        round_trip_rows: list[dict[str, str | float]] = []

        fill_slippage_leg = self._leg_slippage_pnl(side=side, expected=expected, actual=price, qty=qty)
        in_book_ms = _d(time_in_book_ms).quantize(MONEY_Q, rounding=ROUND_HALF_UP)

        # If incoming fill closes opposite inventory, realize PnL.
        while remaining > 0 and state.side != 0 and state.side != fill_sign and state.lots:
            lot = state.lots[0]
            matched = min(remaining, lot.qty).quantize(QTY_Q, rounding=ROUND_HALF_UP)
            exit_slippage_leg = self._leg_slippage_pnl(side=side, expected=expected, actual=price, qty=matched)
            entry_slippage_leg = (
                lot.entry_slippage_pnl * (matched / lot.qty if lot.qty > 0 else Decimal("0"))
            ).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
            spread_leg = self._spread_capture_pnl(
                entry_side="1" if state.side == 1 else "2",
                entry_px=lot.price,
                entry_mid=lot.mid_price,
                exit_side=side,
                exit_px=price,
                exit_mid=mid_px,
                qty=matched,
            )
            if state.side == 1 and fill_sign == -1:
                # Closing long with sell.
                gross_leg = (price - lot.price) * matched
            elif state.side == -1 and fill_sign == 1:
                # Closing short with buy.
                gross_leg = (lot.price - price) * matched
            else:
                gross_leg = Decimal("0.00")
            immediate_move = (
                (price - lot.price) if state.side == 1 else (lot.price - price)
            ).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
            gross += gross_leg
            slippage_leg = (entry_slippage_leg + exit_slippage_leg).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
            holding_leg = (gross_leg - spread_leg - slippage_leg).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
            adverse_leg = self._lot_adverse_pnl(lot=lot, qty=matched, side=state.side)
            spread_pnl += spread_leg
            slippage_pnl += slippage_leg
            holding_pnl += holding_leg
            adverse_pnl += adverse_leg
            closed_qty = (closed_qty + matched).quantize(QTY_Q, rounding=ROUND_HALF_UP)
            self._round_trip_seq += 1
            duration_ms = max(0.0, (fill_ts - lot.entry_ts).total_seconds() * 1000.0)
            mae, mfe = self._lot_mae_mfe(lot=lot, qty=matched, side=state.side)
            round_trip_rows.append(
                {
                    "round_trip_id": f"rt-{self._round_trip_seq:08d}",
                    "entry_trade_id": lot.exec_id,
                    "exit_trade_id": exec_id,
                    "symbol": symbol,
                    "side": "LONG" if state.side == 1 else "SHORT",
                    "duration_ms": duration_ms,
                    "total_pnl": str(gross_leg.quantize(MONEY_Q, rounding=ROUND_HALF_UP)),
                    "mae": str(mae),
                    "mfe": str(mfe),
                    "immediate_move": str(immediate_move),
                    "entry_price": str(lot.price),
                    "exit_price": str(price),
                    "entry_spread": str((lot.ask - lot.bid).quantize(MONEY_Q, rounding=ROUND_HALF_UP)),
                    "entry_volatility_regime": self._volatility_regime(lot.mid_price, lot.bid, lot.ask),
                    "entry_time_in_book_ms": str(lot.entry_time_in_book_ms.quantize(MONEY_Q, rounding=ROUND_HALF_UP)),
                    "entry_ts": lot.entry_ts.isoformat(),
                    "exit_ts": fill_ts.isoformat(),
                }
            )

            lot.qty = (lot.qty - matched).quantize(QTY_Q, rounding=ROUND_HALF_UP)
            remaining = (remaining - matched).quantize(QTY_Q, rounding=ROUND_HALF_UP)
            if lot.qty <= 0:
                state.lots.pop(0)

        # Remaining quantity opens/increases position on fill side.
        if remaining > 0:
            entry_slip = self._leg_slippage_pnl(side=side, expected=expected, actual=price, qty=remaining)
            state.lots.append(
                _Lot(
                    qty=remaining,
                    price=price,
                    expected_price=expected,
                    mid_price=mid_px,
                    bid=bid_px,
                    ask=ask_px,
                    entry_ts=fill_ts,
                    cl_ord_id=cl_ord_id,
                    exec_id=exec_id,
                    entry_slippage_pnl=entry_slip,
                    min_mid=mid_px,
                    max_mid=mid_px,
                    entry_time_in_book_ms=in_book_ms,
                )
            )
            state.side = fill_sign
        elif not state.lots:
            state.side = 0

        opened_qty = (qty - closed_qty).quantize(QTY_Q, rounding=ROUND_HALF_UP)
        fill_role = "OPEN"
        if closed_qty > 0 and opened_qty > 0:
            fill_role = "MIXED"
        elif closed_qty > 0:
            fill_role = "CLOSE"

        gross = gross.quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        net = (gross - fees).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        spread_pnl = spread_pnl.quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        slippage_pnl = slippage_pnl.quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        holding_pnl = holding_pnl.quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        adverse_pnl = adverse_pnl.quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        trade_analytics_rows.append(
            {
                "trade_id": exec_id,
                "order_id": cl_ord_id,
                "symbol": symbol,
                "side": side,
                "gross_pnl": str(gross),
                "net_pnl": str(net),
                "fees": str(fees),
                "slippage": str(fill_slippage_leg.quantize(MONEY_Q, rounding=ROUND_HALF_UP)),
                "spread_pnl": str(spread_pnl),
                "adverse_pnl": str(adverse_pnl),
                "holding_pnl": str(holding_pnl),
                "expected_price": str(expected),
                "actual_price": str(price),
                "volatility_regime": self._volatility_regime(mid_px, bid_px, ask_px),
                "spread_bucket": self._spread_bucket((ask_px - bid_px)),
                "hour_bucket": str(fill_ts.hour),
                "is_adverse_fill": "1" if adverse_pnl < 0 else "0",
                "time_in_book_ms": str(in_book_ms),
                "created_at": fill_ts.isoformat(),
            }
        )

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
            "expected_price": str(expected),
            "actual_price": str(price),
            "slippage": str(fill_slippage_leg.quantize(MONEY_Q, rounding=ROUND_HALF_UP)),
            "spread_pnl": str(spread_pnl),
            "adverse_pnl": str(adverse_pnl),
            "holding_pnl": str(holding_pnl),
            "fill_role": fill_role,
            "closed_qty": str(closed_qty),
            "opened_qty": str(opened_qty),
            "trade_analytics_rows": trade_analytics_rows,
            "round_trip_rows": round_trip_rows,
            "fill_ts": fill_ts.isoformat(),
        }

    @staticmethod
    def _leg_slippage_pnl(*, side: str, expected: Decimal, actual: Decimal, qty: Decimal) -> Decimal:
        if side == "1":
            return ((expected - actual) * qty).quantize(MONEY_Q, rounding=ROUND_HALF_UP)
        return ((actual - expected) * qty).quantize(MONEY_Q, rounding=ROUND_HALF_UP)

    @staticmethod
    def _spread_capture_pnl(
        *,
        entry_side: str,
        entry_px: Decimal,
        entry_mid: Decimal,
        exit_side: str,
        exit_px: Decimal,
        exit_mid: Decimal,
        qty: Decimal,
    ) -> Decimal:
        entry_edge = (entry_mid - entry_px) if entry_side == "1" else (entry_px - entry_mid)
        exit_edge = (exit_px - exit_mid) if exit_side == "2" else (exit_mid - exit_px)
        return ((entry_edge + exit_edge) * qty).quantize(MONEY_Q, rounding=ROUND_HALF_UP)

    @staticmethod
    def _lot_adverse_pnl(*, lot: _Lot, qty: Decimal, side: int) -> Decimal:
        if side > 0:
            adverse = (lot.min_mid - lot.mid_price) * qty
        else:
            adverse = (lot.mid_price - lot.max_mid) * qty
        return adverse.quantize(MONEY_Q, rounding=ROUND_HALF_UP)

    @staticmethod
    def _lot_mae_mfe(*, lot: _Lot, qty: Decimal, side: int) -> tuple[Decimal, Decimal]:
        if side > 0:
            mae = max(Decimal("0.00"), (lot.mid_price - lot.min_mid) * qty)
            mfe = max(Decimal("0.00"), (lot.max_mid - lot.mid_price) * qty)
        else:
            mae = max(Decimal("0.00"), (lot.max_mid - lot.mid_price) * qty)
            mfe = max(Decimal("0.00"), (lot.mid_price - lot.min_mid) * qty)
        return mae.quantize(MONEY_Q, rounding=ROUND_HALF_UP), mfe.quantize(MONEY_Q, rounding=ROUND_HALF_UP)

    @staticmethod
    def _spread_bucket(spread: Decimal) -> str:
        s = float(spread)
        if s < 0.01:
            return "<0.01"
        if s < 0.03:
            return "0.01-0.03"
        if s < 0.07:
            return "0.03-0.07"
        return ">=0.07"

    @staticmethod
    def _volatility_regime(mid: Decimal, bid: Decimal, ask: Decimal) -> str:
        if mid <= 0:
            return "unknown"
        rel_spread = float((ask - bid) / mid)
        if rel_spread < 0.0002:
            return "low"
        if rel_spread < 0.0006:
            return "normal"
        return "high"
