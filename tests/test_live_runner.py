"""Живой раннер: выходы как в бэктесте и защитные проверки."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from fix_engine.config.swing_params import load_swing_params
from fix_engine.data.instruments import open_futures_balance, resolve_front_future
from fix_engine.execution.tbank_broker import TinkoffBrokerController
from fix_engine.order_models import MarketType
from fix_engine.risk.manager import RiskManager
from fix_engine.strategy.swing_breakout import RetestEntryIntent, SwingBreakoutParams
from fix_engine.strategy.swing_live_runner import SwingLiveRunner

FE_DIR = Path(__file__).resolve().parents[1] / "fix_engine"
NOW = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)


class FakeGateway:
    def __init__(self) -> None:
        self.orders: list[object] = []

    def send_order(self, request: object) -> str:
        self.orders.append(request)
        return f"EXIT-{len(self.orders)}"


def _runner(*, qty: float = 1.0, on_trade_closed=None, no_entries_after=None, **params: object) -> tuple[SwingLiveRunner, FakeGateway]:
    gateway = FakeGateway()
    base = dict(tick_size=1.0, commission_rate=0.0, trail_after_r_multiple=2.0, trail_gap_r_multiple=1.5, partial_exit_fraction=0.0, trades_log_csv_path="")
    runner = SwingLiveRunner(
        base_dir=FE_DIR,
        gateway=gateway,
        logger=logging.getLogger("test"),
        symbol="SRZ6",
        market=MarketType.FORTS,
        lot_size=1.0,
        tick_size=1.0,
        params=SwingBreakoutParams(**{**base, **params}),
        candle_interval="1h",
        poll_interval_sec=30.0,
        get_latest=lambda _: None,
        instrument_id="uid",
        token="token",
        host="host",
        on_trade_closed=on_trade_closed,
        no_entries_after=no_entries_after,
    )
    runner._pending_intent = RetestEntryIntent(side=1, limit_price=1000.0, stop_price=900.0, take_profit_price=3000.0, risk_per_unit=100.0, use_market_entry=True)
    runner._pending_limit_cl_ord_id = "ENTRY"
    runner.on_execution_report({"symbol": "SRZ6", "cl_ord_id": "ENTRY", "status_new": "FILLED", "last_qty": qty, "side": "1", "last_px": 1000.0})
    return runner, gateway


def _quote(bid: float) -> SimpleNamespace:
    return SimpleNamespace(symbol="SRZ6", bid=bid, ask=bid + 1.0)


def _bar(high: float, low: float) -> pd.Series:
    return pd.Series({"high": high, "low": low})


def test_trailing_moves_only_on_bar_close() -> None:
    runner, gateway = _runner()
    runner.on_market_data(_quote(1300.0))
    runner.on_market_data(_quote(1100.0))
    assert gateway.orders == []

    runner._update_trail_from_closed_bar(_bar(1300.0, 1000.0))
    runner.on_market_data(_quote(1151.0))
    assert gateway.orders == []
    runner.on_market_data(_quote(1150.0))
    assert len(gateway.orders) == 1
    assert gateway.orders[0].side == "2"
    assert gateway.orders[0].qty == 1.0


def test_stop_loss_exits_before_trailing_is_armed() -> None:
    runner, gateway = _runner()
    runner._update_trail_from_closed_bar(_bar(1100.0, 950.0))
    runner.on_market_data(_quote(900.0))
    assert len(gateway.orders) == 1


def test_partial_exit_never_sends_a_fraction_of_a_contract() -> None:
    runner, gateway = _runner(partial_exit_fraction=0.5)
    runner.on_market_data(_quote(1100.0))
    assert gateway.orders == []


def test_partial_exit_sends_whole_contracts() -> None:
    runner, gateway = _runner(qty=3.0, partial_exit_fraction=0.5)
    runner.on_market_data(_quote(1100.0))
    assert gateway.orders[0].qty == 1.0


def test_closed_trade_reaches_the_daily_loss_limit() -> None:
    risk = RiskManager(
        order_manager=SimpleNamespace(),
        max_exposure_per_instrument=0.0,
        max_trades_in_window=0,
        trades_window_seconds=60,
        cooldown_after_consecutive_losses=0,
        cooldown_seconds=0,
        max_daily_loss_abs=50.0,
    )
    runner, _ = _runner(on_trade_closed=risk.on_trade_result)
    runner.on_execution_report({"symbol": "SRZ6", "cl_ord_id": "X", "status_new": "FILLED", "last_qty": 1.0, "side": "2", "last_px": 900.0})
    decision = risk.pre_check_order(symbol="SRZ6", qty=1.0, market=MarketType.FORTS, side="1")
    assert not decision.allowed
    assert "max_daily_loss_abs" in decision.reason


def test_entries_only_from_a_fresh_bar() -> None:
    runner, _ = _runner()
    assert runner._can_open_on_bar(pd.Timestamp("2026-11-05 11:00", tz="UTC"), NOW)
    assert not runner._can_open_on_bar(pd.Timestamp("2026-11-05 09:00", tz="UTC"), NOW)


def test_no_entries_when_contract_is_near_expiry() -> None:
    runner, _ = _runner(no_entries_after=NOW - timedelta(days=1))
    assert not runner._can_open_on_bar(pd.Timestamp("2026-11-05 10:00", tz="UTC"), NOW)


def _future(ticker: str, days: int) -> SimpleNamespace:
    money = SimpleNamespace(units=1, nano=0)
    return SimpleNamespace(ticker=ticker, uid=ticker.lower(), expiration_date=NOW + timedelta(days=days), min_price_increment=money, min_price_increment_amount=money)


def test_front_future_skips_contracts_close_to_expiry() -> None:
    futures = [_future("SRU6", -40), _future("SRZ6", 5), _future("SRH7", 90), _future("SRM7", 180), _future("GZH7", 90), _future("SRPH7", 90)]
    front = resolve_front_future(futures, "SR", now=NOW, roll_days=7)
    assert front.ticker == "SRH7"
    assert front.no_entries_after == NOW + timedelta(days=83)


def test_front_future_fails_when_nothing_matches() -> None:
    with pytest.raises(RuntimeError):
        resolve_front_future([_future("SRZ6", 5)], "SR", now=NOW, roll_days=7)


def test_open_futures_balance_finds_the_instrument() -> None:
    positions = SimpleNamespace(futures=[SimpleNamespace(instrument_uid="a", balance=-2), SimpleNamespace(instrument_uid="b", balance=1)])
    assert open_futures_balance(positions, "a") == -2
    assert open_futures_balance(positions, "c") == 0


def test_fractional_lots_are_rejected() -> None:
    engine = TinkoffBrokerController.__new__(TinkoffBrokerController)
    engine._shares_per_lot = 1
    assert engine._qty_to_lots(2.0) == 2
    with pytest.raises(ValueError):
        engine._qty_to_lots(0.5)


def test_settings_match_the_tested_candidate() -> None:
    params = load_swing_params(FE_DIR, tick_size=1.0)
    assert params.entry_mode == "volume_breakout"
    assert (params.n_levels, params.k_volume) == (20, 1.5)
    assert params.adaptive_stop_atr_mult == 2.0
    assert (params.trail_after_r_multiple, params.trail_gap_r_multiple) == (2.0, 1.5)
    assert params.risk_reward_multiple == 20.0
    assert params.partial_exit_fraction == 0.0
    assert params.commission_min_profit_mult == 0.0
    assert params.commission_rate == 0.00025
    assert (params.htf_resample_rule, params.htf_secondary_resample_rule) == ("", "")
    assert params.max_trades_per_day == 5
