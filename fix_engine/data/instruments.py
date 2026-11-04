"""Выбор текущего фьючерса и проверка позиции у брокера."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable


@dataclass(frozen=True)
class FrontFuture:
    uid: str
    ticker: str
    tick: float
    tick_rub: float
    # После этого момента новые сделки не открываются: пора переходить на следующий контракт.
    no_entries_after: datetime


def _q_float(q: object) -> float:
    return float(getattr(q, "units", 0) or 0) + float(getattr(q, "nano", 0) or 0) / 1_000_000_000.0


def resolve_front_future(futures: Iterable[object], prefix: str, *, now: datetime, roll_days: int) -> FrontFuture:
    """Ближайший квартальный контракт, до перехода с которого ещё есть время."""
    roll = timedelta(days=roll_days)
    candidates = sorted(
        (
            f
            for f in futures
            if f.ticker.startswith(prefix)
            and len(f.ticker) == 4
            and f.ticker[2] in "HMUZ"
            and f.expiration_date - roll > now
        ),
        key=lambda f: f.expiration_date,
    )
    if not candidates:
        raise RuntimeError(f"Не найден действующий фьючерс с префиксом {prefix!r}")
    front = candidates[0]
    return FrontFuture(
        uid=front.uid,
        ticker=front.ticker,
        tick=_q_float(front.min_price_increment),
        tick_rub=_q_float(front.min_price_increment_amount),
        no_entries_after=front.expiration_date - roll,
    )


def fetch_front_future(*, token: str, host: str, prefix: str, roll_days: int) -> FrontFuture:
    from t_tech.invest import Client
    from t_tech.invest.schemas import InstrumentStatus

    with Client(token, target=host) as client:
        futures = client.instruments.futures(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL).instruments
    return resolve_front_future(futures, prefix, now=datetime.now(timezone.utc), roll_days=roll_days)


def open_futures_balance(positions: object, instrument_uid: str) -> int:
    for item in getattr(positions, "futures", []) or []:
        if getattr(item, "instrument_uid", "") == instrument_uid:
            return int(getattr(item, "balance", 0) or 0)
    return 0


def assert_no_open_position(*, token: str, host: str, account_id: str, instrument_uid: str, use_sandbox: bool) -> None:
    """Бот не знает стоп чужой или старой позиции, поэтому с открытой позицией не стартует."""
    from t_tech.invest import Client

    with Client(token, target=host) as client:
        if use_sandbox:
            positions = client.sandbox.get_sandbox_positions(account_id=account_id)
        else:
            positions = client.operations.get_positions(account_id=account_id)
    balance = open_futures_balance(positions, instrument_uid)
    if balance != 0:
        raise RuntimeError(
            f"На счёте уже есть позиция по инструменту ({balance} лот.). Закрой её вручную и запусти бота снова."
        )
