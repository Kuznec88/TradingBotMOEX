"""Pre-run checks: sandbox token, unary market-data RPC (no orders)."""

from __future__ import annotations

import logging
from pathlib import Path


def load_sandbox_token(base_dir: Path) -> str:
    from fix_engine.tools.common_cfg_dir import read_tinvest_token_from_dir

    return read_tinvest_token_from_dir(base_dir)


def verify_market_data_readonly(
    *,
    token: str,
    host: str,
    instrument_id: str,
    logger: logging.Logger,
) -> str:
    """Raises on failure. Uses only MarketDataService unary calls — no orders."""
    from t_tech.invest import Client

    with Client(token, target=host) as client:
        _ = client.market_data.get_last_prices(instrument_id=[instrument_id])
        _ = client.market_data.get_order_book(instrument_id=instrument_id, depth=1)
    logger.info(
        "[PREFLIGHT] T-Invest token OK; get_last_prices + get_order_book succeeded (read-only).",
    )
    return host


def assert_no_orders_client_exposed() -> None:
    """Устарело: paper-сессия не шлёт ордера; LIVE использует fix_engine.execution.tbank_broker."""
    return
