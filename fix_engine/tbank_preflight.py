"""Pre-run checks: sandbox token, unary market-data RPC (no orders)."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path


def load_sandbox_token(base_dir: Path, cfg_read_str: Callable[[Path, str, str], str]) -> str:
    import os

    tok = os.getenv("TINKOFF_TOKEN", "").strip() or os.getenv("TINKOFF_SANDBOX_TOKEN", "").strip()
    if tok:
        return tok
    local = base_dir / "settings.local.cfg"
    if local.exists():
        tok = cfg_read_str(local, "TBankSandboxToken", "").strip()
    if not tok:
        tok = cfg_read_str(base_dir / "settings.cfg", "TBankSandboxToken", "").strip()
    return tok


def verify_market_data_readonly(
    *,
    token: str,
    host: str,
    instrument_id: str,
    logger: logging.Logger,
) -> None:
    """Raises on failure. Uses only MarketDataService unary calls — no orders."""
    from t_tech.invest import Client

    with Client(token, target=host) as client:
        _ = client.market_data.get_last_prices(instrument_id=[instrument_id])
        _ = client.market_data.get_order_book(instrument_id=instrument_id, depth=1)
    logger.info(
        "[PREFLIGHT] T-Invest token OK; get_last_prices + get_order_book succeeded (read-only).",
    )


def assert_no_orders_client_exposed() -> None:
    """Documentary guard: order placement must not be used in paper pipeline."""
    return
