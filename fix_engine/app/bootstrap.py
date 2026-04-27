from __future__ import annotations

from pathlib import Path


def run() -> None:
    """Единая точка входа бота: wiring и live-цикл в `fix_engine.app.runtime.run`."""
    fix_engine_dir = Path(__file__).resolve().parents[1]
    from fix_engine.data.preflight import prepare_invest_api_tls_trust_store

    prepare_invest_api_tls_trust_store(fix_engine_dir)

    from fix_engine.app.runtime import run as runtime_run

    runtime_run()
