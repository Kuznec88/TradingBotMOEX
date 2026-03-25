from __future__ import annotations


def run() -> None:
    """
    Canonical entrypoint for the bot.

    Phase-1 refactor: keep existing runtime in `fix_engine.main.run`
    while we extract modules safely.
    """

    from fix_engine.app.runtime import run as runtime_run

    runtime_run()

