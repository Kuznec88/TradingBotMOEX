"""Офлайн-бэктесты (CSV и т.п.), без live-стрима."""

__all__ = ["run_swing_csv_backtest_cli"]


def run_swing_csv_backtest_cli() -> int:
    """Ленивый импорт: чтобы `python -m fix_engine.backtest.swing_csv` не ловил предзагрузку модуля."""
    from fix_engine.backtest.swing_csv import main

    return main()
