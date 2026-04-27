"""Обёртка CLI: `fix_engine.backtest.swing_csv` (путь tools/ сохранён для совместимости)."""

from __future__ import annotations

import sys
from pathlib import Path

_root = Path(__file__).resolve().parents[2]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from fix_engine.backtest.swing_csv import main

if __name__ == "__main__":
    raise SystemExit(main())
