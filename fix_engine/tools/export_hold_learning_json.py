"""
Экспорт JSON: trades_with_hold_stats, metrics_by_direction, latency_impact,
spread_tertiles, learning_patch_effects (см. hold_learning_export.build_hold_learning_export).

Запуск из каталога fix_engine:
  python tools/export_hold_learning_json.py
  python tools/export_hold_learning_json.py --out log/hold_learning_export.json --all
"""

from __future__ import annotations

import sys
from pathlib import Path

# Репозиторий: tools/ -> fix_engine
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from hold_learning_export import export_hold_learning_json_main

if __name__ == "__main__":
    raise SystemExit(export_hold_learning_json_main())
