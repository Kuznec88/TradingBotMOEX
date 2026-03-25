"""
После сессии: пишет trading_analytics_export.json в log/.
Запуск: python tools/export_session_metrics_bundle.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    base = Path(__file__).resolve().parents[1]
    db = base / "trade_economics.db"
    logd = base / "log"
    logd.mkdir(parents=True, exist_ok=True)
    out1 = logd / "trading_analytics_export.json"
    py = sys.executable
    r1 = subprocess.run(
        [py, str(base / "tools" / "export_trading_analytics_json.py"), str(db), "-o", str(out1)],
        cwd=str(base),
    )
    print(f"trading_analytics: exit={r1.returncode} path={out1}")
    return 0 if r1.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
