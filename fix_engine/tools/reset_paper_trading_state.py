"""
Сброс paper-истории: очистка сделок в trade_economics.db, analytics_state,
новый session_start_marker, пустой momentum_trades.jsonl.
Зависшие ордера снимаются только перезапуском процесса (OrderManager в памяти).

Запуск: python fix_engine/tools/reset_paper_trading_state.py
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path


def _read_start_balance(cfg: Path) -> float:
    if not cfg.exists():
        return 1000.0
    for line in cfg.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.split(";", 1)[0].strip()
        if not s or s.startswith("["):
            continue
        if "=" in s and s.split("=", 1)[0].strip() == "VirtualAccountStartBalance":
            try:
                return float(s.split("=", 1)[1].strip())
            except ValueError:
                break
    return 1000.0


def reset_state(fix_engine_dir: Path | None = None) -> dict[str, object]:
    fix_engine_dir = fix_engine_dir or Path(__file__).resolve().parents[1]
    db_path = fix_engine_dir / "trade_economics.db"
    log_dir = fix_engine_dir / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    marker_path = log_dir / "session_start_marker.txt"
    momentum_path = log_dir / "momentum_trades.jsonl"
    cfg = fix_engine_dir / "settings.cfg"

    now = datetime.now(timezone.utc)
    marker_path.write_text(now.isoformat() + "\n", encoding="utf-8")
    momentum_path.write_text("", encoding="utf-8")

    if not db_path.exists():
        return {
            "ok": True,
            "warning": "trade_economics.db missing; marker and momentum log cleared only",
            "session_start_marker_utc": now.isoformat(),
            "virtual_start_balance": _read_start_balance(cfg),
        }

    tables_delete = (
        "trade_economics",
        "trade_analytics",
        "round_trip_analytics",
        "cancel_analytics",
        "entry_decisions",
        "executed_entry_observability",
        "trade_observability",
        "round_trip_observability",
    )
    with sqlite3.connect(str(db_path)) as conn:
        for t in tables_delete:
            try:
                conn.execute(f"DELETE FROM {t}")
            except sqlite3.OperationalError:
                pass
        conn.execute(
            """
            UPDATE analytics_state
            SET cumulative_pnl = 0,
                equity_peak = 0,
                max_drawdown = 0,
                win_count = 0,
                total_trades = 0,
                avg_trade_pnl = 0,
                updated_at = datetime('now')
            WHERE id = 1
            """
        )
        conn.commit()

    start_bal = _read_start_balance(cfg)
    return {
        "ok": True,
        "db_path": str(db_path),
        "session_start_marker_utc": now.isoformat(),
        "cleared_tables": tables_delete,
        "analytics_state": "reset to zeros",
        "momentum_trades_jsonl": "emptied",
        "virtual_start_balance_config": start_bal,
        "note": "Restart main.py if it is running so in-memory orders clear.",
    }


def main() -> None:
    out = reset_state()
    import json

    print(json.dumps(out, ensure_ascii=False, indent=2))
    if not out.get("ok"):
        sys.exit(1)


if __name__ == "__main__":
    main()
