from __future__ import annotations

import sqlite3
from pathlib import Path
from threading import RLock


class EconomicsStore:
    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        self._lock = RLock()
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_economics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    cl_ord_id TEXT NOT NULL,
                    exec_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty REAL NOT NULL,
                    price REAL NOT NULL,
                    gross_pnl REAL NOT NULL,
                    fees REAL NOT NULL,
                    net_pnl REAL NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trade_econ_exec_id ON trade_economics(exec_id)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    cumulative_pnl REAL NOT NULL DEFAULT 0,
                    equity_peak REAL NOT NULL DEFAULT 0,
                    max_drawdown REAL NOT NULL DEFAULT 0,
                    win_count INTEGER NOT NULL DEFAULT 0,
                    total_trades INTEGER NOT NULL DEFAULT 0,
                    avg_trade_pnl REAL NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO analytics_state (id, cumulative_pnl, equity_peak, max_drawdown, win_count, total_trades, avg_trade_pnl)
                VALUES (1, 0, 0, 0, 0, 0, 0)
                """
            )
            conn.commit()

    def insert_trade(self, record: dict[str, str]) -> None:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            net_pnl = float(record["net_pnl"])
            conn.execute(
                """
                INSERT INTO trade_economics (
                    market, symbol, cl_ord_id, exec_id, side, qty, price, gross_pnl, fees, net_pnl
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["market"],
                    record["symbol"],
                    record["cl_ord_id"],
                    record["exec_id"],
                    record["side"],
                    float(record["qty"]),
                    float(record["price"]),
                    float(record["gross_pnl"]),
                    float(record["fees"]),
                    net_pnl,
                ),
            )
            row = conn.execute(
                """
                SELECT cumulative_pnl, equity_peak, max_drawdown, win_count, total_trades
                FROM analytics_state WHERE id = 1
                """
            ).fetchone()
            cumulative_pnl = float(row[0]) + net_pnl
            equity_peak = max(float(row[1]), cumulative_pnl)
            current_drawdown = max(0.0, equity_peak - cumulative_pnl)
            max_drawdown = max(float(row[2]), current_drawdown)
            win_count = int(row[3]) + (1 if net_pnl > 0 else 0)
            total_trades = int(row[4]) + 1
            avg_trade_pnl = cumulative_pnl / total_trades if total_trades else 0.0
            conn.execute(
                """
                UPDATE analytics_state
                SET cumulative_pnl = ?,
                    equity_peak = ?,
                    max_drawdown = ?,
                    win_count = ?,
                    total_trades = ?,
                    avg_trade_pnl = ?,
                    updated_at = datetime('now')
                WHERE id = 1
                """,
                (
                    cumulative_pnl,
                    equity_peak,
                    max_drawdown,
                    win_count,
                    total_trades,
                    avg_trade_pnl,
                ),
            )
            conn.commit()

    def get_metrics(self) -> dict[str, float]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT cumulative_pnl, max_drawdown, win_count, total_trades, avg_trade_pnl
                FROM analytics_state WHERE id = 1
                """
            ).fetchone()
        cumulative_pnl = float(row[0]) if row else 0.0
        max_drawdown = float(row[1]) if row else 0.0
        win_count = int(row[2]) if row else 0
        total_trades = int(row[3]) if row else 0
        avg_trade_pnl = float(row[4]) if row else 0.0
        win_rate = (float(win_count) / float(total_trades) * 100.0) if total_trades else 0.0
        return {
            "cumulative_pnl": cumulative_pnl,
            "max_drawdown": max_drawdown,
            "win_rate": float(win_rate),
            "average_trade_pnl": avg_trade_pnl,
            # Backward-compatible aliases
            "avg_pnl_per_trade": avg_trade_pnl,
            "total_pnl": cumulative_pnl,
            "total_trades": float(total_trades),
        }
