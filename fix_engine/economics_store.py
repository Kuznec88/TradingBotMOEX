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
                CREATE TABLE IF NOT EXISTS trade_analytics (
                    trade_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    gross_pnl REAL NOT NULL,
                    net_pnl REAL NOT NULL,
                    fees REAL NOT NULL,
                    slippage REAL NOT NULL,
                    spread_pnl REAL NOT NULL,
                    adverse_pnl REAL NOT NULL,
                    holding_pnl REAL NOT NULL,
                    expected_price REAL NOT NULL,
                    actual_price REAL NOT NULL,
                    adverse_px_10ms REAL,
                    adverse_px_100ms REAL,
                    adverse_px_1s REAL,
                    adverse_fill INTEGER NOT NULL DEFAULT 0,
                    hour_bucket INTEGER NOT NULL DEFAULT 0,
                    volatility_regime TEXT NOT NULL DEFAULT 'unknown',
                    spread_bucket TEXT NOT NULL DEFAULT 'unknown',
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS round_trip_analytics (
                    round_trip_id TEXT PRIMARY KEY,
                    entry_trade_id TEXT NOT NULL,
                    exit_trade_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    duration_ms REAL NOT NULL,
                    total_pnl REAL NOT NULL,
                    mae REAL NOT NULL,
                    mfe REAL NOT NULL,
                    entry_ts TEXT NOT NULL,
                    exit_ts TEXT NOT NULL
                )
                """
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

    def insert_trade_analytics(self, rows: list[dict[str, str]]) -> None:
        if not rows:
            return
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO trade_analytics(
                    trade_id, order_id, symbol, side, gross_pnl, net_pnl, fees, slippage, spread_pnl, adverse_pnl,
                    holding_pnl, expected_price, actual_price, hour_bucket, volatility_regime, spread_bucket,
                    adverse_fill, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["trade_id"],
                        row["order_id"],
                        row["symbol"],
                        row["side"],
                        float(row["gross_pnl"]),
                        float(row["net_pnl"]),
                        float(row["fees"]),
                        float(row["slippage"]),
                        float(row["spread_pnl"]),
                        float(row["adverse_pnl"]),
                        float(row["holding_pnl"]),
                        float(row["expected_price"]),
                        float(row["actual_price"]),
                        int(row.get("hour_bucket", "0")),
                        row.get("volatility_regime", "unknown"),
                        row.get("spread_bucket", "unknown"),
                        1 if row.get("is_adverse_fill", "0") == "1" else 0,
                        row["created_at"],
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def insert_round_trips(self, rows: list[dict[str, str | float]]) -> None:
        if not rows:
            return
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO round_trip_analytics(
                    round_trip_id, entry_trade_id, exit_trade_id, symbol, side, duration_ms, total_pnl, mae, mfe, entry_ts, exit_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["round_trip_id"],
                        row["entry_trade_id"],
                        row["exit_trade_id"],
                        row["symbol"],
                        row["side"],
                        float(row["duration_ms"]),
                        float(row["total_pnl"]),
                        float(row["mae"]),
                        float(row["mfe"]),
                        row["entry_ts"],
                        row["exit_ts"],
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def update_adverse_selection(
        self,
        *,
        trade_id: str,
        px_10ms: float | None,
        px_100ms: float | None,
        px_1s: float | None,
        adverse_pnl: float,
        adverse_fill: bool,
    ) -> None:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                UPDATE trade_analytics
                SET adverse_px_10ms=?,
                    adverse_px_100ms=?,
                    adverse_px_1s=?,
                    adverse_pnl=?,
                    adverse_fill=?
                WHERE trade_id=?
                """,
                (
                    px_10ms,
                    px_100ms,
                    px_1s,
                    float(adverse_pnl),
                    1 if adverse_fill else 0,
                    trade_id,
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
        failed_trades = max(0, total_trades - win_count)
        win_rate = (float(win_count) / float(total_trades) * 100.0) if total_trades else 0.0
        fail_rate = (float(failed_trades) / float(total_trades) * 100.0) if total_trades else 0.0
        success_failure_ratio = (float(win_count) / float(failed_trades)) if failed_trades else (
            float(win_count) if win_count > 0 else 0.0
        )
        return {
            "cumulative_pnl": cumulative_pnl,
            "max_drawdown": max_drawdown,
            "win_rate": float(win_rate),
            "fail_rate": float(fail_rate),
            "average_trade_pnl": avg_trade_pnl,
            "successful_trades": float(win_count),
            "failed_trades": float(failed_trades),
            "success_to_failure_ratio": float(success_failure_ratio),
            # Backward-compatible aliases
            "avg_pnl_per_trade": avg_trade_pnl,
            "total_pnl": cumulative_pnl,
            "total_trades": float(total_trades),
        }

    def top_losing_trades(self, limit: int = 10) -> list[dict[str, float | str]]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT trade_id, order_id, symbol, net_pnl, slippage, spread_pnl, adverse_pnl, holding_pnl, created_at
                FROM trade_analytics
                ORDER BY net_pnl ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        return [
            {
                "trade_id": r[0],
                "order_id": r[1],
                "symbol": r[2],
                "net_pnl": float(r[3]),
                "slippage": float(r[4]),
                "spread_pnl": float(r[5]),
                "adverse_pnl": float(r[6]),
                "holding_pnl": float(r[7]),
                "created_at": r[8],
            }
            for r in rows
        ]

    def top_profitable_trades(self, limit: int = 10) -> list[dict[str, float | str]]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT trade_id, order_id, symbol, net_pnl, slippage, spread_pnl, adverse_pnl, holding_pnl, created_at
                FROM trade_analytics
                ORDER BY net_pnl DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        return [
            {
                "trade_id": r[0],
                "order_id": r[1],
                "symbol": r[2],
                "net_pnl": float(r[3]),
                "slippage": float(r[4]),
                "spread_pnl": float(r[5]),
                "adverse_pnl": float(r[6]),
                "holding_pnl": float(r[7]),
                "created_at": r[8],
            }
            for r in rows
        ]

    def worst_slippage_trades(self, limit: int = 10) -> list[dict[str, float | str]]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT trade_id, order_id, symbol, slippage, net_pnl, expected_price, actual_price, created_at
                FROM trade_analytics
                ORDER BY slippage ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        return [
            {
                "trade_id": r[0],
                "order_id": r[1],
                "symbol": r[2],
                "slippage": float(r[3]),
                "net_pnl": float(r[4]),
                "expected_price": float(r[5]),
                "actual_price": float(r[6]),
                "created_at": r[7],
            }
            for r in rows
        ]

    def pnl_by_component(self) -> dict[str, float]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(net_pnl),0),
                    COALESCE(SUM(spread_pnl),0),
                    COALESCE(SUM(slippage),0),
                    COALESCE(SUM(adverse_pnl),0),
                    COALESCE(SUM(holding_pnl),0),
                    COALESCE(SUM(fees),0)
                FROM trade_analytics
                """
            ).fetchone()
        return {
            "net_pnl": float(row[0]),
            "spread_pnl": float(row[1]),
            "slippage_pnl": float(row[2]),
            "adverse_selection_pnl": float(row[3]),
            "holding_pnl": float(row[4]),
            "fees": float(row[5]),
        }

    def pnl_distribution(self) -> list[dict[str, float | str]]:
        buckets = [
            ("<=-1.0", -10_000.0, -1.0),
            ("-1.0..-0.2", -1.0, -0.2),
            ("-0.2..0.0", -0.2, 0.0),
            ("0.0..0.2", 0.0, 0.2),
            ("0.2..1.0", 0.2, 1.0),
            (">1.0", 1.0, 10_000.0),
        ]
        result: list[dict[str, float | str]] = []
        with self._lock, sqlite3.connect(self._db_path) as conn:
            for label, lo, hi in buckets:
                row = conn.execute(
                    "SELECT COUNT(1), COALESCE(SUM(net_pnl),0) FROM trade_analytics WHERE net_pnl > ? AND net_pnl <= ?",
                    (lo, hi),
                ).fetchone()
                result.append({"bucket": label, "count": int(row[0]), "pnl": float(row[1])})
        return result

    def pnl_by_time_of_day(self) -> list[dict[str, float | int]]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT hour_bucket, COUNT(1), COALESCE(SUM(net_pnl),0)
                FROM trade_analytics
                GROUP BY hour_bucket
                ORDER BY hour_bucket
                """
            ).fetchall()
        return [{"hour": int(r[0]), "count": int(r[1]), "pnl": float(r[2])} for r in rows]

    def pnl_by_volatility_regime(self) -> list[dict[str, float | int | str]]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT volatility_regime, COUNT(1), COALESCE(SUM(net_pnl),0)
                FROM trade_analytics
                GROUP BY volatility_regime
                ORDER BY volatility_regime
                """
            ).fetchall()
        return [{"volatility_regime": r[0], "count": int(r[1]), "pnl": float(r[2])} for r in rows]

    def pnl_by_spread_size(self) -> list[dict[str, float | int | str]]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT spread_bucket, COUNT(1), COALESCE(SUM(net_pnl),0)
                FROM trade_analytics
                GROUP BY spread_bucket
                ORDER BY spread_bucket
                """
            ).fetchall()
        return [{"spread_bucket": r[0], "count": int(r[1]), "pnl": float(r[2])} for r in rows]

    def adverse_fill_stats(self) -> dict[str, float]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(1),
                    COALESCE(SUM(CASE WHEN adverse_fill=1 THEN 1 ELSE 0 END),0),
                    COALESCE(AVG(CASE WHEN adverse_fill=1 THEN adverse_pnl ELSE NULL END),0)
                FROM trade_analytics
                """
            ).fetchone()
        total = int(row[0]) if row else 0
        adverse = int(row[1]) if row else 0
        pct = (adverse / total * 100.0) if total else 0.0
        return {
            "total_fills": float(total),
            "adverse_fills": float(adverse),
            "adverse_fill_pct": float(pct),
            "avg_adverse_selection_loss": float(row[2] or 0.0),
        }
