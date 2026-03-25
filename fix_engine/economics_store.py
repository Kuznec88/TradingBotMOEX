from __future__ import annotations

import json
import math
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
                    adverse_px_500ms REAL,
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
                    immediate_move REAL NOT NULL DEFAULT 0,
                    entry_price REAL NOT NULL DEFAULT 0,
                    exit_price REAL NOT NULL DEFAULT 0,
                    entry_spread REAL NOT NULL DEFAULT 0,
                    entry_volatility_regime TEXT NOT NULL DEFAULT 'unknown',
                    entry_time_in_book_ms REAL NOT NULL DEFAULT 0,
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
                CREATE TABLE IF NOT EXISTS cancel_analytics (
                    cancel_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    cancel_reason TEXT NOT NULL,
                    cancel_price REAL NOT NULL,
                    horizon_price REAL NOT NULL,
                    missed_pnl REAL NOT NULL,
                    horizon_ms INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cancel_analytics_reason ON cancel_analytics(cancel_reason)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS entry_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry_score REAL NOT NULL,
                    spread_score REAL NOT NULL,
                    stability_score REAL NOT NULL,
                    trend_score REAL NOT NULL,
                    imbalance_score REAL NOT NULL,
                    decision TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_entry_decisions_symbol_time ON entry_decisions(symbol, timestamp)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS executed_entry_observability (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    exec_id TEXT NOT NULL UNIQUE,
                    cl_ord_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    mid_price REAL NOT NULL,
                    bid_price REAL NOT NULL,
                    ask_price REAL NOT NULL,
                    bid_size REAL NOT NULL,
                    ask_size REAL NOT NULL,
                    imbalance REAL NOT NULL,
                    microprice_edge REAL NOT NULL,
                    momentum_100ms REAL NOT NULL,
                    momentum_500ms REAL NOT NULL,
                    spread REAL NOT NULL,
                    last_50ms_move REAL NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_executed_entry_obs_symbol_time ON executed_entry_observability(symbol, timestamp)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_observability (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT NOT NULL UNIQUE,
                    round_trip_id TEXT,
                    side TEXT NOT NULL,
                    entry_timestamp TEXT,
                    exit_timestamp TEXT,
                    entry_price REAL NOT NULL DEFAULT 0,
                    exit_price REAL NOT NULL DEFAULT 0,
                    realized_pnl REAL NOT NULL DEFAULT 0,
                    mid_price REAL NOT NULL DEFAULT 0,
                    bid_price REAL NOT NULL DEFAULT 0,
                    ask_price REAL NOT NULL DEFAULT 0,
                    bid_size REAL NOT NULL DEFAULT 0,
                    ask_size REAL NOT NULL DEFAULT 0,
                    imbalance REAL NOT NULL DEFAULT 0,
                    microprice_edge REAL NOT NULL DEFAULT 0,
                    momentum_100ms REAL NOT NULL DEFAULT 0,
                    momentum_500ms REAL NOT NULL DEFAULT 0,
                    spread REAL NOT NULL DEFAULT 0,
                    last_50ms_price_move REAL NOT NULL DEFAULT 0,
                    decision_timestamp TEXT,
                    order_sent_timestamp TEXT,
                    fill_timestamp TEXT,
                    latency_ms REAL NOT NULL DEFAULT 0,
                    lookahead_violation INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trade_observability_created_at ON trade_observability(created_at)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS round_trip_observability (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_trip_id TEXT NOT NULL UNIQUE,
                    trade_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry_timestamp TEXT NOT NULL,
                    exit_timestamp TEXT NOT NULL,
                    entry_price REAL NOT NULL DEFAULT 0,
                    exit_price REAL NOT NULL DEFAULT 0,
                    realized_pnl REAL NOT NULL DEFAULT 0,
                    duration_ms REAL NOT NULL DEFAULT 0,
                    mfe REAL NOT NULL DEFAULT 0,
                    mae REAL NOT NULL DEFAULT 0,
                    adverse_rate REAL NOT NULL DEFAULT 0,
                    immediate_move REAL NOT NULL DEFAULT 0,
                    mid_price REAL NOT NULL DEFAULT 0,
                    bid_price REAL NOT NULL DEFAULT 0,
                    ask_price REAL NOT NULL DEFAULT 0,
                    bid_size REAL NOT NULL DEFAULT 0,
                    ask_size REAL NOT NULL DEFAULT 0,
                    imbalance REAL NOT NULL DEFAULT 0,
                    microprice_edge REAL NOT NULL DEFAULT 0,
                    momentum_100ms REAL NOT NULL DEFAULT 0,
                    momentum_500ms REAL NOT NULL DEFAULT 0,
                    spread REAL NOT NULL DEFAULT 0,
                    last_50ms_price_move REAL NOT NULL DEFAULT 0,
                    decision_timestamp TEXT,
                    order_sent_timestamp TEXT,
                    fill_timestamp TEXT,
                    latency_ms REAL NOT NULL DEFAULT 0,
                    lookahead_violation INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_round_trip_observability_exit_ts ON round_trip_observability(exit_timestamp)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS learning_patch_effects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    applied_ts TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_learning_patch_applied_ts ON learning_patch_effects(applied_ts)"
            )
            self._ensure_column(conn, "trade_analytics", "adverse_px_500ms", "REAL")
            self._ensure_column(conn, "trade_analytics", "time_in_book_ms", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(conn, "round_trip_analytics", "entry_price", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(conn, "round_trip_analytics", "exit_price", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(conn, "round_trip_analytics", "entry_spread", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(conn, "round_trip_analytics", "immediate_move", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(
                conn, "round_trip_analytics", "entry_volatility_regime", "TEXT NOT NULL DEFAULT 'unknown'"
            )
            self._ensure_column(conn, "round_trip_analytics", "entry_time_in_book_ms", "REAL NOT NULL DEFAULT 0")
            conn.execute(
                """
                INSERT OR IGNORE INTO analytics_state (id, cumulative_pnl, equity_peak, max_drawdown, win_count, total_trades, avg_trade_pnl)
                VALUES (1, 0, 0, 0, 0, 0, 0)
                """
            )
            conn.commit()

    def insert_learning_patch_effects(self, payload: dict[str, object], *, applied_ts: str | None = None) -> None:
        from datetime import datetime, timezone

        ts = applied_ts or datetime.now(timezone.utc).isoformat()
        blob = json.dumps(payload, ensure_ascii=False)
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.execute(
                "INSERT INTO learning_patch_effects (applied_ts, payload_json) VALUES (?, ?)",
                (ts, blob),
            )
            conn.commit()

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

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
                    adverse_fill, time_in_book_ms, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        float(row.get("time_in_book_ms", "0")),
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
                    round_trip_id, entry_trade_id, exit_trade_id, symbol, side, duration_ms, total_pnl, mae, mfe,
                    immediate_move, entry_price, exit_price, entry_spread, entry_volatility_regime, entry_time_in_book_ms, entry_ts, exit_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        float(row.get("immediate_move", 0.0)),
                        float(row.get("entry_price", 0.0)),
                        float(row.get("exit_price", 0.0)),
                        float(row.get("entry_spread", 0.0)),
                        str(row.get("entry_volatility_regime", "unknown")),
                        float(row.get("entry_time_in_book_ms", 0.0)),
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
        px_500ms: float | None,
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
                    adverse_px_500ms=?,
                    adverse_px_1s=?,
                    adverse_pnl=?,
                    adverse_fill=?
                WHERE trade_id=?
                """,
                (
                    px_10ms,
                    px_100ms,
                    px_500ms,
                    px_1s,
                    float(adverse_pnl),
                    1 if adverse_fill else 0,
                    trade_id,
                ),
            )
            conn.commit()

    def insert_cancel_analytics(self, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO cancel_analytics(
                    cancel_id, order_id, symbol, side, cancel_reason, cancel_price, horizon_price,
                    missed_pnl, horizon_ms, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        str(row.get("cancel_id", "")),
                        str(row.get("order_id", "")),
                        str(row.get("symbol", "")),
                        str(row.get("side", "")),
                        str(row.get("cancel_reason", "unknown")),
                        float(row.get("cancel_price", 0.0)),
                        float(row.get("horizon_price", 0.0)),
                        float(row.get("missed_pnl", 0.0)),
                        int(row.get("horizon_ms", 0)),
                        str(row.get("created_at", "")),
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def insert_entry_decisions(self, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.executemany(
                """
                INSERT INTO entry_decisions(
                    timestamp, symbol, side, entry_score, spread_score, stability_score,
                    trend_score, imbalance_score, decision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        str(row.get("timestamp", "")),
                        str(row.get("symbol", "")),
                        str(row.get("side", "")),
                        float(row.get("entry_score", 0.0)),
                        float(row.get("spread_score", 0.0)),
                        float(row.get("stability_score", 0.0)),
                        float(row.get("trend_score", 0.0)),
                        float(row.get("imbalance_score", 0.0)),
                        str(row.get("decision", "SKIP")),
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def insert_executed_entry_observability(self, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO executed_entry_observability(
                    timestamp, exec_id, cl_ord_id, symbol, side,
                    mid_price, bid_price, ask_price, bid_size, ask_size, imbalance,
                    microprice_edge, momentum_100ms, momentum_500ms, spread, last_50ms_move
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        str(row.get("timestamp", "")),
                        str(row.get("exec_id", "")),
                        str(row.get("cl_ord_id", "")),
                        str(row.get("symbol", "")),
                        str(row.get("side", "")),
                        float(row.get("mid_price", 0.0)),
                        float(row.get("bid_price", 0.0)),
                        float(row.get("ask_price", 0.0)),
                        float(row.get("bid_size", 0.0)),
                        float(row.get("ask_size", 0.0)),
                        float(row.get("imbalance", 0.0)),
                        float(row.get("microprice_edge", 0.0)),
                        float(row.get("momentum_100ms", 0.0)),
                        float(row.get("momentum_500ms", 0.0)),
                        float(row.get("spread", 0.0)),
                        float(row.get("last_50ms_move", 0.0)),
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def insert_trade_observability(self, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO trade_observability(
                    trade_id, round_trip_id, side, entry_timestamp, exit_timestamp, entry_price, exit_price, realized_pnl,
                    mid_price, bid_price, ask_price, bid_size, ask_size, imbalance,
                    microprice_edge, momentum_100ms, momentum_500ms, spread, last_50ms_price_move,
                    decision_timestamp, order_sent_timestamp, fill_timestamp, latency_ms, lookahead_violation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        str(row.get("trade_id", "")),
                        str(row.get("round_trip_id", "")) if row.get("round_trip_id") is not None else None,
                        str(row.get("side", "")),
                        str(row.get("entry_timestamp", "")),
                        str(row.get("exit_timestamp", "")),
                        float(row.get("entry_price", 0.0)),
                        float(row.get("exit_price", 0.0)),
                        float(row.get("realized_pnl", 0.0)),
                        float(row.get("mid_price", 0.0)),
                        float(row.get("bid_price", 0.0)),
                        float(row.get("ask_price", 0.0)),
                        float(row.get("bid_size", 0.0)),
                        float(row.get("ask_size", 0.0)),
                        float(row.get("imbalance", 0.0)),
                        float(row.get("microprice_edge", 0.0)),
                        float(row.get("momentum_100ms", 0.0)),
                        float(row.get("momentum_500ms", 0.0)),
                        float(row.get("spread", 0.0)),
                        float(row.get("last_50ms_price_move", 0.0)),
                        str(row.get("decision_timestamp", "")),
                        str(row.get("order_sent_timestamp", "")),
                        str(row.get("fill_timestamp", "")),
                        float(row.get("latency_ms", 0.0)),
                        1 if bool(row.get("lookahead_violation", False)) else 0,
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def insert_round_trip_observability(self, rows: list[dict[str, object]]) -> None:
        if not rows:
            return
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO round_trip_observability(
                    round_trip_id, trade_id, side, entry_timestamp, exit_timestamp, entry_price, exit_price,
                    realized_pnl, duration_ms, mfe, mae, adverse_rate, immediate_move,
                    mid_price, bid_price, ask_price, bid_size, ask_size, imbalance,
                    microprice_edge, momentum_100ms, momentum_500ms, spread, last_50ms_price_move,
                    decision_timestamp, order_sent_timestamp, fill_timestamp, latency_ms, lookahead_violation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        str(row.get("round_trip_id", "")),
                        str(row.get("trade_id", "")),
                        str(row.get("side", "")),
                        str(row.get("entry_timestamp", "")),
                        str(row.get("exit_timestamp", "")),
                        float(row.get("entry_price", 0.0)),
                        float(row.get("exit_price", 0.0)),
                        float(row.get("realized_pnl", 0.0)),
                        float(row.get("duration_ms", 0.0)),
                        float(row.get("mfe", 0.0)),
                        float(row.get("mae", 0.0)),
                        float(row.get("adverse_rate", 0.0)),
                        float(row.get("immediate_move", 0.0)),
                        float(row.get("mid_price", 0.0)),
                        float(row.get("bid_price", 0.0)),
                        float(row.get("ask_price", 0.0)),
                        float(row.get("bid_size", 0.0)),
                        float(row.get("ask_size", 0.0)),
                        float(row.get("imbalance", 0.0)),
                        float(row.get("microprice_edge", 0.0)),
                        float(row.get("momentum_100ms", 0.0)),
                        float(row.get("momentum_500ms", 0.0)),
                        float(row.get("spread", 0.0)),
                        float(row.get("last_50ms_price_move", 0.0)),
                        str(row.get("decision_timestamp", "")),
                        str(row.get("order_sent_timestamp", "")),
                        str(row.get("fill_timestamp", "")),
                        float(row.get("latency_ms", 0.0)),
                        1 if bool(row.get("lookahead_violation", False)) else 0,
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def get_trade_observability(self, trade_id: str) -> dict[str, object] | None:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT
                    trade_id, round_trip_id, side, entry_timestamp, exit_timestamp, entry_price, exit_price, realized_pnl,
                    mid_price, bid_price, ask_price, bid_size, ask_size, imbalance,
                    microprice_edge, momentum_100ms, momentum_500ms, spread, last_50ms_price_move,
                    decision_timestamp, order_sent_timestamp, fill_timestamp, latency_ms, lookahead_violation
                FROM trade_observability
                WHERE trade_id=?
                """,
                (trade_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "trade_id": str(row[0] or ""),
            "round_trip_id": str(row[1] or ""),
            "side": str(row[2] or ""),
            "entry_timestamp": str(row[3] or ""),
            "exit_timestamp": str(row[4] or ""),
            "entry_price": float(row[5] or 0.0),
            "exit_price": float(row[6] or 0.0),
            "realized_pnl": float(row[7] or 0.0),
            "mid_price": float(row[8] or 0.0),
            "bid_price": float(row[9] or 0.0),
            "ask_price": float(row[10] or 0.0),
            "bid_size": float(row[11] or 0.0),
            "ask_size": float(row[12] or 0.0),
            "imbalance": float(row[13] or 0.0),
            "microprice_edge": float(row[14] or 0.0),
            "momentum_100ms": float(row[15] or 0.0),
            "momentum_500ms": float(row[16] or 0.0),
            "spread": float(row[17] or 0.0),
            "last_50ms_price_move": float(row[18] or 0.0),
            "decision_timestamp": str(row[19] or ""),
            "order_sent_timestamp": str(row[20] or ""),
            "fill_timestamp": str(row[21] or ""),
            "latency_ms": float(row[22] or 0.0),
            "lookahead_violation": bool(int(row[23] or 0)),
        }

    def link_trade_to_round_trip(self, *, trade_id: str, round_trip_id: str) -> None:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                UPDATE trade_observability
                SET round_trip_id=?
                WHERE trade_id=?
                """,
                (str(round_trip_id), str(trade_id)),
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
        stats = self.fill_quality_stats()
        return {
            "total_fills": float(stats["total_fills"]),
            "adverse_fills": float(stats["adverse_fills_1s"]),
            "adverse_fill_pct": float(stats["adverse_fill_rate_1s_pct"]),
            "avg_adverse_selection_loss": float(stats["avg_adverse_selection_loss"]),
        }

    def fill_quality_stats(self) -> dict[str, float]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT side, actual_price, adverse_pnl, adverse_px_10ms, adverse_px_100ms, adverse_px_500ms, adverse_px_1s
                FROM trade_analytics
                """
            ).fetchall()

        total_fills = len(rows)
        adverse_10ms = 0
        adverse_100ms = 0
        adverse_1s = 0
        adverse_500ms = 0
        eval_10ms = 0
        eval_100ms = 0
        eval_500ms = 0
        eval_1s = 0
        move_sum_10ms = 0.0
        move_sum_100ms = 0.0
        move_sum_1s = 0.0
        abs_move_sum_10ms = 0.0
        abs_move_sum_100ms = 0.0
        abs_move_sum_1s = 0.0
        move_sum_500ms = 0.0
        abs_move_sum_500ms = 0.0
        adverse_loss_sum = 0.0
        adverse_loss_count = 0

        for side_raw, actual_price, adverse_pnl, px_10ms, px_100ms, px_500ms, px_1s in rows:
            side = str(side_raw).upper()
            fill_px = float(actual_price or 0.0)
            if fill_px <= 0:
                continue
            is_buy = side in {"1", "BUY"}
            is_sell = side in {"2", "SELL"}
            if not (is_buy or is_sell):
                continue

            adverse_pnl_value = float(adverse_pnl or 0.0)
            if adverse_pnl_value < 0:
                adverse_loss_sum += adverse_pnl_value
                adverse_loss_count += 1

            if px_10ms is not None:
                px = float(px_10ms)
                eval_10ms += 1
                raw_move = px - fill_px
                move_sum_10ms += raw_move
                abs_move_sum_10ms += abs(raw_move)
                if (is_buy and px < fill_px) or (is_sell and px > fill_px):
                    adverse_10ms += 1
            if px_100ms is not None:
                px = float(px_100ms)
                eval_100ms += 1
                raw_move = px - fill_px
                move_sum_100ms += raw_move
                abs_move_sum_100ms += abs(raw_move)
                if (is_buy and px < fill_px) or (is_sell and px > fill_px):
                    adverse_100ms += 1
            if px_500ms is not None:
                px = float(px_500ms)
                eval_500ms += 1
                raw_move = px - fill_px
                move_sum_500ms += raw_move
                abs_move_sum_500ms += abs(raw_move)
                if (is_buy and px < fill_px) or (is_sell and px > fill_px):
                    adverse_500ms += 1
            if px_1s is not None:
                px = float(px_1s)
                eval_1s += 1
                raw_move = px - fill_px
                move_sum_1s += raw_move
                abs_move_sum_1s += abs(raw_move)
                if (is_buy and px < fill_px) or (is_sell and px > fill_px):
                    adverse_1s += 1

        immediate_negative_pct = (adverse_10ms / eval_10ms * 100.0) if eval_10ms else 0.0
        adverse_100ms_pct = (adverse_100ms / eval_100ms * 100.0) if eval_100ms else 0.0
        adverse_500ms_pct = (adverse_500ms / eval_500ms * 100.0) if eval_500ms else 0.0
        adverse_1s_pct = (adverse_1s / eval_1s * 100.0) if eval_1s else 0.0
        total_evaluated_points = eval_10ms + eval_100ms + eval_500ms + eval_1s
        total_adverse_points = adverse_10ms + adverse_100ms + adverse_500ms + adverse_1s
        overall_adverse_selection_rate = (
            total_adverse_points / total_evaluated_points * 100.0 if total_evaluated_points else 0.0
        )
        return {
            "total_fills": float(total_fills),
            "evaluated_fills_10ms": float(eval_10ms),
            "evaluated_fills_100ms": float(eval_100ms),
            "evaluated_fills_1s": float(eval_1s),
            "evaluated_fills_500ms": float(eval_500ms),
            "adverse_fills_10ms": float(adverse_10ms),
            "adverse_fills_100ms": float(adverse_100ms),
            "adverse_fills_500ms": float(adverse_500ms),
            "adverse_fills_1s": float(adverse_1s),
            "immediate_negative_fills": float(adverse_10ms),
            "immediate_negative_pct": float(immediate_negative_pct),
            "adverse_fill_rate_10ms_pct": float(immediate_negative_pct),
            "adverse_fill_rate_100ms_pct": float(adverse_100ms_pct),
            "adverse_fill_rate_500ms_pct": float(adverse_500ms_pct),
            "adverse_fill_rate_1s_pct": float(adverse_1s_pct),
            "overall_adverse_selection_rate_pct": float(overall_adverse_selection_rate),
            "avg_move_10ms": float(move_sum_10ms / eval_10ms) if eval_10ms else 0.0,
            "avg_move_100ms": float(move_sum_100ms / eval_100ms) if eval_100ms else 0.0,
            "avg_move_500ms": float(move_sum_500ms / eval_500ms) if eval_500ms else 0.0,
            "avg_move_1s": float(move_sum_1s / eval_1s) if eval_1s else 0.0,
            "avg_abs_move_10ms": float(abs_move_sum_10ms / eval_10ms) if eval_10ms else 0.0,
            "avg_abs_move_100ms": float(abs_move_sum_100ms / eval_100ms) if eval_100ms else 0.0,
            "avg_abs_move_500ms": float(abs_move_sum_500ms / eval_500ms) if eval_500ms else 0.0,
            "avg_abs_move_1s": float(abs_move_sum_1s / eval_1s) if eval_1s else 0.0,
            "avg_adverse_selection_loss": float(adverse_loss_sum / adverse_loss_count) if adverse_loss_count else 0.0,
        }

    def trade_outcome_analysis(self) -> dict[str, object]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT
                    rt.entry_trade_id,
                    rt.exit_trade_id,
                    rt.symbol,
                    rt.side,
                    rt.entry_price,
                    rt.exit_price,
                    rt.total_pnl,
                    rt.duration_ms,
                    rt.mae,
                    rt.mfe,
                    rt.immediate_move,
                    rt.entry_spread,
                    rt.entry_volatility_regime,
                    rt.entry_time_in_book_ms,
                    ta.adverse_px_10ms,
                    ta.adverse_px_100ms,
                    ta.adverse_px_500ms
                FROM round_trip_analytics rt
                LEFT JOIN trade_analytics ta ON ta.trade_id = rt.entry_trade_id
                ORDER BY rt.exit_ts
                """
            ).fetchall()

        trades: list[dict[str, object]] = []
        good: list[dict[str, object]] = []
        bad: list[dict[str, object]] = []
        immediate_adverse = 0
        immediate_eval = 0
        bad_immediate = 0
        bad_eval = 0
        bad_later = 0
        spread_groups: dict[str, list[float]] = {"narrow": [], "medium": [], "wide": []}
        vol_groups: dict[str, list[float]] = {}
        mae_sum = 0.0
        mfe_sum = 0.0
        mae_count = 0

        for row in rows:
            side = str(row[3] or "")
            entry_price = float(row[4] or 0.0)
            exit_price = float(row[5] or 0.0)
            pnl = float(row[6] or 0.0)
            duration = float(row[7] or 0.0)
            mae = float(row[8] or 0.0)
            mfe = float(row[9] or 0.0)
            immediate_move = float(row[10] or 0.0)
            entry_spread = float(row[11] or 0.0)
            entry_volatility = str(row[12] or "unknown")
            entry_time_in_book_ms = float(row[13] or 0.0)
            px_10ms = float(row[14]) if row[14] is not None else None
            px_100ms = float(row[15]) if row[15] is not None else None
            px_500ms = float(row[16]) if row[16] is not None else None

            sign = 1.0 if side.upper() == "LONG" else -1.0
            move_10ms = (px_10ms - entry_price) if px_10ms is not None else None
            move_100ms = (px_100ms - entry_price) if px_100ms is not None else None
            move_500ms = (px_500ms - entry_price) if px_500ms is not None else None
            adverse_10ms = bool(px_10ms is not None and sign * move_10ms < 0)  # type: ignore[arg-type]
            adverse_100ms = bool(px_100ms is not None and sign * move_100ms < 0)  # type: ignore[arg-type]
            adverse_500ms = bool(px_500ms is not None and sign * move_500ms < 0)  # type: ignore[arg-type]

            record = {
                "entry_trade_id": row[0],
                "exit_trade_id": row[1],
                "symbol": row[2],
                "side": side,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl": pnl,
                "duration_ms": duration,
                "mae": mae,
                "mfe": mfe,
                "immediate_move": immediate_move,
                "entry_spread": entry_spread,
                "entry_volatility_regime": entry_volatility,
                "entry_time_in_book_ms": entry_time_in_book_ms,
                "move_10ms": move_10ms if move_10ms is not None else 0.0,
                "move_100ms": move_100ms if move_100ms is not None else 0.0,
                "move_500ms": move_500ms if move_500ms is not None else 0.0,
                "adverse_10ms": adverse_10ms,
                "adverse_100ms": adverse_100ms,
                "adverse_500ms": adverse_500ms,
                "classification": "good_trade" if pnl > 0 else ("bad_trade" if pnl < 0 else "flat_trade"),
            }
            trades.append(record)

            if pnl > 0:
                good.append(record)
            elif pnl < 0:
                bad.append(record)

            if px_10ms is not None:
                immediate_eval += 1
                if adverse_10ms:
                    immediate_adverse += 1
            if pnl < 0 and px_10ms is not None:
                bad_eval += 1
                if adverse_10ms:
                    bad_immediate += 1
            if pnl < 0 and (not adverse_10ms) and (adverse_100ms or adverse_500ms):
                bad_later += 1

            if entry_spread <= 0.02:
                spread_groups["narrow"].append(pnl)
            elif entry_spread <= 0.05:
                spread_groups["medium"].append(pnl)
            else:
                spread_groups["wide"].append(pnl)
            vol_groups.setdefault(entry_volatility, []).append(pnl)
            mae_sum += mae
            mfe_sum += mfe
            mae_count += 1

        def _avg(values: list[float]) -> float:
            return float(sum(values) / len(values)) if values else 0.0

        good_spreads = [float(r["entry_spread"]) for r in good]
        bad_spreads = [float(r["entry_spread"]) for r in bad]
        good_time = [float(r["entry_time_in_book_ms"]) for r in good]
        bad_time = [float(r["entry_time_in_book_ms"]) for r in bad]

        return {
            "total_round_trips": float(len(trades)),
            "good_trades": float(len(good)),
            "bad_trades": float(len(bad)),
            "immediately_adverse_pct": float((immediate_adverse / immediate_eval * 100.0) if immediate_eval else 0.0),
            "avg_mae": float(mae_sum / mae_count) if mae_count else 0.0,
            "avg_mfe": float(mfe_sum / mae_count) if mae_count else 0.0,
            "avg_pnl_by_spread_bucket": {k: _avg(v) for k, v in spread_groups.items()},
            "avg_pnl_by_volatility_regime": {k: _avg(v) for k, v in vol_groups.items()},
            "good_vs_bad_entry": {
                "good_avg_entry_spread": _avg(good_spreads),
                "bad_avg_entry_spread": _avg(bad_spreads),
                "good_avg_time_in_book_ms": _avg(good_time),
                "bad_avg_time_in_book_ms": _avg(bad_time),
            },
            "patterns": {
                "bad_trades_immediately_negative_pct": float((bad_immediate / bad_eval * 100.0) if bad_eval else 0.0),
                "bad_trades_turn_negative_later_pct": float((bad_later / len(bad) * 100.0) if bad else 0.0),
                "good_trades_avg_spread_gt_bad": _avg(good_spreads) > _avg(bad_spreads),
            },
            "trades": trades,
        }

    def entry_move_10ms(self, *, trade_id: str, side: str, entry_price: float) -> float:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            row = conn.execute(
                "SELECT adverse_px_10ms FROM trade_analytics WHERE trade_id=?",
                (trade_id,),
            ).fetchone()
        if row is None or row[0] is None:
            return 0.0
        px_10ms = float(row[0])
        ep = float(entry_price)
        if side.upper() == "LONG":
            return px_10ms - ep
        return ep - px_10ms

    def entry_decisions_by_side(self) -> list[dict[str, float | int | str]]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT side, decision, COUNT(1), AVG(entry_score)
                FROM entry_decisions
                GROUP BY side, decision
                ORDER BY side, decision
                """
            ).fetchall()
        return [
            {
                "side": str(r[0]),
                "decision": str(r[1]),
                "count": int(r[2]),
                "avg_entry_score": float(r[3] or 0.0),
            }
            for r in rows
        ]

    def analytics_missing_fills(self, *, limit: int = 500) -> list[dict[str, object]]:
        limit = max(1, int(limit))
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT
                    te.exec_id,
                    te.cl_ord_id,
                    te.symbol,
                    te.side,
                    te.price,
                    te.created_at
                FROM trade_economics te
                LEFT JOIN trade_analytics ta ON ta.trade_id = te.exec_id
                WHERE ta.trade_id IS NULL
                ORDER BY te.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "trade_id": str(r[0] or ""),
                "order_id": str(r[1] or ""),
                "symbol": str(r[2] or ""),
                "side": str(r[3] or ""),
                "price": float(r[4] or 0.0),
                "created_at": str(r[5] or ""),
            }
            for r in rows
        ]

    def entry_score_pnl_correlation(self) -> dict[str, float]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT
                    rt.total_pnl,
                    (
                        SELECT ed.entry_score
                        FROM entry_decisions ed
                        WHERE ed.symbol = rt.symbol
                          AND ed.decision = 'EXECUTE'
                          AND (
                              (rt.side = 'LONG' AND ed.side = 'BUY')
                              OR (rt.side = 'SHORT' AND ed.side = 'SELL')
                          )
                          AND ed.timestamp <= rt.entry_ts
                        ORDER BY ed.timestamp DESC
                        LIMIT 1
                    ) AS matched_entry_score
                FROM round_trip_analytics rt
                ORDER BY rt.exit_ts
                """
            ).fetchall()
        pairs: list[tuple[float, float]] = []
        for pnl_raw, score_raw in rows:
            if score_raw is None:
                continue
            pairs.append((float(score_raw), float(pnl_raw or 0.0)))
        if len(pairs) < 2:
            return {
                "matched_round_trips": float(len(pairs)),
                "total_round_trips": float(len(rows)),
                "pearson_corr_entry_score_vs_pnl": 0.0,
            }
        scores = [p[0] for p in pairs]
        pnls = [p[1] for p in pairs]
        mean_score = sum(scores) / len(scores)
        mean_pnl = sum(pnls) / len(pnls)
        cov = sum((s - mean_score) * (p - mean_pnl) for s, p in pairs)
        var_s = sum((s - mean_score) ** 2 for s in scores)
        var_p = sum((p - mean_pnl) ** 2 for p in pnls)
        denom = math.sqrt(var_s * var_p)
        corr = (cov / denom) if denom > 1e-12 else 0.0
        return {
            "matched_round_trips": float(len(pairs)),
            "total_round_trips": float(len(rows)),
            "pearson_corr_entry_score_vs_pnl": float(corr),
            "avg_entry_score_matched": float(mean_score),
            "avg_pnl_matched": float(mean_pnl),
        }

    def analytics_presence_for_trade(self, trade_id: str) -> dict[str, int]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            ta = conn.execute(
                "SELECT COUNT(1) FROM trade_analytics WHERE trade_id=?",
                (trade_id,),
            ).fetchone()
            rt = conn.execute(
                "SELECT COUNT(1) FROM round_trip_analytics WHERE entry_trade_id=? OR exit_trade_id=?",
                (trade_id, trade_id),
            ).fetchone()
        return {
            "trade_analytics_rows": int(ta[0]) if ta and ta[0] is not None else 0,
            "round_trip_rows": int(rt[0]) if rt and rt[0] is not None else 0,
        }

    def backfill_trade_analytics(self, *, limit: int = 100) -> dict[str, int]:
        limit = max(1, int(limit))
        scanned = 0
        existing = 0
        inserted = 0
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT
                    exec_id,
                    cl_ord_id,
                    symbol,
                    side,
                    gross_pnl,
                    net_pnl,
                    fees,
                    price,
                    created_at,
                    CAST(strftime('%H', created_at) AS INTEGER)
                FROM trade_economics
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            for r in rows:
                scanned += 1
                trade_id = str(r[0] or "")
                if not trade_id:
                    continue
                exists_row = conn.execute(
                    "SELECT COUNT(1) FROM trade_analytics WHERE trade_id=?",
                    (trade_id,),
                ).fetchone()
                if exists_row and int(exists_row[0]) > 0:
                    existing += 1
                    continue
                conn.execute(
                    """
                    INSERT OR REPLACE INTO trade_analytics(
                        trade_id, order_id, symbol, side, gross_pnl, net_pnl, fees, slippage, spread_pnl, adverse_pnl,
                        holding_pnl, expected_price, actual_price, adverse_fill, hour_bucket, volatility_regime, spread_bucket,
                        time_in_book_ms, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?, ?, 0, ?, 'unknown', 'unknown', 0, ?)
                    """,
                    (
                        trade_id,
                        str(r[1] or ""),
                        str(r[2] or ""),
                        str(r[3] or ""),
                        float(r[4] or 0.0),
                        float(r[5] or 0.0),
                        float(r[6] or 0.0),
                        float(r[7] or 0.0),
                        float(r[7] or 0.0),
                        int(r[9] or 0),
                        str(r[8] or ""),
                    ),
                )
                inserted += 1
            conn.commit()
        return {"scanned": scanned, "existing": existing, "inserted": inserted}

    def missed_pnl_by_cancel_reason(self) -> list[dict[str, float | int | str]]:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT
                    cancel_reason,
                    COUNT(1),
                    COALESCE(SUM(missed_pnl),0),
                    COALESCE(AVG(missed_pnl),0),
                    COALESCE(SUM(CASE WHEN missed_pnl > 0 THEN 1 ELSE 0 END),0)
                FROM cancel_analytics
                GROUP BY cancel_reason
                ORDER BY SUM(missed_pnl) DESC
                """
            ).fetchall()
        return [
            {
                "cancel_reason": r[0],
                "count": int(r[1]),
                "missed_pnl": float(r[2]),
                "avg_missed_pnl": float(r[3]),
                "harmful_cancels": int(r[4]),
            }
            for r in rows
        ]
