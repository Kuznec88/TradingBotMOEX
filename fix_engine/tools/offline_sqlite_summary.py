from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python offline_sqlite_summary.py <economics_db_path>")
    db_path = Path(sys.argv[1])
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cols = {r[1] for r in cur.execute("PRAGMA table_info(round_trip_analytics)").fetchall()}
    pnl_col = "total_pnl" if "total_pnl" in cols else "pnl"
    immediate_col = "immediate_move" if "immediate_move" in cols else None

    adverse_expr = "0.0"
    if immediate_col is not None:
        adverse_expr = f"CASE WHEN {immediate_col} < 0 THEN 1.0 ELSE 0.0 END"

    query = f"""
        SELECT
            COUNT(1) AS round_trips,
            AVG({pnl_col}) AS avg_pnl,
            AVG(CASE WHEN {pnl_col} > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
            AVG({adverse_expr}) AS adverse_rate,
            AVG(mfe) AS avg_mfe,
            AVG(mae) AS avg_mae
        FROM round_trip_analytics
        WHERE {pnl_col} IS NOT NULL
    """
    row = cur.execute(query).fetchone()
    result = {
        "db": str(db_path),
        "round_trips": row[0] or 0,
        "avg_pnl": row[1] or 0.0,
        "win_rate": row[2] or 0.0,
        "adverse_rate": row[3] or 0.0,
        "avg_mfe": row[4] or 0.0,
        "avg_mae": row[5] or 0.0,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
