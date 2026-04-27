"""
Live session summary from economics DB (trade_economics.db).

Usage (repo root):
  python -m fix_engine.tools.session_summary
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


def _iso_to_sqlite_dt(value: str) -> str:
    # session_start_marker stores ISO like "2026-04-06T11:13:13.764654+00:00"
    # trade_economics.created_at is sqlite datetime('now') format: "YYYY-MM-DD HH:MM:SS"
    v = (value or "").strip()
    if not v:
        return ""
    return v.replace("T", " ").split("+", 1)[0].split("Z", 1)[0].split(".", 1)[0]


def main() -> None:
    base = Path(__file__).resolve().parents[1]
    db = base / "trade_economics.db"
    marker = base / "log" / "session_start_marker.txt"
    start_iso = marker.read_text(encoding="utf-8", errors="ignore").strip() if marker.exists() else ""
    start_sql = _iso_to_sqlite_dt(start_iso)

    print(f"db={db}")
    print(f"session_start_utc={start_iso or 'N/A'}")
    if not db.exists():
        print("db_missing=1")
        return

    with sqlite3.connect(str(db)) as conn:
        cur = conn.cursor()

        if start_sql:
            cur.execute(
                """
                SELECT
                  COUNT(*),
                  COALESCE(SUM(net_pnl), 0),
                  COALESCE(SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END), 0)
                FROM trade_economics
                WHERE created_at >= ?
                """,
                (start_sql,),
            )
        else:
            cur.execute(
                """
                SELECT
                  COUNT(*),
                  COALESCE(SUM(net_pnl), 0),
                  COALESCE(SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END), 0)
                FROM trade_economics
                """
            )
        fills_count, fills_sum, fills_wins = cur.fetchone()
        fills_count = int(fills_count or 0)
        fills_sum = float(fills_sum or 0.0)
        fills_wr = (float(fills_wins or 0) / fills_count) if fills_count else 0.0

        if start_iso:
            cur.execute(
                """
                SELECT
                  COUNT(*),
                  COALESCE(SUM(total_pnl), 0),
                  COALESCE(AVG(total_pnl), 0),
                  COALESCE(SUM(CASE WHEN total_pnl > 0 THEN 1 ELSE 0 END), 0)
                FROM round_trip_analytics
                WHERE exit_ts >= ?
                """,
                (start_iso,),
            )
        else:
            cur.execute(
                """
                SELECT
                  COUNT(*),
                  COALESCE(SUM(total_pnl), 0),
                  COALESCE(AVG(total_pnl), 0),
                  COALESCE(SUM(CASE WHEN total_pnl > 0 THEN 1 ELSE 0 END), 0)
                FROM round_trip_analytics
                """
            )
        rt_count, rt_sum, rt_avg, rt_wins = cur.fetchone()
        rt_count = int(rt_count or 0)
        rt_sum = float(rt_sum or 0.0)
        rt_avg = float(rt_avg or 0.0)
        rt_wr = (float(rt_wins or 0) / rt_count) if rt_count else 0.0

        cur.execute(
            """
            SELECT cumulative_pnl, equity_peak, max_drawdown, win_count, total_trades, avg_trade_pnl, updated_at
            FROM analytics_state WHERE id=1
            """
        )
        st = cur.fetchone()

        print(f"fills_count={fills_count} fills_sum_net_pnl={fills_sum:.4f} fills_win_rate={fills_wr:.3f}")
        print(f"round_trips_count={rt_count} round_trips_sum_pnl={rt_sum:.4f} round_trips_avg_pnl={rt_avg:.4f} round_trips_win_rate={rt_wr:.3f}")
        if st:
            cp, ep, mdd, wc, tt, atp, upd = st
            print(f"analytics_cumulative_pnl={float(cp or 0.0):.4f} equity_peak={float(ep or 0.0):.4f} max_drawdown={float(mdd or 0.0):.4f}")
            print(f"analytics_total_trades={int(tt or 0)} analytics_win_count={int(wc or 0)} analytics_avg_trade_pnl={float(atp or 0.0):.4f} analytics_updated_at={upd}")

        cur.execute(
            "SELECT exit_ts, side, total_pnl, mae, mfe, immediate_move, duration_ms FROM round_trip_analytics ORDER BY exit_ts DESC LIMIT 10"
        )
        rows = cur.fetchall()
        print(f"last10_round_trips={len(rows)}")
        for r in rows:
            print(r)


if __name__ == "__main__":
    main()

