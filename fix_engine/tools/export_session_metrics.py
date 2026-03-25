"""
Сбор ключевых метрик из trade_economics.db за сессию (по маркеру session_start_marker.txt).
Запуск после прогона: python fix_engine/tools/export_session_metrics.py
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path


def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _read_virtual_start_balance(fix_engine_dir: Path) -> float:
    cfg = fix_engine_dir / "settings.cfg"
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


def collect_session_metrics(fix_engine_dir: Path | None = None) -> dict[str, object]:
    fix_engine_dir = fix_engine_dir or Path(__file__).resolve().parents[1]
    db_path = fix_engine_dir / "trade_economics.db"
    marker = fix_engine_dir / "log" / "session_start_marker.txt"

    session_start: str | None = None
    if marker.exists():
        raw = marker.read_text(encoding="utf-8").strip().splitlines()
        if raw:
            session_start = raw[0].strip()

    if not db_path.exists():
        return {"error": "db_missing", "path": str(db_path)}

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    out: dict[str, object] = {
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "session_start_utc": session_start,
        "db_path": str(db_path),
    }

    def since_clause(col: str) -> tuple[str, tuple]:
        if session_start:
            return f" WHERE {col} >= ?", (session_start,)
        return "", ()

    if _table_exists(cur, "analytics_state"):
        row = cur.execute(
            "SELECT cumulative_pnl, equity_peak, max_drawdown, win_count, total_trades, avg_trade_pnl, updated_at FROM analytics_state WHERE id = 1"
        ).fetchone()
        if row:
            out["analytics_state"] = {
                "cumulative_pnl": row[0],
                "equity_peak": row[1],
                "max_drawdown": row[2],
                "win_count": row[3],
                "total_trades": row[4],
                "avg_trade_pnl": row[5],
                "updated_at": row[6],
            }

    if _table_exists(cur, "trade_economics"):
        wc, wc_sql = since_clause("created_at")
        q = f"SELECT COUNT(*), COALESCE(SUM(net_pnl),0), COALESCE(AVG(net_pnl),0), COALESCE(SUM(gross_pnl),0), COALESCE(SUM(fees),0) FROM trade_economics{wc}"
        r = cur.execute(q, wc_sql).fetchone()
        if r:
            out["trade_economics"] = {
                "count": int(r[0] or 0),
                "sum_net_pnl": float(r[1] or 0),
                "avg_net_pnl": float(r[2] or 0),
                "sum_gross_pnl": float(r[3] or 0),
                "sum_fees": float(r[4] or 0),
            }

    if _table_exists(cur, "trade_analytics"):
        wc, wc_sql = since_clause("created_at")
        q = f"""
            SELECT COUNT(*),
                   COALESCE(SUM(net_pnl),0), COALESCE(AVG(net_pnl),0),
                   COALESCE(AVG(slippage),0), COALESCE(AVG(spread_pnl),0),
                   COALESCE(AVG(adverse_pnl),0),
                   COALESCE(SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END),0)
            FROM trade_analytics{wc}
        """
        r = cur.execute(q, wc_sql).fetchone()
        if r:
            out["trade_analytics"] = {
                "count": int(r[0] or 0),
                "sum_net_pnl": float(r[1] or 0),
                "avg_net_pnl": float(r[2] or 0),
                "avg_slippage": float(r[3] or 0),
                "avg_spread_pnl": float(r[4] or 0),
                "avg_adverse_pnl": float(r[5] or 0),
                "wins": int(r[6] or 0),
                "win_rate": (float(r[6]) / float(r[0])) if r[0] else 0.0,
            }

    if _table_exists(cur, "round_trip_analytics"):
        wc, wc_sql = since_clause("exit_ts")
        q = f"""
            SELECT COUNT(*),
                   COALESCE(SUM(total_pnl),0), COALESCE(AVG(total_pnl),0),
                   COALESCE(AVG(mae),0), COALESCE(AVG(mfe),0),
                   COALESCE(AVG(duration_ms),0),
                   COALESCE(SUM(CASE WHEN immediate_move < 0 THEN 1 ELSE 0 END),0),
                   COALESCE(SUM(CASE WHEN total_pnl > 0 THEN 1 ELSE 0 END),0)
            FROM round_trip_analytics{wc}
        """
        r = cur.execute(q, wc_sql).fetchone()
        if r:
            cnt = int(r[0] or 0)
            wins_rt = int(r[7] or 0)
            out["round_trips"] = {
                "count": cnt,
                "sum_pnl": float(r[1] or 0),
                "avg_pnl": float(r[2] or 0),
                "avg_mae": float(r[3] or 0),
                "avg_mfe": float(r[4] or 0),
                "avg_duration_ms": float(r[5] or 0),
                "adverse_immediate_moves": int(r[6] or 0),
                "adverse_rate": (float(r[6]) / float(cnt)) if cnt else 0.0,
                "wins": wins_rt,
                "win_rate": (float(wins_rt) / float(cnt)) if cnt else 0.0,
            }

    if _table_exists(cur, "cancel_analytics"):
        wc, wc_sql = since_clause("created_at")
        q = f"SELECT COUNT(*), COALESCE(SUM(missed_pnl),0) FROM cancel_analytics{wc}"
        r = cur.execute(q, wc_sql).fetchone()
        if r:
            out["cancel_analytics"] = {"count": int(r[0] or 0), "sum_missed_pnl": float(r[1] or 0)}
        q2 = f"SELECT cancel_reason, COUNT(*) FROM cancel_analytics{wc} GROUP BY cancel_reason"
        r2 = cur.execute(q2, wc_sql).fetchall()
        if r2:
            out["cancel_analytics_by_reason"] = {str(a): int(b) for a, b in r2}

    if _table_exists(cur, "entry_decisions"):
        wc, wc_sql = since_clause("timestamp")
        q = f"SELECT COUNT(*), COALESCE(AVG(entry_score),0) FROM entry_decisions{wc}"
        r = cur.execute(q, wc_sql).fetchone()
        if r:
            out["entry_decisions"] = {"count": int(r[0] or 0), "avg_entry_score": float(r[1] or 0)}

    start_bal = _read_virtual_start_balance(fix_engine_dir)
    te = out.get("trade_economics") if isinstance(out.get("trade_economics"), dict) else {}
    realized = float(te.get("sum_net_pnl", 0) or 0.0)
    out["virtual_account"] = {
        "start_balance": start_bal,
        "realized_pnl_session": realized,
        "equity_session": start_bal + realized,
    }

    con.close()
    return out


def print_post_run_summary(fix_engine_dir: Path | None = None) -> None:
    out = collect_session_metrics(fix_engine_dir)
    ta = out.get("trade_analytics") if isinstance(out.get("trade_analytics"), dict) else {}
    rt = out.get("round_trips") if isinstance(out.get("round_trips"), dict) else {}
    va = out.get("virtual_account") if isinstance(out.get("virtual_account"), dict) else {}
    summary = {
        "total_trades": int(ta.get("count", 0) or 0),
        "round_trips": int(rt.get("count", 0) or 0),
        "avg_pnl": float(ta.get("avg_net_pnl", 0) or 0.0),
        "win_rate_fills": float(ta.get("win_rate", 0) or 0.0),
        "adverse_rate": float(rt.get("adverse_rate", 0) or 0.0),
        "avg_mfe": float(rt.get("avg_mfe", 0) or 0.0),
        "avg_mae": float(rt.get("avg_mae", 0) or 0.0),
        "round_trip_wins": int(rt.get("wins", 0) or 0),
        "win_rate_round_trips": float(rt.get("win_rate", 0) or 0.0),
        "virtual_start_balance": float(va.get("start_balance", 0) or 0.0),
        "virtual_equity_session": float(va.get("equity_session", 0) or 0.0),
    }
    print(json.dumps({"POST_RUN_SUMMARY": summary}, ensure_ascii=False, indent=2))


def main() -> None:
    fix_engine_dir = Path(__file__).resolve().parents[1]
    out_dir = fix_engine_dir / "log"
    out = collect_session_metrics(fix_engine_dir)
    if "error" in out:
        print(json.dumps(out, ensure_ascii=False, indent=2))
        sys.exit(1)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"session_metrics_{stamp}.json"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out_path.read_text(encoding="utf-8"))
    print_post_run_summary(fix_engine_dir)


if __name__ == "__main__":
    main()
