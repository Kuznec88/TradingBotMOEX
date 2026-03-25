from __future__ import annotations

import argparse
import json
import re
import sqlite3
import statistics
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser(description="Extended interim metrics snapshot for active run.")
    p.add_argument("--econ-db", default="fix_engine/e2e_economics.db")
    p.add_argument("--terminal-log", required=True)
    p.add_argument("--session-start-utc", required=True)
    args = p.parse_args()

    conn = sqlite3.connect(args.econ_db)
    cur = conn.cursor()
    start = str(args.session_start_utc)
    out: dict[str, object] = {"session_start_utc": start}

    cur.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(net_pnl), 0),
            COALESCE(AVG(net_pnl), 0),
            COALESCE(SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END), 0)
        FROM trade_analytics
        WHERE created_at >= ?
        """,
        (start,),
    )
    t_count, t_sum, t_avg, t_win = cur.fetchone()
    out["trades"] = {
        "count": int(t_count or 0),
        "sum_net_pnl": float(t_sum or 0.0),
        "avg_net_pnl": float(t_avg or 0.0),
        "win_rate": (float(t_win) / float(t_count)) if t_count else 0.0,
    }

    cur.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(total_pnl), 0),
            COALESCE(AVG(total_pnl), 0),
            COALESCE(AVG(mae), 0),
            COALESCE(AVG(mfe), 0),
            COALESCE(SUM(CASE WHEN immediate_move < 0 THEN 1 ELSE 0 END), 0)
        FROM round_trip_analytics
        WHERE exit_ts >= ?
        """,
        (start,),
    )
    r_count, r_sum, r_avg, r_mae, r_mfe, r_adv = cur.fetchone()
    out["round_trips"] = {
        "count": int(r_count or 0),
        "sum_pnl": float(r_sum or 0.0),
        "avg_pnl": float(r_avg or 0.0),
        "avg_mae": float(r_mae or 0.0),
        "avg_mfe": float(r_mfe or 0.0),
        "adverse_rate": (float(r_adv) / float(r_count)) if r_count else 0.0,
    }

    cur.execute(
        """
        SELECT total_pnl, mfe, immediate_move
        FROM round_trip_analytics
        WHERE exit_ts >= ?
        ORDER BY exit_ts DESC
        LIMIT 20
        """,
        (start,),
    )
    last20 = cur.fetchall()
    if last20:
        last20_pnl = [float(x[0] or 0.0) for x in last20]
        last20_mfe = [float(x[1] or 0.0) for x in last20]
        last20_adv = [1.0 if float(x[2] or 0.0) < 0.0 else 0.0 for x in last20]
        out["last20_round_trips"] = {
            "avg_pnl": sum(last20_pnl) / len(last20_pnl),
            "avg_mfe": sum(last20_mfe) / len(last20_mfe),
            "adverse_rate": sum(last20_adv) / len(last20_adv),
        }
    else:
        out["last20_round_trips"] = {"avg_pnl": 0.0, "avg_mfe": 0.0, "adverse_rate": 0.0}

    cur.execute(
        """
        SELECT mfe, total_pnl
        FROM round_trip_analytics
        WHERE exit_ts >= ?
        """,
        (start,),
    )
    rows = cur.fetchall()
    if rows:
        mfes = [float(x[0] or 0.0) for x in rows]
        pnls = [float(x[1] or 0.0) for x in rows]
        n = len(rows)
        mx = sum(mfes) / n
        my = sum(pnls) / n
        num = sum((x - mx) * (y - my) for x, y in zip(mfes, pnls))
        den_x = sum((x - mx) ** 2 for x in mfes)
        den_y = sum((y - my) ** 2 for y in pnls)
        den = (den_x * den_y) ** 0.5
        corr = (num / den) if den > 1e-12 else 0.0
        cut = statistics.median(mfes)
        high = [p for m, p in zip(mfes, pnls) if m >= cut]
        low = [p for m, p in zip(mfes, pnls) if m < cut]
        out["directional"] = {
            "corr_mfe_pnl": corr,
            "avg_pnl_high_mfe": (sum(high) / len(high)) if high else 0.0,
            "avg_pnl_low_mfe": (sum(low) / len(low)) if low else 0.0,
            "mfe_median_cut": cut,
        }
    else:
        out["directional"] = {
            "corr_mfe_pnl": 0.0,
            "avg_pnl_high_mfe": 0.0,
            "avg_pnl_low_mfe": 0.0,
            "mfe_median_cut": 0.0,
        }

    cur.execute(
        """
        SELECT COUNT(*)
        FROM round_trip_analytics
        WHERE exit_ts >= ? AND mae = 0 AND mfe > 1.0
        """,
        (start,),
    )
    out["perfect_trade_pattern_count"] = int(cur.fetchone()[0] or 0)

    cur.execute(
        """
        SELECT COUNT(*), COALESCE(AVG(microprice_edge),0), COALESCE(AVG(momentum_100ms),0),
               COALESCE(AVG(momentum_500ms),0), COALESCE(AVG(spread),0), COALESCE(AVG(imbalance),0)
        FROM executed_entry_observability
        WHERE created_at >= datetime('now', '-90 minutes')
        """
    )
    eo_count, eo_micro, eo_m100, eo_m500, eo_spread, eo_imb = cur.fetchone()
    out["entry_observability_recent90m"] = {
        "rows": int(eo_count or 0),
        "mean_microprice_edge": float(eo_micro or 0.0),
        "mean_momentum_100ms": float(eo_m100 or 0.0),
        "mean_momentum_500ms": float(eo_m500 or 0.0),
        "mean_spread": float(eo_spread or 0.0),
        "mean_imbalance": float(eo_imb or 0.0),
    }
    conn.close()

    log_text = Path(args.terminal_log).read_text(encoding="utf-8", errors="ignore")
    latencies = [float(x) for x in re.findall(r"send_to_fill_latency_ms=([0-9]+\.[0-9]+)", log_text)]
    out["exec_timing"] = {
        "samples": len(latencies),
        "latency_ms_avg": (sum(latencies) / len(latencies)) if latencies else 0.0,
        "latency_ms_p50": statistics.median(latencies) if latencies else 0.0,
        "latency_ms_max": max(latencies) if latencies else 0.0,
    }
    out["log_counters"] = {
        "lookahead_violations": len(re.findall(r"\[SIM\]\[LOOKAHEAD_VIOLATION\]", log_text)),
        "edge_degradation_detected": len(re.findall(r"\[E2E\]\[EDGE_DEGRADATION_DETECTED\]", log_text)),
        "low_frequency": len(re.findall(r"\[E2E\]\[LOW_FREQUENCY\]", log_text)),
        "frequency_logs": len(re.findall(r"\[E2E\]\[FREQUENCY\]", log_text)),
    }
    print(json.dumps(out, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()

