import json
import math
import sqlite3
from datetime import datetime
from pathlib import Path

from fix_engine.tools.common_stats import avg, safe_div as _safe_div


ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "trade_economics.db"
MARKER = (ROOT / "log" / "session_start_marker.txt").read_text(encoding="utf-8").strip()
SYMBOL = "SRM6"
TERMINAL_LOG = Path(
    "C:/Users/Admin/.cursor/projects/c-Users-Admin-Python-Projects-TradingBotMOEX/terminals/591426.txt"
)
def win_rate(rows):
    if not rows:
        return None
    return sum(1 for r in rows if (r.get("pnl") or 0) > 0) / len(rows)


def parse_iso(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def load_fast_stop_timestamps():
    out = []
    if not TERMINAL_LOG.exists():
        return out
    for line in TERMINAL_LOG.read_text(encoding="utf-8", errors="ignore").splitlines():
        if "IMMEDIATE_FAST_STOP" not in line:
            continue
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
            ts = obj.get("timestamp")
            dt = parse_iso(ts)
            if dt is not None:
                out.append(dt)
        except Exception:
            continue
    return out


def first_tick_outcome(immediate, eps=0.01):
    if immediate is None:
        return "neutral"
    if immediate > eps:
        return "good"
    if immediate < -eps:
        return "bad"
    return "neutral"


def spread_bucket(spread):
    if spread is None:
        return "unknown"
    if spread <= 1:
        return "le_1"
    if spread <= 2:
        return "gt_1_le_2"
    return "gt_2"


def safe_div(a, b):
    return _safe_div(a, b)


def main():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT
          r.round_trip_id,
          r.entry_trade_id,
          r.exit_trade_id,
          r.side,
          r.entry_price,
          r.exit_price,
          r.total_pnl,
          r.entry_ts,
          r.exit_ts,
          r.duration_ms,
          r.entry_spread,
          r.mfe,
          r.mae,
          r.immediate_move,
          ro.latency_ms,
          ro.adverse_rate,
          ro.momentum_500ms
        FROM round_trip_analytics r
        LEFT JOIN round_trip_observability ro ON ro.round_trip_id = r.round_trip_id
        WHERE r.symbol = ?
          AND r.exit_ts IS NOT NULL
          AND r.entry_ts >= ?
        ORDER BY r.entry_ts
        """,
        (SYMBOL, MARKER),
    ).fetchall()
    data = [dict(r) for r in rows]

    fast_stops = load_fast_stop_timestamps()
    trades = []
    for r in data:
        immediate = r.get("immediate_move")
        entry_dt = parse_iso(r.get("entry_ts"))
        exit_dt = parse_iso(r.get("exit_ts"))
        was_fast = False
        if exit_dt is not None and fast_stops:
            for fts in fast_stops:
                if abs((exit_dt - fts).total_seconds()) <= 3.0:
                    was_fast = True
                    break
        m500 = r.get("momentum_500ms")
        if m500 is None or abs(m500) < 1e-12:
            micro = "neutral"
        elif (r.get("side") == "LONG" and m500 > 0) or (r.get("side") == "SHORT" and m500 < 0):
            micro = "aligned"
        else:
            micro = "opposite"
        trade = {
            "trade_id": r.get("round_trip_id"),
            "direction": "BUY" if r.get("side") == "LONG" else "SELL",
            "entry_price": r.get("entry_price"),
            "exit_price": r.get("exit_price"),
            "qty": 1.0,
            "pnl": r.get("total_pnl"),
            "net_pnl": r.get("total_pnl"),
            "entry_time": r.get("entry_ts"),
            "exit_time": r.get("exit_ts"),
            "duration_ms": r.get("duration_ms"),
            "spread_at_entry": r.get("entry_spread"),
            "latency_ms": r.get("latency_ms"),
            "mfe": r.get("mfe"),
            "mae": r.get("mae"),
            "immediate_move_1_tick": immediate,
            "immediate_move_3_ticks": None,
            "immediate_move_5_ticks": None,
            "microtrend": micro,
            "entry_reason": None,
            "expected_edge": None,
            "was_fast_stopped": was_fast,
            "first_tick_outcome": first_tick_outcome(immediate),
        }
        trades.append(trade)

    # 4.1
    by_outcome = {"good": [], "bad": [], "neutral": []}
    for t in trades:
        by_outcome[t["first_tick_outcome"]].append(t)
    immediate_breakdown = {
        k: {
            "count": len(v),
            "avg_pnl": avg([x["pnl"] for x in v]),
            "win_rate": win_rate(v),
        }
        for k, v in by_outcome.items()
    }

    # 4.2
    spread_groups = {"le_1": [], "gt_1_le_2": [], "gt_2": []}
    for t in trades:
        b = spread_bucket(t["spread_at_entry"])
        if b in spread_groups:
            spread_groups[b].append(t)
    spread_buckets = {
        k: {"avg_pnl": avg([x["pnl"] for x in v]), "win_rate": win_rate(v)}
        for k, v in spread_groups.items()
    }

    # 4.3
    fs_true = [t for t in trades if t["was_fast_stopped"]]
    fs_false = [t for t in trades if not t["was_fast_stopped"]]
    fast_stop_eff = {
        "true": {
            "avg_pnl": avg([x["pnl"] for x in fs_true]),
            "avg_loss": avg([x["pnl"] for x in fs_true if (x["pnl"] or 0) < 0]),
            "avg_duration": avg([x["duration_ms"] for x in fs_true]),
        },
        "false": {
            "avg_pnl": avg([x["pnl"] for x in fs_false]),
            "avg_loss": avg([x["pnl"] for x in fs_false if (x["pnl"] or 0) < 0]),
            "avg_duration": avg([x["duration_ms"] for x in fs_false]),
        },
    }

    # 4.4
    good_im = [t for t in trades if (t["immediate_move_1_tick"] or 0) > 0]
    hold_profit = {
        "avg_mfe": avg([x["mfe"] for x in good_im]),
        "avg_realized_pnl": avg([x["pnl"] for x in good_im]),
        "capture_ratio": safe_div(avg([x["pnl"] for x in good_im]), avg([x["mfe"] for x in good_im])),
    }

    # 4.5
    bad_im = [t for t in trades if (t["immediate_move_1_tick"] or 0) < 0]
    bad_loss = {
        "avg_loss": avg([x["pnl"] for x in bad_im if (x["pnl"] or 0) < 0]),
        "avg_mae": avg([x["mae"] for x in bad_im]),
        "duration": avg([x["duration_ms"] for x in bad_im]),
    }

    # 5
    top = sorted(trades, key=lambda x: x["pnl"] if x["pnl"] is not None else -1e18, reverse=True)[:5]
    worst = sorted(trades, key=lambda x: x["pnl"] if x["pnl"] is not None else 1e18)[:5]
    top_best = [
        {
            "trade_id": t["trade_id"],
            "pnl": t["pnl"],
            "immediate_move": t["immediate_move_1_tick"],
            "spread": t["spread_at_entry"],
            "duration": t["duration_ms"],
        }
        for t in top
    ]
    top_worst = [
        {
            "trade_id": t["trade_id"],
            "pnl": t["pnl"],
            "immediate_move": t["immediate_move_1_tick"],
            "spread": t["spread_at_entry"],
            "duration": t["duration_ms"],
        }
        for t in worst
    ]

    # 6
    total = len(trades)
    session_metrics = {
        "total_trades": total,
        "win_rate": win_rate(trades),
        "avg_pnl": avg([t["pnl"] for t in trades]),
        "adverse_rate": (len([t for t in trades if (t["mae"] is not None and t["mae"] < 0)]) / total) if total else None,
        "good_entry_rate": (len(by_outcome["good"]) / total) if total else None,
        "bad_entry_rate": (len(by_outcome["bad"]) / total) if total else None,
        "fast_stop_usage_rate": (len(fs_true) / total) if total else None,
        "avg_spread": avg([t["spread_at_entry"] for t in trades]),
        "avg_latency": avg([t["latency_ms"] for t in trades]),
    }

    out = {
        "filters": {
            "session_start_marker": MARKER,
            "symbol": SYMBOL,
            "completed_round_trips_only": True,
        },
        "trades": trades,
        "aggregations": {
            "immediate_move_breakdown": immediate_breakdown,
            "spread_buckets": spread_buckets,
            "fast_stop_effectiveness": fast_stop_eff,
            "holding_profitable_trades": hold_profit,
            "losses_on_bad_entries": bad_loss,
        },
        "top_analytics": {
            "top_5_best": top_best,
            "top_5_worst": top_worst,
        },
        "session_metrics": session_metrics,
        "data_quality_notes": {
            "immediate_move_3_ticks": "not available in current DB schema",
            "immediate_move_5_ticks": "not available in current DB schema",
            "entry_reason": "not available in round_trip tables",
            "expected_edge": "not available in round_trip tables",
        },
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
