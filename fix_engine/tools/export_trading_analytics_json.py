"""
Сбор статистики по сделкам (round trips) и агрегатов из trade_economics.db.
Только SQLite + stdlib. Стратегия не меняется.

По умолчанию — только последняя сессия: round trips с entry_ts >= первой строки
log/session_start_marker.txt (ту же метку пишет main.py при старте TINKOFF_SANDBOX).

Вывод: JSON с trades[], metrics{}, by_direction{}, entry_zones{}.

Запуск:
  python tools/export_trading_analytics_json.py
  python tools/export_trading_analytics_json.py --out log/trading_analytics_export.json
  python tools/export_trading_analytics_json.py --all
  python tools/export_trading_analytics_json.py --since 2026-03-22T17:21:55+00:00
  python tools/export_trading_analytics_json.py --adverse-pct 0.0005
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import sys
from pathlib import Path
from typing import Any


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 2 or n != len(ys):
        return None
    mx = statistics.mean(xs)
    my = statistics.mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = sum((x - mx) ** 2 for x in xs) ** 0.5
    dy = sum((y - my) ** 2 for y in ys) ** 0.5
    if dx < 1e-18 or dy < 1e-18:
        return None
    return num / (dx * dy)


def _entry_direction(side_raw: str) -> str:
    s = str(side_raw).strip().upper()
    if s in ("1", "BUY", "LONG"):
        return "BUY"
    if s in ("2", "SELL", "SHORT"):
        return "SELL"
    return s or "UNKNOWN"


def _max_drawdown_rt(pnls: list[float]) -> float:
    if not pnls:
        return 0.0
    peak = 0.0
    cum = 0.0
    mdd = 0.0
    for p in pnls:
        cum += p
        peak = max(peak, cum)
        mdd = max(mdd, peak - cum)
    return float(mdd)


def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    r = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return r is not None


def _read_marker_session_start(marker_path: Path) -> str | None:
    if not marker_path.exists():
        return None
    raw = marker_path.read_text(encoding="utf-8").strip().splitlines()
    if not raw:
        return None
    s = raw[0].strip()
    return s if s else None


def main() -> None:
    ap = argparse.ArgumentParser(description="Export trading analytics JSON from trade_economics.db")
    ap.add_argument(
        "db_path",
        nargs="?",
        default=None,
        help="Path to trade_economics.db (default: ../trade_economics.db next to tools/)",
    )
    ap.add_argument("--out", "-o", type=Path, default=None, help="Write UTF-8 JSON to file")
    ap.add_argument(
        "--all",
        action="store_true",
        help="Export all round trips in DB (ignore session marker)",
    )
    ap.add_argument(
        "--since",
        default=None,
        metavar="ISO",
        help="Only entry_ts >= this (overrides marker), e.g. 2026-03-22T17:21:55+00:00",
    )
    ap.add_argument(
        "--marker",
        type=Path,
        default=None,
        help="Path to session_start_marker.txt (default: fix_engine/log/session_start_marker.txt)",
    )
    ap.add_argument(
        "--adverse-pct",
        type=float,
        default=0.0,
        help="Adverse flag = 1 if immediate_move<0 AND |immediate_move|/entry_price >= this (0 = only sign)",
    )
    args = ap.parse_args()

    base = Path(__file__).resolve().parents[1]
    db_path = Path(args.db_path) if args.db_path else base / "trade_economics.db"
    if not db_path.exists():
        print(json.dumps({"error": "db_not_found", "path": str(db_path)}))
        sys.exit(1)

    adverse_pct_threshold = max(0.0, float(args.adverse_pct))

    marker_path = args.marker if args.marker is not None else base / "log" / "session_start_marker.txt"
    session_start: str | None = None
    filter_meta: dict[str, Any]
    if args.all:
        filter_meta = {"mode": "all", "session_start_utc": None, "marker_path": str(marker_path)}
    elif args.since:
        session_start = str(args.since).strip()
        filter_meta = {
            "mode": "since_arg",
            "session_start_utc": session_start,
            "marker_path": str(marker_path),
        }
    else:
        session_start = _read_marker_session_start(marker_path)
        if session_start:
            filter_meta = {
                "mode": "session_marker",
                "session_start_utc": session_start,
                "marker_path": str(marker_path),
            }
        else:
            filter_meta = {
                "mode": "all",
                "session_start_utc": None,
                "marker_path": str(marker_path),
                "note": "marker missing or empty; exported full DB (use --since to bound)",
            }

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    te_exists = _table_exists(cur, "trade_economics")
    tro_exists = _table_exists(cur, "trade_observability")

    qty_by_exec: dict[str, float] = {}
    if te_exists:
        for row in cur.execute(
            "SELECT exec_id, qty FROM trade_economics"
        ).fetchall():
            eid = str(row["exec_id"])
            if eid not in qty_by_exec:
                qty_by_exec[eid] = float(row["qty"])

    ta_by_id: dict[str, sqlite3.Row] = {}
    if _table_exists(cur, "trade_analytics"):
        for row in cur.execute("SELECT * FROM trade_analytics").fetchall():
            ta_by_id[str(row["trade_id"])] = row

    lat_by_trade: dict[str, float] = {}
    if tro_exists:
        for row in cur.execute("SELECT trade_id, latency_ms FROM trade_observability").fetchall():
            lat_by_trade[str(row["trade_id"])] = float(row["latency_ms"] or 0.0)

    rt_sql = """
        SELECT
            round_trip_id,
            entry_trade_id,
            exit_trade_id,
            symbol,
            side,
            duration_ms,
            total_pnl,
            mae,
            mfe,
            immediate_move,
            entry_price,
            exit_price,
            entry_spread,
            entry_time_in_book_ms,
            entry_ts,
            exit_ts
        FROM round_trip_analytics
    """
    if session_start:
        rt_rows = cur.execute(rt_sql + " WHERE entry_ts >= ? ORDER BY entry_ts", (session_start,)).fetchall()
    else:
        rt_rows = cur.execute(rt_sql + " ORDER BY entry_ts").fetchall()

    trades: list[dict[str, Any]] = []
    pnls: list[float] = []

    for r in rt_rows:
        eid = str(r["entry_trade_id"])
        xid = str(r["exit_trade_id"])
        qty = qty_by_exec.get(eid) or qty_by_exec.get(xid) or 1.0
        ta_e = ta_by_id.get(eid)
        ta_x = ta_by_id.get(xid)
        gross = None
        fees = None
        if ta_e is not None and ta_x is not None:
            gross = float(ta_e["gross_pnl"]) + float(ta_x["gross_pnl"])
            fees = float(ta_e["fees"]) + float(ta_x["fees"])
        elif ta_x is not None:
            gross = float(ta_x["gross_pnl"])
            fees = float(ta_x["fees"])
        elif ta_e is not None:
            gross = float(ta_e["gross_pnl"])
            fees = float(ta_e["fees"])

        net = float(r["total_pnl"])
        if gross is None:
            gross = net
        if fees is None:
            fees = 0.0

        ep = float(r["entry_price"])
        im = float(r["immediate_move"])
        adverse_move = im < 0.0
        pct_against = (abs(im) / ep) if ep > 1e-12 else 0.0
        if adverse_pct_threshold > 0.0:
            adverse_flag = 1 if (adverse_move and pct_against >= adverse_pct_threshold) else 0
        else:
            adverse_flag = 1 if adverse_move else 0

        win_flag = 1 if net > 0 else 0
        direction = _entry_direction(str(r["side"]))
        lat_ms = lat_by_trade.get(eid)
        if lat_ms is None:
            lat_ms = float(r["entry_time_in_book_ms"])

        notional = abs(ep * qty) if ep > 0 and qty > 0 else 0.0
        roi = (net / notional) if notional > 1e-12 else None

        trades.append(
            {
                "trade_id": str(r["round_trip_id"]),
                "timestamp": str(r["entry_ts"]),
                "direction": direction,
                "entry_price": ep,
                "exit_price": float(r["exit_price"]),
                "qty": qty,
                "gross_pnl": gross,
                "net_pnl": net,
                "fees": fees,
                "win_flag": win_flag,
                "adverse_flag": adverse_flag,
                "immediate_move": im,
                "entry_time_in_book_ms": float(r["entry_time_in_book_ms"]),
                "latency_ms": lat_ms,
                "spread_at_entry": float(r["entry_spread"]),
                "mfe": float(r["mfe"]),
                "mae": float(r["mae"]),
                "duration_ms": float(r["duration_ms"]),
                "symbol": str(r["symbol"]),
                "unit_economics": {
                    "fees": fees,
                    "roi_on_notional": roi,
                    "slippage_component": float(ta_x["slippage"]) if ta_x is not None else None,
                    "spread_pnl_component": float(ta_x["spread_pnl"]) if ta_x is not None else None,
                },
            }
        )
        pnls.append(net)

    n = len(trades)
    total_realized = sum(pnls) if pnls else 0.0
    wins = sum(t["win_flag"] for t in trades)
    adverse_ct = sum(t["adverse_flag"] for t in trades)

    state_row = None
    if _table_exists(cur, "analytics_state"):
        try:
            state_row = cur.execute(
                "SELECT cumulative_pnl, equity_peak, max_drawdown, win_count, total_trades, avg_trade_pnl, updated_at FROM analytics_state WHERE id = 1"
            ).fetchone()
        except sqlite3.OperationalError:
            state_row = None

    mdd_rt = _max_drawdown_rt(pnls)
    session_scoped = bool(session_start)
    mdd_db = None
    if state_row and len(state_row) > 2:
        mdd_db = float(state_row[2])
    if session_scoped:
        mdd_db = None

    metrics: dict[str, Any] = {
        "total_trades": n,
        "total_realized_pnl": total_realized,
        "total_unrealized_pnl": None,
        "total_unrealized_note": "not stored per round-trip in this schema; use live book if needed",
        "avg_pnl_per_trade": (total_realized / n) if n else 0.0,
        "avg_win_rate": (wins / n) if n else 0.0,
        "avg_adverse_rate": (adverse_ct / n) if n else 0.0,
        "max_drawdown_round_trip_series": mdd_rt,
        "max_drawdown_analytics_state": mdd_db,
        "max_drawdown_analytics_state_note": (
            "omitted for session-scoped export (table is cumulative)"
            if session_scoped
            else None
        ),
        "avg_mfe": statistics.mean(float(t["mfe"]) for t in trades) if trades else 0.0,
        "avg_mae": statistics.mean(float(t["mae"]) for t in trades) if trades else 0.0,
        "adverse_pct_threshold_used": adverse_pct_threshold,
        "latency_impact": {},
        "spread_tertiles": {},
    }

    spreads = [float(t["spread_at_entry"]) for t in trades]
    if len(spreads) >= 3:
        qs = statistics.quantiles(spreads, n=3)
        s_lo, s_hi = float(qs[0]), float(qs[1])
    elif spreads:
        s_lo = s_hi = statistics.median(spreads)
    else:
        s_lo = s_hi = 0.0

    def sp_bin(es: float) -> str:
        if es <= s_lo:
            return "low"
        if es <= s_hi:
            return "medium"
        return "high"

    tert: dict[str, Any] = {
        "thresholds": {"tertile_low_upper": s_lo, "tertile_mid_upper": s_hi},
        "by_bucket": {},
    }
    for name in ("low", "medium", "high"):
        subset = [t for t in trades if sp_bin(float(t["spread_at_entry"])) == name]
        if not subset:
            tert["by_bucket"][name] = {"count": 0, "avg_pnl": 0.0, "win_rate": 0.0, "adverse_rate": 0.0}
            continue
        pn = [float(t["net_pnl"]) for t in subset]
        tert["by_bucket"][name] = {
            "count": len(subset),
            "avg_pnl": statistics.mean(pn),
            "win_rate": sum(t["win_flag"] for t in subset) / len(subset),
            "adverse_rate": sum(t["adverse_flag"] for t in subset) / len(subset),
        }
    metrics["spread_tertiles"] = tert

    lats = [float(t["latency_ms"]) for t in trades]
    nets = [float(t["net_pnl"]) for t in trades]
    adv01 = [float(t["adverse_flag"]) for t in trades]
    metrics["latency_impact"] = {
        "field": "trade_observability.latency_ms if present else entry_time_in_book_ms",
        "pearson_latency_vs_pnl": _pearson(lats, nets) if n >= 2 else None,
        "pearson_latency_vs_adverse_flag": _pearson(lats, adv01) if n >= 2 else None,
    }

    def agg_subset(sub: list[dict[str, Any]], label: str) -> dict[str, Any]:
        if not sub:
            return {
                "label": label,
                "count": 0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "win_rate": 0.0,
                "adverse_rate": 0.0,
                "avg_mfe": 0.0,
                "avg_mae": 0.0,
                "avg_immediate_move": 0.0,
            }
        pn = [float(t["net_pnl"]) for t in sub]
        return {
            "label": label,
            "count": len(sub),
            "total_pnl": sum(pn),
            "avg_pnl": statistics.mean(pn),
            "win_rate": sum(t["win_flag"] for t in sub) / len(sub),
            "adverse_rate": sum(t["adverse_flag"] for t in sub) / len(sub),
            "avg_mfe": statistics.mean(float(t["mfe"]) for t in sub),
            "avg_mae": statistics.mean(float(t["mae"]) for t in sub),
            "avg_immediate_move": statistics.mean(float(t["immediate_move"]) for t in sub),
        }

    buy_trades = [t for t in trades if t["direction"] == "BUY"]
    sell_trades = [t for t in trades if t["direction"] == "SELL"]

    by_direction: dict[str, Any] = {
        "BUY": agg_subset(buy_trades, "BUY / LONG"),
        "SELL": agg_subset(sell_trades, "SELL / SHORT"),
    }

    entry_zones: dict[str, Any] = {
        "spread_tertiles": tert,
        "immediate_move_summary": {
            "avg": statistics.mean(float(t["immediate_move"]) for t in trades) if trades else 0.0,
            "median": statistics.median(float(t["immediate_move"]) for t in trades) if trades else 0.0,
        },
    }

    out: dict[str, Any] = {
        "meta": {
            "db_path": str(db_path),
            "exported_as": "round_trip_analytics rows (completed trades)",
            "filter": filter_meta,
        },
        "trades": trades,
        "metrics": metrics,
        "by_direction": by_direction,
        "entry_zones": entry_zones,
        "charts": {
            "note": "optional; plot win_rate/adverse_rate/pnl from by_direction and metrics.spread_tertiles in notebook or Excel",
        },
    }

    con.close()

    text = json.dumps(out, ensure_ascii=False, indent=2)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text, encoding="utf-8")
    print(json.dumps(out, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
