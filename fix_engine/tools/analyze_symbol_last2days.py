import json
import math
import sqlite3
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fix_engine.tools.common_stats import avg, corr, med, quantile


DB_PATH = Path(__file__).resolve().parents[1] / "trade_economics.db"


def summarize(group):
    if not group:
        return {"count": 0, "avg_pnl": None, "win_rate": None, "adverse_rate": None}
    pnls = [g["total_pnl"] for g in group]
    wins = [1 if (g["total_pnl"] or 0) > 0 else 0 for g in group]
    adverse = [g["adverse_rate"] for g in group if g["adverse_rate"] is not None]
    if not adverse:
        adverse = [1.0 if g["mae"] is not None and g["mae"] < 0 else 0.0 for g in group]
    return {
        "count": len(group),
        "avg_pnl": avg(pnls),
        "win_rate": avg(wins),
        "adverse_rate": avg(adverse) if adverse else None,
    }


def microtrend_category(row):
    m = row["momentum_500ms"]
    side = (row["side"] or "").upper()
    if m is None or abs(m) <= 1e-9:
        return "neutral"
    if (side == "BUY" and m > 0) or (side == "SELL" and m < 0):
        return "aligned"
    if (side == "BUY" and m < 0) or (side == "SELL" and m > 0):
        return "opposite"
    return "neutral"


def expected_edge(row):
    if row["expected_price"] is None or row["actual_price"] is None:
        return None
    side = (row["side"] or "").upper()
    if side == "BUY":
        return row["expected_price"] - row["actual_price"]
    if side == "SELL":
        return row["actual_price"] - row["expected_price"]
    return None


def analyze(symbol):
    utc_today = datetime.now(timezone.utc).date()
    utc_yesterday = utc_today - timedelta(days=1)
    d_today = utc_today.isoformat()
    d_yday = utc_yesterday.isoformat()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    rows = cur.execute(
        """
        SELECT
          r.round_trip_id,
          r.symbol,
          r.side,
          r.total_pnl,
          r.mfe,
          r.mae,
          r.immediate_move,
          r.entry_spread,
          r.entry_price,
          r.entry_ts,
          r.exit_ts,
          ro.adverse_rate,
          ro.mid_price,
          ro.spread AS obs_spread,
          ro.latency_ms,
          ro.momentum_500ms,
          ta.expected_price,
          ta.actual_price
        FROM round_trip_analytics r
        LEFT JOIN round_trip_observability ro ON ro.round_trip_id = r.round_trip_id
        LEFT JOIN trade_analytics ta ON ta.trade_id = r.entry_trade_id
        WHERE r.symbol = ?
          AND r.exit_ts IS NOT NULL
          AND substr(r.entry_ts, 1, 10) IN (?, ?)
        ORDER BY r.entry_ts
        """,
        (symbol, d_yday, d_today),
    ).fetchall()
    data = [dict(r) for r in rows]

    out = {
        "filters": {
            "symbol": symbol,
            "entry_dates_utc": [d_yday, d_today],
            "completed_round_trips_only": True,
        }
    }
    if not data:
        out["error"] = "No completed round trips for requested filters"
        return out

    pnls = [d["total_pnl"] for d in data]
    wins = [1 if (d["total_pnl"] or 0) > 0 else 0 for d in data]
    adverse = [d["adverse_rate"] for d in data if d["adverse_rate"] is not None]
    if not adverse:
        adverse = [1.0 if d["mae"] is not None and d["mae"] < 0 else 0.0 for d in data]
    base = {
        "total_trades": len(data),
        "avg_pnl": avg(pnls),
        "median_pnl": med(pnls),
        "win_rate": avg(wins),
        "adverse_rate": avg(adverse) if adverse else None,
        "avg_mfe": avg([d["mfe"] for d in data]),
        "avg_mae": avg([d["mae"] for d in data]),
    }

    eps = 0.01
    immediate = {
        "epsilon": eps,
        "lt_0": summarize([d for d in data if d["immediate_move"] is not None and d["immediate_move"] < -eps]),
        "approx_0": summarize([d for d in data if d["immediate_move"] is not None and abs(d["immediate_move"]) <= eps]),
        "gt_0": summarize([d for d in data if d["immediate_move"] is not None and d["immediate_move"] > eps]),
    }

    mg = {"aligned": [], "opposite": [], "neutral": []}
    for d in data:
        mg[microtrend_category(d)].append(d)
    micro = {k: summarize(v) for k, v in mg.items()}

    spreads = [d["entry_spread"] if d["entry_spread"] is not None else d["obs_spread"] for d in data]
    spread_vals = sorted([x for x in spreads if x is not None])
    sq1 = quantile(spread_vals, 1 / 3)
    sq2 = quantile(spread_vals, 2 / 3)
    sg = {"low": [], "medium": [], "high": []}
    for d, s in zip(data, spreads):
        if s is None:
            continue
        if s <= sq1:
            sg["low"].append(d)
        elif s <= sq2:
            sg["medium"].append(d)
        else:
            sg["high"].append(d)
    spread = {
        "thresholds": {"q33": sq1, "q66": sq2},
        "low": {"count": len(sg["low"]), "avg_pnl": avg([x["total_pnl"] for x in sg["low"]]), "adverse_rate": summarize(sg["low"])["adverse_rate"]},
        "medium": {"count": len(sg["medium"]), "avg_pnl": avg([x["total_pnl"] for x in sg["medium"]]), "adverse_rate": summarize(sg["medium"])["adverse_rate"]},
        "high": {"count": len(sg["high"]), "avg_pnl": avg([x["total_pnl"] for x in sg["high"]]), "adverse_rate": summarize(sg["high"])["adverse_rate"]},
    }

    latencies = [d["latency_ms"] for d in data]
    lat_vals = sorted([x for x in latencies if x is not None])
    lq1 = quantile(lat_vals, 1 / 3)
    lq2 = quantile(lat_vals, 2 / 3)
    lg = {"low": [], "mid": [], "high": []}
    for d, l in zip(data, latencies):
        if l is None:
            continue
        if l <= lq1:
            lg["low"].append(d)
        elif l <= lq2:
            lg["mid"].append(d)
        else:
            lg["high"].append(d)
    latency = {
        "thresholds": {"q33": lq1, "q66": lq2},
        "low": {"count": len(lg["low"]), "avg_pnl": avg([x["total_pnl"] for x in lg["low"]]), "adverse_rate": summarize(lg["low"])["adverse_rate"]},
        "mid": {"count": len(lg["mid"]), "avg_pnl": avg([x["total_pnl"] for x in lg["mid"]]), "adverse_rate": summarize(lg["mid"])["adverse_rate"]},
        "high": {"count": len(lg["high"]), "avg_pnl": avg([x["total_pnl"] for x in lg["high"]]), "adverse_rate": summarize(lg["high"])["adverse_rate"]},
    }

    def entry_minus_mid(row):
        if row["entry_price"] is None or row["mid_price"] is None:
            return None
        return row["entry_price"] - row["mid_price"]

    em = [entry_minus_mid(d) for d in data]
    buy_above = []
    sell_below = []
    buy_n = 0
    sell_n = 0
    for d in data:
        if d["entry_price"] is None or d["mid_price"] is None:
            continue
        side = (d["side"] or "").upper()
        if side == "BUY":
            buy_n += 1
            buy_above.append(1 if d["entry_price"] > d["mid_price"] else 0)
        elif side == "SELL":
            sell_n += 1
            sell_below.append(1 if d["entry_price"] < d["mid_price"] else 0)
    entry_mid = {
        "avg_entry_price_minus_mid_price": avg(em),
        "share_buy_entry_above_mid": avg(buy_above) if buy_n else None,
        "share_sell_entry_below_mid": avg(sell_below) if sell_n else None,
    }

    for d in data:
        d["_edge"] = expected_edge(d)
    ep = [d for d in data if d["_edge"] is not None and d["_edge"] > 0]
    en = [d for d in data if d["_edge"] is not None and d["_edge"] <= 0]
    edge = {"samples": len(ep) + len(en), "gt_0": summarize(ep), "le_0": summarize(en)}

    ranked = sorted(data, key=lambda d: (d["total_pnl"] if d["total_pnl"] is not None else -1e18), reverse=True)

    def card(d):
        return {
            "round_trip_id": d["round_trip_id"],
            "pnl": d["total_pnl"],
            "direction": d["side"],
            "spread": d["entry_spread"] if d["entry_spread"] is not None else d["obs_spread"],
            "microtrend": microtrend_category(d),
            "immediate_move": d["immediate_move"],
        }

    top_worst = {
        "top_5": [card(d) for d in ranked[:5]],
        "worst_5": [card(d) for d in sorted(ranked, key=lambda d: (d["total_pnl"] if d["total_pnl"] is not None else 1e18))[:5]],
    }

    out.update(
        {
            "base_aggregates": base,
            "immediate_move_analysis": immediate,
            "microtrend_analysis": micro,
            "spread_analysis": spread,
            "latency_analysis": latency,
            "entry_vs_mid": entry_mid,
            "expected_edge_analysis": edge,
            "top_and_worst": top_worst,
            "correlations": {
                "spread_pnl": corr(spreads, pnls),
                "latency_pnl": corr(latencies, pnls),
                "immediate_move_pnl": corr([d["immediate_move"] for d in data], pnls),
            },
        }
    )
    return out


if __name__ == "__main__":
    print(json.dumps(analyze("SRM6"), ensure_ascii=False, indent=2))
