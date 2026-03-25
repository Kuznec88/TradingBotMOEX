"""
Failure-pattern report from trade_economics.db (stdlib + SQLite only).

Joins round_trip_analytics with trade_observability / executed_entry_observability
when present for entry signals and latency_ms.

Strategy code is not modified; this is offline analytics.
"""

from __future__ import annotations

import json
import sqlite3
import statistics
import sys
from pathlib import Path
from typing import Any


def _is_buy_side(side: str) -> bool:
    return str(side).strip().upper() in ("1", "BUY", "LONG")


def _is_sell_side(side: str) -> bool:
    return str(side).strip().upper() in ("2", "SELL", "SHORT")


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


def _agg_direction(
    subset: list[sqlite3.Row],
    adverse_fn: Any,
) -> dict[str, float | int]:
    if not subset:
        return {
            "count": 0,
            "avg_pnl": 0.0,
            "win_rate": 0.0,
            "adverse_rate": 0.0,
            "avg_mfe": 0.0,
            "avg_mae": 0.0,
        }
    pnls = [float(r["total_pnl"]) for r in subset]
    wins = sum(1 for p in pnls if p > 0)
    adv = sum(1 for r in subset if adverse_fn(r))
    return {
        "count": len(subset),
        "avg_pnl": statistics.mean(pnls),
        "win_rate": wins / len(subset),
        "adverse_rate": adv / len(subset),
        "avg_mfe": statistics.mean(float(r["mfe"]) for r in subset),
        "avg_mae": statistics.mean(float(r["mae"]) for r in subset),
    }


def main() -> None:
    db_path = Path(__file__).resolve().parents[1] / "trade_economics.db"
    out_path: Path | None = None
    dump_full = False
    argv = list(sys.argv[1:])
    if "--out" in argv:
        i = argv.index("--out")
        out_path = Path(argv[i + 1])
        del argv[i : i + 2]
    if "--full" in argv:
        dump_full = True
        argv.remove("--full")
    if argv:
        db_path = Path(argv[0])
    if not db_path.exists():
        print(json.dumps({"error": "db_not_found", "path": str(db_path)}))
        sys.exit(1)

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    obs_trade_n = int(cur.execute("SELECT COUNT(*) FROM trade_observability").fetchone()[0])
    obs_rt_n = int(cur.execute("SELECT COUNT(*) FROM round_trip_observability").fetchone()[0])
    obs_exec_n = int(cur.execute("SELECT COUNT(*) FROM executed_entry_observability").fetchone()[0])
    ta_count = int(cur.execute("SELECT COUNT(*) FROM trade_analytics").fetchone()[0])

    ta_by_trade: dict[str, sqlite3.Row] = {}
    for row in cur.execute("SELECT * FROM trade_analytics").fetchall():
        ta_by_trade[str(row["trade_id"])] = row

    rt_rows = cur.execute(
        """
        SELECT
            rt.round_trip_id,
            rt.entry_trade_id,
            rt.exit_trade_id,
            rt.symbol,
            rt.side,
            rt.duration_ms,
            rt.total_pnl,
            rt.mae,
            rt.mfe,
            rt.immediate_move,
            rt.entry_price,
            rt.exit_price,
            rt.entry_spread,
            rt.entry_time_in_book_ms,
            rt.entry_ts,
            rt.exit_ts,
            COALESCE(tro.microprice_edge, eo.microprice_edge) AS sig_microprice_edge,
            COALESCE(tro.momentum_100ms, eo.momentum_100ms) AS sig_momentum_100ms,
            COALESCE(tro.momentum_500ms, eo.momentum_500ms) AS sig_momentum_500ms,
            COALESCE(tro.spread, eo.spread, rt.entry_spread) AS sig_spread,
            COALESCE(tro.imbalance, eo.imbalance) AS sig_imbalance,
            tro.latency_ms AS obs_latency_ms
        FROM round_trip_analytics rt
        LEFT JOIN trade_observability tro ON tro.trade_id = rt.entry_trade_id
        LEFT JOIN executed_entry_observability eo ON eo.exec_id = rt.entry_trade_id
        ORDER BY rt.entry_ts
        """
    ).fetchall()

    n_rt = len(rt_rows)

    def adverse_immediate(r: sqlite3.Row) -> bool:
        return float(r["immediate_move"]) < 0.0

    def adverse_ta_or_immediate(r: sqlite3.Row) -> bool:
        tid = str(r["entry_trade_id"])
        if tid in ta_by_trade:
            return bool(ta_by_trade[tid]["adverse_fill"])
        return adverse_immediate(r)

    buy_rows = [r for r in rt_rows if _is_buy_side(str(r["side"]))]
    sell_rows = [r for r in rt_rows if _is_sell_side(str(r["side"]))]

    directional = {
        "BUY": _agg_direction(buy_rows, adverse_immediate),
        "SELL": _agg_direction(sell_rows, adverse_immediate),
        "adverse_rate_definition": "immediate_move < 0 (first-tick proxy; matches analytics adverse_flag)",
        "optional_adverse_fill_from_trade_analytics": {
            "BUY": _agg_direction(buy_rows, adverse_ta_or_immediate),
            "SELL": _agg_direction(sell_rows, adverse_ta_or_immediate),
        },
        "note": "side from round_trip_analytics; LONG/SHORT map to BUY/SELL for aggregation",
    }

    imoves = [float(r["immediate_move"]) for r in rt_rows]
    adv_flags = [adverse_immediate(r) for r in rt_rows]
    immediate_move = {
        "count": n_rt,
        "pct_price_moved_against_immediately": (sum(adv_flags) / n_rt if n_rt else 0.0),
        "avg_immediate_move": statistics.mean(imoves) if imoves else 0.0,
        "median_immediate_move": statistics.median(imoves) if imoves else 0.0,
        "note": "immediate_move is post-entry price change used as adverse proxy in pipeline",
    }

    rev_pnls = [-float(r["total_pnl"]) for r in rt_rows]
    reversal_test = {
        "hypothesis": "scalar_flip: if entry direction were wrong but magnitude right, PnL ~ -total_pnl",
        "avg_pnl_reversed": statistics.mean(rev_pnls) if rev_pnls else 0.0,
        "win_rate_reversed": (sum(1 for p in rev_pnls if p > 0) / len(rev_pnls) if rev_pnls else 0.0),
        "avg_pnl_original": statistics.mean(float(r["total_pnl"]) for r in rt_rows) if rt_rows else 0.0,
        "win_rate_original": (
            sum(1 for r in rt_rows if float(r["total_pnl"]) > 0) / n_rt if n_rt else 0.0
        ),
    }

    spreads = [float(r["entry_spread"]) for r in rt_rows]
    if len(spreads) >= 3:
        qs = statistics.quantiles(spreads, n=3)
        s_lo, s_hi = float(qs[0]), float(qs[1])
    elif spreads:
        s_lo = s_hi = statistics.median(spreads)
    else:
        s_lo = s_hi = 0.0

    def spread_bin(es: float) -> str:
        if es <= s_lo:
            return "low"
        if es <= s_hi:
            return "medium"
        return "high"

    bins: dict[str, list[sqlite3.Row]] = {"low": [], "medium": [], "high": []}
    for r in rt_rows:
        bins[spread_bin(float(r["entry_spread"]))].append(r)

    spread_tertiles: dict[str, object] = {
        "thresholds": {"tertile_low_upper": s_lo, "tertile_mid_upper": s_hi},
        "by_spread_tertile": {},
    }
    for name, rows in bins.items():
        if not rows:
            spread_tertiles["by_spread_tertile"][name] = {
                "count": 0,
                "avg_pnl": 0.0,
                "adverse_rate": 0.0,
            }
            continue
        pnls = [float(x["total_pnl"]) for x in rows]
        adv = sum(1 for x in rows if adverse_immediate(x))
        spread_tertiles["by_spread_tertile"][name] = {
            "count": len(rows),
            "avg_pnl": statistics.mean(pnls),
            "adverse_rate": adv / len(rows),
        }

    imbalance_block: dict[str, Any] = {
        "available": False,
        "detail": None,
        "from_joined_signals": None,
    }

    with_imb = [r for r in rt_rows if r["sig_imbalance"] is not None and str(r["sig_imbalance"]) != ""]
    with_imb_f = [r for r in with_imb if _is_float(r["sig_imbalance"])]
    if with_imb_f:

        def _imb_zone(x: float) -> str:
            if x < -0.05:
                return "negative"
            if x > 0.05:
                return "positive"
            return "neutral"

        zb: dict[str, list[sqlite3.Row]] = {"negative": [], "neutral": [], "positive": []}
        for r in with_imb_f:
            zb[_imb_zone(float(r["sig_imbalance"]))].append(r)

        def mini_imb(rs: list[sqlite3.Row]) -> dict[str, float | int]:
            if not rs:
                return {"count": 0, "avg_pnl": 0.0, "adverse_rate": 0.0}
            pnls = [float(x["total_pnl"]) for x in rs]
            adv = sum(1 for x in rs if adverse_immediate(x))
            return {
                "count": len(rs),
                "avg_pnl": statistics.mean(pnls),
                "adverse_rate": adv / len(rs),
            }

        imbalance_block["from_joined_signals"] = {
            "round_trips_with_imbalance": len(with_imb_f),
            "negative": mini_imb(zb["negative"]),
            "neutral": mini_imb(zb["neutral"]),
            "positive": mini_imb(zb["positive"]),
        }

    if obs_rt_n > 0:
        rows_obs = cur.execute(
            """
            SELECT r.round_trip_id, r.total_pnl, r.immediate_move, o.imbalance
            FROM round_trip_analytics r
            JOIN round_trip_observability o ON o.round_trip_id = r.round_trip_id
            """
        ).fetchall()
        neg = [r for r in rows_obs if float(r["imbalance"]) < -0.05]
        neu = [r for r in rows_obs if -0.05 <= float(r["imbalance"]) <= 0.05]
        pos = [r for r in rows_obs if float(r["imbalance"]) > 0.05]

        def mini(rs: list[Any]) -> dict[str, float]:
            if not rs:
                return {"count": 0, "avg_pnl": 0.0, "adverse_rate": 0.0}
            pnls = [float(x["total_pnl"]) for x in rs]
            adv = sum(1 for x in rs if float(x["immediate_move"]) < 0)
            return {
                "count": len(rs),
                "avg_pnl": statistics.mean(pnls),
                "adverse_rate": adv / len(rs),
            }

        imbalance_block["available"] = True
        imbalance_block["detail"] = {
            "negative": mini(neg),
            "neutral": mini(neu),
            "positive": mini(pos),
        }

    entry_zones = {
        "spread_tertiles": spread_tertiles,
        "imbalance_neg_neutral_pos": imbalance_block,
    }

    latencies_book = [float(r["entry_time_in_book_ms"]) for r in rt_rows]
    pnls_rt = [float(r["total_pnl"]) for r in rt_rows]
    adv_ints = [1.0 if adverse_immediate(r) else 0.0 for r in rt_rows]

    r_book_pnl = _pearson(latencies_book, pnls_rt) if n_rt >= 2 else None
    r_book_adv = _pearson(latencies_book, adv_ints) if n_rt >= 2 else None

    latency_impact: dict[str, Any] = {
        "entry_time_in_book_ms": {
            "field": "round_trip_analytics.entry_time_in_book_ms",
            "pearson_ms_vs_pnl": r_book_pnl,
            "pearson_ms_vs_adverse_flag": r_book_adv,
            "latency_tertiles_vs_avg_pnl": {},
        },
        "trade_observability_latency_ms": {
            "field": "trade_observability.latency_ms joined on entry_trade_id",
            "rows_with_latency": 0,
            "pearson_latency_ms_vs_pnl": None,
            "pearson_latency_ms_vs_adverse_flag": None,
            "latency_tertiles_vs_avg_pnl": {},
        },
    }

    if len(latencies_book) >= 3:
        t1, t2 = statistics.quantiles(latencies_book, n=3)[:2]

        def lat_bin_book(x: float) -> str:
            if x <= t1:
                return "low"
            if x <= t2:
                return "mid"
            return "high"

        lb: dict[str, list[float]] = {"low": [], "mid": [], "high": []}
        for r in rt_rows:
            lb[lat_bin_book(float(r["entry_time_in_book_ms"]))].append(float(r["total_pnl"]))
        for k, vals in lb.items():
            latency_impact["entry_time_in_book_ms"]["latency_tertiles_vs_avg_pnl"][k] = {
                "count": len(vals),
                "avg_pnl": statistics.mean(vals) if vals else 0.0,
            }

    lat_obs: list[tuple[float, float, float]] = []
    for r in rt_rows:
        v = r["obs_latency_ms"]
        if v is None:
            continue
        try:
            lm = float(v)
        except (TypeError, ValueError):
            continue
        lat_obs.append((lm, float(r["total_pnl"]), 1.0 if adverse_immediate(r) else 0.0))

    if len(lat_obs) >= 2:
        lxs = [a[0] for a in lat_obs]
        lys = [a[1] for a in lat_obs]
        ladv = [a[2] for a in lat_obs]
        latency_impact["trade_observability_latency_ms"]["rows_with_latency"] = len(lat_obs)
        latency_impact["trade_observability_latency_ms"]["pearson_latency_ms_vs_pnl"] = _pearson(
            lxs, lys
        )
        latency_impact["trade_observability_latency_ms"]["pearson_latency_ms_vs_adverse_flag"] = (
            _pearson(lxs, ladv)
        )
        if len(lxs) >= 3:
            q1, q2 = statistics.quantiles(lxs, n=3)[:2]

            def lb_obs(x: float) -> str:
                if x <= q1:
                    return "low"
                if x <= q2:
                    return "mid"
                return "high"

            ob: dict[str, list[float]] = {"low": [], "mid": [], "high": []}
            for a in lat_obs:
                ob[lb_obs(a[0])].append(a[1])
            for k, vals in ob.items():
                latency_impact["trade_observability_latency_ms"]["latency_tertiles_vs_avg_pnl"][k] = {
                    "count": len(vals),
                    "avg_pnl": statistics.mean(vals) if vals else 0.0,
                }

    signals_joined = sum(
        1
        for r in rt_rows
        if r["sig_microprice_edge"] is not None or r["sig_momentum_100ms"] is not None
    )

    per_rt: list[dict[str, Any]] = []
    if dump_full:
        for r in rt_rows:
            tid = str(r["entry_trade_id"])
            ta_row = ta_by_trade.get(tid)
            per_rt.append(
                {
                    "round_trip_id": str(r["round_trip_id"]),
                    "entry_trade_id": tid,
                    "total_pnl": float(r["total_pnl"]),
                    "mfe": float(r["mfe"]),
                    "mae": float(r["mae"]),
                    "adverse_flag": adverse_immediate(r),
                    "adverse_fill_trade_analytics": bool(ta_row["adverse_fill"])
                    if ta_row is not None
                    else None,
                    "entry_side": "BUY"
                    if _is_buy_side(str(r["side"]))
                    else ("SELL" if _is_sell_side(str(r["side"])) else str(r["side"])),
                    "microprice_edge": _f(r["sig_microprice_edge"]),
                    "momentum_100ms": _f(r["sig_momentum_100ms"]),
                    "momentum_500ms": _f(r["sig_momentum_500ms"]),
                    "spread": _f(r["sig_spread"]),
                    "imbalance": _f(r["sig_imbalance"]),
                    "latency_ms": _f(r["obs_latency_ms"]),
                    "entry_time_in_book_ms": float(r["entry_time_in_book_ms"]),
                }
            )

    dataset: dict[str, Any] = {
        "round_trips": n_rt,
        "trade_analytics_rows": ta_count,
        "trade_observability_rows": obs_trade_n,
        "round_trip_observability_rows": obs_rt_n,
        "executed_entry_observability_rows": obs_exec_n,
        "round_trips_with_joined_entry_signals": signals_joined,
        "per_round_trip": per_rt if dump_full else None,
        "use_full_per_round_trip": "pass --full to include per_round_trip array",
    }

    conclusion: list[str] = []
    if n_rt == 0:
        conclusion.append("Нет round trips в БД — нечего анализировать.")
    else:
        wr = float(reversal_test["win_rate_original"])
        wr_rev = float(reversal_test["win_rate_reversed"])
        if wr < 0.2 and wr_rev > 0.6:
            conclusion.append(
                "При полном знаковом инвертировании PnL win_rate сильно выше — гипотеза «инвертированное направление» заслуживает проверки (осторожно: упрощённая модель -pnl)."
            )
        if float(immediate_move["pct_price_moved_against_immediately"]) > 0.55:
            conclusion.append(
                "Высокая доля немедленного движения против позиции (immediate_move < 0) — возможны плохой тайминг входа или лаг относительно mid."
            )
        if directional["BUY"]["count"] and directional["SELL"]["count"]:
            b = float(directional["BUY"]["avg_pnl"])
            s = float(directional["SELL"]["avg_pnl"])
            if b < 0 and s < 0:
                conclusion.append(
                    "И BUY и SELL round-trip в среднем отрицательны — проблема может быть не только в знаке стороны."
                )
        if r_book_pnl is not None and abs(r_book_pnl) > 0.25:
            conclusion.append(
                f"Корреляция entry_time_in_book_ms vs PnL ~= {r_book_pnl:.3f} — время в книге связано с исходом (проверить симуляцию задержек)."
            )
        obs_p = latency_impact["trade_observability_latency_ms"].get("pearson_latency_ms_vs_pnl")
        if obs_p is not None and abs(obs_p) > 0.25:
            conclusion.append(
                f"Корреляция trade_observability.latency_ms vs PnL ~= {obs_p:.3f} — задержка decision→fill может влиять на исход."
            )
        if signals_joined == 0:
            conclusion.append(
                "Нет джойна entry-сигналов (trade_observability / executed_entry пусты или не совпадают entry_trade_id) — микроценовые признаки в отчёте ограничены."
            )
        if not conclusion:
            conclusion.append(
                "Явного одного доминирующего фактора по доступным полям нет; см. directional и immediate_move."
            )

    out: dict[str, object] = {
        "dataset": dataset,
        "directional": directional,
        "immediate_move": immediate_move,
        "reversal_test": reversal_test,
        "entry_zones": entry_zones,
        "latency_impact": latency_impact,
        "conclusion": conclusion,
    }

    con.close()
    text_console = json.dumps(out, ensure_ascii=True, indent=2)
    text_file = json.dumps(out, ensure_ascii=False, indent=2)
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text_file, encoding="utf-8")
    print(text_console)


def _is_float(v: Any) -> bool:
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False


def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    main()
