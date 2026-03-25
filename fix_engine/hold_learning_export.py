"""
Агрегаты по удержанию, фиктивные точки выхода, метрики по направлению/latency/spread tertiles.
Виртуальный баланс = сумма net_pnl (как в round_trip total_pnl).
"""

from __future__ import annotations

import json
import sqlite3
import statistics
from pathlib import Path
from typing import Any

from adaptive_learning_targets import AdaptiveLearningTargets, load_adaptive_learning_targets
from learning_patch_state import (
    LearningPatchState,
    align_patch_state_to_targets,
    apply_learning_adjustment,
    load_learning_patch_state,
    merge_exit_params,
)


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


def fictitious_exit_points(
    *,
    net_pnl: float,
    gross_pnl: float,
    mfe: float,
    mae: float,
    duration_ms: float,
    fees: float,
    target_roi_abs: float,
    max_adverse_abs: float,
    max_hold_ms: float,
) -> dict[str, Any]:
    """
    Оценочные PnL в «идеальных» точках (без тикового пути; используются MFE/MAE/duration).
    """
    fee_half = fees * 0.5 if fees > 0 else 0.0
    # TP: если экскурсия позволяла забрать целевой ROI
    tp_net = None
    if mfe >= target_roi_abs:
        tp_net = float(target_roi_abs) - fee_half
    # SL: если экскурсия против достигла порога
    sl_net = None
    if mae >= max_adverse_abs:
        sl_net = -float(max_adverse_abs) - fee_half
    # Таймаут: если удержание сравнимо с лимитом — фиксируем фактический net как «на горизонте таймаута»
    timeout_net = None
    if max_hold_ms > 0 and duration_ms >= max_hold_ms * 0.95:
        timeout_net = float(net_pnl)
    return {
        "at_target_profit_net": tp_net,
        "at_stop_loss_net": sl_net,
        "at_max_hold_net": timeout_net,
        "assumptions": {
            "target_roi_abs": target_roi_abs,
            "max_adverse_abs": max_adverse_abs,
            "max_hold_ms": max_hold_ms,
        },
    }


def _spread_tertile_labels(spreads: list[float]) -> list[int]:
    if len(spreads) < 3:
        return [1] * len(spreads)
    srt = sorted(spreads)
    n = len(srt)
    t0 = srt[n // 3]
    t1 = srt[2 * n // 3]
    out: list[int] = []
    for s in spreads:
        if s <= t0:
            out.append(0)
        elif s <= t1:
            out.append(1)
        else:
            out.append(2)
    return out


def _metrics_by_direction(trades: list[dict[str, Any]]) -> dict[str, Any]:
    by: dict[str, list[dict[str, Any]]] = {"BUY": [], "SELL": []}
    for t in trades:
        d = str(t.get("direction", ""))
        if d in by:
            by[d].append(t)
    out: dict[str, Any] = {}
    for name, rows in by.items():
        if not rows:
            out[name] = {"n": 0}
            continue
        pnls = [float(r["net_pnl"]) for r in rows]
        wins = sum(1 for r in rows if float(r.get("win_flag", 0)) > 0)
        adv = sum(1 for r in rows if float(r.get("adverse_flag", 0)) > 0)
        mfes = [float(r.get("mfe", 0.0)) for r in rows]
        maes = [float(r.get("mae", 0.0)) for r in rows]
        out[name] = {
            "n": len(rows),
            "avg_pnl": statistics.mean(pnls) if pnls else 0.0,
            "win_rate": wins / len(rows),
            "adverse_rate": adv / len(rows),
            "avg_mfe": statistics.mean(mfes) if mfes else 0.0,
            "avg_mae": statistics.mean(maes) if maes else 0.0,
        }
    return out


def _latency_impact(trades: list[dict[str, Any]]) -> dict[str, Any]:
    lats = [float(t.get("latency_ms", 0.0)) for t in trades]
    pnls = [float(t.get("net_pnl", 0.0)) for t in trades]
    adv = [1.0 if float(t.get("adverse_flag", 0)) > 0 else 0.0 for t in trades]
    p_lat_pnl = _pearson(lats, pnls)
    p_lat_adv = _pearson(lats, adv)
    if not lats:
        return {"pearson_latency_vs_pnl": None, "pearson_latency_vs_adverse_flag": None, "by_latency_quartile": []}
    qs = sorted(lats)
    nq = len(qs)
    q1 = qs[nq // 4]
    q2 = qs[nq // 2]
    q3 = qs[3 * nq // 4]
    buckets: dict[str, list[dict[str, Any]]] = {"q1": [], "q2": [], "q3": [], "q4": []}
    for t in trades:
        lm = float(t.get("latency_ms", 0.0))
        if lm <= q1:
            buckets["q1"].append(t)
        elif lm <= q2:
            buckets["q2"].append(t)
        elif lm <= q3:
            buckets["q3"].append(t)
        else:
            buckets["q4"].append(t)
    quartile_rows = []
    for key, rows in buckets.items():
        if not rows:
            quartile_rows.append({"bucket": key, "n": 0})
            continue
        adv_r = sum(1 for r in rows if float(r.get("adverse_flag", 0)) > 0) / len(rows)
        quartile_rows.append(
            {
                "bucket": key,
                "n": len(rows),
                "avg_pnl": statistics.mean(float(r["net_pnl"]) for r in rows),
                "adverse_rate": adv_r,
            }
        )
    return {
        "pearson_latency_vs_pnl": p_lat_pnl,
        "pearson_latency_vs_adverse_flag": p_lat_adv,
        "by_latency_quartile": quartile_rows,
    }


def _spread_tertile_stats(trades: list[dict[str, Any]]) -> dict[str, Any]:
    spreads = [float(t.get("spread_at_entry", 0.0)) for t in trades]
    labels = _spread_tertile_labels(spreads)
    t_buckets: dict[int, list[dict[str, Any]]] = {0: [], 1: [], 2: []}
    for t, lab in zip(trades, labels):
        t_buckets[int(lab)].append(t)
    out: dict[str, Any] = {"tertile_thresholds_note": "from entry_spread distribution", "buckets": []}
    for lab in (0, 1, 2):
        rows = t_buckets[lab]
        name = ["narrow", "mid", "wide"][lab]
        if not rows:
            out["buckets"].append({"tertile": lab, "name": name, "n": 0})
            continue
        pnls = [float(r["net_pnl"]) for r in rows]
        wins = sum(1 for r in rows if float(r.get("win_flag", 0)) > 0)
        adv = sum(1 for r in rows if float(r.get("adverse_flag", 0)) > 0)
        buy_rows = [r for r in rows if str(r.get("direction")) == "BUY"]
        sell_rows = [r for r in rows if str(r.get("direction")) == "SELL"]
        buy_wr = (
            sum(1 for r in buy_rows if float(r.get("win_flag", 0)) > 0) / len(buy_rows) if buy_rows else 0.0
        )
        sell_wr = (
            sum(1 for r in sell_rows if float(r.get("win_flag", 0)) > 0) / len(sell_rows) if sell_rows else 0.0
        )
        out["buckets"].append(
            {
                "tertile": lab,
                "name": name,
                "n": len(rows),
                "avg_pnl": statistics.mean(pnls),
                "win_rate": wins / len(rows),
                "adverse_rate": adv / len(rows),
                "win_rate_buy": buy_wr,
                "win_rate_sell": sell_wr,
            }
        )
    return out


def _tertile_win_rates_by_side(trades: list[dict[str, Any]]) -> tuple[list[float], list[float]]:
    spreads = [float(t.get("spread_at_entry", 0.0)) for t in trades]
    labels = _spread_tertile_labels(spreads)
    buy_wr = [0.0, 0.0, 0.0]
    sell_wr = [0.0, 0.0, 0.0]
    for i in range(3):
        br = [t for t, lab in zip(trades, labels) if lab == i and str(t.get("direction")) == "BUY"]
        sr = [t for t, lab in zip(trades, labels) if lab == i and str(t.get("direction")) == "SELL"]
        if br:
            buy_wr[i] = sum(1 for x in br if float(x.get("win_flag", 0)) > 0) / len(br)
        if sr:
            sell_wr[i] = sum(1 for x in sr if float(x.get("win_flag", 0)) > 0) / len(sr)
    return buy_wr, sell_wr


def build_hold_learning_export(
    *,
    db_path: Path,
    session_start: str | None,
    adaptive_targets_path: Path | None,
    learning_patch_path: Path | None,
) -> dict[str, Any]:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    ta_by_id: dict[str, sqlite3.Row] = {}
    if _table_exists(cur, "trade_analytics"):
        for row in cur.execute("SELECT * FROM trade_analytics").fetchall():
            ta_by_id[str(row["trade_id"])] = row

    lat_by_trade: dict[str, float] = {}
    if _table_exists(cur, "trade_observability"):
        for row in cur.execute("SELECT trade_id, latency_ms FROM trade_observability").fetchall():
            lat_by_trade[str(row["trade_id"])] = float(row["latency_ms"] or 0.0)

    te_exists = _table_exists(cur, "trade_economics")
    qty_by_exec: dict[str, float] = {}
    if te_exists:
        for row in cur.execute("SELECT exec_id, qty FROM trade_economics").fetchall():
            eid = str(row["exec_id"])
            if eid not in qty_by_exec:
                qty_by_exec[eid] = float(row["qty"])

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
    con.close()

    targets = load_adaptive_learning_targets(adaptive_targets_path)
    patch = align_patch_state_to_targets(load_learning_patch_state(learning_patch_path), targets)

    trades_with_hold: list[dict[str, Any]] = []
    virtual_net = 0.0
    virtual_fees = 0.0

    adverse_pct_threshold = 0.0

    for r in rt_rows:
        eid = str(r["entry_trade_id"])
        xid = str(r["exit_trade_id"])
        qty = qty_by_exec.get(eid) or qty_by_exec.get(xid) or 1.0
        ta_e = ta_by_id.get(eid)
        ta_x = ta_by_id.get(xid)
        gross = None
        fees = 0.0
        slip = None
        spr_comp = None
        if ta_e is not None and ta_x is not None:
            gross = float(ta_e["gross_pnl"]) + float(ta_x["gross_pnl"])
            fees = float(ta_e["fees"]) + float(ta_x["fees"])
            slip = float(ta_x["slippage"])
            spr_comp = float(ta_x["spread_pnl"])
        elif ta_x is not None:
            gross = float(ta_x["gross_pnl"])
            fees = float(ta_x["fees"])
            slip = float(ta_x["slippage"])
            spr_comp = float(ta_x["spread_pnl"])
        elif ta_e is not None:
            gross = float(ta_e["gross_pnl"])
            fees = float(ta_e["fees"])
            slip = float(ta_e["slippage"])
            spr_comp = float(ta_e["spread_pnl"])

        net = float(r["total_pnl"])
        if gross is None:
            gross = net

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

        mfe = float(r["mfe"])
        mae = float(r["mae"])
        dur = float(r["duration_ms"])

        tp_e, sl_e, hold_e = merge_exit_params(
            base_take_profit=0.0,
            base_stop_loss=0.0,
            targets=targets,
            patch=patch,
        )
        fict = fictitious_exit_points(
            net_pnl=net,
            gross_pnl=float(gross),
            mfe=mfe,
            mae=mae,
            duration_ms=dur,
            fees=fees,
            target_roi_abs=tp_e,
            max_adverse_abs=sl_e,
            max_hold_ms=hold_e,
        )

        row_out: dict[str, Any] = {
            "trade_id": str(r["round_trip_id"]),
            "entry_ts": str(r["entry_ts"]),
            "exit_ts": str(r["exit_ts"]),
            "symbol": str(r["symbol"]),
            "direction": direction,
            "entry_price": ep,
            "exit_price": float(r["exit_price"]),
            "qty": qty,
            "duration_ms": dur,
            "gross_pnl": float(gross),
            "net_pnl": net,
            "win_flag": win_flag,
            "adverse_flag": adverse_flag,
            "immediate_move": im,
            "mfe": mfe,
            "mae": mae,
            "spread_at_entry": float(r["entry_spread"]),
            "latency_ms": lat_ms,
            "entry_time_in_book_ms": float(r["entry_time_in_book_ms"]),
            "unit_economics": {
                "fees": fees,
                "slippage_component": slip,
                "spread_pnl_component": spr_comp,
            },
            "hold_excursions": {"mfe": mfe, "mae": mae, "current_pnl_proxy": net},
            "fictitious_exit_points": fict,
        }
        trades_with_hold.append(row_out)
        virtual_net += net
        virtual_fees += fees

    n = len(trades_with_hold)
    mean_pnl = virtual_net / n if n else 0.0
    win_rate = sum(1 for t in trades_with_hold if float(t["net_pnl"]) > 0) / n if n else 0.0
    adverse_rate = sum(1 for t in trades_with_hold if t["adverse_flag"]) / n if n else 0.0
    avg_mfe = statistics.mean(float(t["mfe"]) for t in trades_with_hold) if trades_with_hold else 0.0
    avg_mae = statistics.mean(float(t["mae"]) for t in trades_with_hold) if trades_with_hold else 0.0
    avg_lat = statistics.mean(float(t["latency_ms"]) for t in trades_with_hold) if trades_with_hold else 0.0
    avg_spread = statistics.mean(float(t["spread_at_entry"]) for t in trades_with_hold) if trades_with_hold else 0.0

    metrics_by_dir = _metrics_by_direction(trades_with_hold)
    lat_impact = _latency_impact(trades_with_hold)
    spr_tert = _spread_tertile_stats(trades_with_hold)
    buy_wr_t, sell_wr_t = _tertile_win_rates_by_side(trades_with_hold)

    prev_state = patch
    new_state, effects = apply_learning_adjustment(
        state=prev_state,
        targets=targets,
        win_rate=win_rate,
        avg_pnl=mean_pnl,
        adverse_rate=adverse_rate,
        avg_mfe=avg_mfe,
        avg_mae=avg_mae,
        avg_latency_ms=avg_lat,
        avg_spread=avg_spread,
        spread_tertile_win_rates_buy=buy_wr_t,
        spread_tertile_win_rates_sell=sell_wr_t,
    )

    return {
        "schema_version": 1,
        "session_filter": session_start,
        "virtual_balance": {
            "realized_net": virtual_net,
            "cumulative_fees": virtual_fees,
            "trade_count": n,
        },
        "trades_with_hold_stats": trades_with_hold,
        "metrics_by_direction": metrics_by_dir,
        "latency_impact": lat_impact,
        "spread_tertiles": spr_tert,
        "learning_patch_effects": {
            "previous_patch": {
                "target_roi_abs": prev_state.target_roi_abs,
                "max_adverse_abs": prev_state.max_adverse_abs,
                "max_hold_duration_ms": prev_state.max_hold_duration_ms,
                "buy_weight_tertile": prev_state.buy_weight_tertile,
                "sell_weight_tertile": prev_state.sell_weight_tertile,
            },
            "suggested_patch": effects.get("new_state"),
            "detail": effects,
        },
        "adaptive_targets_ref": targets.to_dict(),
    }


def export_hold_learning_json_main(argv: list[str] | None = None) -> int:
    import argparse
    import sys

    base = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description="Export hold-learning JSON (trades, metrics, patch effects)")
    ap.add_argument("db_path", nargs="?", default=None, type=Path)
    ap.add_argument("--out", "-o", type=Path, default=None)
    ap.add_argument("--all", action="store_true", help="Ignore session marker")
    ap.add_argument("--since", default=None)
    ap.add_argument("--marker", type=Path, default=None)
    ap.add_argument("--adaptive", type=Path, default=None, help="adaptive_learning_targets.json")
    ap.add_argument("--patch-state", type=Path, default=None, help="learning_patch_state.json")
    args = ap.parse_args(argv)

    db_path = args.db_path if args.db_path else base / "trade_economics.db"
    if not db_path.is_file():
        print(json.dumps({"error": "db_not_found", "path": str(db_path)}))
        return 1

    marker_path = args.marker if args.marker is not None else base / "log" / "session_start_marker.txt"
    if args.all:
        session_start = args.since
    elif args.since:
        session_start = str(args.since).strip()
    else:
        session_start = _read_marker_session_start(marker_path)

    adaptive_path = args.adaptive
    if adaptive_path is None:
        _a = base / "adaptive_learning_targets.json"
        adaptive_path = _a if _a.is_file() else None

    patch_path = args.patch_state
    if patch_path is None:
        _p = base / "learning_patch_state.json"
        patch_path = _p if _p.is_file() else None

    payload = build_hold_learning_export(
        db_path=db_path,
        session_start=session_start,
        adaptive_targets_path=adaptive_path,
        learning_patch_path=patch_path,
    )

    out_path = args.out
    if out_path is None:
        out_path = base / "log" / "hold_learning_export.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "path": str(out_path), "trades": len(payload.get("trades_with_hold_stats", []))}))
    return 0


if __name__ == "__main__":
    raise SystemExit(export_hold_learning_json_main())
