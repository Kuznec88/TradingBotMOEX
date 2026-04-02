from __future__ import annotations

import argparse
import json
import sqlite3
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from fix_engine.tools.common_cfg_dir import read_cfg_value_from_dir


_TRAY_RE = re.compile(r"\[MM\]\[TRAY\]\s+symbol=(?P<symbol>[A-Z0-9_]+).*?\bmid=(?P<mid>-?\d+(?:\.\d+)?)")


def _parse_tray_mid(message: str) -> tuple[str, float] | None:
    m = _TRAY_RE.search(message)
    if not m:
        return None
    try:
        return m.group("symbol").upper(), float(m.group("mid"))
    except Exception:
        return None


def _parse_session_start(marker_path: Path) -> datetime | None:
    if not marker_path.exists():
        return None
    raw = marker_path.read_text(encoding="utf-8", errors="ignore").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_db_dt_utc(value: str) -> datetime | None:
    try:
        # SQLite datetime('now') format: "YYYY-MM-DD HH:MM:SS"
        dt = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _position_from_fills(rows: list[tuple[str, float, float]]) -> tuple[float, float]:
    qty = 0.0
    avg = 0.0
    for side, raw_qty, raw_px in rows:
        sign = 1.0 if str(side).strip().upper() in {"1", "BUY", "B"} else -1.0
        fill_qty = abs(float(raw_qty))
        fill_px = float(raw_px)
        if fill_qty <= 0.0:
            continue
        # Same direction add/open.
        if qty == 0.0 or (qty > 0.0 and sign > 0.0) or (qty < 0.0 and sign < 0.0):
            new_qty = qty + sign * fill_qty
            if abs(new_qty) > 1e-9:
                gross_notional = abs(qty) * avg + fill_qty * fill_px
                avg = gross_notional / abs(new_qty)
            qty = new_qty
            continue
        # Opposite side: close/reverse.
        closing_qty = min(abs(qty), fill_qty)
        qty += sign * fill_qty
        if abs(qty) <= 1e-9:
            qty = 0.0
            avg = 0.0
        elif closing_qty < fill_qty:
            # Reversal remainder opens with new avg at fill.
            avg = fill_px
    return float(qty), float(avg)


def main() -> None:
    ap = argparse.ArgumentParser(description="Show live session PnL/position from economics + tray logs.")
    ap.add_argument("--symbol", default="", help="Symbol to filter (default from settings).")
    ap.add_argument("--log", default="log/fix_client.log", help="Path to JSONL bot log.")
    ap.add_argument(
        "--db",
        default="trade_economics.db",
        help="Path to trade_economics.db (relative to fix_engine dir by default).",
    )
    ap.add_argument(
        "--marker",
        default="log/session_start_marker.txt",
        help="Path to session_start_marker.txt (relative to fix_engine dir by default).",
    )
    args = ap.parse_args()

    fix_engine_dir = Path(__file__).resolve().parents[1]
    symbol = (args.symbol or read_cfg_value_from_dir(fix_engine_dir, "MarketMakingSymbol", "")).strip().upper()
    if not symbol:
        print("symbol is empty; pass --symbol", file=sys.stderr)
        raise SystemExit(1)

    log_path = Path(args.log)
    if not log_path.is_absolute():
        log_path = (fix_engine_dir / log_path).resolve()
    if not log_path.exists():
        print(f"log not found: {log_path}", file=sys.stderr)
        raise SystemExit(1)
    db_path = Path(args.db)
    if not db_path.is_absolute():
        db_path = (fix_engine_dir / db_path).resolve()
    if not db_path.exists():
        print(f"db not found: {db_path}", file=sys.stderr)
        raise SystemExit(1)
    marker_path = Path(args.marker)
    if not marker_path.is_absolute():
        marker_path = (fix_engine_dir / marker_path).resolve()

    session_start = _parse_session_start(marker_path)
    last_tray_mid: float | None = None
    last_tray_ts: str = ""
    for raw in log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = raw.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        ts_raw = str(obj.get("timestamp", "") or "")
        if session_start is not None and ts_raw:
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                ts = ts.astimezone(timezone.utc)
                if ts < session_start:
                    continue
            except Exception:
                pass
        msg = str(obj.get("message", "") or "")
        tray = _parse_tray_mid(msg)
        if tray is not None and tray[0] == symbol:
            last_tray_mid = tray[1]
            last_tray_ts = ts_raw

    realized_session = 0.0
    fills_for_position: list[tuple[str, float, float]] = []
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT side, qty, price, net_pnl, created_at FROM trade_economics WHERE symbol=? ORDER BY id ASC",
            (symbol,),
        ).fetchall()
        for side, qty, px, net_pnl, created_at in rows:
            if session_start is not None:
                dt = _parse_db_dt_utc(str(created_at))
                if dt is not None and dt < session_start:
                    continue
            fills_for_position.append((str(side), float(qty), float(px)))
            realized_session += float(net_pnl or 0.0)
        analytics = cur.execute(
            "SELECT cumulative_pnl, equity_peak, max_drawdown, win_count, total_trades, avg_trade_pnl, updated_at FROM analytics_state WHERE id=1"
        ).fetchone()

    pos, avg = _position_from_fills(fills_for_position)
    upnl = 0.0
    if abs(pos) > 1e-9 and avg > 0.0 and last_tray_mid is not None:
        upnl = (last_tray_mid - avg) * pos

    start_balance = float(read_cfg_value_from_dir(fix_engine_dir, "VirtualAccountStartBalance", "0.0") or "0.0")
    max_loss_frac = float(read_cfg_value_from_dir(fix_engine_dir, "VirtualAccountMaxLossFraction", "0.0") or "0.0")
    realized_all = float(analytics[0]) if analytics else 0.0
    virtual_equity = start_balance + realized_all + upnl
    dd_abs = max(0.0, start_balance - virtual_equity) if start_balance > 0.0 else 0.0
    dd_limit_abs = start_balance * max_loss_frac if start_balance > 0.0 else 0.0

    side = "FLAT"
    if pos > 0:
        side = "LONG"
    elif pos < 0:
        side = "SHORT"
    print(f"session_start={session_start.isoformat() if session_start else 'N/A'}")
    print(f"timestamp={last_tray_ts or 'N/A'}")
    print(f"symbol={symbol} side={side} qty={pos:.4f} avg_price={avg:.4f}")
    if last_tray_mid is not None:
        print(f"mark_mid={last_tray_mid:.4f}")
    print(f"realized_pnl_session={realized_session:.4f} unrealized_pnl={upnl:.4f}")
    print(
        f"test_account start_balance={start_balance:.2f} realized_all={realized_all:.4f} "
        f"equity={virtual_equity:.4f} drawdown_abs={dd_abs:.4f} dd_limit_abs={dd_limit_abs:.4f}"
    )


if __name__ == "__main__":
    main()

