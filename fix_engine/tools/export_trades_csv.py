from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path


FIELDS = [
    "timestamp",
    "symbol",
    "order_id",
    "side",
    "qty",
    "price",
    "type",
    "bid",
    "ask",
    "best_bid",
    "best_ask",
    "exit_price",
    "realized_pnl",
    "realized_total",
    "trades_count",
    "winrate",
    "reason",
    "score",
    "delta_score",
    "spread",
    "mid_price",
    "imbalance",
]


def _is_header_line(line: str) -> bool:
    s = line.strip()
    if not s:
        return True
    if s.startswith("---"):
        return True
    for p in ("pid:", "cwd:", "command:", "started_at:", "running_for_ms:"):
        if s.startswith(p):
            return True
    return False


def _iter_raw_json(path: Path):
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if _is_header_line(raw):
            continue
        s = raw.strip()
        if not s.startswith("{"):
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if isinstance(obj, dict):
            yield obj


def _parse_kv_message(msg: str) -> dict[str, str]:
    """
    Parses tokens like `k=v` from a log message.
    Keeps raw values as strings (caller can normalize).
    """
    out: dict[str, str] = {}
    for tok in msg.split():
        if "=" not in tok:
            continue
        k, v = tok.split("=", 1)
        out[k.strip()] = v.strip()
    return out


_RE_SCORE = re.compile(r"^(-?\d+(?:\.\d+)?)(?:/(-?\d+(?:\.\d+)?))?$")
_RE_SPREAD_PAIR = re.compile(r"^(-?\d+(?:\.\d+)?)(?:/(-?\d+(?:\.\d+)?))?$")


def _first_num(s: str) -> str:
    """
    Returns first numeric part from `x` or `x/y`.
    """
    m = _RE_SCORE.match(s.strip())
    if not m:
        return s
    return m.group(1) or s


def _until_cooldown(events: list[dict[str, object]]) -> list[dict[str, object]]:
    for i, obj in enumerate(events):
        comp = str(obj.get("component", "") or "")
        ev = str(obj.get("event", "") or "")
        msg = str(obj.get("message", "") or "")
        if comp == "RiskManager" and ev == "kill_switch_triggered":
            return events[: i + 1]
        if "[RISK][REJECT]" in msg and "cooldown_active_until=" in msg:
            return events[: i + 1]
    return events


def export_csv(*, log_path: Path, out_path: Path, until_cooldown: bool) -> int:
    events = list(_iter_raw_json(log_path))
    if until_cooldown:
        events = _until_cooldown(events)

    # Keep last known market context per symbol (best-effort enrichment).
    md_ctx: dict[str, dict[str, str]] = {}

    rows: list[dict[str, str]] = []

    def base_row(ts: str | None) -> dict[str, str]:
        r = {k: "" for k in FIELDS}
        r["timestamp"] = str(ts or "")
        return r

    for obj in events:
        ts = obj.get("timestamp")
        comp = str(obj.get("component", "") or "")
        ev = str(obj.get("event", "") or "")
        msg = str(obj.get("message", "") or "")

        # Market data
        if ev in {"MD_UPDATE", "market_data_snapshot"}:
            sym = str(obj.get("symbol", "") or "").upper()
            if sym:
                ctx = md_ctx.setdefault(sym, {})
                for k in ("bid", "ask", "spread", "imbalance", "mid_price"):
                    if k in obj and obj.get(k) is not None:
                        ctx[k] = str(obj.get(k))
                # snapshot uses `mid_price`, MD_UPDATE doesn't
                if ev == "market_data_snapshot" and obj.get("mid_price") is not None:
                    ctx["mid_price"] = str(obj.get("mid_price"))

            r = base_row(ts)
            r["type"] = "MD_UPDATE" if ev == "MD_UPDATE" else "MD_SNAPSHOT"
            r["symbol"] = sym
            for k in ("bid", "ask", "spread", "imbalance", "mid_price"):
                if obj.get(k) is not None:
                    r[k] = str(obj.get(k))
            rows.append(r)
            continue

        # Structured order events
        if ev == "ORDER_CREATED":
            sym = str(obj.get("symbol", "") or "").upper()
            r = base_row(ts)
            r["type"] = "ORDER_CREATED"
            r["symbol"] = sym
            r["order_id"] = str(obj.get("order_id", "") or "")
            r["side"] = str(obj.get("side", "") or "")
            r["qty"] = str(obj.get("size", "") or "")
            r["price"] = str(obj.get("price", "") or "")
            r.update(md_ctx.get(sym, {}))
            rows.append(r)
            continue

        if ev == "simulation_order_accepted":
            sym = str(obj.get("symbol", "") or "").upper()
            r = base_row(ts)
            r["type"] = "SIM_ACCEPT"
            r["symbol"] = sym
            r["order_id"] = str(obj.get("order_id", "") or "")
            r["side"] = str(obj.get("side", "") or "")
            r["qty"] = str(obj.get("quantity", "") or "")
            r["price"] = str(obj.get("price", "") or "")
            r.update(md_ctx.get(sym, {}))
            rows.append(r)
            continue

        # Risk manager structured / message
        if comp == "RiskManager" and ev == "kill_switch_triggered":
            r = base_row(ts)
            r["type"] = "RISK_KILL_SWITCH"
            r["reason"] = str(obj.get("reason", "") or "")
            rows.append(r)
            continue

        if "[RISK][REJECT]" in msg:
            kv = _parse_kv_message(msg)
            sym = str(kv.get("symbol", "")).upper()
            r = base_row(ts)
            r["type"] = "RISK_REJECT"
            r["symbol"] = sym
            r["qty"] = kv.get("qty", "")
            r["reason"] = kv.get("reason", "")
            r.update(md_ctx.get(sym, {}))
            rows.append(r)
            continue

        # Strategy messages
        if "[MM][ENTRY_DECISION]" in msg:
            kv = _parse_kv_message(msg)
            sym = str(kv.get("symbol", "")).upper()
            r = base_row(ts)
            r["type"] = "ENTRY_DECISION"
            r["symbol"] = sym
            r["side"] = kv.get("decision", "")
            r["reason"] = kv.get("reason", "")
            # score=0.965/0.340 → keep only current score
            if "score" in kv:
                r["score"] = _first_num(kv["score"])
            if "delta_score" in kv:
                r["delta_score"] = _first_num(kv["delta_score"])
            if "spread" in kv:
                r["spread"] = _first_num(kv["spread"])
            if "mid" in kv:
                r["mid_price"] = kv["mid"]
            r.update(md_ctx.get(sym, {}))
            rows.append(r)
            continue

        if any(tag in msg for tag in ("[MM][ENTRY_EXECUTE]", "[MM][TRADE_FILLED]", "[MM][EXIT]", "[MM][TRADE_CLOSE]")):
            kv = _parse_kv_message(msg)
            sym = str(kv.get("symbol", "")).upper()
            r = base_row(ts)
            if "[MM][ENTRY_EXECUTE]" in msg:
                r["type"] = "ENTRY_EXECUTE"
                r["symbol"] = sym
                r["side"] = kv.get("side", "")
                r["qty"] = kv.get("qty", "")
                r["price"] = kv.get("px", "")
                r["order_id"] = kv.get("cl_ord_id", "")
            elif "[MM][TRADE_FILLED]" in msg:
                r["type"] = "TRADE_FILLED"
                r["symbol"] = sym
                r["order_id"] = kv.get("order_id", "")
                r["side"] = kv.get("side", "")
                r["qty"] = kv.get("qty", "")
                r["price"] = kv.get("px", "")
                r["best_bid"] = kv.get("best_bid", "")
                r["best_ask"] = kv.get("best_ask", "")
            elif "[MM][EXIT]" in msg:
                r["type"] = "EXIT"
                r["symbol"] = sym
                r["reason"] = kv.get("reason", "")
                r["side"] = kv.get("side", "")
                r["qty"] = kv.get("qty", "")
                r["price"] = kv.get("px", "")
                r["order_id"] = kv.get("cl_ord_id", "")
            else:  # TRADE_CLOSE
                r["type"] = "TRADE_CLOSE"
                r["symbol"] = sym
                r["order_id"] = kv.get("order_id", "")
                r["side"] = kv.get("side", "")
                r["qty"] = kv.get("qty", "")
                r["exit_price"] = kv.get("exit_price", "")
                r["realized_pnl"] = kv.get("realized_pnl", "")
                r["realized_total"] = kv.get("realized_total", "")
                r["trades_count"] = kv.get("trades", "")
                r["winrate"] = kv.get("winrate", "")

            r.update(md_ctx.get(sym, {}))
            rows.append(r)
            continue

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    print(f"events_in={len(events)} rows_out={len(rows)} csv={out_path}")
    return 0


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Export MM bot terminal logs to analysis CSV.")
    p.add_argument(
        "--log",
        required=True,
        help="Path to terminal log file (e.g. terminals/332227.txt).",
    )
    p.add_argument(
        "--out",
        default="trades_analysis.csv",
        help="Output CSV path (default: trades_analysis.csv).",
    )
    p.add_argument(
        "--until-cooldown",
        action="store_true",
        help="Stop export window at first cooldown/kill-switch event.",
    )
    args = p.parse_args(argv[1:])

    return export_csv(
        log_path=Path(args.log),
        out_path=Path(args.out),
        until_cooldown=bool(args.until_cooldown),
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

