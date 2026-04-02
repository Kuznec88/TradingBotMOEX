"""
Create a detailed JSON audit of the current session from fix_engine/log/fix_client.log.

Output:
  fix_engine/log/session_detailed_audit_<UTC>.json

Usage:
  python fix_engine/tools/export_session_detailed_audit.py
  python fix_engine/tools/export_session_detailed_audit.py --structured fix_engine/log/session_live_export_*.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "log" / "fix_client.log"
MARKER_PATH = ROOT / "log" / "session_start_marker.txt"
OUT_DIR = ROOT / "log"


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _safe_float(x: str | None) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _read_marker(marker_path: Path) -> tuple[str, dict[str, str]]:
    """
    Marker format:
      - line 1: ISO timestamp (required)
      - next lines: optional key=value pairs (ignored by legacy tools)
    """
    raw_lines = marker_path.read_text(encoding="utf-8", errors="ignore").strip().splitlines()
    start = (raw_lines[0].strip() if raw_lines else "").strip()
    meta: dict[str, str] = {}
    for ln in raw_lines[1:]:
        s = ln.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k and v:
            meta[k] = v
    return start, meta


def _money(x: object | None) -> float | None:
    if x is None:
        return None
    u = float(getattr(x, "units", 0) or 0)
    n = float(getattr(x, "nano", 0) or 0)
    return u + n / 1_000_000_000.0


@dataclass
class _Event:
    timestamp: str
    tag: str
    message: str
    order_id: str | None = None
    decision: str | None = None
    reason: str | None = None
    score: float | None = None
    score_thr: float | None = None
    spread: float | None = None
    delta: float | None = None
    imb: float | None = None
    trend: float | None = None
    realized_pnl: float | None = None

    def as_dict(self) -> dict[str, object]:
        d: dict[str, object] = {
            "timestamp": self.timestamp,
            "tag": self.tag,
            "message": self.message,
        }
        for k in (
            "order_id",
            "decision",
            "reason",
            "score",
            "score_thr",
            "spread",
            "delta",
            "imb",
            "trend",
            "realized_pnl",
        ):
            v = getattr(self, k)
            if v is not None:
                d[k] = v
        return d


def _iter_log_objects(*, start: str, structured_path: Path | None) -> list[dict]:
    if structured_path is not None:
        payload = json.loads(structured_path.read_text(encoding="utf-8"))
        events = payload.get("events", [])
        if not isinstance(events, list):
            return []
        out: list[dict] = []
        for obj in events:
            if not isinstance(obj, dict):
                continue
            ts = str(obj.get("timestamp") or "")
            if ts and ts < start:
                continue
            out.append(obj)
        return out

    out: list[dict] = []
    for line in LOG.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s.startswith("{"):
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        ts = str(obj.get("timestamp") or "")
        if ts and ts < start:
            continue
        out.append(obj)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--structured",
        type=str,
        default="",
        help="JSON export from export_live_session_logs_json.py (contains {'events': [...]})",
    )
    args = ap.parse_args()

    # Allow running as a script from repo root or fix_engine dir.
    if str(ROOT.parent) not in sys.path:
        sys.path.insert(0, str(ROOT.parent))

    if not MARKER_PATH.is_file():
        raise SystemExit(f"Missing session marker: {MARKER_PATH}")
    if not LOG.is_file() and not args.structured:
        raise SystemExit(f"Missing log file: {LOG}")

    start, marker_meta = _read_marker(MARKER_PATH)

    tags = [
        "[MM][ENTRY_DECISION]",
        "[MM][ENTRY_EXECUTE]",
        "[MM][NEAR_ENTRY]",
        "[MM][TRADE_OPEN]",
        "[MM][TRADE_CLOSE]",
        "[MM][SCORE_OVERRIDE]",
        "[MM][EXIT]",
        "[MM][EXIT_DEBUG]",
    ]

    rx_reason = re.compile(r"reason=([^ ]+)")
    rx_exit_reason = re.compile(r"exit_reason=([^ ]+)")
    rx_decision = re.compile(r"decision=([^ ]+)")
    rx_score = re.compile(r"score=([-0-9.]+)/([0-9.]+)")
    rx_spread = re.compile(r"spread=([-0-9.]+)")
    rx_delta = re.compile(r"delta=([-0-9.]+)")
    rx_imb = re.compile(r"imb=([-0-9.]+)")
    rx_trend = re.compile(r"trend=([-0-9.]+)")
    rx_pnl = re.compile(r"realized_pnl=([-0-9.]+)")
    rx_oid = re.compile(r"(?:order_id|cl_ord_id)=([^ ]+)")

    entry_decisions: list[_Event] = []
    entry_executes: list[_Event] = []
    near_entries: list[_Event] = []
    trade_opens: list[_Event] = []
    trade_closes: list[_Event] = []
    score_overrides: list[_Event] = []
    exits: list[_Event] = []
    exit_debug: list[_Event] = []

    skip_reasons: dict[str, int] = {}
    exit_reasons: dict[str, int] = {}

    def bump(d: dict[str, int], reason: str) -> None:
        d[reason] = d.get(reason, 0) + 1

    structured_path = Path(args.structured).resolve() if str(args.structured).strip() else None
    for obj in _iter_log_objects(start=start, structured_path=structured_path):

        ts = str(obj.get("timestamp") or "")
        msg = str(obj.get("message") or "")
        tag = next((t for t in tags if t in msg), None)
        if tag is None:
            continue

        ev = _Event(timestamp=ts, tag=tag, message=msg)

        m = rx_oid.search(msg)
        if m:
            ev.order_id = m.group(1)
        m = rx_reason.search(msg)
        if m:
            ev.reason = m.group(1)
        m = rx_exit_reason.search(msg)
        if m:
            ev.reason = m.group(1)
        m = rx_decision.search(msg)
        if m:
            ev.decision = m.group(1)
        m = rx_score.search(msg)
        if m:
            ev.score = _safe_float(m.group(1))
            ev.score_thr = _safe_float(m.group(2))
        m = rx_pnl.search(msg)
        if m:
            ev.realized_pnl = _safe_float(m.group(1))
        m = rx_spread.search(msg)
        if m:
            ev.spread = _safe_float(m.group(1))
        m = rx_delta.search(msg)
        if m:
            ev.delta = _safe_float(m.group(1))
        m = rx_imb.search(msg)
        if m:
            ev.imb = _safe_float(m.group(1))
        m = rx_trend.search(msg)
        if m:
            ev.trend = _safe_float(m.group(1))

        if tag == "[MM][ENTRY_DECISION]":
            entry_decisions.append(ev)
            if ev.decision == "NONE":
                bump(skip_reasons, ev.reason or "unknown")
        elif tag == "[MM][ENTRY_EXECUTE]":
            entry_executes.append(ev)
        elif tag == "[MM][NEAR_ENTRY]":
            near_entries.append(ev)
        elif tag == "[MM][TRADE_OPEN]":
            trade_opens.append(ev)
        elif tag == "[MM][TRADE_CLOSE]":
            trade_closes.append(ev)
        elif tag == "[MM][SCORE_OVERRIDE]":
            score_overrides.append(ev)
        elif tag == "[MM][EXIT]":
            exits.append(ev)
            bump(exit_reasons, ev.reason or "unknown")
        else:
            exit_debug.append(ev)

    closed_pnls = [e.realized_pnl for e in trade_closes if isinstance(e.realized_pnl, (int, float))]
    closed_pnls_f = [float(x) for x in closed_pnls]
    wins = sum(1 for x in closed_pnls_f if x > 0)
    losses = sum(1 for x in closed_pnls_f if x < 0)
    flat = sum(1 for x in closed_pnls_f if x == 0)

    balance_change: dict[str, object] | None = None
    try:
        from fix_engine.tools.common_cfg_dir import TBANK_INVEST_GRPC_HOST_PROD, read_cfg_value_from_dir, read_tinvest_token_from_dir

        token = read_tinvest_token_from_dir(ROOT)
        host = read_cfg_value_from_dir(ROOT, "TBankSandboxHost", TBANK_INVEST_GRPC_HOST_PROD).strip()
        account_id = (marker_meta.get("account_id") or read_cfg_value_from_dir(ROOT, "TradingAccountEquities", "")).strip()

        start_total = _safe_float(marker_meta.get("portfolio_total"))
        start_kind = marker_meta.get("portfolio_total_kind") or "operations.get_portfolio.total_amount_portfolio"

        end_total: float | None = None
        from t_tech.invest import Client
        import os

        # Make balance fetch resilient on Windows with Russian trusted chain.
        try:
            cert_bundle = (ROOT / "certs" / "russian_trusted_chain.pem").resolve()
            if cert_bundle.exists():
                os.environ.setdefault("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", str(cert_bundle))
                os.environ.setdefault("SSL_CERT_FILE", str(cert_bundle))
                os.environ.setdefault("REQUESTS_CA_BUNDLE", str(cert_bundle))
        except Exception:
            pass

        if token and host:
            with Client(token, target=host) as client:
                if not account_id:
                    accs = client.users.get_accounts()
                    a0 = (getattr(accs, "accounts", None) or [None])[0]
                    account_id = str(getattr(a0, "id", "") or getattr(a0, "account_id", "")).strip()
                if account_id:
                    port = client.operations.get_portfolio(account_id=account_id)
                    end_total = _money(getattr(port, "total_amount_portfolio", None))

        delta_total: float | None = None
        if isinstance(start_total, (int, float)) and isinstance(end_total, (int, float)):
            delta_total = float(end_total) - float(start_total)

        balance_change = {
            "account_id": account_id or None,
            "host": host or None,
            "start_total": start_total,
            "end_total": end_total,
            "delta_total": delta_total,
            "kind": start_kind,
            "has_token": bool(token),
        }
    except Exception as exc:
        balance_change = {"error": "failed_to_fetch_portfolio_total", "detail": str(exc)}

    out = {
        "session_start_marker": start,
        "session_start_marker_meta": marker_meta,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "counts": {
            "ENTRY_DECISION": len(entry_decisions),
            "ENTRY_EXECUTE": len(entry_executes),
            "NEAR_ENTRY": len(near_entries),
            "TRADE_OPEN": len(trade_opens),
            "TRADE_CLOSE": len(trade_closes),
            "SCORE_OVERRIDE": len(score_overrides),
            "EXIT": len(exits),
            "EXIT_DEBUG": len(exit_debug),
        },
        "trade_close_metrics": {
            "closed_trades": len(closed_pnls_f),
            "wins": wins,
            "losses": losses,
            "flat": flat,
            "winrate": (wins / len(closed_pnls_f)) if closed_pnls_f else None,
            "sum_pnl": sum(closed_pnls_f) if closed_pnls_f else 0.0,
            "avg_pnl": (sum(closed_pnls_f) / len(closed_pnls_f)) if closed_pnls_f else None,
        },
        "top_skip_reasons": sorted(skip_reasons.items(), key=lambda kv: kv[1], reverse=True)[:20],
        "top_exit_reasons": sorted(exit_reasons.items(), key=lambda kv: kv[1], reverse=True)[:20],
        "balance_change": balance_change,
        "last": {
            "entry_decisions": [e.as_dict() for e in entry_decisions[-50:]],
            "entry_executes": [e.as_dict() for e in entry_executes[-50:]],
            "exits": [e.as_dict() for e in exits[-50:]],
            "trade_closes": [e.as_dict() for e in trade_closes[-50:]],
            "near_entries": [e.as_dict() for e in near_entries[-30:]],
        },
    }

    out_path = OUT_DIR / f"session_detailed_audit_{_utc_stamp()}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

