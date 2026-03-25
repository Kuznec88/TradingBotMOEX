from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketSnapshot:
    ts: datetime
    bid: float
    ask: float
    bid_size: float | None
    ask_size: float | None

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) * 0.5


@dataclass(frozen=True)
class RoundTripRow:
    round_trip_id: str
    entry_trade_id: str
    exit_trade_id: str
    symbol: str
    side: str
    pnl: float
    mfe: float
    mae: float
    duration_ms: float
    entry_time: str
    exit_time: str
    entry_price: float


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _table_columns(engine: Engine, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return {str(r[1]) for r in rows}


def _load_round_trips(engine: Engine, *, descending: bool, n: int) -> list[RoundTripRow]:
    order = "DESC" if descending else "ASC"
    query = text(
        f"""
        SELECT
            round_trip_id,
            entry_trade_id,
            exit_trade_id,
            symbol,
            side,
            total_pnl,
            mfe,
            mae,
            duration_ms,
            entry_ts,
            exit_ts,
            entry_price
        FROM round_trip_analytics
        WHERE
            entry_trade_id IS NOT NULL AND entry_trade_id <> ''
            AND exit_trade_id IS NOT NULL AND exit_trade_id <> ''
            AND entry_ts IS NOT NULL AND entry_ts <> ''
            AND exit_ts IS NOT NULL AND exit_ts <> ''
        ORDER BY total_pnl {order}
        LIMIT :limit_n
        """
    )
    out: list[RoundTripRow] = []
    with engine.connect() as conn:
        for row in conn.execute(query, {"limit_n": int(n)}):
            out.append(
                RoundTripRow(
                    round_trip_id=str(row[0]),
                    entry_trade_id=str(row[1]),
                    exit_trade_id=str(row[2]),
                    symbol=str(row[3]),
                    side=str(row[4]),
                    pnl=float(row[5]),
                    mfe=float(row[6]),
                    mae=float(row[7]),
                    duration_ms=float(row[8]),
                    entry_time=str(row[9]),
                    exit_time=str(row[10]),
                    entry_price=float(row[11] or 0.0),
                )
            )
    return out


def _load_trade_map(engine: Engine, trade_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not trade_ids:
        return {}
    placeholders = ", ".join([f":t{i}" for i in range(len(trade_ids))])
    params = {f"t{i}": trade_id for i, trade_id in enumerate(sorted(trade_ids))}
    query = text(
        f"""
        SELECT trade_id, side, actual_price, adverse_px_100ms
        FROM trade_analytics
        WHERE trade_id IN ({placeholders})
        """
    )
    out: dict[str, dict[str, Any]] = {}
    with engine.connect() as conn:
        for row in conn.execute(query, params):
            out[str(row[0])] = {
                "side": str(row[1]),
                "actual_price": float(row[2] or 0.0),
                "adverse_px_100ms": float(row[3]) if row[3] is not None else None,
            }
    return out


def _nearest_entry_decision(
    engine: Engine,
    *,
    symbol: str,
    side: str,
    entry_time: str,
    entry_cols: set[str],
) -> dict[str, Any] | None:
    if "entry_decisions" not in _list_tables(engine):
        return None
    base_cols = ["timestamp", "symbol", "side", "decision"]
    optional = [
        "microprice_edge",
        "momentum_100ms",
        "momentum_500ms",
        "spread",
        "immediate_move_50ms",
        "immediate_move_100ms",
        "imbalance_score",
        "trend_score",
        "spread_score",
    ]
    selected = [c for c in base_cols + optional if c in entry_cols]
    if not selected:
        return None
    col_sql = ", ".join(selected)
    query = text(
        f"""
        SELECT {col_sql}
        FROM entry_decisions
        WHERE symbol = :symbol
          AND side = :side
          AND timestamp <= :entry_ts
        ORDER BY timestamp DESC
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"symbol": symbol, "side": side, "entry_ts": entry_time}).fetchone()
    if row is None:
        return None
    rec = {selected[i]: row[i] for i in range(len(selected))}
    out: dict[str, Any] = {}
    out["microprice_edge"] = _to_float(rec.get("microprice_edge"), rec.get("imbalance_score"))
    out["momentum_100ms"] = _to_float(rec.get("momentum_100ms"), rec.get("trend_score"))
    out["momentum_500ms"] = _to_float(rec.get("momentum_500ms"))
    out["spread"] = _to_float(rec.get("spread"), rec.get("spread_score"))
    out["immediate_move_50ms"] = _to_float(rec.get("immediate_move_50ms"))
    out["immediate_move_100ms"] = _to_float(rec.get("immediate_move_100ms"))
    return out


def _to_float(*values: Any) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _list_tables(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
    return {str(r[0]) for r in rows}


def _fetch_neighbor_snapshots(
    engine: Engine,
    *,
    symbol: str,
    target_ts: datetime,
    md_cols: set[str],
    max_ts: datetime | None = None,
) -> tuple[MarketSnapshot | None, MarketSnapshot | None]:
    if "market_data" not in _list_tables(engine):
        return None, None
    wanted = ["ts", "bid", "ask"]
    if "bid_size" in md_cols:
        wanted.append("bid_size")
    if "ask_size" in md_cols:
        wanted.append("ask_size")
    before_query = text(
        f"""
        SELECT {", ".join(wanted)}
        FROM market_data
        WHERE symbol = :symbol AND ts <= :ts
        ORDER BY ts DESC
        LIMIT 1
        """
    )
    upper_bound_sql = "AND ts <= :max_ts" if max_ts is not None else ""
    after_query = text(
        f"""
        SELECT {", ".join(wanted)}
        FROM market_data
        WHERE symbol = :symbol AND ts >= :ts {upper_bound_sql}
        ORDER BY ts ASC
        LIMIT 1
        """
    )

    def _build_snapshot(row: Any) -> MarketSnapshot | None:
        if row is None:
            return None
        rec = {wanted[i]: row[i] for i in range(len(wanted))}
        return MarketSnapshot(
            ts=_parse_iso(str(rec["ts"])),
            bid=float(rec["bid"]),
            ask=float(rec["ask"]),
            bid_size=_to_float(rec.get("bid_size")),
            ask_size=_to_float(rec.get("ask_size")),
        )

    with engine.connect() as conn:
        before_row = conn.execute(before_query, {"symbol": symbol, "ts": target_ts.isoformat()}).fetchone()
        params: dict[str, Any] = {"symbol": symbol, "ts": target_ts.isoformat()}
        if max_ts is not None:
            params["max_ts"] = max_ts.isoformat()
        after_row = conn.execute(after_query, params).fetchone()
    return _build_snapshot(before_row), _build_snapshot(after_row)


def _interpolate_value(v0: float, t0: datetime, v1: float, t1: datetime, target: datetime) -> float:
    denom = (t1 - t0).total_seconds()
    if abs(denom) <= 1e-12:
        return float(v0)
    alpha = (target - t0).total_seconds() / denom
    return float(v0 + (v1 - v0) * alpha)


def _snapshot_at_target(
    engine: Engine,
    *,
    symbol: str,
    target_ts: datetime,
    md_cols: set[str],
    max_ts: datetime | None,
) -> MarketSnapshot | None:
    """
    Returns a snapshot for target_ts using:
    1) exact/nearest-neighbor if only one side exists
    2) interpolation if both sides exist and timestamps differ
    max_ts bounds data usage to prevent lookahead where required.
    """
    prev_snap, next_snap = _fetch_neighbor_snapshots(
        engine,
        symbol=symbol,
        target_ts=target_ts,
        md_cols=md_cols,
        max_ts=max_ts,
    )
    if prev_snap is None and next_snap is None:
        return None
    if prev_snap is None:
        return next_snap
    if next_snap is None:
        return prev_snap
    if prev_snap.ts == next_snap.ts:
        return prev_snap
    bid = _interpolate_value(prev_snap.bid, prev_snap.ts, next_snap.bid, next_snap.ts, target_ts)
    ask = _interpolate_value(prev_snap.ask, prev_snap.ts, next_snap.ask, next_snap.ts, target_ts)
    bid_size: float | None = None
    ask_size: float | None = None
    if prev_snap.bid_size is not None and next_snap.bid_size is not None:
        bid_size = _interpolate_value(prev_snap.bid_size, prev_snap.ts, next_snap.bid_size, next_snap.ts, target_ts)
    elif prev_snap.bid_size is not None:
        bid_size = prev_snap.bid_size
    elif next_snap.bid_size is not None:
        bid_size = next_snap.bid_size
    if prev_snap.ask_size is not None and next_snap.ask_size is not None:
        ask_size = _interpolate_value(prev_snap.ask_size, prev_snap.ts, next_snap.ask_size, next_snap.ts, target_ts)
    elif prev_snap.ask_size is not None:
        ask_size = prev_snap.ask_size
    elif next_snap.ask_size is not None:
        ask_size = next_snap.ask_size
    return MarketSnapshot(ts=target_ts, bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size)


def _reconstruct_signals(
    market_engine: Engine,
    *,
    symbol: str,
    entry_time: str,
    fallback_entry_price: float,
    trade_row: dict[str, Any] | None,
) -> dict[str, float | None]:
    md_cols = _table_columns(market_engine, "market_data")
    t0 = _parse_iso(entry_time)
    # Entry-state signals: no lookahead beyond entry timestamp.
    snap_0 = _snapshot_at_target(
        market_engine,
        symbol=symbol,
        target_ts=t0,
        md_cols=md_cols,
        max_ts=t0,
    )
    snap_100_back = _snapshot_at_target(
        market_engine,
        symbol=symbol,
        target_ts=t0 - timedelta(milliseconds=100),
        md_cols=md_cols,
        max_ts=t0,
    )
    snap_500_back = _snapshot_at_target(
        market_engine,
        symbol=symbol,
        target_ts=t0 - timedelta(milliseconds=500),
        md_cols=md_cols,
        max_ts=t0,
    )
    # Explicitly forward-looking diagnostics (allowed by requirements).
    snap_50_fwd = _snapshot_at_target(
        market_engine,
        symbol=symbol,
        target_ts=t0 + timedelta(milliseconds=50),
        md_cols=md_cols,
        max_ts=None,
    )
    snap_100_fwd = _snapshot_at_target(
        market_engine,
        symbol=symbol,
        target_ts=t0 + timedelta(milliseconds=100),
        md_cols=md_cols,
        max_ts=None,
    )
    for name, snap in (("snap_0", snap_0), ("snap_100_back", snap_100_back), ("snap_500_back", snap_500_back)):
        if snap is None:
            continue
        if snap.ts > t0:
            LOGGER.error(
                "LOOKAHEAD_VIOLATION symbol=%s entry_ts=%s signal_snapshot=%s signal_ts=%s",
                symbol,
                t0.isoformat(),
                name,
                snap.ts.isoformat(),
            )
        assert snap.ts <= t0, f"{name} timestamp must be <= entry timestamp"
    for name, snap in (("snap_50_fwd", snap_50_fwd), ("snap_100_fwd", snap_100_fwd)):
        if snap is None:
            continue
        if snap.ts < t0:
            LOGGER.error(
                "FUTURE_SIGNAL_VIOLATION symbol=%s entry_ts=%s future_snapshot=%s signal_ts=%s",
                symbol,
                t0.isoformat(),
                name,
                snap.ts.isoformat(),
            )
        assert snap.ts >= t0, f"{name} timestamp must be >= entry timestamp"

    microprice_edge: float | None = None
    spread: float | None = None
    momentum_100ms: float | None = None
    momentum_500ms: float | None = None
    immediate_move_50ms: float | None = None
    immediate_move_100ms: float | None = None

    if snap_0 is not None:
        spread = float(snap_0.ask - snap_0.bid)
        bid_size = _to_float(snap_0.bid_size)
        ask_size = _to_float(snap_0.ask_size)
        if bid_size is not None and ask_size is not None and (bid_size + ask_size) > 0:
            microprice = (float(snap_0.bid) * ask_size + float(snap_0.ask) * bid_size) / (bid_size + ask_size)
            microprice_edge = float(microprice - float(snap_0.mid))
    if snap_0 is not None and snap_100_back is not None:
        momentum_100ms = float(snap_0.mid - snap_100_back.mid)
    if snap_0 is not None and snap_500_back is not None:
        momentum_500ms = float(snap_0.mid - snap_500_back.mid)
    if snap_0 is not None and snap_50_fwd is not None:
        immediate_move_50ms = float(snap_50_fwd.mid - snap_0.mid)
    if snap_0 is not None and snap_100_fwd is not None:
        immediate_move_100ms = float(snap_100_fwd.mid - snap_0.mid)

    if immediate_move_100ms is None and trade_row is not None:
        adverse_px_100ms = _to_float(trade_row.get("adverse_px_100ms"))
        entry_px = _to_float(trade_row.get("actual_price"), fallback_entry_price)
        if adverse_px_100ms is not None and entry_px is not None:
            immediate_move_100ms = float(adverse_px_100ms - entry_px)

    return {
        "microprice_edge": microprice_edge,
        "momentum_100ms": momentum_100ms,
        "momentum_500ms": momentum_500ms,
        "spread": spread,
        "immediate_move_50ms": immediate_move_50ms,
        "immediate_move_100ms": immediate_move_100ms,
    }


def _enrich_round_trip(
    economics_engine: Engine,
    market_engine: Engine,
    *,
    rt: RoundTripRow,
    trade_map: dict[str, dict[str, Any]],
    entry_cols: set[str],
) -> dict[str, Any]:
    side_for_decision = "BUY" if rt.side.upper() == "LONG" else "SELL"
    entry_decision = _nearest_entry_decision(
        economics_engine,
        symbol=rt.symbol,
        side=side_for_decision,
        entry_time=rt.entry_time,
        entry_cols=entry_cols,
    )
    trade_row = trade_map.get(rt.entry_trade_id)
    reconstructed = _reconstruct_signals(
        market_engine,
        symbol=rt.symbol,
        entry_time=rt.entry_time,
        fallback_entry_price=rt.entry_price,
        trade_row=trade_row,
    )
    merged = dict(reconstructed)
    if entry_decision is not None:
        for key, value in entry_decision.items():
            if value is not None:
                merged[key] = value
    return {
        "round_trip_id": rt.round_trip_id,
        "entry_time": rt.entry_time,
        "exit_time": rt.exit_time,
        "side": rt.side,
        "pnl": rt.pnl,
        "mfe": rt.mfe,
        "mae": rt.mae,
        "duration_ms": rt.duration_ms,
        "microprice_edge": merged.get("microprice_edge"),
        "momentum_100ms": merged.get("momentum_100ms"),
        "momentum_500ms": merged.get("momentum_500ms"),
        "spread": merged.get("spread"),
        "immediate_move_50ms": merged.get("immediate_move_50ms"),
        "immediate_move_100ms": merged.get("immediate_move_100ms"),
    }


def export_rt_extremes(
    *,
    economics_db_path: Path,
    market_db_path: Path,
    output_path: Path,
    top_n: int = 20,
) -> dict[str, Any]:
    economics_engine = create_engine(f"sqlite:///{economics_db_path}")
    market_engine = create_engine(f"sqlite:///{market_db_path}")
    entry_cols = _table_columns(economics_engine, "entry_decisions") if "entry_decisions" in _list_tables(economics_engine) else set()

    best = _load_round_trips(economics_engine, descending=True, n=top_n)
    worst = _load_round_trips(economics_engine, descending=False, n=top_n)
    all_trade_ids = {row.entry_trade_id for row in best + worst}
    trade_map = _load_trade_map(economics_engine, all_trade_ids)

    payload = {
        "best": [
            _enrich_round_trip(
                economics_engine,
                market_engine,
                rt=row,
                trade_map=trade_map,
                entry_cols=entry_cols,
            )
            for row in best
        ],
        "worst": [
            _enrich_round_trip(
                economics_engine,
                market_engine,
                rt=row,
                trade_map=trade_map,
                entry_cols=entry_cols,
            )
            for row in worst
        ],
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Export top/bottom round-trip extremes with entry-time signals.")
    parser.add_argument(
        "--economics-db",
        type=Path,
        default=Path("fix_engine/e2e_economics.db"),
        help="Path to economics SQLite DB (contains round_trip_analytics/trade_analytics).",
    )
    parser.add_argument(
        "--market-db",
        type=Path,
        default=Path("fix_engine/e2e_validation.db"),
        help="Path to market-data SQLite DB (contains market_data snapshots).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("rt_extremes.json"),
        help="Output JSON path.",
    )
    parser.add_argument("--top-n", type=int, default=20, help="How many best/worst rows to export.")
    args = parser.parse_args()

    payload = export_rt_extremes(
        economics_db_path=args.economics_db,
        market_db_path=args.market_db,
        output_path=args.output,
        top_n=max(1, int(args.top_n)),
    )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "best_count": len(payload["best"]),
                "worst_count": len(payload["worst"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
