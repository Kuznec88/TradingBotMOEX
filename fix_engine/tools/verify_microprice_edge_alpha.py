from __future__ import annotations

import argparse
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class BookSnapshot:
    ts: datetime
    bid: float
    ask: float
    bid_size: float
    ask_size: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) * 0.5


@dataclass(frozen=True)
class TradeRow:
    round_trip_id: str
    symbol: str
    side: str
    entry_ts: str
    pnl: float
    immediate_move: float
    entry_price: float


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _load_trades(engine: Engine) -> list[TradeRow]:
    query = text(
        """
        SELECT
            round_trip_id,
            symbol,
            side,
            entry_ts,
            total_pnl,
            immediate_move,
            entry_price
        FROM round_trip_analytics
        WHERE
            entry_ts IS NOT NULL AND entry_ts <> ''
            AND exit_ts IS NOT NULL AND exit_ts <> ''
        ORDER BY entry_ts
        """
    )
    out: list[TradeRow] = []
    with engine.connect() as conn:
        for row in conn.execute(query):
            out.append(
                TradeRow(
                    round_trip_id=str(row[0]),
                    symbol=str(row[1]),
                    side=str(row[2]),
                    entry_ts=str(row[3]),
                    pnl=float(row[4] or 0.0),
                    immediate_move=float(row[5] or 0.0),
                    entry_price=float(row[6] or 0.0),
                )
            )
    return out


def _quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    idx = int(round((len(sorted_values) - 1) * min(1.0, max(0.0, q))))
    return float(sorted_values[idx])


def _bin_key(value: float, q20: float, q40: float, q60: float, q80: float) -> str:
    if value <= q20:
        return "q1_lowest"
    if value <= q40:
        return "q2"
    if value <= q60:
        return "q3"
    if value <= q80:
        return "q4"
    return "q5_highest"


def _table_columns(engine: Engine, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return {str(r[1]) for r in rows}


def _load_market_cache(engine: Engine) -> dict[str, tuple[list[datetime], list[BookSnapshot]]]:
    cols = _table_columns(engine, "market_data")
    has_bid_size = "bid_size" in cols
    has_ask_size = "ask_size" in cols
    query = text(
        f"""
        SELECT symbol, ts, bid, ask
        {", bid_size" if has_bid_size else ""}
        {", ask_size" if has_ask_size else ""}
        FROM market_data
        ORDER BY symbol, ts
        """
    )
    grouped: dict[str, tuple[list[datetime], list[BookSnapshot]]] = {}
    with engine.connect() as conn:
        for row in conn.execute(query):
            symbol = str(row[0])
            ts = _parse_iso(str(row[1]))
            bid = float(row[2])
            ask = float(row[3])
            bid_size = float(row[4]) if has_bid_size and row[4] is not None else 0.0
            ask_size = float(row[5]) if has_ask_size and row[5] is not None else 0.0
            if symbol not in grouped:
                grouped[symbol] = ([], [])
            times, snaps = grouped[symbol]
            times.append(ts)
            snaps.append(BookSnapshot(ts=ts, bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size))
    return grouped


def _load_entry_decision_cache(engine: Engine) -> dict[tuple[str, str], tuple[list[datetime], list[float]]]:
    cols = _table_columns(engine, "entry_decisions")
    value_col = "microprice_edge" if "microprice_edge" in cols else ("imbalance_score" if "imbalance_score" in cols else "")
    if not value_col:
        return {}
    query = text(
        f"""
        SELECT symbol, side, timestamp, {value_col}
        FROM entry_decisions
        WHERE timestamp IS NOT NULL AND timestamp <> ''
        ORDER BY symbol, side, timestamp
        """
    )
    grouped: dict[tuple[str, str], tuple[list[datetime], list[float]]] = {}
    with engine.connect() as conn:
        for row in conn.execute(query):
            symbol = str(row[0])
            side = str(row[1]).upper()
            ts = _parse_iso(str(row[2]))
            value = float(row[3] or 0.0)
            key = (symbol, side)
            if key not in grouped:
                grouped[key] = ([], [])
            times, vals = grouped[key]
            times.append(ts)
            vals.append(value)
    return grouped


def _micro_from_entry_decisions(
    cache: dict[tuple[str, str], tuple[list[datetime], list[float]]],
    *,
    symbol: str,
    side: str,
    entry_ts: str,
) -> float | None:
    mapped_side = "BUY" if side.upper() == "LONG" else "SELL"
    key = (symbol, mapped_side)
    pair = cache.get(key)
    if pair is None:
        return None
    times, vals = pair
    target = _parse_iso(entry_ts)
    idx = bisect_right(times, target) - 1
    if idx < 0:
        return None
    return float(vals[idx])


def _snapshot_at_or_before(
    cache: dict[str, tuple[list[datetime], list[BookSnapshot]]],
    *,
    symbol: str,
    target: datetime,
) -> BookSnapshot | None:
    pair = cache.get(symbol)
    if pair is None:
        return None
    times, snaps = pair
    idx = bisect_right(times, target) - 1
    if idx < 0:
        return None
    return snaps[idx]


def _reconstruct_microprice_edge(
    cache: dict[str, tuple[list[datetime], list[BookSnapshot]]],
    decision_cache: dict[tuple[str, str], tuple[list[datetime], list[float]]],
    *,
    symbol: str,
    side: str,
    entry_ts: str,
) -> float | None:
    snap = _snapshot_at_or_before(cache, symbol=symbol, target=_parse_iso(entry_ts))
    if snap is not None:
        total = float(snap.bid_size + snap.ask_size)
        if total > 0:
            microprice = (snap.bid * snap.ask_size + snap.ask * snap.bid_size) / total
            return float(microprice - snap.mid)
    return _micro_from_entry_decisions(decision_cache, symbol=symbol, side=side, entry_ts=entry_ts)


def run(
    *,
    economics_db: Path,
    market_db: Path,
) -> None:
    econ_engine = create_engine(f"sqlite:///{economics_db}")
    market_engine = create_engine(f"sqlite:///{market_db}")
    trades = _load_trades(econ_engine)
    cache = _load_market_cache(market_engine)
    decision_cache = _load_entry_decision_cache(econ_engine)
    if not trades:
        print("No trades found.")
        return

    reconstructed: list[dict[str, float | str]] = []
    for t in trades:
        micro = _reconstruct_microprice_edge(
            cache,
            decision_cache,
            symbol=t.symbol,
            side=t.side,
            entry_ts=t.entry_ts,
        )
        if micro is None:
            continue
        reconstructed.append(
            {
                "round_trip_id": t.round_trip_id,
                "microprice_edge": float(micro),
                "pnl": float(t.pnl),
                "adverse_flag": 1.0 if float(t.immediate_move) < 0.0 else 0.0,
            }
        )

    if not reconstructed:
        print("No trades with reconstructable microprice_edge.")
        return

    micro_vals = sorted(float(r["microprice_edge"]) for r in reconstructed)
    q20 = _quantile(micro_vals, 0.20)
    q40 = _quantile(micro_vals, 0.40)
    q60 = _quantile(micro_vals, 0.60)
    q80 = _quantile(micro_vals, 0.80)

    bins: dict[str, dict[str, float]] = {
        "q1_lowest": {"count": 0.0, "pnl_sum": 0.0, "wins": 0.0, "adverse": 0.0},
        "q2": {"count": 0.0, "pnl_sum": 0.0, "wins": 0.0, "adverse": 0.0},
        "q3": {"count": 0.0, "pnl_sum": 0.0, "wins": 0.0, "adverse": 0.0},
        "q4": {"count": 0.0, "pnl_sum": 0.0, "wins": 0.0, "adverse": 0.0},
        "q5_highest": {"count": 0.0, "pnl_sum": 0.0, "wins": 0.0, "adverse": 0.0},
    }
    for row in reconstructed:
        key = _bin_key(float(row["microprice_edge"]), q20, q40, q60, q80)
        b = bins[key]
        pnl = float(row["pnl"])
        b["count"] += 1.0
        b["pnl_sum"] += pnl
        b["wins"] += 1.0 if pnl > 0.0 else 0.0
        b["adverse"] += float(row["adverse_flag"])

    baseline_adverse = sum(float(r["adverse_flag"]) for r in reconstructed) / float(len(reconstructed))
    print("bin | count | avg_pnl | win_rate | adverse_rate")
    print("-" * 52)
    order = ("q1_lowest", "q2", "q3", "q4", "q5_highest")
    for key in order:
        b = bins[key]
        n = int(b["count"])
        if n <= 0:
            print(f"{key} | 0 | 0.000000 | 0.00% | 0.00%")
            continue
        avg_pnl = b["pnl_sum"] / float(n)
        win_rate = b["wins"] / float(n)
        adverse_rate = b["adverse"] / float(n)
        print(f"{key} | {n} | {avg_pnl:.6f} | {win_rate * 100.0:.2f}% | {adverse_rate * 100.0:.2f}%")

    top = bins["q5_highest"]
    top_n = int(top["count"])
    if top_n > 0:
        top_avg_pnl = top["pnl_sum"] / float(top_n)
        top_adverse = top["adverse"] / float(top_n)
        has_edge = top_avg_pnl > 0.0 and top_adverse < baseline_adverse
    else:
        top_avg_pnl = 0.0
        top_adverse = 1.0
        has_edge = False

    print("")
    print(f"baseline_adverse_rate={baseline_adverse * 100.0:.2f}%")
    print(f"top_bin=q5_highest avg_pnl={top_avg_pnl:.6f} adverse_rate={top_adverse * 100.0:.2f}%")
    if has_edge:
        print("VERDICT: microprice_edge shows edge (top bin positive pnl and better adverse_rate).")
    else:
        print("VERDICT: no edge, strategy invalid by current criterion.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify microprice-edge alpha using 5 quantile bins.")
    parser.add_argument("--economics-db", type=Path, default=Path("fix_engine/e2e_economics.db"))
    parser.add_argument("--market-db", type=Path, default=Path("fix_engine/e2e_validation.db"))
    args = parser.parse_args()
    run(economics_db=args.economics_db, market_db=args.market_db)


if __name__ == "__main__":
    main()
