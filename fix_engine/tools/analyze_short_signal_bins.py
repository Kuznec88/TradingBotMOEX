from __future__ import annotations

import argparse
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class ShortRoundTrip:
    round_trip_id: str
    symbol: str
    entry_ts: str
    pnl: float
    mfe: float
    immediate_move: float


@dataclass(frozen=True)
class MdSnap:
    ts: datetime
    bid: float
    ask: float
    bid_size: float
    ask_size: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) * 0.5


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    idx = int(round((len(sorted_values) - 1) * min(1.0, max(0.0, q))))
    return float(sorted_values[idx])


def _table_columns(engine: Engine, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return {str(r[1]) for r in rows}


def _load_short_round_trips(engine: Engine) -> list[ShortRoundTrip]:
    q = text(
        """
        SELECT round_trip_id, symbol, entry_ts, total_pnl, mfe, immediate_move
        FROM round_trip_analytics
        WHERE side = 'SHORT'
          AND entry_ts IS NOT NULL AND entry_ts <> ''
          AND exit_ts IS NOT NULL AND exit_ts <> ''
        ORDER BY entry_ts
        """
    )
    out: list[ShortRoundTrip] = []
    with engine.connect() as conn:
        for row in conn.execute(q):
            out.append(
                ShortRoundTrip(
                    round_trip_id=str(row[0]),
                    symbol=str(row[1]),
                    entry_ts=str(row[2]),
                    pnl=float(row[3] or 0.0),
                    mfe=float(row[4] or 0.0),
                    immediate_move=float(row[5] or 0.0),
                )
            )
    return out


def _load_market_cache(engine: Engine) -> dict[str, tuple[list[datetime], list[MdSnap]]]:
    cols = _table_columns(engine, "market_data")
    has_bid_size = "bid_size" in cols
    has_ask_size = "ask_size" in cols
    q = text(
        f"""
        SELECT symbol, ts, bid, ask
        {", bid_size" if has_bid_size else ""}
        {", ask_size" if has_ask_size else ""}
        FROM market_data
        ORDER BY symbol, ts
        """
    )
    out: dict[str, tuple[list[datetime], list[MdSnap]]] = {}
    with engine.connect() as conn:
        for row in conn.execute(q):
            symbol = str(row[0])
            ts = _parse_iso(str(row[1]))
            bid = float(row[2])
            ask = float(row[3])
            bid_size = float(row[4]) if has_bid_size and row[4] is not None else 0.0
            ask_size = float(row[5]) if has_ask_size and row[5] is not None else 0.0
            if symbol not in out:
                out[symbol] = ([], [])
            times, snaps = out[symbol]
            times.append(ts)
            snaps.append(MdSnap(ts=ts, bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size))
    return out


def _snap_at_or_before(
    cache: dict[str, tuple[list[datetime], list[MdSnap]]],
    *,
    symbol: str,
    ts: datetime,
) -> MdSnap | None:
    pair = cache.get(symbol)
    if pair is None:
        return None
    times, snaps = pair
    idx = bisect_right(times, ts) - 1
    if idx < 0:
        return None
    return snaps[idx]


def _load_entry_decision_cache(engine: Engine) -> tuple[
    dict[str, tuple[list[datetime], list[float]]],
    dict[str, tuple[list[datetime], list[float]]],
]:
    cols = _table_columns(engine, "entry_decisions")
    micro_col = "microprice_edge" if "microprice_edge" in cols else ("imbalance_score" if "imbalance_score" in cols else "")
    mom_col = "momentum_100ms" if "momentum_100ms" in cols else ("trend_score" if "trend_score" in cols else "")
    if not micro_col and not mom_col:
        return {}, {}
    select_cols = ["symbol", "timestamp"]
    if micro_col:
        select_cols.append(micro_col)
    if mom_col:
        select_cols.append(mom_col)
    q = text(
        f"""
        SELECT {", ".join(select_cols)}
        FROM entry_decisions
        WHERE side='SELL' AND timestamp IS NOT NULL AND timestamp <> ''
        ORDER BY symbol, timestamp
        """
    )
    micro_cache: dict[str, tuple[list[datetime], list[float]]] = {}
    mom_cache: dict[str, tuple[list[datetime], list[float]]] = {}
    with engine.connect() as conn:
        for row in conn.execute(q):
            symbol = str(row[0])
            ts = _parse_iso(str(row[1]))
            offset = 2
            micro_val = float(row[offset]) if micro_col else None
            mom_val = float(row[offset + (1 if micro_col else 0)]) if mom_col else None
            if micro_val is not None:
                if symbol not in micro_cache:
                    micro_cache[symbol] = ([], [])
                times, vals = micro_cache[symbol]
                times.append(ts)
                vals.append(micro_val)
            if mom_val is not None:
                if symbol not in mom_cache:
                    mom_cache[symbol] = ([], [])
                times, vals = mom_cache[symbol]
                times.append(ts)
                vals.append(mom_val)
    return micro_cache, mom_cache


def _value_at_or_before(cache: dict[str, tuple[list[datetime], list[float]]], symbol: str, ts: datetime) -> float | None:
    pair = cache.get(symbol)
    if pair is None:
        return None
    times, vals = pair
    idx = bisect_right(times, ts) - 1
    if idx < 0:
        return None
    return float(vals[idx])


def _micro_bin(value: float, q20: float, q40: float, q60: float, q80: float) -> str:
    if value <= q20:
        return "q1_lowest"
    if value <= q40:
        return "q2"
    if value <= q60:
        return "q3"
    if value <= q80:
        return "q4"
    return "q5_highest"


def _momentum_bucket(value: float, weak_cutoff: float) -> str:
    if value < 0.0:
        return "negative"
    if value <= weak_cutoff:
        return "weak"
    return "strong"


def main() -> None:
    p = argparse.ArgumentParser(description="Analyze SHORT-only bins by microprice_edge and momentum_100ms")
    p.add_argument("--economics-db", type=Path, default=Path("fix_engine/e2e_economics.db"))
    p.add_argument("--market-db", type=Path, default=Path("fix_engine/e2e_validation.db"))
    args = p.parse_args()

    econ = create_engine(f"sqlite:///{args.economics_db}")
    market = create_engine(f"sqlite:///{args.market_db}")

    trades = _load_short_round_trips(econ)
    if not trades:
        print("No SHORT round-trips found.")
        return

    md_cache = _load_market_cache(market)
    micro_cache, mom_cache = _load_entry_decision_cache(econ)

    rows: list[dict[str, float | str]] = []
    for t in trades:
        t0 = _parse_iso(t.entry_ts)
        s0 = _snap_at_or_before(md_cache, symbol=t.symbol, ts=t0)
        s100 = _snap_at_or_before(md_cache, symbol=t.symbol, ts=t0 - timedelta(milliseconds=100))

        micro = None
        mom100 = None
        if s0 is not None:
            tot = s0.bid_size + s0.ask_size
            if tot > 0:
                microprice = (s0.bid * s0.ask_size + s0.ask * s0.bid_size) / tot
                micro = float(microprice - s0.mid)
            if s100 is not None:
                mom100 = float(s0.mid - s100.mid)
        if micro is None:
            micro = _value_at_or_before(micro_cache, t.symbol, t0)
        if mom100 is None:
            mom100 = _value_at_or_before(mom_cache, t.symbol, t0)
        if micro is None or mom100 is None:
            continue
        rows.append(
            {
                "microprice_edge": float(micro),
                "momentum_100ms": float(mom100),
                "pnl": float(t.pnl),
                "mfe": float(t.mfe),
                "adverse_flag": 1.0 if float(t.immediate_move) < 0.0 else 0.0,
            }
        )

    if not rows:
        print("No SHORT trades with reconstructable microprice_edge+momentum_100ms.")
        return

    micro_sorted = sorted(float(r["microprice_edge"]) for r in rows)
    q20 = _quantile(micro_sorted, 0.20)
    q40 = _quantile(micro_sorted, 0.40)
    q60 = _quantile(micro_sorted, 0.60)
    q80 = _quantile(micro_sorted, 0.80)
    mom_pos = sorted(max(0.0, float(r["momentum_100ms"])) for r in rows)
    weak_cutoff = _quantile(mom_pos, 0.66)

    agg: dict[str, dict[str, float]] = {}
    for r in rows:
        mb = _micro_bin(float(r["microprice_edge"]), q20, q40, q60, q80)
        tb = _momentum_bucket(float(r["momentum_100ms"]), weak_cutoff)
        key = f"{mb}|{tb}"
        if key not in agg:
            agg[key] = {"count": 0.0, "pnl_sum": 0.0, "mfe_sum": 0.0, "adverse_sum": 0.0}
        a = agg[key]
        a["count"] += 1.0
        a["pnl_sum"] += float(r["pnl"])
        a["mfe_sum"] += float(r["mfe"])
        a["adverse_sum"] += float(r["adverse_flag"])

    baseline = {
        "avg_pnl": sum(float(r["pnl"]) for r in rows) / float(len(rows)),
        "avg_mfe": sum(float(r["mfe"]) for r in rows) / float(len(rows)),
        "adverse_rate": sum(float(r["adverse_flag"]) for r in rows) / float(len(rows)),
    }

    print("bin | count | avg_pnl | avg_mfe | adverse_rate")
    print("-" * 64)
    any_improved = False
    for mb in ("q1_lowest", "q2", "q3", "q4", "q5_highest"):
        for tb in ("negative", "weak", "strong"):
            key = f"{mb}|{tb}"
            a = agg.get(key)
            if a is None or a["count"] <= 0:
                print(f"{key} | 0 | 0.000000 | 0.000000 | 0.00%")
                continue
            n = int(a["count"])
            avg_pnl = a["pnl_sum"] / float(n)
            avg_mfe = a["mfe_sum"] / float(n)
            adverse_rate = a["adverse_sum"] / float(n)
            print(f"{key} | {n} | {avg_pnl:.6f} | {avg_mfe:.6f} | {adverse_rate * 100.0:.2f}%")
            if avg_pnl > baseline["avg_pnl"] and avg_mfe > baseline["avg_mfe"] and adverse_rate < baseline["adverse_rate"]:
                any_improved = True

    print("")
    print(
        f"baseline_short avg_pnl={baseline['avg_pnl']:.6f} avg_mfe={baseline['avg_mfe']:.6f} adverse_rate={baseline['adverse_rate']*100.0:.2f}% n={len(rows)}"
    )
    if any_improved:
        print("VERDICT: at least one SHORT bin shows improvement (potential edge subset).")
    else:
        print("VERDICT: no bin shows improvement -> no edge exists.")


if __name__ == "__main__":
    main()
