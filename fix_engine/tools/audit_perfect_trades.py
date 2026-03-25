from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text


@dataclass(frozen=True)
class RoundTrip:
    round_trip_id: str
    symbol: str
    side: str
    entry_ts: str
    exit_ts: str
    entry_price: float
    duration_ms: float


def _parse_iso(ts: str) -> datetime:
    value = str(ts).strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_round_trips(econ_db: Path) -> list[RoundTrip]:
    engine = create_engine(f"sqlite:///{econ_db.as_posix()}")
    query = text(
        """
        SELECT
            round_trip_id,
            symbol,
            side,
            entry_ts,
            exit_ts,
            entry_price,
            duration_ms
        FROM round_trip_analytics
        WHERE
            entry_ts IS NOT NULL AND entry_ts <> ''
            AND exit_ts IS NOT NULL AND exit_ts <> ''
        ORDER BY entry_ts ASC
        """
    )
    rows: list[RoundTrip] = []
    with engine.connect() as conn:
        for rec in conn.execute(query):
            rows.append(
                RoundTrip(
                    round_trip_id=str(rec[0]),
                    symbol=str(rec[1]),
                    side=str(rec[2]).upper(),
                    entry_ts=str(rec[3]),
                    exit_ts=str(rec[4]),
                    entry_price=_to_float(rec[5]),
                    duration_ms=_to_float(rec[6]),
                )
            )
    return rows


def _load_price_path(validation_db: Path, *, symbol: str, entry_ts: str, exit_ts: str) -> list[dict[str, Any]]:
    engine = create_engine(f"sqlite:///{validation_db.as_posix()}")
    query = text(
        """
        SELECT ts, bid, ask
        FROM market_data
        WHERE symbol = :symbol
          AND ts >= :entry_ts
          AND ts <= :exit_ts
        ORDER BY ts ASC
        """
    )
    path: list[dict[str, Any]] = []
    with engine.connect() as conn:
        for rec in conn.execute(query, {"symbol": symbol, "entry_ts": entry_ts, "exit_ts": exit_ts}):
            bid = _to_float(rec[1], default=0.0)
            ask = _to_float(rec[2], default=0.0)
            mid = (bid + ask) * 0.5
            path.append({"ts": str(rec[0]), "mid": mid})
    return path


def _compute_mae_mfe(*, side: str, entry_price: float, mids: list[float]) -> tuple[float, float]:
    if not mids:
        return 0.0, 0.0
    min_mid = min(mids)
    max_mid = max(mids)
    if side == "LONG":
        mae = max(0.0, entry_price - min_mid)
        mfe = max(0.0, max_mid - entry_price)
        return mae, mfe
    # SHORT
    mae = max(0.0, max_mid - entry_price)
    mfe = max(0.0, entry_price - min_mid)
    return mae, mfe


def run_audit(
    *,
    econ_db: Path,
    validation_db: Path,
    output_path: Path | None,
) -> dict[str, Any]:
    round_trips = _load_round_trips(econ_db)
    audit_rows: list[dict[str, Any]] = []
    zero_mae_count = 0
    suspicious_zero_mae_mfe_count = 0
    sum_mfe = 0.0
    sum_duration = 0.0

    for rt in round_trips:
        price_path = _load_price_path(
            validation_db,
            symbol=rt.symbol,
            entry_ts=rt.entry_ts,
            exit_ts=rt.exit_ts,
        )
        mids = [_to_float(x.get("mid"), default=0.0) for x in price_path]
        mae, mfe = _compute_mae_mfe(side=rt.side, entry_price=rt.entry_price, mids=mids)
        if abs(mae) <= 1e-12:
            zero_mae_count += 1
        if abs(mae) <= 1e-12 and mfe > 1.0:
            suspicious_zero_mae_mfe_count += 1
        sum_mfe += mfe
        sum_duration += rt.duration_ms
        audit_rows.append(
            {
                "round_trip_id": rt.round_trip_id,
                "symbol": rt.symbol,
                "side": rt.side,
                "entry_ts": rt.entry_ts,
                "exit_ts": rt.exit_ts,
                "entry_price": rt.entry_price,
                "duration_ms": rt.duration_ms,
                "mae_recomputed": mae,
                "mfe_recomputed": mfe,
                "price_path": price_path,
            }
        )

    total = len(audit_rows)
    zero_mae_trades_pct = (100.0 * zero_mae_count / total) if total > 0 else 0.0
    suspicious_pct = (100.0 * suspicious_zero_mae_mfe_count / total) if total > 0 else 0.0
    avg_mfe = (sum_mfe / total) if total > 0 else 0.0
    avg_duration = (sum_duration / total) if total > 0 else 0.0

    summary = {
        "total_round_trips": total,
        "mae_eq_0_count": zero_mae_count,
        "mae_eq_0_and_mfe_gt_1_count": suspicious_zero_mae_mfe_count,
        "zero_mae_trades_pct": zero_mae_trades_pct,
        "avg_mfe": avg_mfe,
        "avg_duration": avg_duration,
        "suspicious": suspicious_pct > 50.0,
        "suspicious_pct": suspicious_pct,
    }
    payload = {"summary": summary, "trades": audit_rows}

    if output_path is not None:
        output_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit perfect-trade patterns using full price paths.")
    parser.add_argument(
        "--econ-db",
        default="fix_engine/e2e_economics.db",
        help="Path to e2e_economics.db",
    )
    parser.add_argument(
        "--validation-db",
        default="fix_engine/e2e_validation.db",
        help="Path to e2e_validation.db",
    )
    parser.add_argument(
        "--output",
        default="perfect_trade_audit.json",
        help="Output JSON with per-round-trip paths and recomputed MAE/MFE",
    )
    args = parser.parse_args()

    summary = run_audit(
        econ_db=Path(args.econ_db),
        validation_db=Path(args.validation_db),
        output_path=Path(args.output) if args.output else None,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()

