from __future__ import annotations

import argparse
import json
from pathlib import Path

from sqlalchemy import create_engine, text


def run_diagnosis(db_path: Path, mfe_threshold: float = 0.5) -> dict[str, object]:
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        total_round_trips = int(
            conn.execute(text("SELECT COUNT(1) FROM round_trip_analytics")).scalar() or 0
        )
        bad_exit_count = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(1)
                    FROM round_trip_analytics
                    WHERE mfe > :mfe_thr AND total_pnl < 0
                    """
                ),
                {"mfe_thr": float(mfe_threshold)},
            ).scalar()
            or 0
        )
        stats_row = conn.execute(
            text(
                """
                SELECT AVG(mfe), AVG(total_pnl)
                FROM round_trip_analytics
                WHERE mfe > :mfe_thr AND total_pnl < 0
                """
            ),
            {"mfe_thr": float(mfe_threshold)},
        ).fetchone()
        avg_mfe = float(stats_row[0]) if stats_row and stats_row[0] is not None else 0.0
        avg_pnl = float(stats_row[1]) if stats_row and stats_row[1] is not None else 0.0
        examples_rows = conn.execute(
            text(
                """
                SELECT round_trip_id, mfe, total_pnl, duration_ms
                FROM round_trip_analytics
                WHERE mfe > :mfe_thr AND total_pnl < 0
                ORDER BY mfe DESC, total_pnl ASC
                LIMIT 5
                """
            ),
            {"mfe_thr": float(mfe_threshold)},
        ).fetchall()

    pct = (100.0 * float(bad_exit_count) / float(total_round_trips)) if total_round_trips > 0 else 0.0
    examples = [
        {
            "round_trip_id": str(r[0]),
            "mfe": float(r[1]),
            "pnl": float(r[2]),
            "duration_ms": float(r[3]),
        }
        for r in examples_rows
    ]
    verdict = (
        "EXIT LOGIC BROKEN: fix exits before entry optimization"
        if pct > 20.0
        else "Exit inefficiency present but below critical threshold"
    )
    return {
        "mfe_threshold": float(mfe_threshold),
        "total_round_trips": total_round_trips,
        "bad_exit_count": bad_exit_count,
        "bad_exit_pct": pct,
        "avg_mfe_bad_exit": avg_mfe,
        "avg_pnl_bad_exit": avg_pnl,
        "examples_top5": examples,
        "verdict": verdict,
    }


def print_report(result: dict[str, object]) -> None:
    print("=== Exit Inefficiency Diagnosis ===")
    print(f"mfe_threshold: {result['mfe_threshold']}")
    print(f"total_round_trips: {result['total_round_trips']}")
    print(f"count(mfe>thr & pnl<0): {result['bad_exit_count']}")
    print(f"percentage_of_all: {float(result['bad_exit_pct']):.2f}%")
    print(f"avg_mfe (flagged): {float(result['avg_mfe_bad_exit']):.6f}")
    print(f"avg_pnl (flagged): {float(result['avg_pnl_bad_exit']):.6f}")
    print("")
    print("Top 5 examples:")
    for row in result["examples_top5"]:
        item = dict(row)  # type: ignore[arg-type]
        print(json.dumps(item, ensure_ascii=False))
    print("")
    print(f"Verdict: {result['verdict']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose exit inefficiency from round-trip analytics.")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("fix_engine/e2e_economics.db"),
        help="Path to economics DB with round_trip_analytics table.",
    )
    parser.add_argument(
        "--mfe-threshold",
        type=float,
        default=0.5,
        help="MFE threshold for inefficiency condition.",
    )
    args = parser.parse_args()
    result = run_diagnosis(db_path=args.db, mfe_threshold=float(args.mfe_threshold))
    print_report(result)


if __name__ == "__main__":
    main()
