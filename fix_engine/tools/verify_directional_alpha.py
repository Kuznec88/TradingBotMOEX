from __future__ import annotations

import argparse
import math
from pathlib import Path
from statistics import median

from sqlalchemy import create_engine, text


def _pearson_corr(xs: list[float], ys: list[float]) -> float:
    n = min(len(xs), len(ys))
    if n < 2:
        return 0.0
    x = xs[:n]
    y = ys[:n]
    mx = sum(x) / n
    my = sum(y) / n
    num = sum((a - mx) * (b - my) for a, b in zip(x, y))
    den_x = sum((a - mx) ** 2 for a in x)
    den_y = sum((b - my) ** 2 for b in y)
    den = math.sqrt(den_x * den_y)
    if den <= 1e-12:
        return 0.0
    return num / den


def main() -> None:
    p = argparse.ArgumentParser(description="Verify directional alpha via MFE/PnL linkage.")
    p.add_argument("--db", type=Path, default=Path("fix_engine/e2e_economics.db"))
    p.add_argument(
        "--near-zero-mfe-threshold",
        type=float,
        default=0.05,
        help="Mean MFE threshold treated as ~0.",
    )
    p.add_argument(
        "--near-zero-corr-threshold",
        type=float,
        default=0.10,
        help="Absolute correlation threshold treated as ~0.",
    )
    args = p.parse_args()

    engine = create_engine(f"sqlite:///{args.db}")
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT total_pnl, mfe
                FROM round_trip_analytics
                WHERE entry_ts IS NOT NULL AND entry_ts <> ''
                  AND exit_ts IS NOT NULL AND exit_ts <> ''
                """
            )
        ).fetchall()

    if not rows:
        print("No round-trip rows found.")
        return

    pnls = [float(r[0] or 0.0) for r in rows]
    mfes = [float(r[1] or 0.0) for r in rows]
    eff = [p / m for p, m in zip(pnls, mfes) if m > 0.0]

    corr = _pearson_corr(mfes, pnls)
    mean_mfe = sum(mfes) / len(mfes)
    mean_pnl = sum(pnls) / len(pnls)
    mfe_cut = median(mfes)
    high_mfe_pnls = [p for p, m in zip(pnls, mfes) if m >= mfe_cut]
    low_mfe_pnls = [p for p, m in zip(pnls, mfes) if m < mfe_cut]
    avg_pnl_high_mfe = (sum(high_mfe_pnls) / len(high_mfe_pnls)) if high_mfe_pnls else 0.0
    avg_pnl_low_mfe = (sum(low_mfe_pnls) / len(low_mfe_pnls)) if low_mfe_pnls else 0.0
    mean_eff = (sum(eff) / len(eff)) if eff else 0.0
    med_eff = median(eff) if eff else 0.0
    share_pos_eff = (sum(1 for v in eff if v > 0.0) / len(eff) * 100.0) if eff else 0.0
    mfe_nonpos = sum(1 for m in mfes if m <= 0.0)

    no_alpha = (abs(mean_mfe) <= float(args.near_zero_mfe_threshold)) and (
        abs(corr) <= float(args.near_zero_corr_threshold)
    )

    print("=== Directional Alpha Verification ===")
    print(f"round_trips={len(rows)}")
    print(f"mean_mfe={mean_mfe:.6f}")
    print(f"mean_pnl={mean_pnl:.6f}")
    print(f"corr(mfe,pnl)={corr:.6f}")
    print(f"mfe_split_cut_median={mfe_cut:.6f}")
    print(f"avg_pnl_high_mfe={avg_pnl_high_mfe:.6f}")
    print(f"avg_pnl_low_mfe={avg_pnl_low_mfe:.6f}")
    print(f"directional_efficiency_count(mfe>0)={len(eff)}")
    print(f"directional_efficiency_mean={mean_eff:.6f}")
    print(f"directional_efficiency_median={med_eff:.6f}")
    print(f"directional_efficiency_positive_share={share_pos_eff:.2f}%")
    print(f"mfe<=0_count={mfe_nonpos}")
    print("")
    if no_alpha:
        print("Conclusion: Strategy has no directional alpha (mfe≈0 and corr≈0).")
    else:
        if corr > 0.5:
            print("Conclusion: Strong directional edge detected (corr>0.5).")
        else:
            print("Conclusion: Directional linkage exists, but strong edge threshold not reached.")
    print("")
    summary = {
        "correlation_mfe_pnl": float(corr),
        "avg_pnl_high_mfe": float(avg_pnl_high_mfe),
        "avg_pnl_low_mfe": float(avg_pnl_low_mfe),
    }
    print(summary)


if __name__ == "__main__":
    main()
