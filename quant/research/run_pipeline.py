"""
CLI: python -m quant.research.run_pipeline --csv ...
     python -m cli.research ...
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

from quant.core.constants import DEFAULT_PLANNED_CONTRACTS, MIN_TRADES_RELIABLE
from quant.core.logging_setup import configure_logging
from quant.data.pipeline import run_research_backtest
from quant.data.trade_dataset import discover_ohlcv_csvs
from quant.models.edge_models import fit_models, model_metrics_to_jsonable
from quant.research.ablation import run_ablation_study
from quant.research.bootstrap import bootstrap_pnl_distribution
from quant.research.edge_gate import edge_hypothesis_accepted
from quant.research.factor_stats import build_factor_analysis_tables, default_feature_columns
from quant.research.input_paths import resolve_csv_inputs
from quant.research.cross_instrument import build_cross_instrument_factor_table
from quant.research.interactions import generate_pairwise_interactions, generate_triple_interactions
from quant.research.pnl_metrics import (
    aggregate_sample_metrics,
    expectancy_decomposition,
    extrapolate_6m_pnl_rub,
    holdout_metrics,
    sizing_scale_for_6m_target_rub,
)
from quant.research.walk_forward import chronological_trade_bucket_metrics
from quant.research.reconstruct import reevaluate_with_weights, weights_from_rf_importance

log = logging.getLogger(__name__)


def discover_ohlcv_csvs_public(directory: Path) -> list[Path]:
    return discover_ohlcv_csvs(directory)


def _batch_summary_display_table(batch_df: pd.DataFrame) -> pd.DataFrame:
    d = batch_df.copy()
    if "sum_pnl" in d.columns and "n_trades" in d.columns:
        nt = pd.to_numeric(d["n_trades"], errors="coerce")
        sp = pd.to_numeric(d["sum_pnl"], errors="coerce")
        d["avg_pnl_rub"] = (sp / nt.replace(0, float("nan"))).round(2)
    if "winrate" in d.columns:
        d["winrate_pct"] = (pd.to_numeric(d["winrate"], errors="coerce") * 100).round(1)
    preferred = [
        "frame",
        "n_trades",
        "sum_pnl",
        "avg_pnl_rub",
        "winrate_pct",
        "implied_qty_multiplier_6m_target",
        "reliable",
        "edge_gate",
        "projected_6m_pnl_rub",
        "projected_6m_account_rub",
        "planned_contracts",
        "model_ok",
        "n_interactions_passing_filter",
        "n_interactions_pairwise",
        "n_interactions_triple",
        "reconstruction_ran",
        "error",
    ]
    cols = [c for c in preferred if c in d.columns]
    return d[cols]


def _effective_target_per_contract_6m_rub(
    *,
    target_6m_account_rub: float,
    planned_contracts: float,
    target_6m_per_contract_rub: float | None,
) -> float:
    """PnL is RUB per 1 contract; optional explicit per-contract target overrides account/planned split."""
    if target_6m_per_contract_rub is not None and float(target_6m_per_contract_rub) > 0:
        return float(target_6m_per_contract_rub)
    if target_6m_account_rub and float(target_6m_account_rub) > 0:
        return float(target_6m_account_rub) / max(float(planned_contracts), 1.0)
    return 0.0


def _build_pnl_estimate_bundle(
    trades_df: pd.DataFrame,
    *,
    target_6m_account_rub: float = 0.0,
    planned_contracts: float = DEFAULT_PLANNED_CONTRACTS,
    target_6m_per_contract_rub: float | None = None,
    walk_forward_buckets: int = 0,
) -> dict[str, Any]:
    pc = max(float(planned_contracts), 1.0)
    if len(trades_df) < 2:
        out: dict[str, Any] = {"note": "too_few_trades", "sample": aggregate_sample_metrics(trades_df)}
        eff = _effective_target_per_contract_6m_rub(
            target_6m_account_rub=target_6m_account_rub,
            planned_contracts=pc,
            target_6m_per_contract_rub=target_6m_per_contract_rub,
        )
        if eff > 0:
            out["target_6m_sizing"] = sizing_scale_for_6m_target_rub(
                trades_df,
                target_6m_pnl_rub=float(eff),
                planned_contracts=pc,
            )
        return out
    agg = aggregate_sample_metrics(trades_df)
    p = trades_df["pnl"].astype(float).values
    dec = expectancy_decomposition(p)
    ex = extrapolate_6m_pnl_rub(trades_df, expectancy_rub=float(agg["expectancy"]) if agg.get("expectancy") is not None else None)
    proj = ex.get("projected_6m_pnl_rub")
    if proj is not None:
        ex = {
            **ex,
            "projected_6m_account_rub_at_planned_contracts": float(proj) * pc,
            "planned_contracts_for_account_view": pc,
        }
    ho = holdout_metrics(trades_df, train_frac=0.7)
    boot = bootstrap_pnl_distribution(p, n_bootstrap=2000, random_state=42)
    bundle: dict[str, Any] = {
        "pnl_convention": {
            "unit": "rub_per_contract",
            "backtest_qty": 1.0,
            "planned_contracts_for_account_view": pc,
        },
        "sample": agg,
        "expectancy_decomposition": dec,
        "extrapolation_6m": ex,
        "holdout_train_test": ho,
        "bootstrap": boot,
    }
    eff = _effective_target_per_contract_6m_rub(
        target_6m_account_rub=target_6m_account_rub,
        planned_contracts=pc,
        target_6m_per_contract_rub=target_6m_per_contract_rub,
    )
    if eff > 0:
        bundle["target_6m_sizing"] = sizing_scale_for_6m_target_rub(
            trades_df,
            target_6m_pnl_rub=float(eff),
            projected_6m_pnl_rub=float(proj) if proj is not None else None,
            planned_contracts=pc,
        )
    if walk_forward_buckets and int(walk_forward_buckets) >= 2:
        bundle["walk_forward_trade_buckets"] = chronological_trade_bucket_metrics(
            trades_df,
            n_buckets=int(walk_forward_buckets),
        )
    return bundle


def _write_conclusion(
    path: Path,
    *,
    n: int,
    edge: bool,
    unreliable: bool,
    pnl_bundle: dict[str, Any],
    edge_detail: dict[str, Any] | None,
) -> None:
    lines = [
        "FACTOR RESEARCH — CONCLUSION",
        "===========================",
        "",
        f"Trade count: {n}",
        f"Reliable (n >= {MIN_TRADES_RELIABLE}): {'yes' if not unreliable else 'NO — results marked unreliable'}",
        "",
    ]
    ex = pnl_bundle.get("extrapolation_6m") or {}
    if ex.get("projected_6m_pnl_rub") is not None:
        lines.append(
            f"Rough 6M PnL per 1 contract (NOT a target): {ex['projected_6m_pnl_rub']:.2f} RUB"
        )
        if ex.get("projected_6m_account_rub_at_planned_contracts") is not None:
            lines.append(
                f"  → at planned size ×{float(ex.get('planned_contracts_for_account_view', ex.get('planned_contracts', 1))):.0f}: "
                f"~{ex['projected_6m_account_rub_at_planned_contracts']:.2f} RUB / 6M on account (linear scale)."
            )
        lines.append("(Expectancy × estimated frequency over 6 months; not optimized.)")
        lines.append("")
    ts = pnl_bundle.get("target_6m_sizing") or {}
    m = ts.get("implied_qty_multiplier")
    t1 = ts.get("target_6m_per_contract_rub")
    if m is not None and t1 is not None:
        tac = ts.get("target_6m_account_rub")
        lines.append(
            f"Vs target {float(t1):.0f} RUB / 6M per contract "
            f"(account goal {float(tac):.0f} RUB if planned ×{float(ts.get('planned_contracts', 1)):.0f}): "
            f"implied qty multiplier vs research qty=1 ≈ {m:.2f}x (only if edge > 0 and linear scaling holds)."
        )
        lines.append("")
    elif ts.get("note") == "negative_or_zero_edge_scaling_wont_fix":
        lines.append(
            "Business target sizing: projected 6M edge ≤ 0 — hitting a positive RUB goal requires "
            "better expectancy, not larger size."
        )
        lines.append("")
    sm = pnl_bundle.get("sample") or {}
    if sm.get("pnl_wo_top1") is not None:
        lines.append(f"PnL without top-1 trade (sample): {sm['pnl_wo_top1']:.2f} RUB")
        lines.append("")
    if edge_detail and edge_detail.get("reasons"):
        lines.append(f"Edge gate detail: {', '.join(edge_detail['reasons'])}")
        lines.append("")
    if unreliable or n < 15:
        lines.append("No statistical edge can be claimed on this sample size.")
    elif not edge:
        lines.append("CONCLUSION: No edge under PnL-aware gates (expectancy, PF, pnl_wo_top1, evidence).")
        lines.append("This is a valid falsification outcome — do not tune thresholds to force a pass.")
    else:
        lines.append("CONCLUSION: Edge gate passed — validate out-of-sample before production.")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


@dataclass
class PipelineArgs:
    """Research pipeline. PnL columns are RUB per 1 contract; account goal is split by planned_contracts."""

    min_score: float
    q_bins: int
    interaction_min_trades: int
    score_filter: bool = True
    enable_continuation: bool = False
    # 6M goal on the whole account (RUB); per-contract target = account / planned_contracts
    target_6m_account_rub: float = 40_000.0
    planned_contracts: float = float(DEFAULT_PLANNED_CONTRACTS)
    # If set, overrides account/planned for sizing only
    target_6m_per_contract_rub: float | None = None
    # edge_hypothesis_accepted(min_pf=...); default 1.2 matches edge_gate.py; use 1.3 for stricter checks
    edge_min_pf: float = 1.2
    # 0 = off; >=2 chronological trade buckets in pnl_estimate (see docs/walk_forward_spec.md)
    walk_forward_buckets: int = 0


def run_pipeline_single(
    csv_path: Path,
    out: Path,
    *,
    instrument: str,
    pa: PipelineArgs,
) -> dict[str, Any]:
    out.mkdir(parents=True, exist_ok=True)
    log.info("Backtest + enrich: %s", csv_path)
    trades_df, _res, _edf, _p = run_research_backtest(
        csv_path,
        instrument=instrument,
        score_filter=pa.score_filter,
        min_score=pa.min_score,
        enable_continuation=pa.enable_continuation,
    )
    trades_df.to_csv(out / "trades_dataset.csv", index=False)

    n = len(trades_df)
    unreliable = n < MIN_TRADES_RELIABLE
    if unreliable:
        log.warning(
            "Small sample: n=%d is below %d; factor stats and edge gate are unreliable",
            n,
            MIN_TRADES_RELIABLE,
        )

    feat_cols = default_feature_columns(trades_df)
    log.info("Factor analysis (%d features, n=%d trades)", len(feat_cols), n)
    fa_df, summary, bin_csvs = build_factor_analysis_tables(trades_df, q_bins=pa.q_bins, feature_cols=feat_cols)
    fa_df.to_csv(out / "factor_analysis.csv", index=False)

    if bin_csvs:
        long_bins = pd.concat(bin_csvs.values(), ignore_index=True)
        long_bins.to_csv(out / "factor_bins_detail.csv", index=False)

    summary["instrument"] = instrument
    summary["csv"] = str(csv_path.resolve())
    (out / "factor_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    abl = run_ablation_study(str(csv_path), min_score=pa.min_score)
    abl.to_csv(out / "ablation_results.csv", index=False)

    inter = generate_pairwise_interactions(
        trades_df,
        feat_cols,
        min_trades=pa.interaction_min_trades,
    )
    inter.to_csv(out / "interactions.csv", index=False)

    inter3 = generate_triple_interactions(
        trades_df,
        feat_cols,
        min_trades=pa.interaction_min_trades,
    )
    inter3.to_csv(out / "interactions_triple.csv", index=False)

    ml = fit_models(trades_df, feat_cols)
    (out / "model_metrics.json").write_text(
        json.dumps(model_metrics_to_jsonable(ml), indent=2, default=str),
        encoding="utf-8",
    )

    pnl_bundle = _build_pnl_estimate_bundle(
        trades_df,
        target_6m_account_rub=float(pa.target_6m_account_rub or 0.0),
        planned_contracts=float(pa.planned_contracts or DEFAULT_PLANNED_CONTRACTS),
        target_6m_per_contract_rub=pa.target_6m_per_contract_rub,
        walk_forward_buckets=int(pa.walk_forward_buckets or 0),
    )
    (out / "pnl_estimate.json").write_text(json.dumps(pnl_bundle, indent=2, default=str), encoding="utf-8")

    n_inter_qualifying = len(inter) + len(inter3)
    edge_ok, edge_detail = edge_hypothesis_accepted(
        trades_df,
        fa_df,
        interaction_hits=n_inter_qualifying,
        min_pf=float(pa.edge_min_pf),
    )
    edge = edge_ok and not unreliable
    (out / "edge_gate_detail.json").write_text(json.dumps(edge_detail, indent=2, default=str), encoding="utf-8")

    recon: dict[str, Any] = {"ran": False, "reason": "gate_failed_or_unreliable"}
    if edge and ml.get("ok"):
        imp = ml.get("feature_importance", {}).get("random_forest_classifier") or {}
        raw_imp = {k: v for k, v in imp.items() if k.startswith("raw_")}
        if len(raw_imp) >= 2:
            w = weights_from_rf_importance(raw_imp)
            recon["ran"] = True
            recon["weights_used"] = w.__dict__
            recon["reeval_topn0"] = reevaluate_with_weights(str(csv_path), w, min_score=pa.min_score, top_n_per_day=0)
            recon["reeval_top3"] = reevaluate_with_weights(
                str(csv_path), w, min_score=max(0.45, pa.min_score - 0.1), top_n_per_day=3
            )
            (out / "reconstruction_eval.json").write_text(json.dumps(recon, indent=2, default=str), encoding="utf-8")
        else:
            recon["reason"] = "insufficient_raw_factor_importance_for_signal_score_mapping"

    _write_conclusion(
        out / "final_conclusion.txt",
        n=n,
        edge=edge,
        unreliable=unreliable,
        pnl_bundle=pnl_bundle,
        edge_detail=edge_detail,
    )

    wins = int((trades_df["pnl"] > 0).sum()) if n else 0
    wr = wins / n if n else None
    exb = pnl_bundle.get("extrapolation_6m") or {}
    ex_proj = exb.get("projected_6m_pnl_rub")
    ex_acc = exb.get("projected_6m_account_rub_at_planned_contracts")
    ts = pnl_bundle.get("target_6m_sizing") or {}
    conv = pnl_bundle.get("pnl_convention") or {}
    return {
        "frame": csv_path.name,
        "instrument": instrument,
        "n_trades": n,
        "reliable": not unreliable,
        "edge_gate": edge,
        "sum_pnl": float(trades_df["pnl"].sum()) if n else 0.0,
        "winrate": float(wr) if wr is not None else None,
        "n_interactions_pairwise": len(inter),
        "n_interactions_triple": len(inter3),
        "n_interactions_passing_filter": n_inter_qualifying,
        "model_ok": bool(ml.get("ok")),
        "reconstruction_ran": bool(recon.get("ran")),
        "projected_6m_pnl_rub": ex_proj,
        "projected_6m_account_rub": ex_acc,
        "planned_contracts": conv.get("planned_contracts_for_account_view"),
        "implied_qty_multiplier_6m_target": ts.get("implied_qty_multiplier"),
        "out_dir": str(out.resolve()),
    }


def _resolve_run_paths(args: argparse.Namespace) -> list[tuple[Path, str]]:
    if getattr(args, "csv", None):
        p = Path(args.csv).resolve()
        inst = (args.instrument or "").strip() or p.stem
        return [(p, inst)]
    if getattr(args, "manifest", None):
        return resolve_csv_inputs(manifest=args.manifest, root=_ROOT)
    bd = getattr(args, "batch_dir", None) or ""
    gl = getattr(args, "glob", None) or None
    if bd:
        return resolve_csv_inputs(batch_dir=bd, glob_pattern=gl, root=Path(bd))
    if gl:
        return resolve_csv_inputs(batch_dir=str(_ROOT), glob_pattern=gl, root=_ROOT)
    raise ValueError("Specify --csv, --manifest, --batch-dir, or --glob")


def main() -> int:
    configure_logging()
    ap = argparse.ArgumentParser(description="Quant factor research (edge discovery, falsification-first)")
    ap.add_argument("--csv", type=str, default="", help="Single OHLCV CSV")
    ap.add_argument("--batch-dir", type=str, default="", help="Directory with history_*.csv")
    ap.add_argument("--glob", type=str, default="", help="Glob under --batch-dir (e.g. history_1h*.csv)")
    ap.add_argument("--manifest", type=str, default="", help="YAML list of csv paths + instrument labels")
    ap.add_argument("--out-dir", type=str, default="research_output")
    ap.add_argument("--instrument", type=str, default="", help="Override instrument label for single --csv")
    ap.add_argument("--min-score", type=float, default=0.52)
    ap.add_argument("--q-bins", type=int, default=5)
    ap.add_argument("--interaction-min-trades", type=int, default=30)
    ap.add_argument(
        "--no-score-filter",
        action="store_true",
        help="Disable signal_score gate (all FSM/extra intents pass except FSM rules)",
    )
    ap.add_argument(
        "--enable-continuation",
        action="store_true",
        help="Allow continuation entries (riskier; off by default in research)",
    )
    ap.add_argument(
        "--target-6m-account-rub",
        type=float,
        default=40_000.0,
        help="6M PnL goal on the account (RUB); per-contract target = this / --planned-contracts. 0 disables sizing block.",
    )
    ap.add_argument(
        "--planned-contracts",
        type=float,
        default=float(DEFAULT_PLANNED_CONTRACTS),
        help="Planned position size (contracts) for account view; default 50 (1 PnL = 1 RUB per contract).",
    )
    ap.add_argument(
        "--target-6m-per-contract-rub",
        type=float,
        default=None,
        help="Optional: 6M goal per 1 contract (RUB); overrides --target-6m-account-rub / planned.",
    )
    ap.add_argument(
        "--edge-min-pf",
        type=float,
        default=1.2,
        help="Edge gate: minimum profit factor (full sample). Default 1.2; use 1.3 for stricter research targets.",
    )
    ap.add_argument(
        "--walk-forward-buckets",
        type=int,
        default=4,
        help="Chronological trade buckets in pnl_estimate (0=off). Default 4. See docs/walk_forward_spec.md.",
    )
    args = ap.parse_args()

    pa = PipelineArgs(
        min_score=args.min_score,
        q_bins=args.q_bins,
        interaction_min_trades=args.interaction_min_trades,
        score_filter=not args.no_score_filter,
        enable_continuation=bool(args.enable_continuation),
        target_6m_account_rub=float(args.target_6m_account_rub or 0.0),
        planned_contracts=float(args.planned_contracts or DEFAULT_PLANNED_CONTRACTS),
        target_6m_per_contract_rub=(
            float(args.target_6m_per_contract_rub)
            if getattr(args, "target_6m_per_contract_rub", None) is not None
            else None
        ),
        edge_min_pf=float(args.edge_min_pf),
        walk_forward_buckets=int(args.walk_forward_buckets or 0),
    )

    try:
        paths = _resolve_run_paths(args)
    except (FileNotFoundError, ValueError, RuntimeError) as e:
        log.error("%s", e)
        print(str(e), file=sys.stderr)
        return 1

    multi = len(paths) > 1
    if not multi and not args.batch_dir and not args.manifest and args.csv:
        # single file
        p, inst = paths[0]
        out = Path(args.out_dir)
        r = run_pipeline_single(p, out, instrument=inst, pa=pa)
        print(f"Wrote outputs to {out.resolve()}")
        print(f"Trades: {r['n_trades']} | reliable: {r['reliable']} | edge_gate: {r['edge_gate']}")
        return 0

    # batch: multiple paths from manifest/glob/batch-dir
    root = Path(args.out_dir)
    root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for p, inst in paths:
        sub = root / p.stem
        try:
            r = run_pipeline_single(p, sub, instrument=inst, pa=pa)
            rows.append(r)
        except Exception as e:
            log.exception("frame failed: %s", p)
            rows.append(
                {
                    "frame": p.name,
                    "instrument": inst,
                    "error": str(e),
                    "n_trades": None,
                    "reliable": False,
                    "edge_gate": False,
                }
            )
    batch_df = pd.DataFrame(rows)
    batch_df.to_csv(root / "batch_summary.csv", index=False)
    display_df = _batch_summary_display_table(batch_df)
    display_df.to_csv(root / "batch_summary_metrics.csv", index=False)
    with pd.option_context("display.max_columns", None, "display.width", 220, "display.max_colwidth", 40):
        print("\n=== Per-frame stats (PnL / winrate / edge) ===\n")
        print(display_df.to_string(index=False))
    xp = build_cross_instrument_factor_table(rows)
    if xp is not None and not xp.empty:
        xp_path = root / "batch_cross_instrument_factors.csv"
        xp.to_csv(xp_path, index=False)
        log.info("Wrote %s (%d factors × instruments)", xp_path.name, len(xp))
    print(f"\nWrote per-frame outputs under {root.resolve()} (batch_summary.csv, batch_summary_metrics.csv)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
