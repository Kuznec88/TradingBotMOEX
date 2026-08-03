# TradingBotMOEX — Quantitative Research & Execution Experiment

> **Note on this project:** This is a personal, AI-assisted ("vibe-coded") experiment built to explore quantitative trading research workflows on MOEX data — not a production trading system and not investment advice. I used it to practice structuring a research pipeline (feature engineering, statistical validation, backtesting) end-to-end, leaning heavily on AI pair-programming to move fast and iterate on ideas. Expect rough edges; the value of the project is in the pipeline design and the falsification-first research process, not in polished production code.

## What this is

A research + backtesting sandbox for short-term systematic strategies on MOEX instruments, built around one core principle: **the default expected outcome is "no edge found."** The pipeline is designed to falsify trading ideas rather than curve-fit them — thresholds and gates exist specifically so that a negative result is a valid, reportable outcome instead of something to tune away.

The repo has two halves:
- **`fix_engine/`** — the live/backtest execution layer (strategy variants, order execution contract; not modified by research changes unless explicitly wired in).
- **`quant/`** — the research stack: data enrichment, feature engineering (three generations of alpha factors), statistical analysis, model probes, and a CLI-driven pipeline that ties it all together.

## Repository layout

| Path | Purpose |
|---|---|
| `fix_engine/` | Live/backtest strategy variants (breakout + retest + ensemble) and execution logic |
| `quant/core/` | Shared constants and logging setup |
| `quant/data/` | OHLCV enrichment and trade-level dataset construction (no lookahead at entry) |
| `quant/features/` | Three generations of engineered features (`alpha_*` → `alpha_v2_*` → `alpha_v3_*`), chained via `attach_alpha_features` |
| `quant/models/` | Sklearn / GBM edge probes with time-series cross-validation |
| `quant/research/` | Pipeline CLI, factor statistics, pairwise/triple interaction analysis, bootstrap validation, cross-instrument comparison |
| `quant/backtest/` | Thin facade over `run_backtest_v2` and `attach_alpha_features` |
| `cli/` | Entry point: `python -m cli.research` |

Design notes and phase-by-phase criteria live in `docs/research_roadmap.md`; the walk-forward validation approach is documented separately in `docs/walk_forward_spec.md`.

## Research philosophy

- **Falsification first.** A clean "this doesn't work" result is treated as useful output, not a failure to fix.
- **No lookahead.** All features are computed at bar close and attached to trades at `entry_bar` only.
- **Configurable, not hidden, thresholds.** The profit-factor gate (`--edge-min-pf`, default 1.2) and walk-forward bucket count (`--walk-forward-buckets`, default 4) are explicit CLI flags rather than buried constants.

### Edge gate (PnL-aware)

A result only passes the gate if **all** of the following hold:
- at least 30 trades
- positive expectancy (mean PnL)
- profit factor above 1.2
- PnL remains positive even excluding the single best trade

...**and** either at least two statistically stable factors (|Spearman| ≥ 0.15, sign-consistent across train/test) or at least one qualifying pairwise/triple factor interaction under the same filters. Below 30 trades, the pipeline flags the run as unreliable rather than reporting a false pass.

## Feature generations

- **v1 (`alpha_*`):** liquidity sweeps, ATR ratios, rolling volatility percentile, compression/expansion patterns, session/hour buckets, wick-to-body ratios, Donchian range position.
- **v2 (`alpha_v2_*`):** false-breakout strength, short-horizon return normalized by ATR, volatility regime tertiles, Donchian distance in ATR units, rolling z-score, cyclical hour encoding.
- **v3 (`alpha_v3_*`):** finer-grained fail-break patterns, shorter-horizon normalized returns, true-range percentile, tighter Donchian distances, local high/low distance, explicit candle-shadow and body-ratio features, pin-bar scoring.

## Multi-timeframe context (1h + 4h)

Indicators combine a 1-hour primary and 4-hour secondary timeframe. By default, both live and research modes require only one of the two timeframes to confirm (`htf_dual_require_both=False`) — requiring both leaves too few trades on lower timeframes to evaluate. Both timeframes feed into `htf_pv_quality`, a 0–1 price-volume quality score (duplicated as `alpha_htf_pv_quality` in research output).

## Data

Research runs expect several months of OHLCV history (long `history_*.csv` files, not the short sample files). Fetching data requires a T-Invest sandbox token and instrument ID, configured locally (not committed):

```bash
# Fetch a full research bundle (~183 days) across configured instruments
python -m fix_engine.tools.fetch_research_bundle --config fix_engine/backtest/research_instruments.yaml

# Or fetch a single series
python -m fix_engine.tools.fetch_tbank_candles_history --interval 1h --days 183 --out fix_engine/backtest/history_1h_6m.csv

# 4-hour candles with volume, for HTF context
python -m fix_engine.tools.fetch_tbank_candles_history --interval 4h --days 400 --out fix_engine/backtest/history_4h.csv
```

## Running the pipeline

```bash
# Single CSV → flat output folder
python -m quant.research.run_pipeline --csv fix_engine/backtest/history_1h_6m.csv --out-dir research_output

# Every history_*.csv in a directory (one subfolder per file)
python -m quant.research.run_pipeline --batch-dir fix_engine/backtest --out-dir research_batch

# Glob pattern under a directory
python -m quant.research.run_pipeline --batch-dir fix_engine/backtest --glob "history_1h*.csv" --out-dir research_batch

# YAML manifest of multiple datasets
python -m quant.research.run_pipeline --manifest fix_engine/backtest/research_pipeline_manifest.example.yaml --out-dir research_batch
```

Equivalent entry points: `python -m cli.research ...`. The older `python -m research` still works but prints a deprecation notice pointing to `quant.research.run_pipeline` / `cli.research`.

## Output artifacts

| Artifact | Description |
|---|---|
| `trades_dataset.csv` | Every trade with raw, v1, v2, and v3 features at entry |
| `factor_analysis.csv` | Pearson/Spearman correlations, quantile-bin performance, monotonicity, train/test stability |
| `factor_bins_detail.csv`, `factor_summary.json` | Long-form bin-level detail |
| `ablation_results.csv` | Leave-one-factor-out impact on the composite signal score |
| `interactions.csv` / `interactions_triple.csv` | Pairwise and triple-factor median-split analysis |
| `model_metrics.json` | Logistic regression, Ridge, Random Forest, GBM (plus LightGBM if installed) |
| `pnl_estimate.json` | Sample metrics, expectancy decomposition, bootstrap and holdout results, 6-month extrapolation |
| `edge_gate_detail.json` | Reasoning behind the PnL-aware gate decision |
| `final_conclusion.txt` | Plain-language summary of the run |

Batch/manifest runs additionally produce `batch_summary.csv`, `batch_summary_metrics.csv`, and `batch_cross_instrument_factors.csv` (a cross-instrument pivot of factor correlations).

All PnL figures are per one contract (`qty=1`); scale by planned contract count to estimate account-level results. Sizing targets can be set via `--target-6m-account-rub` and `--planned-contracts`, or directly via `--target-6m-per-contract-rub` (set to 0 to disable).

## Strategy defaults

Backtests score signals with a unified `signal_score` (weights defined in `fix_engine/strategy/signal_scoring.py`), combined with a no-lookahead context-quality multiplier derived from session, compression, and volatility-regime features. By default, entries with context quality below 0.84 are hard-rejected, and continuation entries are disabled. Use `--no-score-filter` to disable score-based filtering or `--enable-continuation` to re-enable continuation entries.

## Tests

```bash
pip install -r requirements.txt   # includes pytest>=7; lightgbm optional, for extra feature importance
pytest tests/
```

## Scope note

Live trading logic under `fix_engine/` is intentionally decoupled from the research stack — research changes don't affect live behavior unless explicitly wired in. This project has not been run with real capital and is shared purely as a research/engineering sample.
