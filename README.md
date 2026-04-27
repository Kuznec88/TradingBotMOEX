# TradingBotMOEX — execution engine + quantitative research

## Layout

| Path | Purpose |
|------|---------|
| `fix_engine/` | Live/backtest strategy (breakout + retest + v2 ensemble), execution (unchanged contract) |
| `quant/core/` | Constants, logging (`configure_logging`, INFO default) |
| `quant/data/` | OHLCV enrichment, trade-level dataset (no lookahead at entry) |
| `quant/features/` | `alpha_*` v1 → `alpha_v2_*` → `alpha_v3_*` (chained in `attach_alpha_features`) |
| `quant/models/` | Sklearn / GBM edge probes (time-series CV) |
| `quant/research/` | Pipeline CLI, factor stats, pairwise + triple interactions, bootstrap, cross-instrument table |
| `quant/backtest/` | Thin facade: `run_backtest_v2`, `attach_alpha_features` |
| `cli/` | `python -m cli.research` → research pipeline |

## Research workflow (falsification-first)

The default outcome is **“no edge found”**. That is a valid, useful result — do not tune thresholds to force a pass.

**Roadmap (phases, criteria, repo hygiene):** [docs/research_roadmap.md](docs/research_roadmap.md).

Edge gate profit factor threshold is configurable: `--edge-min-pf` (default `1.2`; use `1.3` for stricter checks aligned with that doc).

Walk-forward **trade buckets** (stability of PF/expectancy over time, no bar re-run): `--walk-forward-buckets N` (default `4`, `0` off). Details: [docs/walk_forward_spec.md](docs/walk_forward_spec.md).

### Data (roughly 6+ months of bars)

Use long **`history_*.csv`** (T-Invest OHLCV), not short `_hist*.csv` samples.

1. Put `TBankSandboxToken` and `TBankInstrumentId` in `fix_engine/settings.local.cfg` (see `settings.cfg`).
2. Fetch ~183 days:

```bash
python -m fix_engine.tools.fetch_research_bundle --config fix_engine/backtest/research_instruments.yaml
```

Or one series:

```bash
python -m fix_engine.tools.fetch_tbank_candles_history --interval 1h --days 183 --out fix_engine/backtest/history_1h_6m.csv
```

### Running the pipeline

```bash
# Single CSV → flat output folder
python -m quant.research.run_pipeline --csv fix_engine/backtest/history_1h_6m.csv --out-dir research_output

# All history_*.csv in a directory (subfolder per stem)
python -m quant.research.run_pipeline --batch-dir fix_engine/backtest --out-dir research_batch

# Glob under a directory
python -m quant.research.run_pipeline --batch-dir fix_engine/backtest --glob "history_1h*.csv" --out-dir research_batch

# YAML manifest (paths relative to repo root or absolute)
python -m quant.research.run_pipeline --manifest fix_engine/backtest/research_pipeline_manifest.example.yaml --out-dir research_batch
```

Same entrypoints:

```bash
python -m cli.research --csv path/to/data.csv --out-dir research_output
python -m quant
```

`python -m research` prints a deprecation message — use `quant.research.run_pipeline` or `cli.research`.

### HTF 1h + 4h и объём (T-Invest SDK)

- В **`compute_indicators`** по умолчанию: primary **`1h`**, secondary **`4h`**. В live по умолчанию **`htf_dual_require_both=False`** (OR по ТФ); в research то же в `get_enriched_ohlcv`, иначе на младших ТФ почти нет сделок. Оба ТФ участвуют в `htf_entry_*` и в **price–volume quality** на HTF.
- Колонки: `htf_pv_quality` (0..1), в research дублируется как **`alpha_htf_pv_quality`**.
- Исторические свечи с объёмом: `python -m fix_engine.tools.fetch_tbank_candles_history --interval 4h --days 400 --out ...` (SDK: `CANDLE_INTERVAL_4_HOUR`; объём в CSV уже есть).

### Feature sets (entry-time only)

All features are computed at bar close; trade rows attach values at `entry_bar` (no lookahead).

- **v1 (`alpha_*`)**: liquidity sweep vs prior bar, ATR short/long ratio, rolling vol percentile, compression / expand-after-compress, hour/session buckets, wick/body ratios, Donchian range position (20).
- **v2 (`alpha_v2_*`)**: false-breakout strength (pierce + failure), 3-bar return vs ATR, volatility regime tertiles, Donchian 20 distance in ATR, z-score (50-bar), hour sin/cos, finer session bucket.
- **v3 (`alpha_v3_*`)**: 2-bar fail-break pattern, 5-bar return vs ATR, true-range vs ATR percentile, Donchian 10 distances, z-score (20-bar), minute-of-day, local high/low distance (50-bar), position in 30-bar range, explicit upper/lower shadow fractions, signed body ratio, pin-bar score.

### Outputs (per run)

| Artifact | Description |
|----------|-------------|
| `trades_dataset.csv` | Trades with `raw_*`, `alpha_*`, `alpha_v2_*`, `alpha_v3_*` at entry |
| `factor_analysis.csv` | Pearson/Spearman, quantile bins (avg PnL, PF, winrate, pnl_wo_top1), monotonicity, train/test stability |
| `factor_bins_detail.csv`, `factor_summary.json` | Long-form bin tables |
| `ablation_results.csv` | Leave-one-legacy-factor-out on signal score (renormalized weights) |
| `interactions.csv` | Pairwise median splits (min trades, PF>1.2, pnl wo top-1 > 0) |
| `interactions_triple.csv` | Triple-median splits (8 octants; capped at first 8 factors for speed) |
| `model_metrics.json` | Logistic, Ridge, RF, GBM (+ LightGBM if installed) |
| `pnl_estimate.json` | Sample metrics, **expectancy decomposition**, bootstrap, holdout, 6M extrapolation; **`pnl_convention`**: все суммы в **₽ на 1 контракт**; опционально **`target_6m_sizing`** (по умолчанию цель **40 000 ₽ на счёт / 6 мес.** при **50** контрактах → **800 ₽ / контракт / 6 мес.**) — множитель к `qty=1` в research |
| `edge_gate_detail.json` | PnL-aware gate reasons |
| `final_conclusion.txt` | Human-readable summary |

**Единицы PnL:** 1 в колонке `pnl` = **1 ₽ на 1 контракт** (бэктест `qty=1`). Итог на счёте при плоском размере **N** контрактов ≈ **PnL × N**. CLI: `--target-6m-account-rub 40000 --planned-contracts 50` (или `--target-6m-per-contract-rub 800`); `0` на account — выключить блок sizing.

Batch-only (multi-CSV / manifest):

| Artifact | Description |
|----------|-------------|
| `batch_summary.csv`, `batch_summary_metrics.csv` | Per-frame PnL / winrate / edge |
| `batch_cross_instrument_factors.csv` | Pivot of `spearman_pnl` by factor × instrument + cross-section std/mean |

### Edge gate (PnL-aware)

All of the following must hold:

- `n ≥ 30` trades  
- **Expectancy** > 0 (mean PnL)  
- **Profit factor** > 1.2  
- **PnL without top-1 trade** > 0  

and **either** ≥ 2 stable factors (|Spearman| ≥ 0.15, sign-consistent train/test, stability) **or** at least one qualifying row in **pairwise or triple** interactions (same PF / pnl_wo_top1 filters).

If `n < 30`, the pipeline logs a **WARNING** and marks results as unreliable.

### Tests

```bash
pytest tests/
```

### Requirements

```bash
pip install -r requirements.txt
```

Includes `pytest>=7`. Optional: `lightgbm` for an extra feature-importance block.

## Strategy defaults (research / `quant.data.pipeline`)

Backtests use **unified `signal_score`** with:

- **Weights** emphasizing impulse + retest speed (see `SignalScoreWeights` in `fix_engine/strategy/signal_scoring.py`).
- **Context multiplier** `entry_context_quality(row)` from `alpha_*` (session, compression, vol regime) — no lookahead.
- Optional **hard reject** if context quality &lt; 0.84 (with alpha columns present).
- **`min_score`** default **0.52**, **continuation entries off** by default; HTF **1h+4h** включены в `get_enriched_ohlcv` с `htf_dual_require_both=False` (иначе на 5m/15m почти нет сделок).

CLI: `--no-score-filter` restores the old “all intents pass” behaviour; `--enable-continuation` turns continuation back on.

## Production

Live trading stays under `fix_engine/`. Research modules do not change live paths unless you explicitly wire them.
