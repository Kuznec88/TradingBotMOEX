"""Расширенные метрики бэктеста: диагностика по meta сделок, хрупкость по выбросам."""

from __future__ import annotations

import json
import math
import statistics
from typing import Any

from fix_engine.strategy.swing_breakout import BacktestResult, SwingBreakoutParams, TradeRecord


def _fmeta(t: TradeRecord, key: str) -> float | None:
    m = t.meta or {}
    v = m.get(key)
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _s_meta(t: TradeRecord, key: str) -> str | None:
    m = t.meta or {}
    v = m.get(key)
    if v is None or v == "":
        return None
    return str(v)


def _i_meta(t: TradeRecord, key: str) -> int | None:
    m = t.meta or {}
    v = m.get(key)
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _is_market_entry(t: TradeRecord) -> bool:
    m = t.meta or {}
    if m.get("use_market_entry") is True:
        return True
    er = str(m.get("entry_reason", "") or "")
    return "market" in er.lower()


def extended_metrics(
    res: BacktestResult,
    params: SwingBreakoutParams | None = None,
) -> dict[str, Any]:
    """Сводка поверх res.summary() + средние по meta, доля market, PnL без топ-сделок."""
    trades = res.trades
    n = len(trades)
    base = res.summary()

    retests = [_i_meta(t, "retest_bars_to_touch") for t in trades]
    slopes = [_fmeta(t, "atr_slope") for t in trades]
    impulses = [_fmeta(t, "impulse_strength_ratio") for t in trades]
    ranges = [_fmeta(t, "range_atr_ratio") for t in trades]

    def _avg(vals: list[float | int | None]) -> float | None:
        clean = [float(x) for x in vals if x is not None]
        return sum(clean) / len(clean) if clean else None

    n_mkt = sum(1 for t in trades if _is_market_entry(t))
    pct_mkt = (100.0 * n_mkt / n) if n else None

    pnls = [float(t.pnl_net_rub) for t in trades]
    pnls_sorted = sorted(pnls, reverse=True)
    total = sum(pnls_sorted)
    top1 = pnls_sorted[0] if pnls_sorted else 0.0
    top2 = pnls_sorted[1] if len(pnls_sorted) > 1 else 0.0
    pnl_wo_top1 = total - top1
    pnl_wo_top2 = total - top1 - top2

    sharpe_like: float | None = None
    if n >= 2:
        m = statistics.mean(pnls)
        try:
            sd = statistics.stdev(pnls)
        except statistics.StatisticsError:
            sd = 0.0
        if sd > 1e-12:
            sharpe_like = m / sd

    losses_net = sum(t.pnl_net_rub for t in trades if t.pnl_net_rub < 0)
    pf = base.get("profit_factor")
    if losses_net >= -1e-12:
        pf_ok = n >= 30 and total > 0
    else:
        pf_ok = (pf is not None and pf > 1.3)

    interesting = n >= 30 and pf_ok and pnl_wo_top1 > 0

    scores = [_fmeta(t, "signal_score") for t in trades]
    tags = [_s_meta(t, "setup_tag") for t in trades]
    avg_score = _avg([x for x in scores if x is not None])

    by_tag: dict[str, int] = {}
    for tg in tags:
        if tg:
            by_tag[tg] = by_tag.get(tg, 0) + 1

    out: dict[str, Any] = {
        **base,
        "avg_retest_bars": _avg(retests),
        "avg_atr_slope": _avg(slopes),
        "avg_impulse_strength_ratio": _avg(impulses),
        "avg_range_atr_ratio": _avg(ranges),
        "avg_signal_score": avg_score,
        "n_market_entries": n_mkt,
        "n_limit_entries": n - n_mkt,
        "pct_market_entries": pct_mkt,
        "pnl_without_top_1_trade": pnl_wo_top1,
        "pnl_without_top_2_trades": pnl_wo_top2,
        "sharpe_like_pnl": sharpe_like,
        "trades_by_setup_tag_json": json.dumps(by_tag, sort_keys=True) if by_tag else "",
        "interesting_candidate": interesting,
    }
    if params is not None and n > 0:
        ok2 = sum(1 for r in retests if r is not None and int(r) <= 2)
        out["pct_fast_retest_le2_bars"] = 100.0 * ok2 / n
        thr = float(params.min_atr_expansion)
        if thr > 0:
            ok_a = sum(1 for s in slopes if s is not None and float(s) >= thr)
            out["pct_trades_atr_slope_ge_min_expansion"] = 100.0 * ok_a / n
        mrr = float(params.min_range_atr_ratio)
        if mrr > 0:
            ok_rr = sum(1 for x in ranges if x is not None and float(x) >= mrr)
            out["pct_trades_range_atr_ge_min"] = 100.0 * ok_rr / n
    return out
