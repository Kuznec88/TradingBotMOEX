"""Trade-level dataset: labels + entry-time features (legacy + alpha from enriched OHLCV row)."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pandas as pd

from fix_engine.strategy.swing_breakout import BacktestResult, TradeRecord

from quant.core.constants import FACTOR_LABELS, RAW_FACTOR_KEYS


def _f(meta: dict[str, Any], key: str) -> float | None:
    v = meta.get(key)
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _session_bucket(ts_str: str) -> str:
    if not ts_str or not str(ts_str).strip():
        return "unknown"
    try:
        t = pd.Timestamp(ts_str)
        h = int(t.hour)
    except Exception:
        return "unknown"
    if 0 <= h < 7:
        return "night"
    if 7 <= h < 12:
        return "morning"
    if 12 <= h < 18:
        return "day"
    return "evening"


def trades_to_dataframe(
    res: BacktestResult,
    *,
    instrument: str,
    enriched_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    alpha_cols = (
        [c for c in enriched_df.columns if c.startswith("alpha_")] if enriched_df is not None else []
    )

    for t in res.trades:
        m = t.meta or {}
        # pnl_net_rub at entry qty: RUB per 1 contract when qty=1
        pnl = float(t.pnl_net_rub)
        risk_rub = _f(m, "risk_rub")
        if risk_rub is None or risk_rub <= 1e-12:
            ret_risk = float("nan")
        else:
            ret_risk = pnl / risk_rub
        win = 1 if pnl > 0 else 0
        ts = str(m.get("entry_timestamp", m.get("bar_time", "")))
        row: dict[str, Any] = {
            "instrument": instrument,
            "timestamp": ts,
            "session": _session_bucket(ts),
            "setup_type": str(m.get("setup_tag", "") or ""),
            "side": t.side,
            "pnl": pnl,
            "return_risk_adj": ret_risk,
            "win": win,
            "risk_rub": risk_rub,
            "entry_qty": _f(m, "entry_qty"),
            "signal_score": _f(m, "signal_score"),
        }
        for k in RAW_FACTOR_KEYS:
            col = f"raw_{FACTOR_LABELS[k]}"
            v = _f(m, k)
            if v is None and k == "retest_bars_to_touch":
                try:
                    v = float(m.get("retest_bars_to_touch")) if m.get("retest_bars_to_touch") is not None else None
                except (TypeError, ValueError):
                    v = None
            row[col] = v

        eb = m.get("entry_bar")
        try:
            ei = int(eb) if eb is not None else -1
        except (TypeError, ValueError):
            ei = -1
        row["entry_bar"] = ei

        if enriched_df is not None and ei >= 0 and ei < len(enriched_df):
            er = enriched_df.iloc[ei]
            for ac in alpha_cols:
                try:
                    v = er[ac]
                    row[ac] = float(v) if pd.notna(v) else None
                except Exception:
                    row[ac] = None

        rows.append(row)
    return pd.DataFrame(rows)


def discover_ohlcv_csvs(directory: Path) -> list[Path]:
    """Файлы `history_*.csv` (полноценная выгрузка). Короткие `_hist*.csv` не включаем — для бэктеста бессмысленны."""
    skip = ("research_", "experiment_", "factor_")
    out: list[Path] = []
    for p in sorted(directory.iterdir()):
        if not p.is_file() or p.suffix.lower() != ".csv":
            continue
        n = p.name.lower()
        if any(n.startswith(s) for s in skip):
            continue
        if n.startswith("history_"):
            out.append(p)
    return out
