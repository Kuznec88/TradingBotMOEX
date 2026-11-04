"""Логика лаборатории без UI: запуск бэктеста на склеенной истории и метрики."""

from __future__ import annotations

import csv
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from fix_engine.config.swing_params import load_swing_params
from fix_engine.strategy.swing_breakout import BacktestResult, SwingBreakoutParams, run_backtest

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data" / "futures"
LAB_DIR = REPO_ROOT / "data" / "lab"
# До этой даты можно подбирать параметры; после неё — только отдельная проверка.
RESEARCH_END = pd.Timestamp("2025-01-01", tz="UTC")
FRAMES = ("5m", "15m", "1h")
HtfMode = Literal["hard", "soft", "off"]
Period = Literal["research", "holdout"]

POSITION_COLUMNS = [
    "asset", "frame", "side", "entry_time", "exit_time", "entry_price", "exit_price",
    "gross_rub", "fees_rub", "net_rub", "gross_bp", "net_bp", "exit_reason", "had_partial",
]


def load_meta(data_dir: Path = DATA_DIR) -> dict[str, dict[str, Any]]:
    path = data_dir / "meta.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_candles(asset: str, frame: str, data_dir: Path = DATA_DIR) -> pd.DataFrame:
    df = pd.read_csv(data_dir / f"{asset}_{frame}.csv")
    df["time_utc"] = pd.to_datetime(df["time_utc"], utc=True)
    return df.set_index("time_utc").sort_index()


def bot_defaults() -> SwingBreakoutParams:
    """Параметры стратегии из settings.cfg, без привязки к инструменту."""
    return replace(load_swing_params(REPO_ROOT / "fix_engine", tick_size=1.0), trades_log_csv_path="")


def apply_htf_mode(params: SwingBreakoutParams, mode: HtfMode, frame: str) -> SwingBreakoutParams:
    if mode == "off":
        return replace(params, htf_resample_rule="", htf_secondary_resample_rule="")
    # На 1h старший таймфрейм 1h совпал бы с базовым, и сделок бы не было.
    primary, secondary = ("4h", "") if frame == "1h" else ("1h", "4h")
    gate: Literal["hard", "soft"] = "soft" if mode == "soft" else "hard"
    return replace(params, htf_resample_rule=primary, htf_secondary_resample_rule=secondary, htf_gate_mode=gate)


def build_params(
    overrides: dict[str, Any],
    *,
    frame: str,
    htf_mode: HtfMode,
    tick: float,
    tick_rub: float,
) -> SwingBreakoutParams:
    params = replace(bot_defaults(), **overrides)
    params = replace(params, tick_size=tick, rub_per_point=tick_rub / tick)
    return apply_htf_mode(params, htf_mode, frame)


def positions_from_result(
    res: BacktestResult,
    index: pd.Index,
    *,
    asset: str,
    frame: str,
    rub_per_point: float,
) -> pd.DataFrame:
    """Объединить частичный и финальный выход в одну строку на позицию."""
    grouped: dict[int, list[Any]] = {}
    for trade in res.trades:
        grouped.setdefault(trade.entry_bar, []).append(trade)
    rows: list[dict[str, Any]] = []
    for entry_bar, parts in grouped.items():
        last = parts[-1]
        gross = sum(t.pnl_gross_rub for t in parts)
        fees = sum(t.fees_rub for t in parts)
        notional = max(abs(last.entry_price) * rub_per_point, 1e-12)
        rows.append(
            {
                "asset": asset,
                "frame": frame,
                "side": last.side,
                "entry_time": index[entry_bar],
                "exit_time": index[last.exit_bar],
                "entry_price": last.entry_price,
                "exit_price": last.exit_price,
                "gross_rub": gross,
                "fees_rub": fees,
                "net_rub": gross - fees,
                "gross_bp": gross / notional * 1e4,
                "net_bp": (gross - fees) / notional * 1e4,
                "exit_reason": last.exit_reason,
                "had_partial": len(parts) > 1,
            }
        )
    return pd.DataFrame(rows, columns=POSITION_COLUMNS)


def run_asset(
    asset: str,
    frame: str,
    overrides: dict[str, Any],
    htf_mode: HtfMode,
    period: Period = "research",
    data_dir: Path = DATA_DIR,
) -> pd.DataFrame:
    meta = load_meta(data_dir)[asset]
    candles = load_candles(asset, frame, data_dir)
    if period == "research":
        candles = candles[candles.index < RESEARCH_END]
    ohlcv = candles[["open", "high", "low", "close", "volume"]]
    params = build_params(overrides, frame=frame, htf_mode=htf_mode, tick=meta["tick"], tick_rub=meta["tick_rub"])
    res = run_backtest(ohlcv, params, log_trades=False)
    positions = positions_from_result(res, ohlcv.index, asset=asset, frame=frame, rub_per_point=params.rub_per_point)
    if period == "holdout":
        positions = positions[positions["entry_time"] >= RESEARCH_END]
    return positions.reset_index(drop=True)


def equity_curve(positions: pd.DataFrame) -> pd.DataFrame:
    if positions.empty:
        return pd.DataFrame(columns=["exit_time", "equity"])
    ordered = positions.sort_values("exit_time")
    return pd.DataFrame({"exit_time": ordered["exit_time"], "equity": ordered["net_rub"].cumsum()}).reset_index(drop=True)


def monthly_net(positions: pd.DataFrame) -> pd.DataFrame:
    if positions.empty:
        return pd.DataFrame(columns=["month", "net_rub", "positions"])
    months = pd.to_datetime(positions["exit_time"], utc=True).dt.tz_localize(None).dt.to_period("M").dt.to_timestamp()
    out = positions.groupby(months).agg(net_rub=("net_rub", "sum"), positions=("net_rub", "size")).reset_index()
    return out.rename(columns={"exit_time": "month"})


def summarize(positions: pd.DataFrame, *, months: float) -> dict[str, float | int | None]:
    n = len(positions)
    if n == 0:
        return {"positions": 0, "per_month": 0.0}
    net = positions["net_rub"]
    wins = net[net > 0].sum()
    losses = -net[net < 0].sum()
    equity = equity_curve(positions)["equity"]
    drawdown = float((equity.cummax().clip(lower=0) - equity).max())
    bp = positions["net_bp"]
    spread = float(bp.std(ddof=1)) if n > 1 else 0.0
    return {
        "positions": n,
        "per_month": n / max(months, 1e-9),
        "gross_rub": float(positions["gross_rub"].sum()),
        "fees_rub": float(positions["fees_rub"].sum()),
        "net_rub": float(net.sum()),
        "gross_bp": float(positions["gross_bp"].mean()),
        "net_bp": float(bp.mean()),
        "win_rate": float((net > 0).mean()),
        "profit_factor": float(wins / losses) if losses > 0 else None,
        "max_drawdown_rub": drawdown,
        "t_stat": float(bp.mean() / (spread / np.sqrt(n))) if spread > 0 else None,
    }


def period_months(frame_starts: list[pd.Timestamp], end: pd.Timestamp) -> float:
    if not frame_starts:
        return 0.0
    return max((end - min(frame_starts)).days / 30.44, 0.0)


def log_holdout_check(config: dict[str, Any], summary: dict[str, Any], log_dir: Path = LAB_DIR) -> int:
    """Записать проверку на отложенном периоде и вернуть число проверок этой же конфигурации."""
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / "holdout_log.csv"
    config_json = json.dumps(config, sort_keys=True, ensure_ascii=False)
    previous = 0
    if path.exists():
        with path.open(encoding="utf-8", newline="") as f:
            previous = sum(1 for row in csv.DictReader(f) if row["config"] == config_json)
    is_new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["checked_at", "config", "summary"])
        if is_new:
            writer.writeheader()
        writer.writerow(
            {
                "checked_at": datetime.now(timezone.utc).isoformat(),
                "config": config_json,
                "summary": json.dumps(summary, sort_keys=True),
            }
        )
    return previous + 1


def holdout_check_count(log_dir: Path = LAB_DIR) -> int:
    path = log_dir / "holdout_log.csv"
    if not path.exists():
        return 0
    with path.open(encoding="utf-8", newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


def save_run(name: str, config: dict[str, Any], positions: pd.DataFrame, summary: dict[str, Any], log_dir: Path = LAB_DIR) -> Path:
    runs_dir = log_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    equity = equity_curve(positions)
    payload = {
        "name": name,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "config": config,
        "summary": summary,
        "equity": {
            "time": [pd.Timestamp(t).isoformat() for t in equity["exit_time"]],
            "value": [float(v) for v in equity["equity"]],
        },
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    path = runs_dir / f"{stamp}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    return path


def load_runs(log_dir: Path = LAB_DIR) -> list[dict[str, Any]]:
    runs_dir = log_dir / "runs"
    if not runs_dir.exists():
        return []
    runs = []
    for path in sorted(runs_dir.glob("*.json"), reverse=True):
        run = json.loads(path.read_text(encoding="utf-8"))
        run["path"] = str(path)
        runs.append(run)
    return runs
