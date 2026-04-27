"""Resolve CSV paths for research runs: single file, directory, glob, or YAML manifest."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from quant.data.trade_dataset import discover_ohlcv_csvs


def resolve_csv_inputs(
    *,
    csv: str | None = None,
    batch_dir: str | None = None,
    manifest: str | None = None,
    glob_pattern: str | None = None,
    root: Path | None = None,
) -> list[tuple[Path, str]]:
    """
    Returns list of (path, instrument_label).

    Priority: ``csv`` > ``manifest`` > ``glob_pattern`` > ``batch_dir`` discovery.
    """
    root = root or Path.cwd()

    if csv:
        p = Path(csv)
        if not p.is_file():
            raise FileNotFoundError(f"CSV not found: {p}")
        return [(p.resolve(), Path(csv).stem)]

    if manifest:
        mp = Path(manifest)
        if not mp.is_file():
            raise FileNotFoundError(f"Manifest not found: {mp}")
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError as e:
            raise RuntimeError("pip install pyyaml") from e
        data = yaml.safe_load(mp.read_text(encoding="utf-8")) or {}
        items = data.get("csvs") or data.get("files") or []
        out: list[tuple[Path, str]] = []
        for row in items:
            if isinstance(row, str):
                path = Path(row)
                inst = path.stem
            elif isinstance(row, dict):
                path = Path(str(row.get("path", row.get("csv", ""))).strip())
                inst = str(row.get("instrument", row.get("label", ""))).strip() or path.stem
            else:
                continue
            if not path.is_absolute():
                path = (root / path).resolve()
            if path.is_file():
                out.append((path, inst))
        if not out:
            raise ValueError(f"No valid CSV paths in manifest: {mp}")
        return out

    base = Path(batch_dir) if batch_dir else root
    if not base.is_dir():
        raise FileNotFoundError(f"Not a directory: {base}")

    if glob_pattern:
        paths = sorted(base.glob(glob_pattern))
        paths = [p for p in paths if p.is_file() and p.suffix.lower() == ".csv"]
        if not paths:
            raise ValueError(f"No CSVs matching {glob_pattern!r} under {base}")
        return [(p.resolve(), p.stem) for p in paths]

    discovered = discover_ohlcv_csvs(base)
    if not discovered:
        raise ValueError(f"No history_*.csv under {base}")
    return [(p.resolve(), p.stem) for p in discovered]


def manifest_example_yaml() -> str:
    return """# Research CSV manifest (one or more instruments)
csvs:
  - path: fix_engine/backtest/history_1h_6m.csv
    instrument: primary_1h
  - path: fix_engine/backtest/history_5m_6m.csv
    instrument: primary_5m
"""
