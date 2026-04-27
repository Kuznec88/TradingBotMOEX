"""
Пакетная загрузка свечей по списку инструментов (YAML).

  python -m fix_engine.tools.fetch_research_bundle --config fix_engine/backtest/research_instruments.yaml

Формат конфига см. research_instruments.example.yaml
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> int:
    fix_engine_dir = Path(__file__).resolve().parents[1]
    root = fix_engine_dir.parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", type=str, required=True)
    args = ap.parse_args()

    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as e:
        print("pip install pyyaml", file=sys.stderr)
        return 1

    cfg_path = Path(args.config)
    if not cfg_path.is_file():
        print(f"config not found: {cfg_path}", file=sys.stderr)
        return 1

    data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    instruments = data.get("instruments") or []
    if not isinstance(instruments, list):
        print("instruments must be a list", file=sys.stderr)
        return 1

    py = sys.executable
    fetch_mod = "fix_engine.tools.fetch_tbank_candles_history"

    for inst in instruments:
        if not isinstance(inst, dict):
            continue
        label = inst.get("label", "")
        iid = (inst.get("instrument_id") or "").strip()
        intervals = inst.get("intervals") or []
        for spec in intervals:
            if not isinstance(spec, dict):
                continue
            interval = str(spec.get("interval", "")).strip()
            days = float(spec.get("days", 183))
            out = str(spec.get("out", "")).strip()
            if not interval or not out:
                continue
            outp = root / out
            outp.parent.mkdir(parents=True, exist_ok=True)
            cmd = [
                py,
                "-m",
                fetch_mod,
                "--interval",
                interval,
                "--days",
                str(days),
                "--out",
                str(outp),
            ]
            if iid:
                cmd.extend(["--instrument-id", iid])
            print("RUN", " ".join(cmd))
            r = subprocess.run(cmd, cwd=str(root))
            if r.returncode != 0:
                return r.returncode

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
