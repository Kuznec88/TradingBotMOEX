from __future__ import annotations

from pathlib import Path


def read_default_optional_setting(cfg_path: Path, key: str) -> str | None:
    for raw_line in cfg_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("[") and line.endswith("]"):
            if line[1:-1].strip().upper() == "SESSION":
                break
            continue
        k, v = line.split("=", 1)
        if k.strip() == key:
            value = v.strip()
            return value or None
    return None


def read_float(cfg_path: Path, key: str, default: float) -> float:
    value = read_default_optional_setting(cfg_path, key)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def read_int(cfg_path: Path, key: str, default: int) -> int:
    value = read_default_optional_setting(cfg_path, key)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def read_bool(cfg_path: Path, key: str, default: bool) -> bool:
    value = read_default_optional_setting(cfg_path, key)
    if value is None:
        return default
    return value.strip().upper() in {"Y", "YES", "TRUE", "1", "ON"}


def read_csv_list(cfg_path: Path, key: str, default: list[str]) -> list[str]:
    value = read_default_optional_setting(cfg_path, key)
    if not value:
        return list(default)
    items = [part.strip() for part in value.split(",")]
    return [x for x in items if x]


def read_str(cfg_path: Path, key: str, default: str) -> str:
    value = read_default_optional_setting(cfg_path, key)
    if value is None:
        return default
    return value.strip() or default


def read_execution_mode(cfg_path: Path) -> str:
    """LIVE = real routing (disabled in this build); PAPER_REAL_MARKET = real MD + local fills."""
    value = (read_default_optional_setting(cfg_path, "ExecutionMode") or "").strip().upper()
    if value == "SIMULATION":
        return "PAPER_REAL_MARKET"
    if value in {"LIVE", "PAPER_REAL_MARKET"}:
        return value
    return "PAPER_REAL_MARKET"

