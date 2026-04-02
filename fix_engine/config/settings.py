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
    """LIVE = Tinkoff gRPC post_order + order_state_stream; PAPER_REAL_MARKET = real MD + local fills."""
    value = (read_default_optional_setting(cfg_path, "ExecutionMode") or "").strip().upper()
    if value == "SIMULATION":
        return "PAPER_REAL_MARKET"
    if value in {"LIVE", "PAPER_REAL_MARKET"}:
        return value
    return "PAPER_REAL_MARKET"


def _find_cfg_merged(cfg_dir: Path, key: str) -> str | None:
    from fix_engine.tools.common_cfg_dir import find_cfg_value_from_dir

    return find_cfg_value_from_dir(cfg_dir, key)


def read_str_merged(cfg_dir: Path, key: str, default: str) -> str:
    """local.cfg → runtime.cfg → settings.cfg (первый найденный ключ)."""
    v = _find_cfg_merged(cfg_dir, key)
    if v is None:
        return default
    return v.strip() or default


def read_float_merged(cfg_dir: Path, key: str, default: float) -> float:
    v = _find_cfg_merged(cfg_dir, key)
    if v is None or not str(v).strip():
        return default
    try:
        return float(str(v).strip())
    except ValueError:
        return default


def read_int_merged(cfg_dir: Path, key: str, default: int) -> int:
    v = _find_cfg_merged(cfg_dir, key)
    if v is None or not str(v).strip():
        return default
    try:
        return int(str(v).strip(), 10)
    except ValueError:
        return default


def read_bool_merged(cfg_dir: Path, key: str, default: bool) -> bool:
    v = _find_cfg_merged(cfg_dir, key)
    if v is None:
        return default
    return str(v).strip().upper() in {"Y", "YES", "TRUE", "1", "ON"}


def read_default_optional_setting_merged(cfg_dir: Path, key: str) -> str | None:
    v = _find_cfg_merged(cfg_dir, key)
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def read_execution_mode_merged(cfg_dir: Path) -> str:
    value = (read_default_optional_setting_merged(cfg_dir, "ExecutionMode") or "").strip().upper()
    if value == "SIMULATION":
        return "PAPER_REAL_MARKET"
    if value in {"LIVE", "PAPER_REAL_MARKET"}:
        return value
    return "PAPER_REAL_MARKET"

