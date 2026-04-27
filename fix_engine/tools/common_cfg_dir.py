from __future__ import annotations

from pathlib import Path

# Продовый gRPC Invest API (T-Bank).
TBANK_INVEST_GRPC_HOST_PROD = "invest-public-api.tbank.ru:443"

# Единственный ключ токена в локальном конфиге (песочница / Invest API).
TINVEST_TOKEN_LOCAL_KEY = "TBankSandboxToken"


def read_tinvest_token_from_dir(
    cfg_dir: Path,
    file_names: tuple[str, ...] = ("settings.local.cfg",),
) -> str:
    """Токен из активной (не с #) строки `TBankSandboxToken=...` в settings.local.cfg.

    Читаются только указанные файлы (по умолчанию один settings.local.cfg), без env и без других ключей.
    """
    v = read_cfg_value_from_dir(cfg_dir, TINVEST_TOKEN_LOCAL_KEY, "", file_names=file_names)
    return v.strip()


def find_cfg_value_from_dir(
    cfg_dir: Path,
    key: str,
    file_names: tuple[str, ...] = ("settings.local.cfg", "settings.runtime.cfg", "settings.cfg"),
) -> str | None:
    """
    Первое вхождение key= в цепочке файлов. None — ключа нет ни в одном файле.
    Пустая строка — ключ есть, значение пустое.
    """
    for name in file_names:
        path = cfg_dir / name
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8-sig")
        except OSError:
            continue
        for raw in text.splitlines():
            line = raw.strip().lstrip("\ufeff")
            if not line or line.startswith("#") or line.startswith(";") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() == key:
                val = v.split("#", 1)[0].strip()
                return val
    return None


def read_cfg_value_from_dir(
    cfg_dir: Path,
    key: str,
    default: str = "",
    file_names: tuple[str, ...] = ("settings.local.cfg", "settings.runtime.cfg", "settings.cfg"),
) -> str:
    """
    Read `key=value` from the first existing config file in `file_names`.
    - trims lines, strips BOM on line
    - skips empty / '#' / ';' comment lines
    - key match is exact after trimming whitespace
    """
    v = find_cfg_value_from_dir(cfg_dir, key, file_names=file_names)
    return default if v is None else v

