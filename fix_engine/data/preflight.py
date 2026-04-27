"""Проверки перед стартом: токен из settings.local.cfg, unary market-data (без ордеров)."""

from __future__ import annotations

import logging
import re
from pathlib import Path

from fix_engine.tools.common_cfg_dir import read_tinvest_token_from_dir

_LOCAL_GRPC_CA = "certs/russian_trusted_chain.pem"


def prepare_invest_api_tls_trust_store(base_dir: Path) -> None:
    """Настроить доверие для gRPC/requests до любых вызовов T-Invest.

    Собранная цепочка `certs/russian_trusted_chain.pem` (скрипт export_russian_trusted_chain.ps1)
    покрывает российские корни, но одна только она **заменяет** стандартный store в gRPC — тогда
    хосты вроде *.tinkoff.ru с глобальными CA не проходят. Поэтому объединяем её с bundle из certifi.

    Если в окружении уже задан другой PEM (не наш russian_trusted_chain.pem) — не перезаписываем.
    Чтобы полностью отключить автонастройку: `INVEST_API_TLS_TRUST_EXTERNAL=1`.
    """
    import os
    import tempfile

    if os.environ.get("INVEST_API_TLS_TRUST_EXTERNAL", "").strip() in {"1", "Y", "YES", "TRUE"}:
        return

    russian = (base_dir / _LOCAL_GRPC_CA).resolve()
    if not russian.is_file():
        return

    existing = str(os.environ.get("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", "")).strip()
    if existing:
        try:
            if Path(existing).resolve() != russian:
                return
        except OSError:
            return

    rus_txt = russian.read_text(encoding="utf-8", errors="replace").strip()
    moz_txt = ""
    try:
        import certifi
        from pathlib import Path as _P

        moz_txt = _P(certifi.where()).read_text(encoding="utf-8", errors="replace").strip()
    except Exception:
        pass

    combined = (rus_txt + "\n\n" + moz_txt + "\n") if moz_txt else rus_txt + "\n"

    fd, path = tempfile.mkstemp(prefix="invest_grpc_roots_", suffix=".pem")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(combined)
    except Exception:
        try:
            os.close(fd)
        except Exception:
            pass
        raise

    os.environ["GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"] = path
    os.environ.setdefault("SSL_CERT_FILE", path)
    os.environ.setdefault("REQUESTS_CA_BUNDLE", path)


def apply_local_grpc_ca_bundle_if_present(base_dir: Path) -> None:
    """Устаревшее имя: то же, что prepare_invest_api_tls_trust_store."""
    prepare_invest_api_tls_trust_store(base_dir)


_UUID_ACCOUNT_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def is_sandbox_uuid_account_id(value: str) -> bool:
    """True если строка похожа на UUID счёта T-Invest (sandbox / broker id в формате UUID)."""
    return bool(_UUID_ACCOUNT_RE.match(str(value).strip()))


def ensure_sandbox_account_id(
    *,
    token: str,
    host: str,
    logger: logging.Logger,
    preferred: str = "",
    bundle_dir: Path | None = None,
) -> str:
    """UUID sandbox-счёта: из preferred, если уже UUID; иначе первый из get_sandbox_accounts или open_sandbox_account."""
    if bundle_dir is not None:
        apply_local_grpc_ca_bundle_if_present(bundle_dir)
    p = str(preferred or "").strip()
    if p and is_sandbox_uuid_account_id(p):
        logger.info("[PREFLIGHT] sandbox account_id from config (UUID): %s", p)
        return p
    from t_tech.invest import Client

    with Client(token.strip(), target=host.strip()) as client:
        sb = client.sandbox.get_sandbox_accounts()
        accs = list(getattr(sb, "accounts", []) or [])
        for a in accs:
            aid = str(getattr(a, "id", "") or getattr(a, "account_id", "") or "").strip()
            if aid and is_sandbox_uuid_account_id(aid):
                logger.info("[PREFLIGHT] sandbox account_id from get_sandbox_accounts: %s", aid)
                return aid
        opened = client.sandbox.open_sandbox_account()
        aid = str(getattr(opened, "account_id", "") or getattr(opened, "id", "") or "").strip()
        if aid:
            logger.info("[PREFLIGHT] sandbox account_id from open_sandbox_account: %s", aid)
            return aid
    raise RuntimeError("Не удалось получить sandbox account_id (get_sandbox_accounts / open_sandbox_account).")


def load_sandbox_token(base_dir: Path) -> str:
    """Токен из `fix_engine/settings.local.cfg`: одна активная строка `TBankSandboxToken=...`."""
    return read_tinvest_token_from_dir(base_dir)


def verify_market_data_readonly(
    *,
    token: str,
    host: str,
    instrument_id: str,
    logger: logging.Logger,
    bundle_dir: Path | None = None,
) -> str:
    """Проверка read-only: get_last_prices + get_order_book. Бросает исключение при ошибке."""
    if bundle_dir is not None:
        apply_local_grpc_ca_bundle_if_present(bundle_dir)
    from t_tech.invest import Client

    with Client(token, target=host) as client:
        _ = client.market_data.get_last_prices(instrument_id=[instrument_id])
        _ = client.market_data.get_order_book(instrument_id=instrument_id, depth=1)
    logger.info(
        "[PREFLIGHT] T-Invest token OK; get_last_prices + get_order_book succeeded (read-only).",
    )
    return host
