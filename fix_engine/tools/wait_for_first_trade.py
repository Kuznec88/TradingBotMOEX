r"""
Ждёт первую сделку в LIVE-сессии, читая JSON-lines лог из terminal file.

Срабатывает на одно из событий:
- [MM][ENTRY_EXECUTE]
- [MM][TRADE_OPEN]
- [TBANK][ORDERS] post_order OK

Запуск:
  python fix_engine/tools/wait_for_first_trade.py --terminal-file "...\terminals\867063.txt"
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path


PATTERNS = (
    "[MM][ENTRY_EXECUTE]",
    "[MM][TRADE_OPEN]",
    "[TBANK][ORDERS] post_order OK",
)


def _is_json_line(s: str) -> bool:
    s = s.strip()
    return s.startswith("{") and s.endswith("}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--terminal-file", type=str, required=True)
    ap.add_argument("--poll-ms", type=int, default=500)
    args = ap.parse_args()

    p = Path(args.terminal_file)
    if not p.is_file():
        raise SystemExit(f"terminal file not found: {p}")

    # Seek near end; then follow.
    last_size = 0
    try:
        last_size = p.stat().st_size
    except Exception:
        pass

    while True:
        try:
            size = p.stat().st_size
            if size < last_size:
                last_size = 0
            if size == last_size:
                time.sleep(max(0.05, args.poll_ms / 1000.0))
                continue

            with p.open("r", encoding="utf-8", errors="replace") as f:
                f.seek(last_size)
                chunk = f.read()
                last_size = size
        except Exception:
            time.sleep(max(0.1, args.poll_ms / 1000.0))
            continue

        for ln in chunk.splitlines():
            if not _is_json_line(ln):
                continue
            try:
                obj = json.loads(ln)
            except Exception:
                continue
            msg = str(obj.get("message") or "")
            if any(pat in msg for pat in PATTERNS):
                print(json.dumps(obj, ensure_ascii=False))
                return 0


if __name__ == "__main__":
    raise SystemExit(main())

