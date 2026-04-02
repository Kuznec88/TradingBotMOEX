"""
Собирает structured-логи текущей (или указанной) сессии из файла терминала Cursor
в один JSON-файл (массив объектов).

По умолчанию берёт последний изменённый файл из `.cursor/.../terminals/*.txt`,
парсит YAML-подобный header и далее JSON-lines.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TerminalHeader:
    pid: int | None
    cwd: str | None
    command: str | None
    started_at: str | None


def _parse_header(lines: list[str]) -> tuple[TerminalHeader, int]:
    # Header format:
    # ---
    # pid: 123
    # cwd: "..."
    # command: "..."
    # started_at: 2026-...
    # running_for_ms: ...
    # ---
    if not lines or lines[0].strip() != "---":
        return TerminalHeader(None, None, None, None), 0
    pid = None
    cwd = None
    command = None
    started_at = None
    i = 1
    while i < len(lines):
        if lines[i].strip() == "---":
            return TerminalHeader(pid, cwd, command, started_at), i + 1
        raw = lines[i]
        if ":" in raw:
            k, v = raw.split(":", 1)
            k = k.strip()
            v = v.strip()
            if v.startswith('"') and v.endswith('"') and len(v) >= 2:
                v = v[1:-1]
            if k == "pid":
                try:
                    pid = int(v)
                except Exception:
                    pid = None
            elif k == "cwd":
                cwd = v
            elif k == "command":
                command = v
            elif k == "started_at":
                started_at = v
        i += 1
    return TerminalHeader(pid, cwd, command, started_at), i


def _json_lines(path: Path, start_idx: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    raw_lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    for ln in raw_lines[start_idx:]:
        s = ln.strip()
        if not s.startswith("{") or not s.endswith("}"):
            continue
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                out.append(obj)
        except Exception:
            continue
    return out


def export_from_terminal_file(term_path: Path, out_dir: Path) -> Path:
    lines = term_path.read_text(encoding="utf-8", errors="replace").splitlines()
    header, start_idx = _parse_header(lines)
    events = _json_lines(term_path, start_idx)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"session_live_export_{ts}.json"
    payload = {
        "source_terminal_file": str(term_path),
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "header": {
            "pid": header.pid,
            "cwd": header.cwd,
            "command": header.command,
            "started_at": header.started_at,
        },
        "events": events,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def main() -> None:
    repo = Path(__file__).resolve().parents[2]
    # default Cursor terminals dir for this workspace
    terminals_dir = Path.home() / ".cursor" / "projects" / "c-Users-Admin-Python-Projects-TradingBotMOEX" / "terminals"
    if not terminals_dir.exists():
        raise SystemExit(f"terminals dir not found: {terminals_dir}")
    term_files = sorted(terminals_dir.glob("*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not term_files:
        raise SystemExit(f"no terminal files in: {terminals_dir}")
    term_path = term_files[0]
    out_path = export_from_terminal_file(term_path, repo / "fix_engine" / "log")
    print(str(out_path))


if __name__ == "__main__":
    main()

