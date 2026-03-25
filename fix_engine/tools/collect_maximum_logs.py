"""
Максимальный сбор артефактов сессии и БД в каталог log/full_collect_<UTC>/.

Запуск: python tools/collect_maximum_logs.py
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _table_counts(cur: sqlite3.Cursor) -> dict[str, int]:
    out: dict[str, int] = {}
    for (name,) in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall():
        try:
            n = cur.execute(f'SELECT COUNT(1) FROM "{name}"').fetchone()[0]
            out[str(name)] = int(n)
        except sqlite3.Error:
            out[str(name)] = -1
    return out


def _dump_table_json(
    cur: sqlite3.Cursor,
    table: str,
    *,
    limit: int | None = None,
) -> list[dict[str, object]]:
    cols = [r[1] for r in cur.execute(f'PRAGMA table_info("{table}")').fetchall()]
    if not cols:
        return []
    col_list = ", ".join(f'"{c}"' for c in cols)
    sql = f'SELECT {col_list} FROM "{table}"'
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    rows = cur.execute(sql).fetchall()
    return [dict(zip(cols, row)) for row in rows]


def main() -> int:
    ap = argparse.ArgumentParser(description="Собрать максимальный пакет логов и БД в log/full_collect_<UTC>/")
    ap.add_argument("--no-zip", action="store_true", help="Не создавать ZIP рядом с каталогом")
    ap.add_argument(
        "--no-db-copy",
        action="store_true",
        help="Не копировать trade_economics.db (только JSON-дампы таблиц)",
    )
    args = ap.parse_args()

    base = Path(__file__).resolve().parents[1]
    py = sys.executable
    stamp = _utc_stamp()
    out = base / "log" / f"full_collect_{stamp}"
    out.mkdir(parents=True, exist_ok=True)
    db_path = base / "trade_economics.db"
    manifest: dict[str, object] = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "output_dir": str(out),
        "files": [],
    }

    def add_file(p: Path, note: str = "") -> None:
        if p.is_file():
            manifest["files"].append(
                {
                    "path": str(p.relative_to(base)),
                    "bytes": p.stat().st_size,
                    "note": note,
                }
            )

    # 1) Экспорты JSON (вся БД по round trips)
    r1 = subprocess.run(
        [
            py,
            str(base / "tools" / "export_trading_analytics_json.py"),
            str(db_path),
            "--all",
            "-o",
            str(out / "trading_analytics_export_ALL.json"),
        ],
        cwd=str(base),
        capture_output=True,
        text=True,
    )
    add_file(out / "trading_analytics_export_ALL.json", "export_trading_analytics_json --all")
    if r1.returncode != 0:
        (out / "export_trading_analytics_stderr.txt").write_text(r1.stderr or "", encoding="utf-8")

    # 2) Сессионные копии из log/ (если есть)
    for name in (
        "trading_analytics_export.json",
        "session_metrics_collect.json",
        "strategy_failure_report.json",
        "session_start_marker.txt",
    ):
        src = base / "log" / name
        if src.is_file():
            dst = out / f"copy_{name}"
            shutil.copy2(src, dst)
            add_file(dst, f"copy from log/{name}")

    # 3) Метрики сессии (как в export_session_metrics)
    try:
        tools_dir = str(base / "tools")
        if tools_dir not in sys.path:
            sys.path.insert(0, tools_dir)
        from export_session_metrics import collect_session_metrics

        _write_json(out / "session_metrics_filtered.json", collect_session_metrics(base))
    except Exception as exc:
        _write_json(out / "session_metrics_filtered_error.json", {"error": str(exc)})

    # 4) SQLite: схема, счётчики, дампы ключевых таблиц
    if db_path.is_file():
        con = sqlite3.connect(str(db_path))
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        schema_rows = cur.execute(
            "SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name"
        ).fetchall()
        _write_json(
            out / "sqlite_schema.json",
            [{"type": r[0], "name": r[1], "sql": r[2]} for r in schema_rows],
        )
        _write_json(out / "sqlite_table_row_counts.json", _table_counts(cur))

        for tbl, lim in (
            ("learning_patch_effects", None),
            ("round_trip_analytics", 5000),
            ("trade_analytics", 5000),
            ("trade_economics", 5000),
            ("cancel_analytics", 5000),
            ("trade_observability", 3000),
            ("round_trip_observability", 3000),
            ("executed_entry_observability", 3000),
            ("entry_decisions", 3000),
        ):
            try:
                rows = _dump_table_json(cur, tbl, limit=lim)
                _write_json(out / f"sqlite_dump_{tbl}.json", {"table": tbl, "rows": rows, "row_count": len(rows)})
            except sqlite3.Error as e:
                _write_json(out / f"sqlite_dump_{tbl}_error.json", {"table": tbl, "error": str(e)})
        con.close()
        if not args.no_db_copy:
            snap = out / "trade_economics_snapshot.db"
            max_db = 500 * 1024 * 1024
            sz = db_path.stat().st_size
            if sz <= max_db:
                shutil.copy2(db_path, snap)
                add_file(snap, "полная копия SQLite")
            else:
                _write_json(
                    out / "db_snapshot_skipped.json",
                    {"reason": "db larger than limit", "bytes": sz, "limit_bytes": max_db},
                )

    # 5) Конфиги
    for rel in (
        "settings.cfg",
    ):
        p = base / rel
        if p.is_file():
            dst = out / rel.replace("/", "_")
            shutil.copy2(p, dst)
            add_file(dst, rel)
    runtime = base / "settings.runtime.cfg"
    if runtime.is_file():
        shutil.copy2(runtime, out / "settings.runtime.cfg")
        add_file(out / "settings.runtime.cfg", "optional overrides")

    # 6) Маркеры и текстовые логи из log/
    logd = base / "log"
    for p in sorted(logd.glob("*.marker")):
        dst = out / f"marker_{p.name}"
        shutil.copy2(p, dst)
        add_file(dst, "session marker")

    for log_name in ("fix_client.log",):
        p = logd / log_name
        if p.is_file():
            data = p.read_bytes()
            max_b = 2_000_000
            if len(data) > max_b:
                (out / f"{log_name}.tail_{max_b}_bytes.txt").write_bytes(data[-max_b:])
                add_file(out / f"{log_name}.tail_{max_b}_bytes.txt", f"tail of {log_name}")
            else:
                shutil.copy2(p, out / log_name)
                add_file(out / log_name, "full log")

    for pat in ("run_*_stdout.log", "run_*_stderr.log"):
        for p in logd.glob(pat):
            try:
                shutil.copy2(p, out / p.name)
                add_file(out / p.name, "run capture")
            except OSError:
                pass

    # 7) offline_sqlite_summary
    r3 = subprocess.run(
        [py, str(base / "tools" / "offline_sqlite_summary.py"), str(db_path)],
        cwd=str(base),
        capture_output=True,
        text=True,
    )
    (out / "offline_sqlite_summary_stdout.json").write_text(r3.stdout or "{}", encoding="utf-8")
    add_file(out / "offline_sqlite_summary_stdout.json", "offline_sqlite_summary")

    # 8) Манифест и ZIP
    _write_json(out / "MANIFEST.json", manifest)
    zip_path: str | None = None
    if not args.no_zip:
        zip_path = shutil.make_archive(str(out), "zip", root_dir=str(out.parent), base_dir=out.name)

    out_obj: dict[str, object] = {
        "ok": True,
        "dir": str(out),
        "manifest": str(out / "MANIFEST.json"),
    }
    if zip_path:
        out_obj["zip"] = zip_path
    print(json.dumps(out_obj, indent=2, ensure_ascii=False))
    return 0 if (r1.returncode == 0 and r2.returncode == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
