from __future__ import annotations

import argparse
import ast
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Finding:
    kind: str
    file: str
    line: int
    message: str


def _iter_py_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for base, dirs, files in os.walk(root):
        # Skip typical noise
        dirs[:] = [d for d in dirs if d not in {".git", ".venv", "venv", "__pycache__", ".mypy_cache", ".pytest_cache"}]
        for fn in files:
            if fn.endswith(".py"):
                out.append(Path(base) / fn)
    return sorted(out)


def _read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="replace")


def _safe_parse(p: Path) -> ast.AST | None:
    try:
        return ast.parse(_read_text(p), filename=str(p))
    except SyntaxError:
        return None


def _is_main_guard_if(node: ast.If) -> bool:
    """
    Detect `if __name__ == "__main__": ...` to avoid reporting it as a duplicate.
    """
    test = node.test
    if not isinstance(test, ast.Compare):
        return False
    if len(test.ops) != 1 or len(test.comparators) != 1:
        return False
    if not isinstance(test.ops[0], ast.Eq):
        return False
    left = test.left
    right = test.comparators[0]
    if not (isinstance(left, ast.Name) and left.id == "__name__"):
        return False
    return isinstance(right, ast.Constant) and right.value == "__main__"


def check_for_duplicates(root: Path) -> list[Finding]:
    """
    Heuristic duplicate detector:
    - hashes normalized AST dumps of If/For/While blocks
    - reports blocks repeated 3+ times across the project
    """
    buckets: dict[str, list[tuple[Path, int]]] = defaultdict(list)
    for p in _iter_py_files(root):
        tree = _safe_parse(p)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.If, ast.For, ast.While)):
                if isinstance(node, ast.If) and _is_main_guard_if(node):
                    continue
                # normalize: strip lineno/col and contexts by dumping without attributes
                key = ast.dump(node, annotate_fields=True, include_attributes=False)
                lineno = int(getattr(node, "lineno", 0) or 0)
                buckets[key].append((p, lineno))

    findings: list[Finding] = []
    for key, locs in buckets.items():
        if len(locs) < 3:
            continue
        sample = locs[0]
        msg = f"Duplicate block repeated {len(locs)}x; sample at {sample[0].as_posix()}:{sample[1]}"
        for p, ln in locs:
            findings.append(Finding(kind="duplicate_block", file=str(p), line=int(ln), message=msg))
    return findings


def check_obsolete_filters(root: Path) -> list[Finding]:
    """
    Flags filters that appear as dead/obsolete by name or by always-true/false condition.
    This is intentionally conservative (low false positives).
    """
    findings: list[Finding] = []
    obsolete_markers = {
        "medium_requires_tight_spread",
    }
    for p in _iter_py_files(root):
        # Avoid self-reporting: this script intentionally contains these tokens.
        if p.name == "project_cleanliness_report.py":
            continue
        src = _read_text(p)
        for i, line in enumerate(src.splitlines(), start=1):
            for m in obsolete_markers:
                if m in line:
                    findings.append(
                        Finding(
                            kind="obsolete_filter_marker",
                            file=str(p),
                            line=i,
                            message=f"Marker '{m}' found; review if still affects logic.",
                        )
                    )
        tree = _safe_parse(p)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.If):
                test = node.test
                if isinstance(test, ast.Constant) and isinstance(test.value, bool):
                    findings.append(
                        Finding(
                            kind="obsolete_filter_constant_if",
                            file=str(p),
                            line=int(getattr(node, "lineno", 0) or 0),
                            message=f"If condition is constant {test.value!r}; likely dead code.",
                        )
                    )
    return findings


def check_unused_variables(root: Path) -> list[Finding]:
    """
    Module-level unused assignment detector:
    - tracks names assigned at module scope only
    - checks if those names appear anywhere in the AST as `Load` (including inside functions)
    - ignores names exported via `__all__`

    This is still heuristic (no cross-module import analysis), but it avoids the
    biggest false positives from skipping function bodies.
    """
    findings: list[Finding] = []

    for p in _iter_py_files(root):
        tree = _safe_parse(p)
        if tree is None:
            continue
        ignore: set[str] = set()
        # Prefer not to remove exported symbols (`__all__` is an explicit contract).
        for stmt in tree.body:
            if isinstance(stmt, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "__all__" for t in stmt.targets):
                if isinstance(stmt.value, (ast.List, ast.Tuple)):
                    for elt in stmt.value.elts:
                        if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                            ignore.add(str(elt.value))

        assigned: dict[str, list[int]] = defaultdict(list)
        for stmt in tree.body:
            if isinstance(stmt, ast.Assign):
                for t in stmt.targets:
                    if isinstance(t, ast.Name):
                        assigned[t.id].append(int(getattr(stmt, "lineno", 0) or 0))
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                assigned[stmt.target.id].append(int(getattr(stmt, "lineno", 0) or 0))

        loaded: set[str] = {
            node.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
        }

        for name, lines in assigned.items():
            if name.startswith("_"):
                continue
            if name in ignore:
                continue
            if name not in loaded:
                for ln in lines:
                    findings.append(
                        Finding(
                            kind="unused_module_variable",
                            file=str(p),
                            line=int(ln),
                            message=f"Assigned '{name}' at module scope but never used (heuristic).",
                        )
                    )
    return findings


def check_debug_logs(root: Path) -> list[Finding]:
    """
    Flags debug-style logs that look like temporary instrumentation.
    """
    findings: list[Finding] = []
    needles = ("[DEBUG]", "OVERRIDE_CHECK", "SCORE_CHECK")
    for p in _iter_py_files(root):
        src = _read_text(p)
        for i, line in enumerate(src.splitlines(), start=1):
            if "logger." in line and any(n in line for n in needles):
                findings.append(
                    Finding(
                        kind="debug_log",
                        file=str(p),
                        line=i,
                        message="Debug-style log line; consider removing or downgrading.",
                    )
                )
    return findings


def generate_cleanliness_report(root: Path) -> dict[str, object]:
    all_findings: list[Finding] = []
    all_findings.extend(check_for_duplicates(root))
    all_findings.extend(check_obsolete_filters(root))
    all_findings.extend(check_unused_variables(root))
    all_findings.extend(check_debug_logs(root))

    counts = Counter(f.kind for f in all_findings)
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for f in sorted(all_findings, key=lambda x: (x.kind, x.file, x.line)):
        grouped[f.kind].append({"file": f.file, "line": f.line, "message": f.message})

    return {
        "root": str(root),
        "summary": dict(counts),
        "findings": dict(grouped),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Project cleanliness report (heuristic).")
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[1]), help="Project root (default: fix_engine/..)")
    ap.add_argument("--out", default="", help="Output file (json). If empty, prints to stdout.")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    report = generate_cleanliness_report(root)
    payload = json.dumps(report, ensure_ascii=False, indent=2)

    if args.out:
        Path(args.out).write_text(payload, encoding="utf-8")
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

