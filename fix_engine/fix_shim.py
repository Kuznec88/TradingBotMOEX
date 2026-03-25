"""Paper-trading FIX-shaped messages without the quickfix native dependency."""

from __future__ import annotations


class SyntheticExecutionReport:
    """Minimal FIX-like ExecutionReport for local simulation (tag get/set + wire-ish string)."""

    __slots__ = ("_fields",)

    def __init__(self, fields: dict[int, str]) -> None:
        self._fields = dict(fields)

    def isSetField(self, tag: int) -> bool:
        return int(tag) in self._fields

    def getField(self, tag: int) -> str:
        return str(self._fields.get(int(tag), ""))

    def toString(self) -> str:
        if not self._fields:
            return ""
        parts = [f"{k}={self._fields[k]}" for k in sorted(self._fields)]
        return "\x01".join(parts) + "\x01"
