from __future__ import annotations


class NoopExecutionEngine:
    def send_order(self, *args: object, **kwargs: object) -> str:
        raise RuntimeError("Real exchange order routing is removed. Use paper execution only.")

    def cancel_order(self, *args: object, **kwargs: object) -> str:
        raise RuntimeError("Real exchange order routing is removed. Use paper execution only.")

