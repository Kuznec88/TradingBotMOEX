from __future__ import annotations


class NoopExecutionEngine:
    def send_order(self, *args: object, **kwargs: object) -> str:
        raise RuntimeError(
            "NoopExecutionEngine: use ExecutionMode=PAPER_REAL_MARKET (paper) or LIVE with Tinkoff engines."
        )

    def cancel_order(self, *args: object, **kwargs: object) -> str:
        raise RuntimeError(
            "NoopExecutionEngine: use ExecutionMode=PAPER_REAL_MARKET (paper) or LIVE with Tinkoff engines."
        )
