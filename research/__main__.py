"""Deprecated entrypoint."""

from __future__ import annotations

import sys


def main() -> int:
    sys.stderr.write(
        "Use: python -m quant.research.run_pipeline ...  or  python -m cli.research ...\n"
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
