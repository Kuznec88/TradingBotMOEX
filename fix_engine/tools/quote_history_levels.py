"""
Печать min/max mid и уровней цены (сверху вниз) из quote_history.db.
Запуск из fix_engine: python tools/quote_history_levels.py SBER 0.01
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> None:
    fix_engine_dir = Path(__file__).resolve().parents[1]
    if str(fix_engine_dir) not in sys.path:
        sys.path.insert(0, str(fix_engine_dir))
    if str(fix_engine_dir.parent) not in sys.path:
        sys.path.insert(0, str(fix_engine_dir.parent))

    ap = argparse.ArgumentParser(description="Уровни mid по истории котировок")
    ap.add_argument("symbol", nargs="?", default="SBER", help="тикер")
    ap.add_argument("tick", nargs="?", type=float, default=0.01, help="шаг сетки (как MMTickSize)")
    ap.add_argument("--days", type=float, default=14.0, help="окно, дней")
    args = ap.parse_args()

    from fix_engine.tools.common_cfg_dir import read_cfg_value_from_dir

    db = fix_engine_dir / read_cfg_value_from_dir(fix_engine_dir, "QuoteHistoryDbPath", "quote_history.db")
    if not db.exists():
        print(f"Нет файла {db}; дождитесь накопления или запустите main.", file=sys.stderr)
        sys.exit(1)

    from quote_history_store import QuoteHistoryStore

    st = QuoteHistoryStore(db, retention_days=args.days, sample_interval_ms=1.0)
    sym = args.symbol.upper()
    lo, hi, n = st.mid_range(sym, days=args.days)
    print(f"symbol={sym} rows={n} min_mid={lo} max_mid={hi} days={args.days}")
    if n == 0:
        return
    levels = st.price_levels_desc(sym, args.tick, days=args.days)
    print(f"levels (tick={args.tick}, high -> low), count={len(levels)}")
    for i, (px, cnt) in enumerate(levels[:50]):
        print(f"  {i + 1:3d}  {px:12.6f}  n={cnt}")


if __name__ == "__main__":
    main()
