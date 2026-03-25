"""
Накопление котировок (bid/ask/mid) в SQLite с окном хранения ~N дней.
Для анализа «канала»: обход уровней mid от большей цены к меньшей по сетке тика.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock

from fix_engine.market_data.models import MarketData


def _utc_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


class QuoteHistoryStore:
    def __init__(
        self,
        db_path: str | Path,
        *,
        retention_days: float = 14.0,
        sample_interval_ms: float = 1000.0,
    ) -> None:
        self._db_path = Path(db_path)
        self._retention_days = max(1.0, float(retention_days))
        self._sample_interval_ms = max(50.0, float(sample_interval_ms))
        self._lock = RLock()
        self._last_insert_ms_by_symbol: dict[str, int] = {}
        self._last_purge_mono: float = 0.0
        self._purge_every_sec = 60.0
        self._init_db()

    def _init_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS md_quote_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    ts_ms INTEGER NOT NULL,
                    bid REAL NOT NULL,
                    ask REAL NOT NULL,
                    mid REAL NOT NULL,
                    spread REAL NOT NULL,
                    last_px REAL NOT NULL,
                    bid_size REAL NOT NULL,
                    ask_size REAL NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_md_q_symbol_ts ON md_quote_snapshots(symbol, ts_ms)"
            )

    def _cutoff_ts_ms(self) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._retention_days)
        return _utc_ms(cutoff)

    def _maybe_purge(self, conn: sqlite3.Connection) -> None:
        now = time.monotonic()
        if now - self._last_purge_mono < self._purge_every_sec:
            return
        self._last_purge_mono = now
        conn.execute("DELETE FROM md_quote_snapshots WHERE ts_ms < ?", (self._cutoff_ts_ms(),))

    def on_market_data(self, data: MarketData) -> None:
        sym = data.symbol.upper()
        ts_ms = _utc_ms(data.timestamp)
        with self._lock:
            last = self._last_insert_ms_by_symbol.get(sym, 0)
            if ts_ms - last < self._sample_interval_ms and last > 0:
                return
            self._last_insert_ms_by_symbol[sym] = ts_ms
            mid = float(data.mid_price)
            with sqlite3.connect(self._db_path) as conn:
                self._maybe_purge(conn)
                conn.execute(
                    """
                    INSERT INTO md_quote_snapshots
                    (symbol, ts_ms, bid, ask, mid, spread, last_px, bid_size, ask_size)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        sym,
                        ts_ms,
                        float(data.bid),
                        float(data.ask),
                        mid,
                        float(data.spread),
                        float(data.last),
                        float(data.bid_size),
                        float(data.ask_size),
                    ),
                )
                conn.commit()

    def window_ts_ms(self, days: float | None = None) -> tuple[int, int]:
        """UTC ms [from, to] за последние `days` (по умолчанию retention)."""
        d = float(days) if days is not None else self._retention_days
        to_ms = _utc_ms(datetime.now(timezone.utc))
        from_ms = to_ms - int(d * 86400 * 1000)
        return from_ms, to_ms

    def mid_range(
        self,
        symbol: str,
        *,
        days: float | None = None,
    ) -> tuple[float | None, float | None, int]:
        """min(mid), max(mid), количество строк в окне."""
        sym = symbol.upper()
        lo, hi = self.window_ts_ms(days=days)
        with sqlite3.connect(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT MIN(mid), MAX(mid), COUNT(*)
                FROM md_quote_snapshots
                WHERE symbol = ? AND ts_ms >= ? AND ts_ms <= ?
                """,
                (sym, lo, hi),
            ).fetchone()
        if not row or row[2] == 0:
            return None, None, 0
        return float(row[0]), float(row[1]), int(row[2])

    def price_levels_desc(
        self,
        symbol: str,
        tick_size: float,
        *,
        days: float | None = None,
    ) -> list[tuple[float, int]]:
        """
        Уровни mid, сгруппированные по сетке tick_size, от большей цены к меньшей.
        Возвращает [(уровень, число наблюдений), ...].
        """
        if tick_size <= 0:
            raise ValueError("tick_size must be positive")
        sym = symbol.upper()
        lo, hi = self.window_ts_ms(days=days)
        t = float(tick_size)
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT ROUND(mid / ?) * ? AS lvl, COUNT(*) AS n
                FROM md_quote_snapshots
                WHERE symbol = ? AND ts_ms >= ? AND ts_ms <= ?
                GROUP BY lvl
                ORDER BY lvl DESC
                """,
                (t, t, sym, lo, hi),
            ).fetchall()
        return [(float(r[0]), int(r[1])) for r in rows]

    def iter_channel_high_to_low(
        self,
        symbol: str,
        tick_size: float,
        *,
        days: float | None = None,
    ) -> Iterator[tuple[float, int]]:
        """Итератор по уровням сверху вниз (удобно «ходить» по каналу)."""
        for item in self.price_levels_desc(symbol, tick_size, days=days):
            yield item
