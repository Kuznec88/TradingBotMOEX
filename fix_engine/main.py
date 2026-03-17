from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from threading import RLock

import quickfix as fix

from execution_engine import ExecutionEngine
from market_data.market_data_engine import MarketDataEngine
from market_data.models import MarketData
from order_manager import OrderManager
from trade_client import MoexFixApplication


class SimpleStrategy:
    """Very small MA(5) strategy for integration demo."""

    def __init__(self) -> None:
        self.prices_by_symbol: dict[str, list[float]] = {}
        self._latest_signal_by_symbol: dict[str, str] = {}
        self._lock = RLock()

    def on_market_data(self, data: MarketData) -> None:
        with self._lock:
            prices = self.prices_by_symbol.setdefault(data.symbol, [])
            prices.append(data.mid_price)
            if len(prices) <= 5:
                self._latest_signal_by_symbol[data.symbol] = "HOLD"
                return

            avg = sum(prices[-5:]) / 5
            # Use both mid_price and spread for a tiny quality filter.
            if data.mid_price > avg and data.spread >= 0:
                self._latest_signal_by_symbol[data.symbol] = "BUY"
            elif data.mid_price < avg and data.spread >= 0:
                self._latest_signal_by_symbol[data.symbol] = "SELL"
            else:
                self._latest_signal_by_symbol[data.symbol] = "HOLD"

    def pop_signal(self, symbol: str) -> str:
        with self._lock:
            return self._latest_signal_by_symbol.pop(symbol.upper(), "HOLD")


def _safe_get(message: fix.Message, tag: int) -> str:
    return message.getField(tag) if message.isSetField(tag) else ""


def _safe_get_group(group: fix.Group, tag: int) -> str:
    return group.getField(tag) if group.isSetField(tag) else ""


def _parse_market_data_message(
    message: fix.Message,
    market_data_engine: MarketDataEngine,
    logger: logging.Logger,
) -> None:
    msg_type = _safe_get(message.getHeader(), 35)
    if msg_type not in {fix.MsgType_MarketDataSnapshotFullRefresh, fix.MsgType_MarketDataIncrementalRefresh}:
        return

    no_md_entries = int(_safe_get(message, 268) or "0")
    top_symbol = _safe_get(message, 55).upper()
    snapshots: dict[str, dict[str, float | str]] = {}

    # Use generic Group to avoid depending on generated group classes.
    # Delimiter tag:
    # - W (snapshot): 269
    # - X (incremental): 279
    delimiter = 269 if msg_type == fix.MsgType_MarketDataSnapshotFullRefresh else 279
    for idx in range(1, no_md_entries + 1):
        group = fix.Group(268, delimiter)
        message.getGroup(idx, group)

        symbol = (_safe_get_group(group, 55) or top_symbol).upper()
        if not symbol:
            continue

        entry_type = _safe_get_group(group, 269)
        px_str = _safe_get_group(group, 270)
        size_str = _safe_get_group(group, 271)
        if not px_str:
            continue

        px = float(px_str)
        size = float(size_str) if size_str else 0.0
        snapshot = snapshots.setdefault(
            symbol,
            {
                "symbol": symbol,
                "bid": 0.0,
                "ask": 0.0,
                "last": 0.0,
                "volume": 0.0,
            },
        )

        if entry_type == "0":  # bid
            snapshot["bid"] = px
        elif entry_type == "1":  # ask
            snapshot["ask"] = px
        elif entry_type in {"2", "7", "8"}:  # trade/close/open-ish
            snapshot["last"] = px
            snapshot["volume"] = size

    for symbol, snapshot in snapshots.items():
        latest = market_data_engine.get_latest(symbol)
        bid = float(snapshot["bid"]) or (latest.bid if latest else float(snapshot["last"]) or 0.0)
        ask = float(snapshot["ask"]) or (latest.ask if latest else float(snapshot["last"]) or bid)
        last = float(snapshot["last"]) or (latest.last if latest else (bid + ask) / 2)
        volume = float(snapshot["volume"]) or (latest.volume if latest else 0.0)

        market_data_engine.update_market_data(
            {
                "symbol": symbol,
                "bid": bid,
                "ask": ask,
                "last": last,
                "volume": volume,
            }
        )
        logger.info("[LIVE_MD] source=FIX symbol=%s bid=%s ask=%s last=%s", symbol, bid, ask, last)


def setup_logging(base_dir: Path) -> logging.Logger:
    log_dir = base_dir / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("fix_engine")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = logging.FileHandler(log_dir / "fix_client.log", encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    return logger


def run() -> None:
    base_dir = Path(__file__).resolve().parent
    logger = setup_logging(base_dir)
    settings_path = base_dir / "settings.cfg"
    settings = fix.SessionSettings(str(settings_path.resolve()))
    order_manager = OrderManager()
    market_data_engine = MarketDataEngine(logger=logger)

    def on_execution_report(message: fix.Message, source: str) -> None:
        state = order_manager.on_execution_report(message)
        logger.info(
            "[FILL][%s] symbol=%s filled=%s avg_price=%s",
            source,
            state.get("symbol", ""),
            state.get("last_qty", "0"),
            state.get("avg_px", ""),
        )
        logger.info(
            "[STATE] order_id=%s %s -> %s",
            state.get("cl_ord_id", ""),
            state.get("status_old", ""),
            state.get("status_new", ""),
        )

    def on_market_data_message(message: fix.Message, source: str) -> None:
        del source
        _parse_market_data_message(message, market_data_engine, logger)

    app = MoexFixApplication(
        password="1264",
        logger=logger,
        order_manager=order_manager,
        on_execution_report=on_execution_report,
        on_market_data=on_market_data_message,
    )

    store_factory = fix.FileStoreFactory(settings)
    log_factory = fix.FileLogFactory(settings)
    initiator = fix.SocketInitiator(app, store_factory, settings, log_factory)
    engine = ExecutionEngine(order_manager, app.get_trade_session_id, logger)
    strategy = SimpleStrategy()
    market_data_engine.subscribe(strategy.on_market_data)

    try:
        initiator.start()
    except Exception as exc:
        logger.exception("Failed to start FIX initiator: %s", exc)
        return

    logger.info("FIX TRADE + DROP_COPY started. Wait for TRADE onLogon before sending orders.")
    logger.info(
        "Commands:\n"
        "  m SYMBOL SIDE QTY\n"
        "  l SYMBOL SIDE QTY PRICE\n"
        "  c CL_ORD_ID\n"
        "  sig LAST_PRICE SYMBOL QTY\n"
        "  md SYMBOL BID ASK LAST VOLUME [QTY]\n"
        "  pos SYMBOL\n"
        "  open\n"
        "  q"
    )

    try:
        while True:
            command = input("> ").strip()
            if not command:
                continue
            if command.lower() == "q":
                break

            parts = command.split()
            kind = parts[0].lower()
            if kind == "m" and len(parts) == 5:
                _, symbol, side, qty = parts
                try:
                    cl_ord_id = engine.send_order(symbol, side, float(qty), price=None)
                    logger.info("Market order queued, ClOrdID=%s", cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed to send market order: %s", exc)
            elif kind == "l" and len(parts) == 6:
                _, symbol, side, qty, price = parts
                try:
                    cl_ord_id = engine.send_order(symbol, side, float(qty), price=float(price))
                    logger.info("Limit order queued, ClOrdID=%s", cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed to send limit order: %s", exc)
            elif kind == "c" and len(parts) == 2:
                _, cl_ord_id = parts
                try:
                    cancel_id = engine.cancel_order(cl_ord_id)
                    logger.info("Cancel sent. CancelClOrdID=%s", cancel_id)
                except Exception as exc:
                    logger.exception("Failed to cancel order: %s", exc)
            elif kind == "sig" and len(parts) == 4:
                _, last_price, symbol, qty = parts
                try:
                    lp = float(last_price)
                    market_data_engine.update_market_data(
                        {
                            "symbol": symbol,
                            "bid": lp,
                            "ask": lp,
                            "last": lp,
                            "volume": 0,
                        }
                    )
                    signal = strategy.pop_signal(symbol)
                    logger.info("[STRATEGY] symbol=%s last=%s signal=%s", symbol, last_price, signal)
                    if signal in {"BUY", "SELL"}:
                        cl_ord_id = engine.send_order(symbol, signal, float(qty), price=None)
                        logger.info("Signal order sent, ClOrdID=%s", cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed strategy signal handling: %s", exc)
            elif kind == "md" and len(parts) in {6, 7}:
                _, symbol, bid, ask, last, volume, *rest = parts
                try:
                    market_data = market_data_engine.update_market_data(
                        {
                            "symbol": symbol,
                            "bid": float(bid),
                            "ask": float(ask),
                            "last": float(last),
                            "volume": float(volume),
                        }
                    )
                    signal = strategy.pop_signal(symbol)
                    logger.info(
                        "[STRATEGY] symbol=%s mid=%s spread=%s signal=%s",
                        market_data.symbol,
                        round(market_data.mid_price, 6),
                        round(market_data.spread, 6),
                        signal,
                    )
                    if signal in {"BUY", "SELL"}:
                        qty = float(rest[0]) if rest else 1.0
                        cl_ord_id = engine.send_order(symbol, signal, qty, price=None)
                        logger.info("MarketData signal order sent, ClOrdID=%s", cl_ord_id)
                except Exception as exc:
                    logger.exception("Failed market data processing: %s", exc)
            elif kind == "pos" and len(parts) == 2:
                _, symbol = parts
                logger.info("Position %s = %s", symbol, order_manager.get_position(symbol))
            elif kind == "open" and len(parts) == 1:
                open_orders = order_manager.get_open_orders()
                logger.info("Open orders: %s", [o.__dict__ for o in open_orders])
            else:
                logger.warning("Bad command format.")
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        time.sleep(0.2)
        initiator.stop()


if __name__ == "__main__":
    run()
