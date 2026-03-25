from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Callable


def _quotation_to_float(value: object | None) -> float:
    if value is None:
        return 0.0
    units = float(getattr(value, "units", 0) or 0)
    nano = float(getattr(value, "nano", 0) or 0)
    return units + nano / 1_000_000_000.0


def _to_datetime(value: object | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    to_dt = getattr(value, "ToDatetime", None)
    if callable(to_dt):
        dt = to_dt()
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    seconds = getattr(value, "seconds", None)
    nanos = getattr(value, "nanos", None)
    if seconds is not None:
        base = datetime.fromtimestamp(float(seconds), tz=timezone.utc)
        if nanos is not None:
            base = base + timedelta(microseconds=float(nanos) / 1000.0)
        return base
    return datetime.now(timezone.utc)


def _first_level_price_and_size(levels: object | None) -> tuple[float, float]:
    if not levels:
        return 0.0, 0.0
    first = levels[0]
    price = _quotation_to_float(getattr(first, "price", None))
    qty = float(getattr(first, "quantity", 0.0) or 0.0)
    return price, qty


def _top_levels_total_size(levels: object | None, depth: int) -> float:
    if not levels:
        return 0.0
    n = max(1, int(depth))
    total = 0.0
    for lvl in list(levels)[:n]:
        total += float(getattr(lvl, "quantity", 0.0) or 0.0)
    return float(total)


def _event_kind(event: object) -> str:
    which = getattr(event, "WhichOneof", None)
    if callable(which):
        for oneof_name in ("payload", "market_data", "event"):
            try:
                kind = which(oneof_name)
                if kind:
                    return str(kind)
            except Exception:
                continue
    for name in ("orderbook", "last_price", "trade", "ping", "trading_status"):
        try:
            if getattr(event, name, None) is not None:
                return name
        except Exception:
            continue
    return "unknown"


def run_tbank_sandbox_market_data(
    *,
    token: str,
    host: str,
    instrument_id: str,
    symbol: str,
    orderbook_depth: int,
    include_trades: bool,
    on_raw_market_data: Callable[[dict[str, object]], None],
    logger: logging.Logger,
    run_duration_sec: int = 0,
    reconnect_initial_delay_sec: float = 1.0,
    reconnect_max_delay_sec: float = 60.0,
    stale_reconnect_sec: float = 90.0,
) -> None:
    # Keep the consumer loop non-blocking: slow handlers can cause the server to drop MarketDataStream.
    try:
        from t_tech.invest import Client
        from t_tech.invest.schemas import (
            LastPriceInstrument,
            MarketDataRequest,
            OrderBookInstrument,
            SubscribeLastPriceRequest,
            SubscribeOrderBookRequest,
            SubscribeTradesRequest,
            SubscriptionAction,
            TradeInstrument,
        )
    except Exception as exc:
        raise RuntimeError(
            "t-tech-investments is required. Install with: "
            "pip install t-tech-investments "
            "--index-url https://opensource.tbank.ru/api/v4/projects/238/packages/pypi/simple"
        ) from exc

    if not token.strip():
        raise RuntimeError("TBank token is empty. Set TBankSandboxToken or TINKOFF_TOKEN / TINKOFF_SANDBOX_TOKEN.")
    if not instrument_id.strip():
        raise RuntimeError("TBank instrument id/figi is empty. Set TBankInstrumentId in settings.")

    start_ts = time.monotonic()
    stop_after = int(run_duration_sec)
    reconnect_delay = max(0.1, float(reconnect_initial_delay_sec))
    reconnect_cap = max(reconnect_delay, float(reconnect_max_delay_sec))

    def _duration_reached() -> bool:
        return stop_after > 0 and (time.monotonic() - start_ts) >= stop_after

    def _sleep_reconnect() -> bool:
        """Sleep up to reconnect_delay; return True if run should exit (duration reached)."""
        if _duration_reached():
            return True
        if stop_after > 0:
            remaining = stop_after - (time.monotonic() - start_ts)
            if remaining <= 0:
                return True
            time.sleep(min(reconnect_delay, remaining))
        else:
            time.sleep(reconnect_delay)
        return _duration_reached()

    active_orderbook_depth = [max(1, int(orderbook_depth))]

    def _mk_orderbook_instr() -> object:
        try:
            return OrderBookInstrument(instrument_id=instrument_id, depth=active_orderbook_depth[0])
        except TypeError:
            return OrderBookInstrument(figi=instrument_id, depth=active_orderbook_depth[0])

    def _mk_last_price_instr() -> object:
        try:
            return LastPriceInstrument(instrument_id=instrument_id)
        except TypeError:
            return LastPriceInstrument(figi=instrument_id)

    def _mk_trade_instr() -> object:
        try:
            return TradeInstrument(instrument_id=instrument_id)
        except TypeError:
            return TradeInstrument(figi=instrument_id)

    def request_iterator() -> object:
        yield MarketDataRequest(
            subscribe_order_book_request=SubscribeOrderBookRequest(
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=[_mk_orderbook_instr()],
            )
        )
        yield MarketDataRequest(
            subscribe_last_price_request=SubscribeLastPriceRequest(
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=[_mk_last_price_instr()],
            )
        )
        if include_trades:
            try:
                yield MarketDataRequest(
                    subscribe_trades_request=SubscribeTradesRequest(
                        subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                        instruments=[_mk_trade_instr()],
                    )
                )
            except Exception:
                logger.warning(
                    "[TBANK][MD] subscribe_trades_request is not available in this SDK version; continuing without it."
                )
        while True:
            time.sleep(5.0)
            yield MarketDataRequest()

    # Timestamp of the last received stream event and last successful MD update.
    last_event_mono = [time.monotonic()]
    last_md_update_mono = [time.monotonic()]
    seen_event_kinds: set[str] = set()

    logger.info(
        "[TBANK][MD] connecting host=%s instrument_id=%s symbol=%s depth=%s include_trades=%s "
        "reconnect_delay=%.1f..%.1fs stale_reconnect=%s",
        host,
        instrument_id,
        symbol,
        max(1, int(orderbook_depth)),
        include_trades,
        reconnect_initial_delay_sec,
        reconnect_max_delay_sec,
        f"{stale_reconnect_sec:g}s" if stale_reconnect_sec > 0 else "off",
    )
    while True:
        if _duration_reached():
            logger.info("[TBANK][MD] run duration reached (%ss), stopping feed loop.", stop_after)
            return
        try:
            with Client(token=token, target=host) as client:
                last_event_mono[0] = time.monotonic()
                last_md_update_mono[0] = time.monotonic()
                stream_fn = client.market_data_stream.market_data_stream
                stream = stream_fn(request_iterator())
                need_reconnect_after_depth_fallback = False
                for event in stream:
                    if _duration_reached():
                        logger.info("[TBANK][MD] run duration reached (%ss), stopping feed loop.", stop_after)
                        return

                    reconnect_delay = max(0.1, float(reconnect_initial_delay_sec))
                    last_event_mono[0] = time.monotonic()

                    kind = _event_kind(event)
                    if kind not in seen_event_kinds:
                        seen_event_kinds.add(kind)
                        logger.info("[TBANK][MD] first_event_kind=%s", kind)
                        if kind == "unknown":
                            logger.info("[TBANK][MD] unknown_event_payload=%s", event)

                    sub_ob = getattr(event, "subscribe_order_book_response", None)
                    if sub_ob is not None:
                        subs = list(getattr(sub_ob, "order_book_subscriptions", []) or [])
                        for sub in subs:
                            status_obj = getattr(sub, "subscription_status", None)
                            status_name = str(getattr(status_obj, "name", status_obj))
                            if "DEPTH_IS_INVALID" in status_name and active_orderbook_depth[0] > 1:
                                prev_depth = active_orderbook_depth[0]
                                active_orderbook_depth[0] = 1
                                need_reconnect_after_depth_fallback = True
                                logger.warning(
                                    "[TBANK][MD] depth=%s is invalid for instrument=%s; fallback depth=1 and reconnect",
                                    prev_depth,
                                    instrument_id,
                                )
                                break
                        if need_reconnect_after_depth_fallback:
                            break

                    orderbook = getattr(event, "orderbook", None) or getattr(event, "order_book", None)
                    if orderbook is not None:
                        bids = getattr(orderbook, "bids", None)
                        asks = getattr(orderbook, "asks", None)
                        bid, bid_size_l1 = _first_level_price_and_size(bids)
                        ask, ask_size_l1 = _first_level_price_and_size(asks)
                        # Keep best prices from L1, but use aggregated top-N sizes as volume signal.
                        bid_size = _top_levels_total_size(bids, orderbook_depth)
                        ask_size = _top_levels_total_size(asks, orderbook_depth)
                        if bid_size <= 0.0 and ask_size <= 0.0:
                            bid_size = bid_size_l1
                            ask_size = ask_size_l1
                        if bid > 0 and ask > 0:
                            on_raw_market_data(
                                {
                                    "symbol": symbol.upper(),
                                    "bid": bid,
                                    "ask": ask,
                                    "last": (bid + ask) / 2.0,
                                    "volume": float(bid_size + ask_size),
                                    "bid_size": bid_size,
                                    "ask_size": ask_size,
                                    "timestamp": _to_datetime(getattr(orderbook, "time", None)),
                                }
                            )
                            last_md_update_mono[0] = time.monotonic()
                        continue

                    last_price = getattr(event, "last_price", None) or getattr(event, "lastprice", None)
                    if last_price is not None:
                        px = _quotation_to_float(getattr(last_price, "price", None))
                        if px > 0:
                            on_raw_market_data(
                                {
                                    "symbol": symbol.upper(),
                                    "bid": px,
                                    "ask": px,
                                    "last": px,
                                    "volume": 0.0,
                                    "bid_size": 0.0,
                                    "ask_size": 0.0,
                                    "timestamp": _to_datetime(getattr(last_price, "time", None)),
                                }
                            )
                            last_md_update_mono[0] = time.monotonic()
                if need_reconnect_after_depth_fallback:
                    logger.warning(
                        "[TBANK][MD] reconnecting after order-book depth fallback depth=%s",
                        active_orderbook_depth[0],
                    )
                md_age = time.monotonic() - last_md_update_mono[0]
                if stale_reconnect_sec > 0 and md_age > float(stale_reconnect_sec):
                    logger.warning(
                        "[TBANK][MD] no market-data update for %.0fs (>%ss); reconnecting stream",
                        md_age,
                        float(stale_reconnect_sec),
                    )
            logger.warning(
                "[TBANK][MD] market_data_stream ended; reconnecting in %.1fs (max %.1fs)",
                reconnect_delay,
                reconnect_cap,
            )
        except Exception as exc:
            logger.warning(
                "[TBANK][MD] stream error, reconnecting in %.1fs (max %.1fs): %s",
                reconnect_delay,
                reconnect_cap,
                exc,
            )
        if _sleep_reconnect():
            logger.info("[TBANK][MD] run duration reached (%ss), stopping feed loop.", stop_after)
            return
        reconnect_delay = min(reconnect_cap, max(0.1, reconnect_delay * 2.0))
