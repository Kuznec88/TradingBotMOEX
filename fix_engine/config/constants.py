"""Значения по умолчанию для слоя market data (вынесены из кода в один модуль)."""

from __future__ import annotations

# MarketDataEngine: периодичность снапшот-логов и порог «скачка» mid относительно предыдущего.
MD_SNAPSHOT_LOG_INTERVAL_SEC: float = 5.0
MD_MID_SPIKE_THRESHOLD_RATIO: float = 0.01

# MdHealthMonitor
MD_HEALTH_POLL_INTERVAL_SEC: float = 5.0
