from __future__ import annotations

# Backward-compatible shim.
# The momentum-only strategy now lives in `fix_engine.strategy.momentum_mm`.

from fix_engine.strategy.momentum_mm import BasicMarketMaker as _MomentumBasicMarketMaker

BasicMarketMaker = _MomentumBasicMarketMaker

__all__ = ["BasicMarketMaker"]

