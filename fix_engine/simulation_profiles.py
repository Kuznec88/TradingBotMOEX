from __future__ import annotations

from typing import Mapping

SIM_PROFILE_REALISTIC = "REALISTIC"
SIM_PROFILE_STRESS = "STRESS"
SIM_PROFILE_EXTREME = "EXTREME"
SIM_PROFILE_ALLOWED = {
    SIM_PROFILE_REALISTIC,
    SIM_PROFILE_STRESS,
    SIM_PROFILE_EXTREME,
}


def normalize_simulation_profile(value: str | None) -> str:
    profile = (value or "").strip().upper()
    if profile in SIM_PROFILE_ALLOWED:
        return profile
    return SIM_PROFILE_STRESS


def simulation_profile_overrides(profile: str) -> dict[str, float | int]:
    p = normalize_simulation_profile(profile)
    if p == SIM_PROFILE_REALISTIC:
        return {
            "simulation_slippage_bps": 1.0,
            "simulation_slippage_max_bps": 5.0,
            "simulation_volatility_slippage_multiplier": 1.2,
            "simulation_latency_network_ms": 30,
            "simulation_latency_exchange_ms": 40,
            "simulation_latency_jitter_ms": 10,
            "simulation_adverse_selection_bias": 0.25,
        }
    if p == SIM_PROFILE_EXTREME:
        return {
            "simulation_slippage_bps": 12.0,
            "simulation_slippage_max_bps": 50.0,
            "simulation_volatility_slippage_multiplier": 2.8,
            "simulation_latency_network_ms": 180,
            "simulation_latency_exchange_ms": 90,
            "simulation_latency_jitter_ms": 30,
            "simulation_adverse_selection_bias": 0.9,
        }
    return {}


def resolve_simulation_profile(
    *,
    base_params: Mapping[str, float | int],
    configured_profile: str | None,
    runtime_override_profile: str | None = None,
) -> tuple[str, dict[str, float | int], str]:
    source = "config"
    active_profile = normalize_simulation_profile(configured_profile)
    override = (runtime_override_profile or "").strip()
    if override:
        active_profile = normalize_simulation_profile(override)
        source = "runtime_override"
    resolved = dict(base_params)
    resolved.update(simulation_profile_overrides(active_profile))
    return active_profile, resolved, source
