"""Параметры SwingBreakoutParams из settings*.cfg."""

from __future__ import annotations

from pathlib import Path

from fix_engine.config.settings import (
    read_bool_merged,
    read_float_merged,
    read_int_merged,
    read_str_merged,
)
from fix_engine.strategy.swing_breakout import SwingBreakoutParams

_READERS = {
    "int": read_int_merged,
    "float": read_float_merged,
    "bool": read_bool_merged,
    "str": read_str_merged,
}

# Поле SwingBreakoutParams -> (тип, ключ в settings.cfg)
SWING_SETTING_KEYS: dict[str, tuple[str, str]] = {
    "n_levels": ("int", "SwingNLevels"),
    "k_volume": ("float", "SwingKVolume"),
    "ma_period": ("int", "SwingMAPeriod"),
    "max_entry_spread_ticks": ("float", "SwingMaxEntrySpreadTicks"),
    "chop_window_bars": ("int", "SwingChopWindowBars"),
    "chop_max_range_ticks": ("float", "SwingChopMaxRangeTicks"),
    "signal_on_close_only": ("bool", "SwingSignalOnCloseOnly"),
    "take_profit_rub": ("float", "SwingTpRub"),
    "stop_loss_rub": ("float", "SwingSlRub"),
    "rub_per_point": ("float", "SwingRubPerPoint"),
    "slippage_ticks": ("int", "SwingSlippageTicks"),
    "commission_rate": ("float", "SwingCommissionRate"),
    "notional_scale": ("float", "SwingNotionalScale"),
    "trailing_activation_rub": ("float", "SwingTrailingActivationRub"),
    "trailing_gap_rub": ("float", "SwingTrailingGapRub"),
    "use_retest_fsm": ("bool", "SwingUseRetestFsm"),
    "atr_period": ("int", "SwingAtrPeriod"),
    "atr_min_ticks": ("float", "SwingAtrMinTicks"),
    "atr_min_price": ("float", "SwingAtrMinPrice"),
    "retest_touch_epsilon_ticks": ("float", "SwingRetestTouchEpsilonTicks"),
    "retest_max_penetration_ticks": ("float", "SwingRetestMaxPenetrationTicks"),
    "volume_spike_required": ("bool", "SwingVolumeSpikeRequired"),
    "volume_directional": ("bool", "SwingVolumeDirectional"),
    "htf_resample_rule": ("str", "SwingHtfResampleRule"),
    "htf_sma_period": ("int", "SwingHtfSmaPeriod"),
    "htf_hh_hl_lag": ("int", "SwingHtfHhHlLag"),
    "risk_reward_multiple": ("float", "SwingRiskRewardMultiple"),
    "stop_buffer_ticks": ("float", "SwingStopBufferTicks"),
    "trail_after_r_multiple": ("float", "SwingTrailAfterRMultiple"),
    "trail_gap_r_multiple": ("float", "SwingTrailGapRMultiple"),
    "commission_min_profit_mult": ("float", "SwingCommissionMinProfitMult"),
    "limit_order_timeout_bars": ("int", "SwingLimitOrderTimeoutBars"),
    "impulse_k": ("float", "SwingImpulseK"),
    "impulse_close_top_range_frac": ("float", "SwingImpulseCloseTopRangeFrac"),
    "impulse_body_vs_shadow": ("bool", "SwingImpulseBodyVsShadow"),
    "retest_max_depth_ticks": ("float", "SwingRetestMaxDepthTicks"),
    "retest_max_bars": ("int", "SwingRetestMaxBars"),
    "retest_max_runaway_ticks": ("float", "SwingRetestMaxRunawayTicks"),
    "false_breakout_max_bars": ("int", "SwingFalseBreakoutMaxBars"),
    "htf_slope_lag": ("int", "SwingHtfSlopeLag"),
    "htf_min_slope_norm": ("float", "SwingHtfMinSlopeNorm"),
    "htf_overextended_atr_mult": ("float", "SwingHtfOverextendedAtrMult"),
    "htf_atr_period": ("int", "SwingHtfAtrPeriod"),
    "swing_session_tz": ("str", "SwingSessionTz"),
    "swing_trade_start_hhmm": ("str", "SwingTradeStartHhmm"),
    "swing_trade_end_hhmm": ("str", "SwingTradeEndHhmm"),
    "swing_lunch_skip": ("bool", "SwingLunchSkip"),
    "swing_lunch_start_hhmm": ("str", "SwingLunchStartHhmm"),
    "swing_lunch_end_hhmm": ("str", "SwingLunchEndHhmm"),
    "adaptive_stop_atr_mult": ("float", "SwingAdaptiveStopAtrMult"),
    "partial_exit_fraction": ("float", "SwingPartialExitFraction"),
    "partial_exit_r_multiple": ("float", "SwingPartialExitRMultiple"),
    "post_entry_time_stop_bars": ("int", "SwingPostEntryTimeStopBars"),
    "post_entry_time_stop_min_tp_progress": ("float", "SwingPostEntryTimeStopMinTpProgress"),
    "atr_slope_lag": ("int", "SwingAtrSlopeLag"),
    "min_atr_expansion": ("float", "SwingMinAtrExpansion"),
    "impulse_range_lookback": ("int", "SwingImpulseRangeLookback"),
    "impulse_strength": ("float", "SwingImpulseStrength"),
    "max_retest_bars_fast": ("int", "SwingMaxRetestBarsFast"),
    "min_range_atr_ratio": ("float", "SwingMinRangeAtrRatio"),
    "max_trades_per_day": ("int", "SwingMaxTradesPerDay"),
    "max_loss_streak": ("int", "SwingMaxLossStreak"),
    "loss_cooldown_bars": ("int", "SwingLossCooldownBars"),
    "use_market_on_strong_breakout": ("bool", "SwingUseMarketOnStrongBreakout"),
    "market_impulse_mult": ("float", "SwingMarketImpulseMult"),
    "risk_per_trade_rub": ("float", "SwingRiskPerTradeRub"),
    "sizing_min_qty": ("float", "SwingSizingMinQty"),
    "sizing_max_qty": ("float", "SwingSizingMaxQty"),
    "trades_log_csv_path": ("str", "SwingTradesLogCsvPath"),
}


def load_swing_params(cfg_dir: Path, *, tick_size: float) -> SwingBreakoutParams:
    defaults = SwingBreakoutParams()
    values: dict[str, object] = {}
    for field, (kind, key) in SWING_SETTING_KEYS.items():
        value = _READERS[kind](cfg_dir, key, getattr(defaults, field))
        values[field] = value.strip() if isinstance(value, str) else value
    remainder = read_str_merged(cfg_dir, "SwingPartialRemainderMode", "trail").strip().lower()
    values["partial_remainder_mode"] = "tp" if remainder == "tp" else "trail"
    return SwingBreakoutParams(tick_size=tick_size, **values)
