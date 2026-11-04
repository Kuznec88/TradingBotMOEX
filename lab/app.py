"""
Лаборатория стратегии. Только история: заявок и подключения к брокеру здесь нет.

  streamlit run lab/app.py
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab import engine  # noqa: E402

LIGHT = {"surface": "#fcfcfb", "text": "#52514e", "grid": "#e4e3df", "series": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"], "gain": "#2a78d6", "loss": "#eb6834"}
DARK = {"surface": "#1a1a19", "text": "#c3c2b7", "grid": "#383835", "series": ["#3987e5", "#d95926", "#199e70", "#c98500"], "gain": "#3987e5", "loss": "#d95926"}
ASSET_NAMES = {"SR": "Сбербанк", "GZ": "Газпром", "LK": "Лукойл", "Si": "Доллар/рубль (Si)", "MX": "Индекс Мосбиржи"}
HTF_LABELS = {"hard": "Жёсткий", "soft": "Мягкий", "off": "Выключен"}
EXIT_LABELS = {
    "stop_loss": "Стоп",
    "trailing_stop": "Трейлинг",
    "take_profit": "Тейк",
    "time_stop": "По времени",
    "partial_tp": "Частичный",
}

# Поле параметра, подпись, минимум, максимум, шаг, подсказка. Ноль выключает фильтр, если сказано.
ENTRY_FIELDS = [
    ("k_volume", "Всплеск объёма, × к среднему", 1.0, 3.0, 0.05, "Используется, только если включено требование всплеска."),
    ("impulse_k", "Сила тела пробойной свечи, × ATR", 0.0, 1.0, 0.05, "0 — фильтр выключен."),
    ("impulse_strength", "Размах свечи к прошлым, ×", 0.0, 2.0, 0.05, "0 — фильтр выключен."),
    ("min_atr_expansion", "Расширение ATR, ×", 0.0, 1.5, 0.01, "0 — фильтр выключен."),
    ("min_range_atr_ratio", "Диапазон к ATR, ×", 0.0, 5.0, 0.1, "Отсекает боковик. 0 — фильтр выключен."),
    ("max_retest_bars_fast", "Ретест не позже, баров", 0, 20, 1, "0 — фильтр выключен."),
    ("max_trades_per_day", "Сделок в день, максимум", 1, 20, 1, ""),
]
EXIT_FIELDS = [
    ("adaptive_stop_atr_mult", "Стоп, × ATR", 0.3, 3.0, 0.05, "Расстояние стопа. R — это риск на сделку, то есть расстояние до стопа."),
    ("risk_reward_multiple", "Тейк, R", 1.0, 20.0, 0.5, "20 — фактически без тейка."),
    ("trail_after_r_multiple", "Трейлинг включается на, R", 0.0, 5.0, 0.25, "0 — трейлинг выключен."),
    ("trail_gap_r_multiple", "Отступ трейлинга, R", 0.0, 5.0, 0.05, "0 — трейлинг выключен."),
    ("partial_exit_fraction", "Частичный выход, доля позиции", 0.0, 0.9, 0.1, "0 — выключен. На одном контракте в реальной торговле невозможен."),
    ("partial_exit_r_multiple", "Частичный выход на, R", 0.25, 5.0, 0.25, ""),
    ("post_entry_time_stop_bars", "Выход по времени, баров", 0, 200, 1, "0 — выключен."),
]
COST_FIELDS = [
    ("slippage_ticks", "Проскальзывание, тиков", 0, 10, 1, "Применяется к рыночным входам и выходам."),
    ("commission_min_profit_mult", "Тейк больше комиссии за круг, ×", 0.0, 5.0, 0.5, "Фильтр входа. 0 — выключен."),
]
ALL_FIELDS = ENTRY_FIELDS + EXIT_FIELDS + COST_FIELDS
PRESETS: dict[str, dict[str, Any]] = {
    "Настройки бота": {"htf_mode": "hard"},
    "Без старшего таймфрейма": {"htf_mode": "off"},
    "Мягкий фильтр, без всплеска объёма": {"htf_mode": "soft", "volume_spike_required": False},
}


def palette() -> dict[str, Any]:
    return DARK if st.context.theme.type == "dark" else LIGHT


def apply_preset(name: str) -> None:
    defaults = asdict(engine.bot_defaults())
    for field, *_ in ALL_FIELDS:
        st.session_state[field] = defaults[field]
    st.session_state["volume_spike_required"] = defaults["volume_spike_required"]
    st.session_state["use_market_on_strong_breakout"] = defaults["use_market_on_strong_breakout"]
    st.session_state["commission_pct"] = round(defaults["commission_rate"] * 100, 4)
    st.session_state["htf_mode"] = "hard"
    for key, value in PRESETS[name].items():
        st.session_state[key] = value


def field_widget(spec: tuple[Any, ...]) -> Any:
    field, label, low, high, step, tip = spec
    return st.slider(label, low, high, step=step, key=field, help=tip or None)


@st.cache_data(show_spinner=False, persist="disk", max_entries=500)
def cached_run(asset: str, frame: str, overrides_json: str, htf_mode: str, period: str, settings_stamp: float) -> pd.DataFrame:
    return engine.run_asset(asset, frame, json.loads(overrides_json), htf_mode, period)  # type: ignore[arg-type]


def settings_stamp() -> float:
    # Смена кода стратегии или настроек должна сбрасывать кэш результатов.
    files = [
        engine.REPO_ROOT / "fix_engine" / "strategy" / "swing_breakout.py",
        engine.REPO_ROOT / "fix_engine" / "settings.cfg",
        Path(engine.__file__),
    ]
    return max(f.stat().st_mtime for f in files)


def run_all(assets: list[str], frame: str, overrides: dict[str, Any], htf_mode: str, period: str) -> pd.DataFrame:
    overrides_json = json.dumps(overrides, sort_keys=True)
    stamp = settings_stamp()
    frames = []
    bar = st.progress(0.0, text="Считаю бэктест…")
    for i, asset in enumerate(assets):
        bar.progress(i / len(assets), text=f"Считаю {ASSET_NAMES.get(asset, asset)}, {frame}…")
        frames.append(cached_run(asset, frame, overrides_json, htf_mode, period, stamp))
    bar.empty()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=engine.POSITION_COLUMNS)


def months_covered(assets: list[str], frame: str, period: str) -> float:
    starts = [engine.load_candles(a, frame).index for a in assets]
    if period == "research":
        return engine.period_months([s.min() for s in starts], engine.RESEARCH_END)
    return engine.period_months([engine.RESEARCH_END], max(s.max() for s in starts))


def rub(value: float | None) -> str:
    return "—" if value is None else f"{value:+,.0f} ₽".replace(",", " ")


def base_layout(fig: go.Figure, *, height: int, y_title: str) -> go.Figure:
    colors = palette()
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor=colors["surface"],
        plot_bgcolor=colors["surface"],
        font=dict(color=colors["text"]),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.0, x=0),
        bargap=0.3,
        xaxis=dict(showgrid=False, linecolor=colors["grid"]),
        yaxis=dict(title=y_title, gridcolor=colors["grid"], zerolinecolor=colors["text"], zerolinewidth=1),
    )
    return fig


def equity_figure(series: list[tuple[str, pd.DataFrame]]) -> go.Figure:
    colors = palette()["series"]
    fig = go.Figure()
    for i, (name, curve) in enumerate(series[:4]):
        fig.add_trace(
            go.Scatter(
                x=curve["exit_time"],
                y=curve["equity"],
                name=name,
                mode="lines",
                line=dict(color=colors[i], width=2),
                hovertemplate="%{y:+,.0f} ₽<extra>" + name + "</extra>",
            )
        )
    return base_layout(fig, height=340, y_title="Чистая прибыль, ₽")


def monthly_figure(months: pd.DataFrame) -> go.Figure:
    colors = palette()
    bars = [colors["gain"] if v >= 0 else colors["loss"] for v in months["net_rub"]]
    fig = go.Figure(
        go.Bar(
            x=months["month"],
            y=months["net_rub"],
            marker=dict(color=bars, cornerradius=4),
            customdata=months["positions"],
            hovertemplate="%{y:+,.0f} ₽, сделок: %{customdata}<extra></extra>",
        )
    )
    return base_layout(fig, height=240, y_title="За месяц, ₽")


def trade_figure(candles: pd.DataFrame, trade: pd.Series) -> go.Figure:
    colors = palette()
    fig = go.Figure(
        go.Candlestick(
            x=candles.index,
            open=candles["open"],
            high=candles["high"],
            low=candles["low"],
            close=candles["close"],
            increasing=dict(line=dict(color=colors["gain"]), fillcolor=colors["gain"]),
            decreasing=dict(line=dict(color=colors["loss"]), fillcolor=colors["loss"]),
            name="Цена",
            showlegend=False,
        )
    )
    # Цены сделки уже в шкале склеенной истории, сдвиг контрактов на них учтён.
    fig.add_trace(
        go.Scatter(
            x=[trade["entry_time"], trade["exit_time"]],
            y=[trade["entry_price"], trade["exit_price"]],
            mode="lines+markers+text",
            text=["Вход", "Выход"],
            textposition="top center",
            line=dict(color=colors["text"], width=2, dash="dot"),
            marker=dict(size=10, color=colors["text"], line=dict(color=colors["surface"], width=2)),
            name="Сделка",
            showlegend=False,
            hovertemplate="%{text}: %{y:,.0f}<extra></extra>",
        )
    )
    fig.update_layout(xaxis_rangeslider_visible=False)
    return base_layout(fig, height=380, y_title="Цена, пункты")


def show_summary(summary: dict[str, Any]) -> None:
    row1 = st.columns(4)
    row1[0].metric("Сделок", summary.get("positions", 0))
    row1[1].metric("Сделок в месяц", f"{summary.get('per_month', 0):.1f}")
    row1[2].metric("До комиссии", rub(summary.get("gross_rub")))
    row1[3].metric("Чистая прибыль", rub(summary.get("net_rub")))
    row2 = st.columns(4)
    net_bp = summary.get("net_bp")
    row2[0].metric("В среднем на сделку", "—" if net_bp is None else f"{net_bp:+.1f} б.п.", help="После комиссии, в сотых долях процента от цены контракта.")
    win = summary.get("win_rate")
    row2[1].metric("Прибыльных", "—" if win is None else f"{win:.0%}")
    pf = summary.get("profit_factor")
    row2[2].metric("Профит-фактор", "—" if pf is None else f"{pf:.2f}")
    row2[3].metric("Макс. просадка", "—" if summary.get("max_drawdown_rub") is None else f"{summary['max_drawdown_rub']:,.0f} ₽".replace(",", " "))
    n = summary.get("positions", 0)
    t_stat = summary.get("t_stat")
    if n and n < 30:
        st.warning(f"Всего {n} сделок: этого мало для выводов, результат может быть случайным.")
    elif t_stat is not None and abs(t_stat) < 2:
        st.info(f"t-статистика {t_stat:.2f}: при |t| меньше 2 результат не отличим от случайного.")


def positions_table(positions: pd.DataFrame) -> pd.DataFrame:
    table = positions.sort_values("entry_time", ascending=False).copy()
    table["exit_reason"] = table["exit_reason"].map(lambda r: EXIT_LABELS.get(r, r))
    table["asset"] = table["asset"].map(lambda a: ASSET_NAMES.get(a, a))
    table["side"] = table["side"].map({"LONG": "Лонг", "SHORT": "Шорт"})
    table = table[["entry_time", "exit_time", "asset", "side", "entry_price", "exit_price", "gross_rub", "fees_rub", "net_rub", "net_bp", "exit_reason"]]
    return table.rename(
        columns={
            "entry_time": "Вход", "exit_time": "Выход", "asset": "Инструмент", "side": "Сторона",
            "entry_price": "Цена входа", "exit_price": "Цена выхода", "gross_rub": "До комиссии, ₽",
            "fees_rub": "Комиссия, ₽", "net_rub": "Итог, ₽", "net_bp": "Итог, б.п.", "exit_reason": "Причина выхода",
        }
    ).round({"Цена входа": 1, "Цена выхода": 1, "До комиссии, ₽": 1, "Комиссия, ₽": 1, "Итог, ₽": 1, "Итог, б.п.": 1})


def show_results(positions: pd.DataFrame, summary: dict[str, Any], frame: str) -> None:
    show_summary(summary)
    if positions.empty:
        st.info("С этими настройками сделок нет. Попробуй ослабить фильтры входа.")
        return
    st.subheader("Кривая прибыли")
    st.caption("На 1 контракт каждого инструмента, после комиссии и проскальзывания.")
    st.plotly_chart(equity_figure([("Текущий запуск", engine.equity_curve(positions))]), width="stretch")
    st.subheader("Прибыль по месяцам")
    st.plotly_chart(monthly_figure(engine.monthly_net(positions)), width="stretch")

    left, right = st.columns(2)
    with left:
        st.subheader("Как закрывались сделки")
        reasons = positions.groupby("exit_reason").agg(n=("net_rub", "size"), net=("net_rub", "sum"), bp=("net_bp", "mean")).reset_index()
        reasons["exit_reason"] = reasons["exit_reason"].map(lambda r: EXIT_LABELS.get(r, r))
        reasons.columns = ["Причина", "Сделок", "Итог, ₽", "В среднем, б.п."]
        st.dataframe(reasons.round(1), hide_index=True, width="stretch")
    with right:
        st.subheader("По инструментам")
        assets = positions.groupby("asset").agg(n=("net_rub", "size"), gross=("gross_rub", "sum"), net=("net_rub", "sum"), bp=("net_bp", "mean")).reset_index()
        assets["asset"] = assets["asset"].map(lambda a: ASSET_NAMES.get(a, a))
        assets.columns = ["Инструмент", "Сделок", "До комиссии, ₽", "Итог, ₽", "В среднем, б.п."]
        st.dataframe(assets.round(1), hide_index=True, width="stretch")

    st.subheader("Сделка на графике")
    ordered = positions.sort_values("entry_time", ascending=False).reset_index(drop=True)
    labels = [
        f"{row.entry_time:%Y-%m-%d %H:%M} · {ASSET_NAMES.get(row.asset, row.asset)} · {'лонг' if row.side == 'LONG' else 'шорт'} · {row.net_rub:+.0f} ₽"
        for row in ordered.itertuples()
    ]
    choice = st.selectbox("Сделка", range(len(labels)), format_func=lambda i: labels[i])
    trade = ordered.iloc[choice]
    candles = engine.load_candles(trade["asset"], frame)
    start = candles.index.searchsorted(trade["entry_time"])
    stop = candles.index.searchsorted(trade["exit_time"])
    st.plotly_chart(trade_figure(candles.iloc[max(0, start - 40) : stop + 20], trade), width="stretch")

    st.subheader("Все сделки")
    st.dataframe(positions_table(positions), hide_index=True, width="stretch", height=320)


def show_compare() -> None:
    runs = engine.load_runs()
    if not runs:
        st.info("Сохранённых запусков пока нет. Запусти бэктест и нажми «Сохранить запуск».")
        return
    names = {f"{r['name']} · {r['saved_at'][:16].replace('T', ' ')}": r for r in runs}
    picked = st.multiselect("Запуски, до четырёх", list(names), default=list(names)[:2], max_selections=4)
    if not picked:
        return
    series = []
    rows = []
    for label in picked:
        run = names[label]
        curve = pd.DataFrame({"exit_time": pd.to_datetime(run["equity"]["time"], utc=True), "equity": run["equity"]["value"]})
        series.append((run["name"], curve))
        s = run["summary"]
        rows.append(
            {
                "Запуск": run["name"],
                "Сделок": s.get("positions"),
                "В месяц": round(s.get("per_month") or 0, 1),
                "До комиссии, ₽": round(s.get("gross_rub") or 0),
                "Итог, ₽": round(s.get("net_rub") or 0),
                "На сделку, б.п.": round(s.get("net_bp") or 0, 1),
                "Просадка, ₽": round(s.get("max_drawdown_rub") or 0),
            }
        )
    st.plotly_chart(equity_figure(series), width="stretch")
    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")


def main() -> None:
    st.set_page_config(page_title="Лаборатория стратегии", layout="wide")
    meta = engine.load_meta()
    st.title("Лаборатория стратегии")
    if not meta:
        st.error("Нет истории. Загрузи её: `python -m fix_engine.tools.fetch_continuous_futures`")
        return

    if "initialized" not in st.session_state:
        apply_preset("Настройки бота")
        st.session_state["initialized"] = True

    with st.sidebar:
        st.header("Данные")
        assets = st.multiselect("Инструменты", list(meta), default=["SR"], format_func=lambda a: ASSET_NAMES.get(a, a))
        frame = st.selectbox("Таймфрейм", engine.FRAMES, index=2, help="5m считается заметно дольше, особенно на нескольких инструментах.")
        st.header("Заготовка")
        preset = st.selectbox("Настройки", list(PRESETS))
        st.button("Применить заготовку", on_click=apply_preset, args=(preset,), width="stretch")

        st.header("Входы")
        st.selectbox("Старший таймфрейм", list(HTF_LABELS), format_func=lambda m: HTF_LABELS[m], key="htf_mode")
        st.toggle("Требовать всплеск объёма", key="volume_spike_required")
        st.toggle("Рынком при сильном пробое", key="use_market_on_strong_breakout")
        for spec in ENTRY_FIELDS:
            field_widget(spec)
        st.header("Выходы")
        for spec in EXIT_FIELDS:
            field_widget(spec)
        st.header("Издержки")
        st.number_input("Комиссия за сделку, %", 0.0, 0.2, step=0.005, format="%.3f", key="commission_pct", help="Тариф «Премиум»: 0,025%.")
        for spec in COST_FIELDS:
            field_widget(spec)

    overrides: dict[str, Any] = {field: st.session_state[field] for field, *_ in ALL_FIELDS}
    overrides["volume_spike_required"] = st.session_state["volume_spike_required"]
    overrides["use_market_on_strong_breakout"] = st.session_state["use_market_on_strong_breakout"]
    overrides["commission_rate"] = st.session_state["commission_pct"] / 100
    htf_mode = st.session_state["htf_mode"]
    config = {"assets": sorted(assets), "frame": frame, "htf_mode": htf_mode, "params": overrides}

    research_tab, compare_tab, holdout_tab = st.tabs(["Исследование", "Сравнение запусков", "Проверка на 2025–2026"])

    with research_tab:
        st.caption(f"Только данные до {engine.RESEARCH_END:%d.%m.%Y}. Более поздний период оставлен для отдельной проверки.")
        if st.session_state.get("last_config") not in (None, config):
            st.info("Настройки изменились. Нажми «Запустить», чтобы пересчитать.")
        if st.button("Запустить", type="primary", disabled=not assets):
            positions = run_all(assets, frame, overrides, htf_mode, "research")
            st.session_state["last_config"] = config
            st.session_state["last_positions"] = positions
            st.session_state["last_summary"] = engine.summarize(positions, months=months_covered(assets, frame, "research"))
        positions = st.session_state.get("last_positions")
        if positions is not None:
            last = st.session_state["last_config"]
            save_col, name_col = st.columns([1, 3])
            name = name_col.text_input("Название запуска", value=f"{HTF_LABELS[last['htf_mode']]} HTF, {last['frame']}", label_visibility="collapsed")
            if save_col.button("Сохранить запуск"):
                engine.save_run(name, last, positions, st.session_state["last_summary"])
                st.toast("Запуск сохранён")
            show_results(positions, st.session_state["last_summary"], last["frame"])

    with compare_tab:
        show_compare()

    with holdout_tab:
        st.write(
            "Здесь проверяются настройки, выбранные на данных до 2025 года. Чем чаще смотреть сюда и менять настройки, "
            "тем меньше значит результат, поэтому каждая проверка записывается."
        )
        st.metric("Проверок всего", engine.holdout_check_count())
        confirmed = st.checkbox("Настройки выбраны, и я понимаю, что менять их по этому результату нельзя")
        if st.button("Проверить на 2025–2026", disabled=not (confirmed and assets)):
            held = run_all(assets, frame, overrides, htf_mode, "holdout")
            summary = engine.summarize(held, months=months_covered(assets, frame, "holdout"))
            count = engine.log_holdout_check(config, summary)
            st.session_state["holdout_result"] = (held, summary, count, frame)
        result = st.session_state.get("holdout_result")
        if result is not None:
            held, summary, count, held_frame = result
            st.caption(f"Эта конфигурация проверялась {count} раз(а).")
            show_results(held, summary, held_frame)


main()
