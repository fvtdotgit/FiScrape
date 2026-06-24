# tech_ui_sandbox.py
# Giant-function version of the Technicals UI sandbox (UI-first, mock data).
# - Two dropdowns (Presets, Custom).
# - Preset & Custom cells share a consistent 2-row layout.
# - Safe session-state (no widget-key overwrites).
# - Returns a unified chart spec; renders a mock chart (line/candlestick) with simple indicators.

from __future__ import annotations

import uuid
from copy import deepcopy


import app_format as af

import streamlit as st

st.set_page_config(page_title="Technicals Sandbox", layout="wide")

import numpy as np
import pandas as pd
import altair as alt

from typing import Any, Dict, List, Tuple

from fiscrape_core import period_to_api, get_history, interval_to_api, technical_snapshot


# -----------------------------
# Constants & palettes
# -----------------------------
OVERLAY_TYPES = ("ema", "sma", "bb", "vwap")
OSC_TYPES = ("rsi", "macd")

PRESET_MAP = {
    "SMA(20)": {"type": "sma", "slot": "overlay", "params": {"period": 20}},
    "SMA(50)": {"type": "sma", "slot": "overlay", "params": {"period": 50}},
    "SMA(200)": {"type": "sma", "slot": "overlay", "params": {"period": 200}},
    "EMA(9)": {"type": "ema", "slot": "overlay", "params": {"period": 9}},
    "EMA(12)": {"type": "ema", "slot": "overlay", "params": {"period": 12}},
    "EMA(21)": {"type": "ema", "slot": "overlay", "params": {"period": 21}},
    "VWAP": {"type": "vwap", "slot": "overlay", "params": {"session": "auto"}},
}

FAMILY_COLORS = {
    "sma": ["#EF5350", "#E57373", "#C62828"],
    "ema": ["#42A5F5", "#64B5F6", "#1565C0"],
    "vwap": ["#8E5AD7"],
    "bb": ["#9E9E9E", "#B0B0B0"],  # mid, bands fill
    "rsi": ["#26A69A"],
    "macd": ["#FB8C00", "#8D6E63"],  # macd, signal
}

CUSTOM_DEFAULTS = {
    "ema": {"period": 34},
    "sma": {"period": 10},
    "bb": {"period": 20, "stdDev": 2},
    "rsi": {"period": 14},
    "macd": {"fast": 12, "slow": 26, "signal": 9},
}

TYPE_CAP = {"ema": 3, "sma": 3, "bb": 1, "vwap": 1, "rsi": 1, "macd": 1}

# Two-row layout ratios, custom
CONST_CUS_W, COLOR_CUS_W, DEL_CUS_W = 3.0, 0.5, 0.5

# Preset grid density & gaps
NUM_COLS_PRESET = 3  # ← try 4 first (was 5); bump down to 3 for more width
GRID_GAP_PRESET = "medium"  # "small" | "medium" | "large"

# Custom grid
NUM_COLS_CUSTOM = 2

# Row-2 splits (presets vs custom)
ROW2_PRESET = [1.0, 0.8, 0.0, 0.26]  # [Color | Spacer | Help | ✕]
ROW2_CUSTOM = [3.0, 1.0, 0.6, 0.26]  # [Constants | Color | Spacer | ✕]


# -----------------------------
# Small UI helpers
# -----------------------------
def family_color(ind_type: str, shade_index: int = 0) -> str:
    shades = FAMILY_COLORS.get(ind_type, ["#888888"])
    return shades[min(shade_index, len(shades) - 1)]


def label_for(item: dict | str) -> str:
    if isinstance(item, str):
        return item
    t, p = item["type"], item["params"]
    if t in ("ema", "sma"):
        return f"{t.upper()}({p.get('period')})"
    if t == "bb":
        return f"Bollinger({p.get('period')},{p.get('stdDev')})"
    if t == "rsi":
        return f"RSI({p.get('period')})"
    if t == "macd":
        return f"MACD({p.get('fast')}/{p.get('slow')}/{p.get('signal')})"
    if t == "vwap":
        return "VWAP"
    return t.upper()


def mk_item(ind_type: str, params: dict, on: bool = True) -> dict:
    return {
        "id": str(uuid.uuid4())[:8],
        "type": ind_type,
        "on": on,
        "params": deepcopy(params),
        "color": family_color(ind_type, 0),
    }


# ============================================================
# UI→engine translator & column mappers (new)
# ============================================================


def _spec_to_requests(spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Translate UI spec into technical_snapshot() requests."""
    reqs: List[Dict[str, Any]] = []
    seen = set()

    # overlays
    for it in spec.get("overlays") or []:
        if not it.get("on"):
            continue
        t = (it.get("type") or "").lower()
        p = it.get("params") or {}
        if t == "sma":
            req = {"name": "SMA", "length": int(p.get("period", 20))}
        elif t == "ema":
            req = {"name": "EMA", "length": int(p.get("period", 12))}
        elif t == "bb":
            req = {"name": "BB", "length": int(p.get("period", 20)), "k": float(p.get("stdDev", 2))}
        elif t == "vwap":
            # single VWAP supported; engine handles default basis (e.g., HLC3)
            req = {"name": "VWAP"}
        else:
            continue
        key = tuple(sorted(req.items()))
        if key not in seen:
            reqs.append(req)
            seen.add(key)

    # oscillators
    for it in spec.get("oscillators") or []:
        if not it.get("on"):
            continue
        t = (it.get("type") or "").lower()
        p = it.get("params") or {}
        if t == "rsi":
            req = {"name": "RSI", "length": int(p.get("period", 14))}
        elif t == "macd":
            req = {
                "name": "MACD",
                "fast": int(p.get("fast", 12)),
                "slow": int(p.get("slow", 26)),
                "signal": int(p.get("signal", 9)),
            }
        else:
            continue
        key = tuple(sorted(req.items()))
        if key not in seen:
            reqs.append(req)
            seen.add(key)
    return reqs


def _col_for_overlay(item: Dict[str, Any]) -> List[str]:
    """Map overlay UI item → engine output column names."""
    t = (item.get("type") or "").lower()
    p = item.get("params") or {}
    if t == "sma":
        n = int(p.get("period", 20))
        return [f"sma{n}"]
    if t == "ema":
        n = int(p.get("period", 12))
        return [f"ema{n}"]
    if t == "bb":
        n = int(p.get("period", 20))
        return [f"bbMid{n}", f"bbUpper{n}", f"bbLower{n}"]
    if t == "vwap":
        # engine default (hlc3) column
        return ["vwap"]
    return []


def _cols_for_rsi(item: Dict[str, Any]) -> str | None:
    n = int((item.get("params") or {}).get("period", 14))
    return f"rsi{n}"


def _cols_for_macd(item: Dict[str, Any]) -> Tuple[str, str, str] | None:
    p = item.get("params") or {}
    f, s, sig = int(p.get("fast", 12)), int(p.get("slow", 26)), int(p.get("signal", 9))
    return (f"macd_{f}_{s}_{sig}", f"macdSignal_{f}_{s}_{sig}", f"macdHist_{f}_{s}_{sig}")


# -----------------------------
# Giant function
# -----------------------------
def render_technicals_ui() -> dict:
    # ---- State init ----
    ss = st.session_state
    ss.setdefault("card_controls", {"period": "6M", "interval": "1d", "style": "Candlestick"})
    ss.setdefault("presets_selected", [])
    ss.setdefault(
        "presets_state",
        {
            label: {"on": True, "color": FAMILY_COLORS[cfg["type"]][0]}
            for label, cfg in PRESET_MAP.items()
        },
    )
    ss.setdefault("indicators", {"overlays": [], "oscillators": []})
    ss.setdefault("custom_type_last", "—")
    ss.setdefault("_do_clear", False)
    ss.setdefault("_apply_strategy", "")
    ss.setdefault("_remove_preset_label", "")
    ss.setdefault("_suppress_custom_add", False)
    ss.setdefault("_just_cleared", False)

    # ---- Page shell ----
    st.set_page_config(page_title="Technicals — UI Sandbox", layout="wide")
    st.title("Technicals — UI Sandbox")
    st.caption(
        "UI-first playground for indicator selection and layout. No data processing here yet."
    )

    # Functions
    # --- Compressed-x helpers (local to sandbox; no globals) ----------------------
    def _compress_for_price(hist: pd.DataFrame, style_label: str) -> tuple[pd.DataFrame, str]:
        """
        Mirror Overview filtering + compressed index.
        Returns (df_ready, time_col). df_ready has __x_idx__ and retains time_col.
        """
        df = hist.copy()
        for col in ("Open", "High", "Low", "Close", "Volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.reset_index(drop=False)

        # detect datetime column from first column after reset_index
        time_col = df.columns[0] if pd.api.types.is_datetime64_any_dtype(df.iloc[:, 0]) else None
        if time_col is None:
            df["__time__"] = pd.RangeIndex(len(df))
            time_col = "__time__"

        if style_label == "Candlestick":
            price_ok = df[["Open", "High", "Low", "Close"]].apply(np.isfinite).all(axis=1)
            price_ok &= df["Low"] <= df[["Open", "Close"]].min(axis=1)
            price_ok &= df[["Open", "Close"]].max(axis=1) <= df["High"]
        else:
            price_ok = (
                np.isfinite(df["Close"])
                if "Close" in df.columns
                else pd.Series(False, index=df.index)
            )

        df_ready = df.loc[price_ok].sort_values(by=time_col).reset_index(drop=True)
        df_ready["__x_idx__"] = np.arange(len(df_ready), dtype=int)
        return df_ready, time_col

    # ---- Data --------------------------------------------------------------------
    with st.container(border=True):
        st.subheader("Data")
        st.text_input(
            "Symbol",
            value=st.session_state.get("activeSymbol", "AAPL"),
            key="activeSymbol",
            help="Type a ticker (e.g., AAPL, MSFT, SPY)",
        )

    with st.container(border=True):
        st.subheader("Chart Card — Technicals")
        c1, c2, c3 = st.columns([2, 2, 2], vertical_alignment="center")

        with c1:
            st.selectbox(
                "Period",
                ["1D", "5D", "1M", "3M", "6M", "YTD", "1Y", "5Y", "MAX"],
                index=4,
                key="period_select",
            )
        with c2:
            st.selectbox(
                "Interval",
                ["1m", "2m", "5m", "15m", "30m", "60m", "1d", "1wk", "1mo"],
                index=6,
                key="interval_select",
            )
        with c3:
            st.selectbox("Style", ["Candlestick", "Line"], key="style_select")

        # Apply queued mutations BEFORE widgets that bind those keys render
        if ss._do_clear:
            ss.presets_selected = []
            ss.indicators = {"overlays": [], "oscillators": []}
            # NEW: fully reset the custom select gate & suppress auto-add for one rerun
            st.session_state["custom_type_select"] = "—"
            ss.custom_type_last = "—"
            ss._suppress_custom_add = True
            ss._just_cleared = True
            ss._do_clear = False
            st.rerun()

        if ss._remove_preset_label:
            lbl = ss._remove_preset_label
            ss.presets_selected = [x for x in ss.presets_selected if x != lbl]
            ss._remove_preset_label = ""
            st.rerun()

        if ss._apply_strategy:
            s = ss._apply_strategy
            if s == "Classic":
                ss.presets_selected = ["SMA(20)", "SMA(50)", "SMA(200)"]
                for lbl in ss.presets_selected:
                    ss.presets_state[lbl]["on"] = True
            elif s == "Momentum":
                # add RSI + MACD once
                def count_type(items, t):
                    return sum(1 for it in items if it["type"] == t)

                if count_type(ss.indicators["oscillators"], "rsi") == 0:
                    ss.indicators["oscillators"].append(
                        mk_item("rsi", deepcopy(CUSTOM_DEFAULTS["rsi"]))
                    )
                if count_type(ss.indicators["oscillators"], "macd") == 0:
                    ss.indicators["oscillators"].append(
                        mk_item("macd", deepcopy(CUSTOM_DEFAULTS["macd"]))
                    )
            elif s == "Trend":
                ss.presets_selected = ["EMA(9)", "EMA(12)", "EMA(21)", "VWAP"]
                for lbl in ss.presets_selected:
                    ss.presets_state[lbl]["on"] = True
            ss._apply_strategy = ""
            st.rerun()

        st.divider()

        # Presets dropdown
        st.multiselect(
            "Preset Indicators (SMA, EMA, VWAP)",
            options=list(PRESET_MAP.keys()),
            key="presets_selected",
            help="Fixed-parameter overlays. Visibility & color below.",
        )

        # Strategy + Custom + Clear
        rowL, rowM, rowR = st.columns([6, 5, 1], gap="small", vertical_alignment="bottom")
        with rowL:
            ssel, sapply = st.columns([3, 1], gap="small", vertical_alignment="bottom")
            with ssel:
                strategy = st.selectbox(
                    "Strategy",
                    ["Classic", "Momentum", "Trend"],
                    key="strategy_select",
                    help="Classic: SMA 20/50/200 • Momentum: RSI+MACD • Trend: EMA 9/12/21 + VWAP",
                )
            with sapply:
                if st.button("Apply", use_container_width=True):
                    ss._apply_strategy = strategy
                    st.rerun()
        with rowM:
            custom_options = ["—", "EMA", "SMA", "Bollinger", "RSI", "MACD"]
            choice = st.selectbox(
                "Custom Indicators",
                custom_options,
                index=0,
                key="custom_type_select",
                help="Adds an editable row below. Caps: EMA/SMA=3; BB/RSI/MACD=1.",
            )

            # NEW: one-shot suppress after Clear to avoid auto re-add
            if ss._suppress_custom_add:
                ss._suppress_custom_add = False
                ss.custom_type_last = choice  # sync the gate, so next change is intentional
            else:
                # only act when the *value actually changes*
                if choice != ss.custom_type_last:
                    ss.custom_type_last = choice
                    if choice != "—":
                        # Normalize UI label to registry type
                        t = {"Bollinger": "bb"}.get(choice, choice).lower()
                        if t in CUSTOM_DEFAULTS:
                            bucket = "overlays" if t in OVERLAY_TYPES else "oscillators"
                            # respect per-type cap
                            if sum(
                                1 for it in ss.indicators[bucket] if it["type"] == t
                            ) < TYPE_CAP.get(t, 3):
                                ss.indicators[bucket].append(
                                    mk_item(t, deepcopy(CUSTOM_DEFAULTS[t]))
                                )
                                st.rerun()

        with rowR:
            if st.button(
                "Clear",
                type="secondary",
                help="Remove all presets & custom rows",
                use_container_width=True,
            ):
                ss._do_clear = True
                st.rerun()

        st.caption(
            "Caps: EMA/SMA = 3 each; BB, VWAP, RSI, MACD = 1 each. Presets are fixed; customize via Custom Indicators."
        )
        st.divider()

        # ---------- Indicators (expander) ----------
        with st.expander("Indicators", expanded=True):
            # ---- Presets grid (2-row cells) ----
            st.markdown("**Preset Indicators**")
            selected = ss.presets_selected
            if not selected:
                st.caption("No presets selected yet.")
            else:

                def preset_sort_key(lbl: str):
                    cfg = PRESET_MAP[lbl]
                    t = cfg["type"]
                    p = cfg["params"]
                    fam = 0 if t == "ema" else 1 if t == "sma" else 2 if t == "vwap" else 9
                    per = int(p.get("period", 9999))
                    return (fam, per, lbl)

                items = sorted(selected, key=preset_sort_key)

                def render_preset_cell(lbl: str):
                    cfg = PRESET_MAP[lbl]
                    t = cfg["type"]
                    on_key = f"preset-{lbl}-on"
                    col_key = f"preset-{lbl}-color"
                    del_key = f"preset-{lbl}-del"

                    # Row 1: title + show
                    r1c1, r1c2 = st.columns([2, 1], vertical_alignment="center")
                    with r1c1:
                        st.markdown(f"**{lbl}**")
                    with r1c2:
                        st.toggle(
                            "Show",
                            value=ss.presets_state[lbl]["on"],
                            key=on_key,
                            label_visibility="collapsed",
                        )

                    # Row 2: [ constants | color | ✕ ] (presets: no constants)
                    c_color, c_del = st.columns([2, 1], vertical_alignment="bottom")
                    with c_color:
                        st.color_picker(
                            "Color",
                            value=ss.presets_state[lbl]["color"],
                            key=col_key,
                            label_visibility="collapsed",
                        )
                    with c_del:
                        if st.button("✕", key=del_key, help="Remove"):
                            ss._remove_preset_label = lbl
                            st.rerun()

                    # persist show/color to presets_state
                    ss.presets_state[lbl]["on"] = st.session_state[on_key]
                    ss.presets_state[lbl]["color"] = st.session_state[col_key]

                for row in range(0, len(items), NUM_COLS_PRESET):
                    cols = st.columns(
                        NUM_COLS_PRESET, gap=GRID_GAP_PRESET, vertical_alignment="top"
                    )
                    for col, lbl in zip(cols, items[row : row + NUM_COLS_PRESET]):
                        with col:
                            render_preset_cell(lbl)

            st.divider()

            # ---- Custom grid (2-row cells) ----
            st.markdown("**Custom Indicators (editable)**")
            overlays = ss.indicators["overlays"]
            oscillators = ss.indicators["oscillators"]

            def _overlay_key(it):
                t = it["type"]
                p = it["params"]
                rank = 0 if t == "ema" else 1 if t == "sma" else 2 if t == "bb" else 9
                per = int(p.get("period", p.get("fast", 0)))
                return (rank, per)

            def _osc_key(it):
                return 0 if it["type"] == "rsi" else 1

            items_ordered: list[tuple[str, dict]] = [
                ("overlays", x) for x in sorted(overlays, key=_overlay_key)
            ] + [("oscillators", x) for x in sorted(oscillators, key=_osc_key)]

            if not items_ordered:
                st.caption("No custom indicators added.")
            else:

                def render_custom_cell(bucket: str, item: dict):
                    t, p = item["type"], item["params"]

                    # row1: title + show
                    r1c1, r1c2 = st.columns([3.5, 0.5], vertical_alignment="center")
                    with r1c1:
                        st.markdown(f"**{label_for(item)}**")
                    with r1c2:
                        st.toggle(
                            "Show",
                            value=item.get("on", True),
                            key=f"{item['id']}-on",
                            label_visibility="collapsed",
                        )

                    # fields for row2
                    if t in ("ema", "sma", "rsi"):
                        fields = [
                            (
                                "Period",
                                "period",
                                "p",
                                int(p.get("period", 14 if t == "rsi" else 20)),
                            )
                        ]
                    elif t == "bb":
                        fields = [
                            ("Period", "period", "p", int(p.get("period", 20))),
                            ("Std Dev", "stdDev", "sd", float(p.get("stdDev", 2))),
                        ]
                    elif t == "macd":
                        fields = [
                            ("Fast", "fast", "f", int(p.get("fast", 12))),
                            ("Slow", "slow", "s", int(p.get("slow", 26))),
                            ("Signal", "signal", "g", int(p.get("signal", 9))),
                        ]
                    else:
                        fields = []

                    c_consts, c_color, c_del = st.columns(
                        [3, 0.5, 0.5], vertical_alignment="bottom"
                    )
                    param_vals = {}
                    with c_consts:
                        if fields:
                            sub = st.columns(len(fields), vertical_alignment="bottom")
                            for (lbl, key_name, suf, default_val), subcol in zip(fields, sub):
                                with subcol:
                                    wkey = f"{item['id']}-{suf}"
                                    if key_name == "stdDev":
                                        val = st.number_input(
                                            lbl,
                                            min_value=0.1,
                                            step=0.1,
                                            value=float(default_val),
                                            key=wkey,
                                        )
                                    else:
                                        val = st.number_input(
                                            lbl,
                                            min_value=1,
                                            step=1,
                                            value=int(default_val),
                                            key=wkey,
                                        )
                                    param_vals[key_name] = val
                        else:
                            st.write("")
                    with c_color:
                        st.color_picker(
                            "Color",
                            value=item.get("color", family_color(t, 0)),
                            key=f"{item['id']}-color",
                            label_visibility="collapsed",
                        )
                    with c_del:
                        if st.button("✕", key=f"{item['id']}-del", help="Remove"):
                            # remove by id from proper bucket
                            lst = ss.indicators[bucket]
                            for j, obj in enumerate(lst):
                                if obj["id"] == item["id"]:
                                    lst.pop(j)
                                    st.rerun()

                    # persist to model
                    item["on"] = st.session_state[f"{item['id']}-on"]
                    item["color"] = st.session_state[f"{item['id']}-color"]
                    for k, v in param_vals.items():
                        item["params"][k] = v
                    # MACD validation
                    if t == "macd" and not (item["params"]["fast"] < item["params"]["slow"]):
                        st.caption(":orange[Fast must be less than Slow]")

                for row in range(0, len(items_ordered), NUM_COLS_CUSTOM):
                    cols = st.columns(NUM_COLS_CUSTOM, gap="small", vertical_alignment="top")
                    for col, (bucket, it) in zip(cols, items_ordered[row : row + NUM_COLS_CUSTOM]):
                        with col:
                            render_custom_cell(bucket, it)

    # ---- Build chart spec (unified) ----
    def to_style(item_type: str, color: str) -> dict:
        # sensible defaults; BB gets fillOpacity & showMidline
        base = {"color": color, "opacity": 1.0, "width": 1.5, "dash": "solid"}
        if item_type == "vwap":
            base["width"] = 2.0  # slightly more prominent
            base["dash"] = "solid"  # default solid (was dot)
        if item_type == "bb":
            base.update({"fillColor": color, "fillOpacity": 0.15, "showMidline": True})
        return base

    overlays_spec = []
    for lbl in ss.presets_selected:
        cfg = PRESET_MAP[lbl]
        if not ss.presets_state[lbl]["on"]:
            continue
        overlays_spec.append(
            {
                "id": f"preset-{lbl}",
                "type": cfg["type"],
                "on": True,
                "params": cfg["params"],
                "style": to_style(cfg["type"], ss.presets_state[lbl]["color"]),
                "z": 10,
                "label": lbl,
            }
        )
    for it in ss.indicators["overlays"]:
        if it.get("on", True):
            overlays_spec.append(
                {
                    "id": it["id"],
                    "type": it["type"],
                    "on": True,
                    "params": it["params"],
                    "style": to_style(it["type"], it.get("color", "#888")),
                    "z": 20,
                    "label": label_for(it),
                }
            )

    oscillators_spec = []
    for it in ss.indicators["oscillators"]:
        if it.get("on", True):
            oscillators_spec.append(
                {
                    "id": it["id"],
                    "type": it["type"],
                    "on": True,
                    "params": it["params"],
                    "style": to_style(it["type"], it.get("color", "#888")),
                    "z": 5,
                    "label": label_for(it),
                }
            )

    spec = {
        "controls": ss.card_controls,
        "overlays": overlays_spec,
        "oscillators": oscillators_spec,
        "meta": {"lastUpdated": {"live": None, "history": None, "pipeline": None}},
    }

    # ---- Chart Preview (live data) ----------------------------------------------
    with st.container(border=True):
        st.subheader("Chart Preview (live data)")

        # ---- Inputs --------------------------------------------------------------
        sym = (st.session_state.get("activeSymbol") or "AAPL").strip().upper()
        period_ui = st.session_state.get("period_select", "6M")
        interval_ui = st.session_state.get("interval_select", "1d")
        style_label = st.session_state.get("style_select", "Candlestick")

        # ---- Data ----------------------------------------------------------------
        period_api = period_to_api(period_ui)
        interval_api = interval_to_api(interval_ui)
        hist = get_history(sym, period_api, interval_api)

        if hist is None or hist.empty:
            st.info(f"No history for {sym} ({period_api}, {interval_api}).")
        else:
            # ---- Alignment & shared x --------------------------------------------
            period_api = period_to_api(period_ui)
            interval_api = interval_to_api(interval_ui)

            requests = _spec_to_requests(spec)

            df_enriched, meta = technical_snapshot(
                symbol=sym,
                period=period_api,
                interval=interval_api,
                requests=requests,
                style_label=style_label,
                tz=None,  # optional: pass a display tz if you want to force one
            )

            if df_enriched is None or df_enriched.empty:
                st.info(f"No data for {sym} ({period_api}, {interval_api}).")
                st.stop()

            # ---------- Unified DF ----------
            df_aligned = df_enriched.copy()
            time_col = (
                "Date" if "Date" in df_aligned.columns else (df_aligned.index.name or "index")
            )

            # ---------- Identify oscillators & bottom pane ----------
            rsi_item = next(
                (
                    it
                    for it in (spec.get("oscillators") or [])
                    if it.get("on") and (it.get("type") or "").lower() == "rsi"
                ),
                None,
            )
            macd_item = next(
                (
                    it
                    for it in (spec.get("oscillators") or [])
                    if it.get("on") and (it.get("type") or "").lower() == "macd"
                ),
                None,
            )
            has_rsi, has_macd = rsi_item is not None, macd_item is not None
            bottom_axis_target = "macd" if has_macd else ("rsi" if has_rsi else "price")

            # ---------- Build mask for visible rows of the bottom pane ----------
            mask = pd.Series(True, index=df_aligned.index)
            if bottom_axis_target == "rsi" and rsi_item:
                rsi_col = _cols_for_rsi(rsi_item)
                if rsi_col in df_aligned.columns:
                    m_rsi = df_aligned[rsi_col].notna()
                    if m_rsi.any():
                        mask = m_rsi
            elif bottom_axis_target == "macd" and macd_item:
                macd_col, sig_col, hist_col = _cols_for_macd(macd_item)
                if hist_col in df_aligned.columns:
                    m_macd = df_aligned[hist_col].notna()
                    if m_macd.any():
                        mask = m_macd

            # ---------- X-axis scaffold (Overview-like; x-axis only) ----------
            # 1) Decide which pane owns the bottom axis (you likely already have this)
            rsi_item = next(
                (
                    it
                    for it in (spec.get("oscillators") or [])
                    if it.get("on") and (it.get("type") or "").lower() == "rsi"
                ),
                None,
            )
            macd_item = next(
                (
                    it
                    for it in (spec.get("oscillators") or [])
                    if it.get("on") and (it.get("type") or "").lower() == "macd"
                ),
                None,
            )
            has_rsi, has_macd = rsi_item is not None, macd_item is not None
            bottom_axis_target = "macd" if has_macd else ("rsi" if has_rsi else "price")

            # 2) Build a mask representing the bottom pane's visible rows
            mask = pd.Series(True, index=df_aligned.index)
            if bottom_axis_target == "rsi" and rsi_item:
                rsi_col = _cols_for_rsi(rsi_item)
                if rsi_col in df_aligned.columns:
                    m = df_aligned[rsi_col].notna()
                    if m.any():
                        mask = m
            elif bottom_axis_target == "macd" and macd_item:
                macd_col, sig_col, hist_col = _cols_for_macd(macd_item)
                if hist_col in df_aligned.columns:
                    m = df_aligned[hist_col].notna()
                    if m.any():
                        mask = m

            # 3) Get the timestamp series (column or index)
            if time_col in df_aligned.columns:
                ts = pd.to_datetime(df_aligned[time_col], errors="coerce")
            else:
                ts = pd.to_datetime(
                    pd.Series(df_aligned.index, index=df_aligned.index, name=time_col),
                    errors="coerce",
                )

            # 4) Normalize timestamps to the display timezone (env → viewer local → NY)
            import os
            from datetime import datetime
            from zoneinfo import ZoneInfo

            tz_env = os.getenv("FISCRAPE_DISPLAY_TZ")
            _disp_tz = (
                ZoneInfo(tz_env)
                if tz_env
                else (datetime.now().astimezone().tzinfo or ZoneInfo("America/New_York"))
            )

            if getattr(ts.dt, "tz", None) is not None:
                ts_disp = ts.dt.tz_convert(_disp_tz)
            else:
                ts_disp = ts.dt.tz_localize(ZoneInfo("America/New_York")).dt.tz_convert(_disp_tz)

            df_aligned["__ts_display__"] = ts_disp

            # 5) Build labels from the *visible* timeline only (organised anchors)
            ts_vis = ts_disp[mask]
            x_axis_for_labels = af.build_indexed_time_axis(ts_vis, target_ticks=6)

            # 6) Tight x-domain over the visible rows (no padding)
            vis_idx = df_aligned.loc[mask, "__x_idx__"]
            x_scale = alt.Scale(domain=(int(vis_idx.iloc[0]), int(vis_idx.iloc[-1])), nice=False)

            # 7) Tooltip spec (unchanged; keep using your time_col)
            time_tt = alt.Tooltip("__ts_display__:T", title="Date/Time")

            # ---------- Overlays (unchanged logic; draw only if column exists) ----------
            overlay_layers: List[alt.Chart] = []
            for item in spec.get("overlays") or []:
                if not item.get("on"):
                    continue
                for y_col in _col_for_overlay(item):
                    if y_col in df_aligned.columns:
                        overlay_layers.append(
                            alt.Chart(df_aligned)
                            .mark_line(opacity=0.85)
                            .encode(
                                x=alt.X("__x_idx__:Q"),
                                y=alt.Y(f"{y_col}:Q"),
                                tooltip=[time_tt, alt.Tooltip(f"{y_col}:Q", title="Value")],
                            )
                        )

            # ---------- Price + Volume ----------
            price_plus_volume, msg = af.ov_render_history_chart(
                hist=df_aligned,  # use the same df as axis
                style_label=style_label,
                show_bottom_x_axis=(bottom_axis_target == "price"),
                overlays=overlay_layers or None,
            )
            if price_plus_volume is None:
                st.info(msg or "No chart")

            panes = [price_plus_volume]

            # ---------- RSI pane ----------
            if has_rsi:
                rsi_col = _cols_for_rsi(rsi_item)
                if rsi_col and rsi_col in df_aligned.columns:
                    rsi_line = (
                        alt.Chart(df_aligned)
                        .mark_line()
                        .encode(
                            x=alt.X(
                                "__x_idx__:Q",
                                scale=x_scale,
                                axis=(x_axis_for_labels if bottom_axis_target == "rsi" else None),
                            ),
                            y=alt.Y(f"{rsi_col}:Q", axis=alt.Axis(title="RSI", tickCount=4)),
                            tooltip=[time_tt, alt.Tooltip(f"{rsi_col}:Q", title="Value")],
                        )
                        .properties(height=100)
                    )
                    rsi_refs = (
                        alt.Chart(pd.DataFrame({"lvl": [30, 70]}))
                        .mark_rule(strokeDash=[4, 4])
                        .encode(y="lvl:Q")
                    )
                    panes.append(rsi_line + rsi_refs)

            # ---------- MACD pane ----------
            if has_macd:
                macd_col, sig_col, hist_col = _cols_for_macd(macd_item)
                if all(c in df_aligned.columns for c in (macd_col, sig_col, hist_col)):
                    macd_color = alt.condition(
                        f"datum.{hist_col} >= 0", alt.value("#2ca02c"), alt.value("#d62728")
                    )

                macd_bars = (
                    alt.Chart(df_aligned)
                    .mark_bar()
                    .encode(
                        x=alt.X("__x_idx__:Q", scale=x_scale, axis=x_axis_for_labels),
                        y=alt.Y(f"{hist_col}:Q", title="MACD"),
                        color=macd_color,
                        tooltip=[time_tt, alt.Tooltip(f"{hist_col}:Q", title="Value")],
                    )
                    .properties(height=120)
                )
                macd_line = (
                    alt.Chart(df_aligned)
                    .mark_line()
                    .encode(
                        x=alt.X("__x_idx__:Q", scale=x_scale, axis=x_axis_for_labels),
                        y=alt.Y(f"{macd_col}:Q"),
                        tooltip=[time_tt, alt.Tooltip(f"{macd_col}:Q", title="Value")],
                    )
                )
                sig_line = (
                    alt.Chart(df_aligned)
                    .mark_line(strokeDash=[4, 4])
                    .encode(
                        x=alt.X("__x_idx__:Q", scale=x_scale, axis=x_axis_for_labels),
                        y=alt.Y(f"{sig_col}:Q"),
                        tooltip=[time_tt, alt.Tooltip(f"{sig_col}:Q", title="Value")],
                    )
                )
                panes.append(macd_bars + macd_line + sig_line)

            # ---------- Render ----------
            if has_rsi or has_macd:
                tech_chart = alt.vconcat(*panes, spacing=4).resolve_scale(
                    x="shared", y="independent"
                )
                st.altair_chart(tech_chart, use_container_width=True)
            else:
                st.altair_chart(price_plus_volume, use_container_width=True)

    # ---- JSON spec ----
    st.subheader("Spec (live)")
    st.json(spec, expanded=False)

    return spec


# -------------
# Run it
# -------------
if __name__ == "__main__":
    with st.sidebar:
        st.write("Placeholder Sidebar")
    render_technicals_ui()
