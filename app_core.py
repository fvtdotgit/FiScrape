# app_core.py: centralized helpers for FiScrape.
"""app_core.py — UI-facing core helpers

Boundaries:
- UI calls here for thin facades; no direct yfinance (network at services.*).
- Formatting lives in app_format.py; computations remain pure here.
- This module does not fetch intraday/EOD history directly.
"""

from __future__ import annotations

import time
from typing import Any, Optional

import pandas as pd
import streamlit as st
from pandas import DataFrame

from fiscrape_core import (
    INTERVAL_API2UI,
    INTERVAL_UI2API,
    PERIOD_API2UI,
    PERIOD_UI2API,
    FundamentalsAnalyzer,
    Portfolio,
    hasRetrievablePrice,
    interval_to_api,
    json_digest,
    keys_for,
    normalize_interval_key,
    normalize_period_key,
    period_to_api,
    resolve_timeframe,
    snapshot,
    ttl_bucket,
    utc_now,
)


# ========================= PUBLIC API & SESSION HELPERS =========================
def compute_ui_refresh_secs(portfolio_name: str) -> int:
    """
    UI refresh cadence = MIN of relevant TTLs:
      - Always include ttl_quote
      - Include ttl_intraday if active interval is intraday, else ttl_eod
      - Include ttl_pipeline (so UI wakes up to trigger a rescrape)
    Falls back to intraday TTL if no interval has been chosen yet.
    """
    rcfg = get_pf_settings(portfolio_name)["refresh"]
    ttls = [int(rcfg.get("ttl_quote", 3))]

    ov_itv = st.session_state.get(f"ov_interval_{portfolio_name}")  # e.g. '1m' or '1D'

    if ov_itv:
        if is_intraday_interval(ov_itv):
            ttls.append(int(max(60, rcfg.get("ttl_intraday", 60))))
        else:
            ttls.append(int(rcfg.get("ttl_eod", 300)))
    else:
        ttls.append(int(max(60, rcfg.get("ttl_intraday", 60))))

    ttls.append(int(rcfg.get("ttl_pipeline", 120)))
    return max(1, min(ttls))


def get_chart_selections(sel: str) -> tuple[str, str, str]:
    """
    Return (period_choice, interval_choice, plot_style) from session_state with safe defaults.
    We pre-read these before rendering widgets so we can place the controls anywhere in the UI.
    - period_choice: UI key (e.g., "1D", "1M", "YTD")
    - interval_choice: UI key (e.g., "1m", "5m", "1D", "1W")
    - plot_style: "Line" | "Candlestick"
    """
    p_key, i_key, s_key = f"ov_period_{sel}", f"ov_interval_{sel}", f"ov_style_{sel}"

    # Read raw (may be missing or API-like); normalize to UI keys with defaults (1D / 1m)
    period_ui = normalize_period_key(st.session_state.get(p_key))
    interval_ui = normalize_interval_key(st.session_state.get(i_key))

    # Style default (keep your two options; fall back to "Line")
    raw_style = st.session_state.get(s_key, "Line")
    style_ui = raw_style if raw_style in ("Line", "Candlestick") else "Line"

    # Write back normalized values so downstream widgets/state are consistent
    st.session_state[p_key] = period_ui
    st.session_state[i_key] = interval_ui
    st.session_state[s_key] = style_ui

    return period_ui, interval_ui, style_ui


def get_pf_settings(name: str) -> dict[str, Any]:
    """Return mutable settings dict for this portfolio (created if missing)."""
    if "pf_settings" not in st.session_state:
        st.session_state.pf_settings = {}

    pf_settings: dict[str, dict[str, Any]] = st.session_state.pf_settings

    if name not in pf_settings:
        pf_settings[name] = {"parallel": True, "workers": 8, "period": "yearly"}

    s = pf_settings[name]

    # ---- Refresh defaults (no explicit UI interval; derived from TTLs) ----
    r = s.setdefault("refresh", {})
    r.setdefault("live", False)  # enable page auto-reruns
    r.pop("interval_sec", None)  # remove legacy setting if present
    r.setdefault("ttl_quote", 10)  # live quote cadence (seconds)
    r.setdefault("ttl_intraday", 90)  # minute-bar cadence (>=60s)
    r.setdefault("ttl_eod", 1800)  # daily/weekly/monthly cadence
    r.setdefault("ttl_pipeline", 3600)  # FULL rescrape/analyze cadence (seconds)

    return s


def is_intraday_interval(label_or_code: str) -> bool:
    intraday = {"1m", "2m", "5m", "15m", "30m", "60m", "90m"}
    s = str(label_or_code or "")
    return s in intraday or s.endswith("m")


# ========================= PORTFOLIO UTILITIES & SIDEBAR HELPERS =========================
def dedupe_portfolio(portfolio_name: str) -> int:
    """
    Remove duplicate tickers in a portfolio (case-insensitive, keep first).
    Returns the number of removed items.
    """
    ticks = st.session_state.portfolios.get(portfolio_name, [])
    if not ticks:
        return 0

    seen = set()
    new = []
    for t in ticks:
        u = (t or "").strip().upper()
        if u and u not in seen:
            seen.add(u)
            new.append(u)

    removed = len(ticks) - len(new)
    if removed:
        st.session_state.portfolios[portfolio_name] = new
        # keep selection valid
        choose_key = f"ov_choose_{portfolio_name}"
        chosen = st.session_state.get(choose_key)
        if chosen and chosen.upper() not in seen:
            st.session_state[choose_key] = new[0] if new else None
    return removed


def ensure_selection():
    """Keep selected_portfolio valid and non-empty when possible."""
    po = st.session_state.portfolio_order
    sel = st.session_state.selected_portfolio
    if not po:
        st.session_state.selected_portfolio = None
        return
    if sel not in po:
        st.session_state.selected_portfolio = po[0]


def ensure_ticker_choice(portfolio_name: str, tickers: list[str]) -> str | None:
    """Keep st.session_state['ov_choose_<portfolio>'] valid for the current ticker list."""
    key = f"ov_choose_{portfolio_name}"
    if not tickers:
        st.session_state.pop(key, None)
        return None

    chosen = st.session_state.get(key)
    if chosen not in tickers:
        st.session_state[key] = tickers[0]
        return tickers[0]
    return chosen


def _purge_portfolio_widget_keys(name: str):
    """Remove ephemeral widget keys for a given portfolio to avoid key-policy errors."""
    suffix = f"_{name}"
    widget_prefixes = (
        "po_up_",
        "po_down_",
        "po_move_to_pos_btn",
        "po_pos_picker",
        "rename_",
        "rename_btn_",
        "tup_",
        "tdown_",
        "tdel_",
        "bulk_sel_",
        "bulk_dest_",
        "bulk_move_",
        "add_",
        "add_submit_",
        "add_confirm_",
        "add_clear_",
        "ov_choose_",
        "ov_period_",
        "ov_interval_",
        "ov_style_",
        "set_period_",
        "set_parallel_",
        "set_workers_",
        "set_ttl_quote_",
        "set_ttl_intraday_",
        "set_ttl_eod_",
        "set_ttl_pipeline_",
        "ui_live_",
        "tick_sel_",
        "tick_pos_",
        "tick_move_btn_",
        "tick_sort_az_",
        "tick_sort_rev_",
        "tick_dedupe_",
    )

    for k in list(st.session_state.keys()):
        if k.endswith(suffix) and any(k.startswith(pfx) for pfx in widget_prefixes):
            st.session_state.pop(k, None)


# ========================= SNAPSHOT & PROFILE CACHE =========================
@st.cache_data(show_spinner=False)
def _cached_profile(*, portfolio_name: str, sym: str, profile_digest: str, refresh_nonce: int):
    # fetched once per (portfolio, sym, fundamentals_version) and manual Refresh
    try:
        return get_profile_snapshot(portfolio_name, sym)
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def _cached_snapshot(
    *,
    portfolio_name: str,
    sym: str,
    period: str,
    interval: str,
    include_history: bool,
    cache_salt: tuple[int, int, int],  # unchanged (quote_bucket, hist_bucket, nonce)
    profile_digest: str,
) -> tuple[dict[str, Any], DataFrame, DataFrame]:
    refresh_nonce = cache_salt[2]  # reuse same nonce to allow manual bust

    # NEW: try to use the stashed profile to avoid a second fetch
    try:
        _stash = st.session_state.get("_profile_stash") or {}
        profile = _stash.pop((portfolio_name, sym, profile_digest, int(refresh_nonce)), None)
    except Exception:
        profile = None

    if profile is None:
        # Fallback to cached profile fetch (unchanged behavior)
        profile = _cached_profile(
            portfolio_name=portfolio_name,
            sym=sym,
            profile_digest=profile_digest,
            refresh_nonce=refresh_nonce,
        )

    # ---- UI timing markers (success) ----
    try:
        rcfg = get_pf_settings(portfolio_name)["refresh"]
        ttl_quote = int(rcfg.get("ttl_quote", 10))
        ttl_intraday = int(max(60, rcfg.get("ttl_intraday", 60)))
        ttl_eod = int(rcfg.get("ttl_eod", 1800))
        hist_lane = "intraday" if is_intraday_interval(interval) else "eod"

        # Quote: treat "Fetched" == lastFetchTs (no provider timestamps)
        _update_lane_timing(portfolio_name, sym, "quote", event="success", ttl=ttl_quote)

        # History: treat "Fetched" == lastFetchTs (no last-bar timestamp)
        if include_history:
            ttl_hist = ttl_intraday if hist_lane == "intraday" else ttl_eod
            _update_lane_timing(portfolio_name, sym, hist_lane, event="success", ttl=ttl_hist)
    except Exception:
        pass  # best-effort UI meta

    info, metrics, hist = snapshot(
        sym,
        period=period,
        interval=interval,
        include_history=include_history,
        profile=profile,
    )

    # ---- UI timing markers (success) ----
    try:
        rcfg = get_pf_settings(portfolio_name)["refresh"]
        ttl_quote = int(rcfg.get("ttl_quote", 10))
        ttl_intraday = int(max(60, rcfg.get("ttl_intraday", 60)))
        ttl_eod = int(rcfg.get("ttl_eod", 1800))
        hist_lane = "intraday" if is_intraday_interval(interval) else "eod"

        data_ts = None
        try:
            src_flag = (info or {}).get("activePriceSource")
            if src_flag == "regular":
                data_ts = (info or {}).get("regularMarketTime")
            elif src_flag == "pre":
                data_ts = (info or {}).get("preMarketTime")
            elif src_flag == "post":
                data_ts = (info or {}).get("postMarketTime")
            if isinstance(data_ts, float):
                data_ts = int(data_ts)
        except Exception:
            data_ts = None

        _update_lane_timing(portfolio_name, sym, "quote", event="success", ttl=ttl_quote)

        # History lane: last bar time
        if include_history:
            ttl_hist = ttl_intraday if hist_lane == "intraday" else ttl_eod
            _update_lane_timing(portfolio_name, sym, hist_lane, event="success", ttl=ttl_hist)
    except Exception:
        pass  # best-effort UI meta

    if not isinstance(hist, pd.DataFrame):
        hist = pd.DataFrame()
    return info, metrics, hist


def _ensure_profile_store():
    if "profiles" not in st.session_state:
        st.session_state.profiles = {}  # {(portfolio_name, sym): dict}


def _extract_profile_from_inst(inst) -> dict:
    info = getattr(inst, "ticker_info", {}) or {}
    profile_keys = keys_for("profile")
    return {k: info.get(k) for k in profile_keys}


def get_profile_snapshot(portfolio_name: str, sym: str) -> dict:
    """Return cached profile snapshot for (portfolio, symbol); empty dict if missing."""
    _ensure_profile_store()
    return st.session_state.profiles.get((portfolio_name, sym), {})


def set_profile_snapshot(portfolio_name: str, sym: str, profile: dict) -> None:
    _ensure_profile_store()
    st.session_state.profiles[(portfolio_name, sym)] = dict(profile or {})


# ========================= TIMING STASH (per ticker × lane) =========================
# Lanes: 'quote', 'intraday', 'eod', 'profile' (timing only; no network)
def _ensure_timing_store() -> None:
    if "lane_timings" not in st.session_state:
        st.session_state.lane_timings = {}


def get_lane_timing(portfolio_name: str, sym: str, lane: str) -> dict:
    _ensure_timing_store()
    return st.session_state.lane_timings.get((portfolio_name, sym, lane), {})


def _update_lane_timing(
    portfolio_name: str,
    sym: str,
    lane: str,
    *,
    event: str,  # 'attempt' | 'success' | 'failure'
    ttl: int,
    error: str | None = None,
) -> None:
    _ensure_timing_store()
    key = (portfolio_name, sym, lane)
    rec = dict(st.session_state.lane_timings.get(key) or {})
    now_ts = int(time.time())
    rec.setdefault("lastAttemptTs", None)
    rec.setdefault("lastFetchTs", None)
    rec.setdefault("nextDueTs", None)
    rec.setdefault("lastError", None)
    rec.setdefault("inFlight", False)

    if event == "attempt":
        rec["lastAttemptTs"] = now_ts
        rec["inFlight"] = True
    elif event == "success":
        rec["lastFetchTs"] = now_ts
        rec["nextDueTs"] = now_ts + int(ttl)
        rec["lastError"] = None
        rec["inFlight"] = False
    elif event == "failure":
        rec["lastAttemptTs"] = now_ts
        rec["lastError"] = str(error) if error else "ERROR"
        rec["inFlight"] = False

    st.session_state.lane_timings[key] = rec


# ========================= BACKEND OBJECTS & PIPELINE ORCHESTRATION =========================
def get_portfolio(portfolio_name: str) -> Portfolio:
    """Create or fetch the Portfolio object for a given portfolio name (no analyzer)."""
    pf = st.session_state.portfolio_objs.get(portfolio_name)
    if pf is None:
        pf = Portfolio()
        st.session_state.portfolio_objs[portfolio_name] = pf
    return pf


def get_ticker_warnings(pf: Portfolio, symbol: str) -> list[dict[str, Any]]:
    ti = getattr(pf, "ticker_instances", {}).get(symbol)
    if not ti:
        return []
    try:
        return ti.get_warnings()  # type: ignore[attr-defined]
    except Exception:
        return []


def _pf_inst(portfolio_name: str, sym: str):
    """Return the portfolio's ticker instance if it exists; else None."""
    try:
        pf = get_portfolio(portfolio_name)
        return getattr(pf, "ticker_instances", {}).get(sym)
    except Exception:
        return None


def run_pipeline_for_portfolio(
    portfolio_name: str,
    tickers: list[str],
    period: str | None = None,
    parallel: bool | None = None,
    workers: int | None = None,
    force: bool = False,
    _pipeline_bucket: int | None = None,
) -> None:
    """
    Pipeline = Scrape → Fundamentals (statements-only) → snapshot profile.
    Live/technicals are handled on-demand by snapshot.
    """
    # ---- Effective settings ----
    s = get_pf_settings(portfolio_name)
    period = s["period"] if period is None else period
    parallel = s["parallel"] if parallel is None else parallel
    workers = s["workers"] if workers is None else workers

    # ---- Normalize & gate by price ----
    raw_norm = list(dict.fromkeys([t.strip().upper() for t in (tickers or []) if t and t.strip()]))
    valid_syms = [t for t in raw_norm if hasRetrievablePrice(t)]
    invalid_syms = [t for t in raw_norm if t not in valid_syms]
    if invalid_syms:
        st.session_state.portfolios[portfolio_name] = valid_syms
        choose_key = f"ov_choose_{portfolio_name}"
        if st.session_state.get(choose_key) in invalid_syms:
            st.session_state[choose_key] = valid_syms[0] if valid_syms else None
        st.warning(f"Removed invalid tickers (no retrievable price): {', '.join(invalid_syms)}")

    pf = get_portfolio(portfolio_name)

    # ---- Initial sync BEFORE deciding stages
    _sync_symbol_lists(pf, valid_syms)

    # ---- Pipeline bucket (periodic fundamentals/profile refresh cadence) ----
    if _pipeline_bucket is None:
        rcfg = s["refresh"]
        _pipeline_bucket = int(time.time() // int(rcfg.get("ttl_pipeline", 120)))

    # ---- Keys based on validated list + pipeline bucket ----
    PIPELINE_VER = 2
    scrape_key = (
        "scrape",
        PIPELINE_VER,
        tuple(valid_syms),
        bool(parallel),
        int(workers),
        _pipeline_bucket,
    )
    funds_key = ("fundamentals", PIPELINE_VER, tuple(valid_syms), str(period), _pipeline_bucket)
    last_runs = st.session_state.last_runs

    if not valid_syms:
        last_runs[(portfolio_name, "scrape")] = scrape_key
        last_runs[(portfolio_name, "fundamentals")] = funds_key
        return

    need_scrape = force or (last_runs.get((portfolio_name, "scrape")) != scrape_key)
    need_funds = (
        force or need_scrape or (last_runs.get((portfolio_name, "fundamentals")) != funds_key)
    )

    # ---- SCRAPE ----
    if need_scrape:
        # UI timing markers: profile lane attempt for all valid symbols
        try:
            ttl_profile = int(get_pf_settings(portfolio_name)["refresh"].get("ttl_pipeline", 3600))
            for _sym in valid_syms:
                _update_lane_timing(
                    portfolio_name, _sym, "profile", event="attempt", ttl=ttl_profile
                )
        except Exception:
            pass

        pf.scrape(valid_syms, parallel=parallel, max_workers=workers)
        ...
        # Persist one-and-done profile snapshot (no live calls)
        try:
            ti_pf_now = getattr(pf, "ticker_instances", {})
            for sym in valid_syms:
                inst = ti_pf_now.get(sym)
                if inst is None:
                    continue
                profile = _extract_profile_from_inst(inst)
                if profile:
                    set_profile_snapshot(portfolio_name, sym, profile)

                    # UI timing marker: profile lane success
                    try:
                        ttl_profile = int(
                            get_pf_settings(portfolio_name)["refresh"].get("ttl_pipeline", 3600)
                        )
                        _update_lane_timing(
                            portfolio_name, sym, "profile", event="success", ttl=ttl_profile
                        )
                    except Exception:
                        pass
        except Exception:
            pass

        last_runs[(portfolio_name, "scrape")] = scrape_key

        if not valid_syms:
            last_runs[(portfolio_name, "fundamentals")] = funds_key
            return

    # ---- FUNDAMENTALS (stateless) ----
    if need_funds:
        _sync_symbol_lists(pf, valid_syms)
        if valid_syms:
            FundamentalsAnalyzer().analyze(pf, period=period)
        last_runs[(portfolio_name, "fundamentals")] = funds_key


def _sync_symbol_lists(pf: Portfolio, symbols: list[str]) -> None:
    """
    Keep the Portfolio aligned with the filtered symbol set.
    - Drops stray/None instances
    - Writes list-like attributes ('tickers', 'symbols', 'ticker_list') on the portfolio only
    """
    # 1) Prune portfolio instances to the surviving set
    ti_pf = getattr(pf, "ticker_instances", {})
    for sym in list(ti_pf.keys()):
        if sym not in symbols or ti_pf[sym] is None:
            ti_pf.pop(sym, None)

    # 2) Reflect symbol lists on the portfolio (if it keeps a list)
    for attr in ("tickers", "symbols", "ticker_list"):
        if hasattr(pf, attr):
            try:
                setattr(pf, attr, [t for t in symbols])
            except Exception:
                pass


# ========================= LIVE SNAPSHOT / METRICS FACADE =========================
def get_metrics_and_history(
    portfolio_name: str,  # kept for call-site compatibility
    sym: str,
    period: str,
    interval: str,
    *,
    include_history: bool = True,
    refresh_nonce: Optional[int] = None,
    ttl_quote: Optional[int] = None,
    ttl_intraday: Optional[int] = None,
    ttl_eod: Optional[int] = None,
) -> tuple[dict[str, Any], DataFrame, DataFrame]:
    """
    Returns (metrics, hist, info) with caching fully managed HERE.

    TTL PRIORITY:
      1) Layout settings (st.session_state.settings[...] or top-level keys)
      2) Function kwargs (ttl_quote / ttl_intraday / ttl_eod)
      3) Defaults in _DEFAULT_TTLS

    Manual refresh:
      - Pass refresh_nonce or define st.session_state["refresh_nonce"] in UI.
      - Any change to refresh_nonce forces an immediate cache miss.
    """
    now = utc_now()

    # TTLs (layout-first)
    ttl_q = _resolve_ttl("quote", ttl_quote)
    ttl_i = _resolve_ttl("intraday", ttl_intraday)
    ttl_d = _resolve_ttl("eod", ttl_eod)

    # Pick history TTL by granularity
    is_intraday = interval.endswith("m") or interval.endswith("h")
    ttl_hist = ttl_i if is_intraday else ttl_d

    # Buckets (used ONLY in cache key)
    q_bucket = ttl_bucket(now, ttl_q)
    h_bucket = ttl_bucket(now, ttl_hist)

    # Manual refresh bump (layout-first)
    if refresh_nonce is None:
        refresh_nonce = int(st.session_state.get("refresh_nonce", 0))
    else:
        refresh_nonce = int(refresh_nonce)

    # Derive a lightweight version token for the profile so cache invalidates when it changes
    try:
        _profile = get_profile_snapshot(portfolio_name, sym)
        profile_digest = json_digest(_profile) if _profile else "0"
    except Exception:
        _profile = None
        profile_digest = "0"

    # Stash the preloaded profile for this exact call key
    try:
        _stash = st.session_state.setdefault("_profile_stash", {})
        _stash[(portfolio_name, sym, profile_digest, int(refresh_nonce))] = _profile
    except Exception:
        pass

    return _cached_snapshot(
        portfolio_name=portfolio_name,
        sym=sym,
        period=period,
        interval=interval,
        include_history=include_history,
        cache_salt=(q_bucket, h_bucket, refresh_nonce),
        profile_digest=profile_digest,
    )


# ========================= PICKERS & CLASSIFIERS (pure) =========================
def pick_aum(info: dict) -> float | None:
    """ETF AUM: prefer netAssets, fallback totalAssets."""
    try:
        return info.get("netAssets") or info.get("totalAssets")
    except Exception:
        return None


def pick_today_volume(info: dict[str, Any] | Any) -> float | None:
    """Prefer regularMarketVolume; fallback to volume."""
    try:
        return info.get("regularMarketVolume") or info.get("volume")
    except Exception:
        return None


# ========================= INTERNAL HELPERS (TTL / UI) =========================
_DEFAULT_TTLS = {
    "quote": 15,  # quote-derived metrics
    "intraday": 60,  # minute/hour history
    "eod": 3600,  # daily + history
}


def _resolve_ttl(ttl_name: str, arg_value: Optional[int]) -> int:
    """
    PRIORITY: (1) layout settings -> (2) function kwarg -> (3) module default.
    Accepts multiple common key variants from the UI.
    """
    key_variants = [
        f"{ttl_name}_seconds",  # e.g., "quote_seconds"
        f"ttl_{ttl_name}_seconds",  # e.g., "ttl_quote_seconds"
        f"ttl_{ttl_name}",  # e.g., "ttl_quote"
        ttl_name,  # e.g., "quote"
    ]
    ui_val = _resolve_ui_setting(key_variants)
    if ui_val is not None:
        return int(ui_val)
    if arg_value is not None:
        return int(arg_value)
    return int(_DEFAULT_TTLS[ttl_name])


def _resolve_ui_setting(name_variants: list[str]) -> Optional[int]:
    """
    Look in layout-configured settings FIRST, then session_state top-level.
    Returns int or None if not found.
    """
    settings = st.session_state.get("settings") or {}
    # 1) nested settings dict in layout
    for k in name_variants:
        v = settings.get(k)
        if v is not None:
            try:
                return int(v)
            except Exception:
                pass
    # 2) top-level session_state keys as fallback
    for k in name_variants:
        v = st.session_state.get(k)
        if v is not None:
            try:
                return int(v)
            except Exception:
                pass
    return None


# ========================= UI / CORE ADAPTERS =========================
def map_chart_selections(period_choice: str, interval_choice: str) -> tuple[str, str]:
    period_map = {
        "1D": "1d",
        "5D": "5d",
        "1M": "1mo",
        "3M": "3mo",
        "6M": "6mo",
        "YTD": "ytd",
        "1Y": "1y",
        "5Y": "5y",
        "Max": "max",
    }
    interval_map = {
        "1m": "1m",
        "2m": "2m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "60m": "60m",
        "90m": "90m",
        "1D": "1d",
        "5D": "5d",
        "1W": "1wk",
        "1M": "1mo",
        "3M": "3mo",
    }
    return period_map[period_choice], interval_map[interval_choice]
