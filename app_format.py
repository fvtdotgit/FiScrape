# app_format.py
"""UI formatting helpers (display-only; no network/state)

Number/unit formatters, table/label utilities, and small render helpers for Streamlit.
Keep computations pure; no provider logic. Do not round beyond display needs.
"""

import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo
import os

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from altair import VConcatChart, LayerChart, Chart, Axis, Scale

from fiscrape_core import (
    INTERVAL_UI2API,
    PERIOD_UI2API,
    change_pct,
    classify_market_session,
    normalize_interval_key,
    normalize_period_key,
    period_to_api,
    range_endpoints_as_pct_of_price,
    resolve_timeframe,
    to_float,
)


# ========================= NUMBER & UNIT FORMATTING =========================
def format_sign(s: str) -> str:
    """Normalize Unicode minus/spacing to plain '-' so Streamlit metrics color correctly."""
    return s.replace("\u2212", "-").replace("−", "-").replace(" ", "")


def format_abbrev(x, *, decimals=2, below_1000_commas=True):
    """Abbreviate large numbers to K/M/B/T with fixed decimals.
    - Returns '—' for None.
    - For values < 1,000, uses thousands separators if below_1000_commas is True.
    """
    if x is None:
        return "—"
    try:
        n = float(x)
    except Exception:
        return str(x)

    a = abs(n)
    for unit, div in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if a >= div:
            return f"{n / div:.{decimals}f}{unit}"
    return f"{n:,.0f}" if below_1000_commas else f"{n:.0f}"


def format_date(ts):
    """Format a date-like value.
    - If epoch seconds (int/float), return ISO date (YYYY-MM-DD).
    - Else, return str(ts).
    - Return '—' on error.
    """
    try:
        # yfinance often returns epoch seconds for dates
        if isinstance(ts, (int, float)):
            import pandas as pd

            return pd.to_datetime(ts, unit="s", utc=True).date().isoformat()
        return str(ts)
    except Exception:
        return "—"


def format_timestamp(ts):
    """Format epoch seconds as 'YYYY-MM-DD HH:MM' (local time).
    Returns None on error.
    """
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return None


def format_number(x, decimals: int | None = None) -> str:
    """
    Human-friendly number formatter.
    - ints → thousands separators (e.g., 12,345)
    - floats → default 2 decimals (or 4 if |x| < 1), unless `decimals` is provided
    - None / non-finite → "—"
    """
    import math

    try:
        if x is None:
            return "—"

        # Booleans are ints in Python; avoid showing True/False as 1/0
        if isinstance(x, bool):
            return "—"

        if isinstance(x, int):
            return f"{x:,}"

        if isinstance(x, float):
            if not math.isfinite(x):
                return "—"
            d = decimals if decimals is not None else (2 if abs(x) >= 1 else 4)
            s = f"{x:,.{d}f}"
            # Strip trailing zeros and possible trailing dot
            s = s.rstrip("0").rstrip(".")
            return s if s else "0"

        # Try coercing other numeric-like types (e.g., numpy, Decimal)
        val = float(x)
        if not math.isfinite(val):
            return "—"
        d = decimals if decimals is not None else (2 if abs(val) >= 1 else 4)
        s = f"{val:,.{d}f}".rstrip("0").rstrip(".")
        return s if s else "0"
    except Exception:
        return "—"


def format_pct(p):
    """Format percentage with sign, e.g., '+1.23%'.
    Returns None if value is not parseable.
    """
    try:
        return f"{float(p):+.2f}%"
    except Exception:
        return None


def format_price(x):
    """Format a numeric price to '#,###.##' with two decimals.
    Returns '—' on error.
    """
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return "—"


# ========================= UI RENDER HELPERS (display-only) =========================
def render_warnings(
    warnings: Iterable[Dict[str, Any]],
    *,
    title: str = "Warnings",
    compact: bool = False,
) -> None:
    """Render a de-duplicated, non-blocking warning list.

    Deduplicates by (code, where, message). Intended for advisory notices only; do not
    block UI flows here. Use st.error only for hard failures users can resolve.
    """
    warns = list(warnings or [])
    if not warns:
        return
    st.subheader(title) if not compact else st.caption(title)

    # Dedup by (code, where, message) to reduce noise
    seen = set()
    for w in warns:
        key = (w.get("code"), w.get("where"), w.get("message"))
        if key in seen:
            continue
        seen.add(key)

        code = w.get("code") or "INFO"
        where = w.get("where") or "—"
        msg = w.get("message") or "—"
        ctx = w.get("ctx")

        st.markdown(f"**{code}** · `{where}`")
        st.write(msg)
        if ctx:
            with st.expander("Context", expanded=False):
                st.json(ctx)
        st.divider()


# UI-only helpers for bulk actions
def render_bulk_delete_portfolios_controls(
    portfolios: Sequence[str],
    *,
    key_prefix: str,
    title: Optional[str] = "Delete portfolios",
) -> Tuple[List[str], bool, bool]:
    """
    Returns (selected_portfolios, confirmed, clicked).
    Confirmation is required to enable the delete action.
    """
    if title:
        st.markdown(f"### {title}")
    sel = multiselect_with_numbered_labels(
        "Select portfolios to delete",
        portfolios,
        key=f"{key_prefix}_sel",
        help="This action is permanent.",
    )
    confirmed = st.checkbox(
        "I'm sure — permanently delete selected portfolios",
        key=f"{key_prefix}_confirm",
    )
    clicked = st.button(
        "Delete selected",
        key=f"{key_prefix}_btn",
        use_container_width=True,
        disabled=(not sel or not confirmed),
    )
    return sel, confirmed, clicked


def render_bulk_move_controls(
    source_name: str,
    tickers: Sequence[str],
    other_portfolios: Sequence[str],
    *,
    key_prefix: str,
    title: Optional[str] = "Bulk move tickers",
) -> tuple[List[str], Optional[str], bool]:
    """UI-only: choose tickers from `source_name`, pick a destination, click Move."""
    if title:
        st.markdown(f"### {title}")
    sel = multiselect_with_numbered_labels(
        f"From **{source_name}**",
        tickers,
        key=f"{key_prefix}_sel",
        help="Choose one or more tickers to move",
    )
    dest = (
        select_with_numbered_labels(
            "Destination portfolio",
            other_portfolios,
            key=f"{key_prefix}_dest",
            help="Pick where to move them",
            index=0 if other_portfolios else None,
        )
        if other_portfolios
        else None
    )
    did = st.button("Move selected", key=f"{key_prefix}_btn", use_container_width=True)
    return sel, dest, did


# ========================= Time-aware formatting + slider help (DISPLAY ONLY) =========================
# Local display timezone (auto-detect)
_LOCAL_TZ = datetime.now().astimezone().tzinfo  # safe local default
_NYSE_TZ = ZoneInfo("America/New_York")


def format_age_short(age_s: int) -> str:
    """Compact age string: seconds→'Xs', minutes→'Xm', hours→'Xh', days→'Xd'."""
    if age_s < 0:
        return "—"
    if age_s < 60:
        return f"{age_s}s"
    if age_s < 3600:
        return f"{age_s // 60}m"
    if age_s < 86400:
        return f"{age_s // 3600}h"
    return f"{age_s // 86400}d"


def freshness_dot(age_s: Optional[int], ttl_s: int, *, in_flight: bool = False) -> str:
    """Traffic-light glyph for freshness given `age_s` vs `ttl_s` (⏳/⚪/🟢/🟡/🔴)."""
    if in_flight:
        return "⏳"
    if age_s is None:
        return "⚪"
    if age_s <= 0.5 * ttl_s:
        return "🟢"
    if age_s <= ttl_s:
        return "🟡"
    return "🔴"


def fmt_abs(ts: Optional[int | float], tz: ZoneInfo | None = None) -> str:
    """Format epoch seconds as 'Sep 16, 19:22:04 TZ' in given tz (default: local)."""
    if not ts:
        return "—"
    tz = tz or _LOCAL_TZ
    dt = datetime.fromtimestamp(int(ts), tz)
    label = getattr(tz, "key", "")  # e.g., 'America/New_York'
    return f"{dt.strftime('%b %d, %H:%M:%S')} {label}".strip()


def compose_refresh_slider_label(title: str, rec: Optional[Mapping[str, Any]], ttl_s: int) -> str:
    """Inline label: 'Title  ·  🟢 12s' (no state mutation)."""
    now = int(time.time())
    last_fetch = (rec or {}).get("lastFetchTs")
    in_flight = bool((rec or {}).get("inFlight"))
    age = None if not last_fetch else max(0, now - int(last_fetch))
    dot = freshness_dot(age, ttl_s, in_flight=in_flight)
    age_txt = "—" if age is None else format_age_short(age)
    return f"{title}  ·  {dot} {age_txt}"


def _short_error(err: str, *, maxlen: int = 60) -> str:
    """
    Return 'CODE — snippet...' for context:
    - CODE = first token before ':' or whitespace (max 12 chars)
    - snippet = first maxlen chars of remainder or original
    """
    if not err:
        return ""
    s = str(err).strip()
    code = s.split(":", 1)[0].split(None, 1)[0][:12]
    rest = s[len(code) :].lstrip(": ").strip()
    snippet = (rest or s)[:maxlen]
    return f"{code} — {snippet}"


def build_lane_help(
    rec: Optional[Mapping[str, Any]],
    ttl_s: int,
    *,
    market_tz: Optional[str] = None,  # kept for compatibility; ignored (we show local tz)
    extras: Optional[str] = None,  # lines with 'inline: ' are folded into the previous bullet
) -> str:
    """
    Bullet-per-line tooltip (unified):
    • Fetched: <abs local> (Age: X ago)
    • Next due: in <rel> — <abs local>
    • Last attempt: <abs local> (CODE — snippet...)     [only if newer than success OR error]
    • Currently fetching…                               [only if inFlight]
    Extras: if a line starts with 'inline: ', it's folded into the previous bullet; otherwise a new bullet.
    """
    rec = rec or {}
    now = int(time.time())

    last_fetch = rec.get("lastFetchTs")
    last_attempt = rec.get("lastAttemptTs")
    next_due = rec.get("nextDueTs")
    last_err = rec.get("lastError")
    in_flight = bool(rec.get("inFlight"))

    # Bullet 1: Fetched (formerly 'Last success')
    age = None if not last_fetch else max(0, now - int(last_fetch))
    age_txt = "—" if age is None else f"{format_age_short(age)} ago"
    bullets: list[str] = [f"• Fetched: {fmt_abs(last_fetch)} (Age: {age_txt})"]

    # Bullet 2: Next due (one bullet: relative + absolute)
    if next_due:
        delta = max(0, int(next_due) - now)
        bullets.append(f"• Next due: in {format_age_short(delta)} — {fmt_abs(next_due)}")

    # Bullet 3: Last attempt (conditional)
    if last_attempt and (not last_fetch or int(last_attempt) > int(last_fetch) or last_err):
        tail = f" ({_short_error(last_err)})" if last_err else ""
        bullets.append(f"• Last attempt: {fmt_abs(last_attempt)}{tail}")

    # Bullet 4: In-flight
    if in_flight:
        bullets.append("• Currently fetching…")

    # Extras
    if extras:
        for raw in extras.splitlines():
            ln = (raw or "").strip()
            if not ln:
                continue
            if ln.lower().startswith("inline:"):
                # fold into previous bullet
                msg = ln[len("inline:") :].strip()
                if bullets:
                    bullets[-1] = f"{bullets[-1]} ({msg})"
                else:
                    bullets.append(f"• {msg}")
            else:
                # separate bullet; strip any leading bullets/dashes
                bullets.append(f"• {ln.lstrip('•- ').strip()}")

    return "\n".join(bullets)


# ============================= NUMBERED LABEL UTILITIES =============================
def numbered_label_map(items: Sequence[str], start: int = 1) -> Dict[str, str]:
    """Map each item to '1) item', '2) item', ... for stable, readable UI choices."""
    return {name: f"{i}. {name}" for i, name in enumerate(items, start=start)}


def select_with_numbered_labels(
    label: str,
    items: Sequence[str],
    *,
    key: str,
    help: str | None = None,
    index: int | None = 0,
) -> str:
    """`st.selectbox` that displays items as '1) name', '2) name', returning the raw value."""
    labmap = numbered_label_map(items)
    return st.selectbox(
        label,
        list(items),
        index=index if index is not None else 0,
        format_func=lambda x: labmap.get(x, x),
        key=key,
        help=help,
    )


def multiselect_with_numbered_labels(
    label: str,
    items: Sequence[str],
    *,
    key: str,
    help: str | None = None,
    default: List[str] | None = None,
) -> List[str]:
    """`st.multiselect` that displays items as '1) name', returning the raw selected values list."""
    labmap = numbered_label_map(items)
    return st.multiselect(
        label,
        list(items),
        default=default or [],
        format_func=lambda x: labmap.get(x, x),
        key=key,
        help=help,
    )


# Overview format functions
def render_cap_section(
    info: Mapping[str, Any], metrics: Mapping[str, Any]
) -> Dict[str, Optional[str]]:
    """
    Return {label, value_raw, help}. Leaves NUMBER formatting to caller.
    - ETF: Net Assets (AUM) -> netAssets | totalAssets
    - Equity: Market Cap -> info.marketCap | metrics.marketCap | price * shares (fallback)
    """
    qtype = (info.get("quoteType") or "").upper()
    sh_out = info.get("sharesOutstanding") or ""

    # ETF
    if qtype == "ETF":
        aum = info.get("netAssets") or info.get("totalAssets")
        return {
            "label": "Net Assets & Shares Outstanding",
            "value_raw": aum if isinstance(aum, (int, float)) else None,
            "delta": sh_out if isinstance(sh_out, (int, float)) else None,
            "help": "ETF total net assets (AUM) and total number of shares.",
        }

    # Equity
    mc = info.get("marketCap") or metrics.get("marketCap")
    if not isinstance(mc, (int, float)):
        pr = metrics.get("price") or info.get("regularMarketPrice")
        sh = (
            info.get("sharesOutstanding")
            or info.get("impliedSharesOutstanding")
            or info.get("floatShares")
        )
        if isinstance(pr, (int, float)) and isinstance(sh, (int, float)):
            mc = float(pr) * float(sh)
        else:
            mc = None

    return {
        "label": "Market Cap & Shares Outstanding",
        "value_raw": mc if isinstance(mc, (int, float)) else None,
        "delta": sh_out if isinstance(sh_out, (int, float)) else None,
        "help": "Equity market capitalization and total number of shares.",
    }


def render_price_section(info: dict) -> dict:
    """
    UI-only builder for price metric strings. Returns a dict with both the
    legacy keys (labelSuffix, topText, deltaText) and the new st.metric-ready
    fields (metricLabel, metricValue, metricDelta, metricHelp, deltaForArrow).
    No network calls; uses existing helpers and canon classification.
    """
    # ---- Inputs ----
    market_state = str((info or {}).get("marketState") or "").upper()
    session = classify_market_session(market_state)
    currency = str((info or {}).get("currency") or (info or {}).get("financialCurrency")).upper()

    # Prices & pct deltas (already computed upstream; no math here)
    reg_p = info.get("regularMarketPrice")
    reg_pc = info.get("regularMarketChangePercent")  # e.g., +0.45 (% units)
    pre_p, pre_pc = info.get("preMarketPrice"), info.get("preMarketChangePercent")
    post_p, post_pc = info.get("postMarketPrice"), info.get("postMarketChangePercent")

    # Timestamps (epoch seconds if present)
    t_reg = info.get("regularMarketTime")
    t_pre = info.get("preMarketTime")
    t_post = info.get("postMarketTime")

    # ---- Label suffix (friendly state) ----
    if session == "PRE":
        label_suffix, ext_tag = "Regular · Pre-market", "PM"
    elif session == "POST":
        label_suffix, ext_tag = "Regular · After-hours", "AH"
    elif session == "OVERNIGHT":
        label_suffix, ext_tag = "Regular · Overnight", "ON"
    elif session == "CLOSED":
        label_suffix, ext_tag = "Closed", None
    elif session == "REGULAR":
        label_suffix, ext_tag = "Regular", None
    else:  # OTHER → Title-case the raw (e.g., HALTED → Halted)
        label_suffix = market_state.capitalize() if market_state else "Regular"
        ext_tag = None

    # ---- Extended price pick (prefer POST else PRE) ----
    ext_p, ext_pc, ext_label = None, None, ext_tag
    if post_p is not None:
        ext_p, ext_pc, ext_label = post_p, post_pc, "AH" if session != "OVERNIGHT" else "ON"
    elif pre_p is not None:
        ext_p, ext_pc, ext_label = pre_p, pre_pc, "PM" if session != "OVERNIGHT" else "ON"

    # ---- Compose top value string ----
    if reg_p is not None and ext_p is not None:
        value = f"{format_price(reg_p)} · {format_price(ext_p)}"
    else:
        value = format_price(reg_p if reg_p is not None else ext_p)

    # ---- Normalize pct inputs (percents, e.g., +0.45 means +0.45%) ----
    reg_pct = float(reg_pc) if isinstance(reg_pc, (int, float)) else None
    ext_pct = float(ext_pc) if isinstance(ext_pc, (int, float)) else None

    # ---- Combined percent for headline color/sign ----
    # If both exist, combine multiplicatively; else use whichever exists.
    if reg_pct is not None and ext_pct is not None:
        combined_pct = ((1.0 + reg_pct / 100.0) * (1.0 + ext_pct / 100.0) - 1.0) * 100.0
    elif reg_pct is not None:
        combined_pct = reg_pct
    elif ext_pct is not None:
        combined_pct = ext_pct
    else:
        combined_pct = 0.0  # neutral when neither is present

    # Compose the label
    label = f"Price in {currency} ({label_suffix})"

    # Compose delta text strings (manual arrows since streamlit arrows are disabled by default)
    parts = []

    if combined_pct is not None:
        arrow = "○" if reg_pct is None else "⬆" if reg_pct > 0 else "⬇" if reg_pct < 0 else "○"
        parts.append(f"{arrow}")

    if reg_pct is not None:
        primary_delta_str = format_pct(reg_pct)
        parts.append(f"REG {primary_delta_str}")

    if ext_pct is not None and ext_label:
        parts.append(f"· {ext_label} {format_pct(ext_pct)}")

    delta = " ".join(parts) if parts else ""

    # Setting the delta color
    delta_color = "inverse" if combined_pct < 0 else "normal" if combined_pct > 0 else "off"

    # ---- Help text (concise; no price-source labels) ----
    help = []

    # Baseline mention: show reg/pre/post time if present
    reg_line = format_timestamp(t_reg)
    pre_line = format_timestamp(t_pre)
    post_line = format_timestamp(t_post)
    if reg_line:
        help.append(f"baseline {reg_line}")
    if session == "PRE" and pre_line:
        help.append(f"pre {pre_line}")
    if session == "POST" and post_line:
        help.append(f"post {post_line}")
    if session in ("PRE", "POST", "OVERNIGHT"):
        help.append(f"Regular shown first; {label_suffix.split('·')[-1].strip()} second.")
    help = " ; ".join(help)

    return {
        "label": label,
        "value": value,
        "delta": delta,
        "deltaColor": delta_color,
        "help": help,
    }


def render_volume_section(info: dict, metrics: Mapping[str, Any]) -> Dict[str, Optional[str]]:
    """
    Build the volume display. No network, minimal compute:
    - value: today's volume (formatted elsewhere via format_abbrev in caller)
    - delta: "±X.X% (+Y.Yσ) · <basis>" if metrics are present, else simple % vs info avg
    - help: concise explainer
    """
    # 1) Value (number itself is formatted by caller)
    vol = info.get("regularMarketVolume") or info.get("volume")
    vol_is_num = isinstance(vol, (int, float))

    # 2) Preferred metrics path
    vs_frac = metrics.get("volumePctVsAvg")  # e.g., -0.272 (fraction)
    zscore = metrics.get("volumeZScore")  # float | None
    basis = metrics.get("volumeBaselineLabel")  # "3-month avg" / "10-day avg" / None

    parts = []

    if vs_frac is not None:
        arrow = "⬆" if vs_frac > 0 else "⬇" if vs_frac < 0 else "○"
        parts.append(arrow)

    if isinstance(vs_frac, (int, float)) and basis:
        parts.append(f"{vs_frac * 100:+.1f}%")
        if isinstance(zscore, (int, float)):
            parts.append(f"({zscore:+.1f}σ)")
        parts.append(f"· {basis}")
        delta_txt = " ".join(parts)
    else:
        # 3) Minimal fallback from info-only averages
        avg3m = info.get("averageVolume") or info.get("averageDailyVolume3Month")
        avg10d = info.get("averageVolume10days") or info.get("averageDailyVolume10Day")

        r3 = change_pct(vol, avg3m)
        r10 = change_pct(vol, avg10d)
        ratio = r3 if r3 is not None else r10
        basis_fallback = (
            "3-month avg" if r3 is not None else ("10-day avg" if r10 is not None else None)
        )

        parts = []
        if isinstance(ratio, (int, float)):
            parts.append(f"{ratio:+.1f}%")
        if basis_fallback:
            parts.append(f"· {basis_fallback}")
        delta_txt = " ".join(parts) if parts else None

    return {
        "value_raw": vol if vol_is_num else None,  # handy if caller wants raw
        "delta": delta_txt,
        "help": "Change vs baseline; σ computed from daily volume (3 months or last 10 days).",
    }


def render_52w_section(info: dict, price: Any) -> Dict[str, str]:
    """
    Return {label, value, help} where value is 'lo% - hi%' or '—'.
    """
    low = to_float(info.get("fiftyTwoWeekLow"))
    high = to_float(info.get("fiftyTwoWeekHigh"))

    value = f"{low} - {high}"

    p = to_float(price)

    lo_pct, hi_pct = range_endpoints_as_pct_of_price(p, low, high)
    delta = f"{lo_pct:.1f}% - {hi_pct:.1f}%" if lo_pct is not None and hi_pct is not None else "—"

    return {
        "label": "52-Week Range",
        "value": value,
        "delta": delta,
        "deltaColor": "off",
        "help": "Low/High expressed in absolute value and as percentage of current price.",
    }


# === Overview chart helpers (UI + compute) ===================================
# --- UI labels & mapping for intervals ---------------------------------------


def _period_options() -> list[str]:
    ordered = ["1D", "5D", "1M", "3M", "6M", "YTD", "1Y", "5Y", "MAX"]
    return [p for p in ordered if p in PERIOD_UI2API]


def _interval_options() -> list[str]:
    minute_order = ["1m", "2m", "5m", "15m", "30m", "60m", "90m"]
    long_order = ["1D", "5D", "1W", "1M", "3M"]
    return [k for k in minute_order + long_order if k in INTERVAL_UI2API]


# ----------------------
# Controls (widgets only)
# ----------------------
def ov_chart_controls(selected_pf: str) -> tuple[str, str, str]:
    period_opts = _period_options()
    style_opts = ["Line", "Candlestick"]

    period_key = f"ov_period_{selected_pf}"
    interval_key = f"ov_interval_{selected_pf}"
    style_key = f"ov_style_{selected_pf}"

    c1, c2, c3 = st.columns([1, 1, 1])

    # ---- Period (default 1D; widget owns the key) ----
    period_default_val = normalize_period_key(st.session_state.get(period_key))
    period_default_idx = period_opts.index(period_default_val)
    with c1:
        p_label = st.selectbox("Period", options=period_opts, key=period_key)

    # ---- Interval (default 1m on invalid/missing; normalize to UI) ----
    interval_opts = _interval_options()
    session_interval = st.session_state.get(interval_key)
    interval_default_val = normalize_interval_key(session_interval)  # invalid/missing -> "1m"
    # Make sure default exists in current options
    if interval_default_val not in interval_opts:
        interval_default_val = "1m"
    interval_default_idx = interval_opts.index(interval_default_val)

    with c2:
        i_label = st.selectbox("Interval", options=interval_opts, key=interval_key)

    # ---- Style (default Line) ----
    style_default_val = st.session_state.get(style_key, "Line")
    style_default_idx = (
        style_opts.index(style_default_val) if style_default_val in style_opts else 0
    )
    with c3:
        style_label = st.selectbox("Style", options=style_opts, key=style_key)

    return p_label, i_label, style_label


def ov_map_chart_selections(p_label: str, i_label: str) -> Tuple[str, str]:
    """
    Normalize selections.
    Returns:
      - period_ui (UI label, e.g., "1D")
      - interval_api (canonical API key: "1m"…"1wk","1mo","3mo")
    """
    # Normalize via single source of truth; invalid -> period '1D', interval '1m'
    tf = resolve_timeframe(p_label, i_label)
    period_ui = tf["uiPeriod"]
    interval_api = tf["apiInterval"]

    # Optional guardrail: if user picked intraday with long period, fetch daily
    is_intraday = interval_api.endswith("m") or interval_api.endswith("h")
    is_long_period = period_ui not in {"1D", "5D", "1M"}
    if is_intraday and is_long_period:
        interval_api = "1d"

    return period_ui, interval_api


# Helper to map UI period to Yahoo Finance API string when fetching history
def ov_period_api(period_ui: str) -> str:
    return period_to_api(period_ui)


# ----------------------
# Chart builder (Altair)
# ----------------------
# app_format.py — NEW helper
# ---- Shared helper: build a reusable __x_idx__ axis with human time labels ----
def build_indexed_time_axis(ts_all: pd.Series, target_ticks: Optional[int] = None) -> alt.Axis:
    """
    Given a datetime-like Series aligned 1:1 with the display rows (same order as __x_idx__),
    build an Altair Axis that maps integer x positions to human-readable timestamps using
    calendar-aware anchors (hourly for intraday, weekly/monthly for longer windows).
    """
    ts_all = pd.to_datetime(ts_all)
    _n = int(len(ts_all))
    if _n <= 0:
        return alt.Axis(title=None)

    tzinfo = getattr(getattr(ts_all, "dt", ts_all), "tz", None)
    # Label budget (default: sqrt(N), capped), or enforce target_ticks if provided
    if target_ticks is None:
        tgt = _n if _n <= 16 else int(np.clip(np.sqrt(_n), 6, 14))
    else:
        tgt = max(1, int(min(target_ticks, _n)))

    def _nearest_idx(series: pd.Series, target: pd.Timestamp) -> int:
        pos = series.searchsorted(target)
        if pos <= 0:
            return 0
        if pos >= len(series):
            return len(series) - 1
        before = series.iloc[pos - 1]
        after = series.iloc[pos]
        return pos if (after - target) <= (target - before) else (pos - 1)

    def _thin(pos_list: list[int], tgt_count: int) -> list[int]:
        if len(pos_list) <= tgt_count:
            return pos_list
        stride = int(np.ceil(len(pos_list) / tgt_count))
        return pos_list[::stride]

    start = ts_all.iloc[0]
    end = ts_all.iloc[-1]
    intraday_span = (end - start) < pd.Timedelta(days=2)

    if intraday_span:
        total_hours = max(1, int((end - start).total_seconds() // 3600))
        step_h = max(1, int(np.ceil(total_hours / max(1, tgt - 1))))
        anchors = pd.date_range(
            start=start.floor("h"), end=end.ceil("h"), freq=f"{step_h}h", tz=tzinfo
        )
        pos = [_nearest_idx(ts_all, a) for a in anchors]
        fmt_ticks = "%b %d %H:%M"
    else:
        days_span = max(1, int((end.normalize() - start.normalize()).days))
        if days_span > 180:
            anchors = pd.date_range(
                start=start.normalize(), end=end.normalize(), freq="MS", tz=tzinfo
            )
            pos = [_nearest_idx(ts_all, a) for a in anchors]
            fmt_ticks = "%Y-%m"
        elif days_span > 35:
            anchors = pd.date_range(
                start=start.normalize(), end=end.normalize(), freq="W-MON", tz=tzinfo
            )
            pos = [_nearest_idx(ts_all, a) for a in anchors]
            fmt_ticks = "%b %d"
        else:
            pos = np.linspace(0, _n - 1, num=min(tgt, _n), dtype=int).tolist()
            fmt_ticks = "%b %d"

    if _n > 0:
        if 0 not in pos:
            pos = [0] + pos
        if (_n - 1) not in pos:
            pos = pos + [_n - 1]

    pos = sorted(set(int(p) for p in pos))
    pos = _thin(pos, tgt)

    labels = [ts_all.iloc[p].strftime(fmt_ticks) for p in pos]
    _pairs = ", ".join(f'{p}: "{lab}"' for p, lab in zip(pos, labels))

    return alt.Axis(
        title=None,
        values=pos,
        labelExpr=f"({{{_pairs}}})[datum.value]",
        ticks=True,
        grid=False,
        labelFlush=True,
        labelOverlap=True,
    )


def ov_render_history_chart(
    hist: Optional[pd.DataFrame],
    style_label: str,
    show_bottom_x_axis: bool = True,
    overlays: Optional[Sequence[alt.Chart]] = None,
) -> tuple[None, str] | tuple[VConcatChart | LayerChart | Chart, Axis, str]:
    """
    Overview/Technicals history chart:
    - Top: price (candlestick or line) with ±10% padded y-domain.
    - Bottom: volume bars (relative, no axis), shared x-axis, independent y-scales.
    - Adaptive widths: candlestick bodies and volume bars widen/narrow based on point count.
    - Optional: layer "overlays" (SMA/EMA/Bollinger/VWAP/etc.) onto the price pane using identical axes.
    Returns (chart, "") on success or (None, "reason") on failure.
    """

    if hist is None or len(hist) < 2:
        return None, "Not enough data to draw chart."

    # ---- copy & coerce numeric columns we rely on ----
    df = hist.copy()
    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.reset_index(drop=False)
    if df.empty:
        return None, "Not enough data to draw chart."

    time_col = df.columns[0] if pd.api.types.is_datetime64_any_dtype(df.iloc[:, 0]) else None
    if not time_col:
        return None, "Expected a datetime index for history."

    # ---- detect candlestick vs line; optionally thin only the line case ----
    is_candle = style_label.lower() == "candlestick" and all(
        c in df.columns for c in ["Open", "High", "Low", "Close"]
    )
    if not is_candle:
        MAX_LINE_ROWS = 6000
        if len(df) > MAX_LINE_ROWS:
            step = max(1, len(df) // MAX_LINE_ROWS)
            df = df.iloc[::step, :].copy()

    # ---- compute ±10% padded price domain (restores original behavior) ----
    if (
        all(c in df.columns for c in ["Low", "High"])
        and df["Low"].notna().any()
        and df["High"].notna().any()
    ):
        y_min = float(df["Low"].min())
        y_max = float(df["High"].max())
    else:
        if "Close" not in df.columns or not df["Close"].notna().any():
            return None, "Expected 'Close' column for line chart."
        y_min = float(df["Close"].min())
        y_max = float(df["Close"].max())

    if not (np.isfinite(y_min) and np.isfinite(y_max)):
        return None, "Price data contains no finite values."
    if y_max <= y_min:
        eps = max(1e-6, abs(y_max) * 1e-3)
        price_y_scale = alt.Scale(domain=[y_min - eps, y_max + eps], nice=False)
    else:
        pad = 0.10 * (y_max - y_min)
        price_y_scale = alt.Scale(domain=[y_min - pad, y_max + pad], nice=False)

    # ---- collapse closed periods + drop invalid price rows (minimal) ----
    # A. Drop invalid price rows (style-aware)
    if is_candle:
        price_ok = df[["Open", "High", "Low", "Close"]].apply(np.isfinite).all(axis=1)
        price_ok &= df["Low"] <= df[["Open", "Close"]].min(axis=1)
        price_ok &= df[["Open", "Close"]].max(axis=1) <= df["High"]
    else:
        price_ok = (
            np.isfinite(df["Close"]) if "Close" in df.columns else pd.Series(False, index=df.index)
        )

    df = df.loc[price_ok]
    if len(df) < 2:
        return None, "Not enough valid price rows to draw chart."

    # Ensure chronological order before indexing/labeling
    df = df.sort_values(by=time_col).reset_index(drop=True)

    # B. Compressed x-axis (ordinal compression)
    # Keep true timestamps for tooltips; plot on a dense integer axis.
    df["__x_idx__"] = np.arange(len(df), dtype=int)

    # Tight x-scale so the first/last bar sit flush (no side padding)
    _n = int(len(df))
    x_scale = alt.Scale(domain=[-0.5, _n - 0.5], nice=False, clamp=True)

    # 1) Choose display tz: env override > viewer local > America/New_York
    tz_env = os.getenv("FISCRAPE_DISPLAY_TZ")
    _local_tz = (
        ZoneInfo(tz_env)
        if tz_env
        else (datetime.now().astimezone().tzinfo or ZoneInfo("America/New_York"))
    )

    # 2) Convert timestamps to display tz (do NOT force UTC)
    ts = pd.to_datetime(df[time_col], errors="coerce")
    if getattr(ts.dt, "tz", None) is not None:
        ts_disp = ts.dt.tz_convert(_local_tz)
    else:
        # treat naive bars as exchange-local Eastern, then convert to display tz
        ts_disp = ts.dt.tz_localize(ZoneInfo("America/New_York")).dt.tz_convert(_local_tz)

    # 3) Format based on span
    intraday_span = (ts_disp.max() - ts_disp.min()) < pd.Timedelta(days=2)
    _fmt = "%b %d %H:%M" if intraday_span else "%Y-%m-%d"
    df["__ts_label__"] = ts_disp.dt.strftime(_fmt)

    # ---- x-axis ticks: built via shared helper ----
    ts_all = ts_disp  # already in display timezone (Series aligned to df rows)
    x_axis_for_labels = build_indexed_time_axis(ts_all)

    # ---- layout sizing (70/30 split) ----
    TOTAL_HEIGHT = 320
    price_h = int(TOTAL_HEIGHT * 0.80)
    vol_h = max(64, TOTAL_HEIGHT - price_h)

    # ---- guardrails: pick bar sizes from point count (shared thresholds) ----
    n_points = int(pd.Series(df[time_col]).dropna().nunique())

    # thresholds:
    # tune sizes independently but follow the same thresholds
    if n_points <= 4:
        candle_body_size = 12
        vol_bar_size = 160
    elif n_points <= 8:
        candle_body_size = 12
        vol_bar_size = 80
    elif n_points <= 16:
        candle_body_size = 12
        vol_bar_size = 40
    elif n_points <= 32:
        candle_body_size = 12
        vol_bar_size = 20
    elif n_points <= 64:
        candle_body_size = 10
        vol_bar_size = 10
    elif n_points <= 128:
        candle_body_size = 5
        vol_bar_size = 5
    elif n_points <= 512:
        candle_body_size = 2.5
        vol_bar_size = 2.5
    else:
        candle_body_size = 1
        vol_bar_size = 1

    base = alt.Chart(df)

    # =========================
    # PRICE PANEL
    # =========================
    if is_candle:
        wick = base.mark_rule().encode(
            x=alt.X(
                "__x_idx__:Q",
                scale=x_scale,
                axis=alt.Axis(title=None, labels=False, ticks=False, grid=False),
            ),
            y=alt.Y("Low:Q", scale=price_y_scale, axis=alt.Axis(title=None)),
            y2="High:Q",
            color=alt.condition(
                "datum.Close >= datum.Open", alt.value("#2ca02c"), alt.value("#d62728")
            ),
            tooltip=[
                alt.Tooltip("__ts_label__:N", title="Date/Time"),
                alt.Tooltip("Open:Q", format=",.2f"),
                alt.Tooltip("High:Q", format=",.2f"),
                alt.Tooltip("Low:Q", format=",.2f"),
                alt.Tooltip("Close:Q", format=",.2f"),
            ],
        )

        body = base.mark_bar(size=candle_body_size).encode(  # <- adaptive width
            x=alt.X(
                "__x_idx__:Q",
                scale=x_scale,
                axis=alt.Axis(title=None, labels=False, ticks=False, grid=False),
            ),
            y=alt.Y("Open:Q", scale=price_y_scale, axis=alt.Axis(title=None)),
            y2="Close:Q",
            color=alt.condition(
                "datum.Close >= datum.Open", alt.value("#2ca02c"), alt.value("#d62728")
            ),
            tooltip=[
                alt.Tooltip("__ts_label__:N", title="Date/Time"),
                alt.Tooltip("Open:Q", format=",.2f"),
                alt.Tooltip("High:Q", format=",.2f"),
                alt.Tooltip("Low:Q", format=",.2f"),
                alt.Tooltip("Close:Q", format=",.2f"),
            ],
        )
        price_chart = (wick + body).properties(height=price_h)
    else:
        price_chart = (
            base.mark_line()
            .encode(
                x=alt.X(
                    "__x_idx__:Q",
                    scale=x_scale,
                    axis=alt.Axis(title=None, labels=False, ticks=False, grid=False),
                ),
                y=alt.Y("Close:Q", scale=price_y_scale, axis=alt.Axis(title=None)),
                tooltip=[
                    alt.Tooltip("__ts_label__:N", title="Date/Time"),
                    alt.Tooltip("Close:Q", format=",.2f"),
                ],
            )
            .properties(height=price_h)
        )

    # ---- optional overlays on the price pane (share x & y scales + clean tooltip) ----
    if overlays:
        fixed_overlays: list[alt.Chart] = []
        # Use the same human-readable timestamp label that the price/volume panes use
        time_tt = (
            alt.Tooltip(f"{time_col}:T", title="Time")
            if time_col
            else alt.Tooltip("__x_idx__:Q", title="Index")
        )

        for ov in overlays:
            # Try to detect the current y field so we can bind to the price y-scale and show its value in tooltip
            y_enc = getattr(ov.encoding, "y", None)
            y_field = None
            if y_enc is not None:
                y_field = getattr(y_enc, "shorthand", None) or getattr(y_enc, "field", None)

            if y_field:
                ov = ov.encode(
                    x=alt.X("__x_idx__:Q", scale=x_scale, axis=None),
                    y=alt.Y(y_field, scale=price_y_scale, axis=alt.Axis(title=None)),
                    tooltip=[time_tt, alt.Tooltip(y_field, type="quantitative", title="Value")],
                )
            else:
                # If the overlay didn't define y yet, at least align x and attach a time tooltip
                ov = ov.encode(
                    x=alt.X("__x_idx__:Q", scale=x_scale, axis=None),
                    tooltip=[time_tt],
                )
            fixed_overlays.append(ov)

        # Layer on top of base price chart
        price_chart = alt.layer(price_chart, *fixed_overlays)

    # =========================
    # VOLUME PANEL (relative; spike-proof)
    # =========================
    has_vol = "Volume" in df.columns and df["Volume"].notna().any()
    if has_vol:
        # x-axis policy for the volume pane
        axis_for_vol = x_axis_for_labels if show_bottom_x_axis else None

        volume_chart = (
            base.transform_calculate(isUp="datum.Close >= datum.Open")
            .transform_joinaggregate(maxVol="max(Volume)")
            .transform_calculate(volRel="datum.Volume / max(1e-12, datum.maxVol)")
            .mark_bar(size=vol_bar_size, opacity=0.50)  # adaptive width
            .encode(
                x=alt.X("__x_idx__:Q", scale=x_scale, axis=axis_for_vol),
                y=alt.Y("volRel:Q", axis=None),  # relative scale; clean background
                color=alt.condition(
                    "datum.isUp",
                    alt.value("#2ca02c"),  # up (green) — match your palette if you have one
                    alt.value("#d62728"),  # down (red)
                ),
                tooltip=[
                    alt.Tooltip("__ts_label__:N", title="Date/Time"),
                    alt.Tooltip("Volume:Q", title="Volume", format=",.0f"),
                ],
            )
            .properties(height=vol_h)
        )

        # Ensure price pane x-axis is hidden in both modes (volume carries x when show_bottom_xaxis=True)
        price_chart = price_chart.encode(x=alt.X("__x_idx__:Q", scale=x_scale, axis=None))

        chart = alt.vconcat(price_chart, volume_chart, spacing=4).resolve_scale(
            x="shared", y="independent"
        )
    else:
        # No volume pane: respect x-axis policy on the price pane itself
        price_chart = price_chart.encode(
            x=alt.X(
                "__x_idx__:Q",
                scale=x_scale,
                axis=(x_axis_for_labels if show_bottom_x_axis else None),
            )
        )
        chart = price_chart

    return chart, ""


# ----------------------
# Compact stats (right)
# ----------------------
def ov_chart_metric_items(
    hist: Optional[pd.DataFrame],
    fmt_price=None,
    fmt_percent_signed=None,
    fmt_percent_plain=None,
) -> List[Dict[str, Any]]:
    """
    Three metrics for the window: Δ, ⬇, ⬆ (matching your current display).
    """

    def _fmt_price(x: float) -> str:
        return "—" if x is None else f"{x:,.2f}"

    def _fmt_pct_signed(x: float) -> str:
        return "—" if x is None else f"{x:+.2f}%"

    def _fmt_pct_plain(x: float) -> str:
        return "—" if x is None else f"{x:.1f}%"

    fmt_price = fmt_price or _fmt_price
    fmt_percent_signed = fmt_percent_signed or _fmt_pct_signed
    fmt_percent_plain = fmt_percent_plain or _fmt_pct_plain

    if hist is None or len(hist) < 2 or "Close" not in hist.columns:
        return [
            {
                "label": "Δ",
                "value": "—",
                "delta": "—",
                "delta_color": "off",
                "help": "Change in price within the selected window.",
            }
        ]

    close = pd.to_numeric(hist["Close"], errors="coerce").dropna()
    if len(close) < 2:
        return [
            {
                "label": "Δ",
                "value": "—",
                "delta": "—",
                "delta_color": "off",
                "help": "Change in price within the selected window.",
            }
        ]

    first, last = float(close.iloc[0]), float(close.iloc[-1])

    # true window extremes
    if "Low" in hist.columns and "High" in hist.columns:
        low_val = float(pd.to_numeric(hist["Low"], errors="coerce").min())
        high_val = float(pd.to_numeric(hist["High"], errors="coerce").max())
    else:
        price_cols = [c for c in ["Low", "High", "Open", "Close", "Adj Close"] if c in hist.columns]
        if price_cols:
            _vals = pd.DataFrame({c: pd.to_numeric(hist[c], errors="coerce") for c in price_cols})
            low_val = float(_vals.min().min())
            high_val = float(_vals.max().max())
        else:
            low_val, high_val = float(close.min()), float(close.max())

    delta_abs = last - first
    delta_pct = (delta_abs / first * 100.0) if first else 0.0
    arrow = "⬆" if delta_pct > 0 else "⬇" if delta_pct < 0 else "○"

    low_pct_of_last = (low_val / last * 100.0) if last else None
    high_pct_of_last = (high_val / last * 100.0) if last else None

    delta_abs_disp = (
        f"+{fmt_price(abs(delta_abs))}" if delta_abs >= 0 else f"-{fmt_price(abs(delta_abs))}"
    )
    delta_pct_disp = fmt_percent_signed(delta_pct)

    low_s = fmt_price(low_val)
    high_s = fmt_price(high_val)
    low_pct_s = fmt_percent_plain(low_pct_of_last) if low_pct_of_last is not None else "—"
    high_pct_s = fmt_percent_plain(high_pct_of_last) if high_pct_of_last is not None else "—"

    return [
        {
            "label": "Δ",
            "value": delta_abs_disp,
            "delta": f"{arrow} {delta_pct_disp}",
            "delta_color": "normal" if delta_abs > 0 else "off" if delta_abs == 0 else "inverse",
            "help": "Change in price between latest and earliest value in window.",
        },
        {
            "label": "⬇",
            "value": low_s,
            "delta": low_pct_s,
            "delta_color": "off",
            "help": "Window low (as % of latest).",
        },
        {
            "label": "⬆",
            "value": high_s,
            "delta": high_pct_s,
            "delta_color": "off",
            "help": "Window high (as % of latest).",
        },
    ]


# ----------------------
# Entity Description
# ----------------------
def ov_render_entity_description(info: dict) -> None:
    """
    Overview: collapsible 'Description' with Summary (left) + Facts (right).
    Uses ONLY keys from get_info(). Right column shows label → value (no table).
    """
    if not any(
        info.get(k)
        for k in (
            "summary",
            "longBusinessSummary",
            "website",
            "industryDisp",
            "sectorDisp",
            "fullTimeEmployees",
            "city",
            "state",
            "country",
            "fundFamily",
            "category",
            "fundInceptionDate",
            "fullExchangeName",
        )
    ):
        return

    qtype = (info.get("quoteType") or "").upper()
    is_fund = qtype in {"ETF", "MUTUALFUND"}

    with st.expander("Description", expanded=False):
        left, right = st.columns([6, 2], vertical_alignment="top")

        # ---- LEFT: narrative ----
        with left:
            st.subheader("", anchor=False)
            summary = info.get("summary") or info.get("longBusinessSummary")
            st.markdown(summary) if summary else st.caption("No summary available.")

        # ---- RIGHT: label → value (no table) ----
        with right:
            st.subheader("", anchor=False)

            rows = []
            exch = info.get("fullExchangeName")
            if exch:
                rows.append(("Exchange", exch))

            if is_fund:
                if info.get("fundFamily"):
                    rows.append(("Issuer", info["fundFamily"]))
                if info.get("category"):
                    rows.append(("Category", info["category"]))

                # Inception → YYYY-MM if we can parse, else "—"
                inc_val = "—"
                inc_raw = info.get("fundInceptionDate")
                if inc_raw not in (None, ""):
                    try:
                        ival = int(inc_raw)
                        ts = ival / 1000 if ival > 10_000_000_000 else ival
                        inc_val = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")
                    except Exception:
                        try:
                            inc_val = pd.to_datetime(inc_raw, utc=True, errors="raise").strftime(
                                "%Y-%m"
                            )
                        except Exception:
                            inc_val = "—"
                rows.append(("Inception", inc_val))

            else:
                if info.get("industryDisp"):
                    rows.append(("Industry", info["industryDisp"]))
                if info.get("sectorDisp"):
                    rows.append(("Sector", info["sectorDisp"]))

                emp_raw = info.get("fullTimeEmployees")
                if emp_raw is None or emp_raw == "":
                    rows.append(("Employees", "—"))
                else:
                    try:
                        rows.append(("Employees", f"{int(emp_raw):,}"))
                    except Exception:
                        rows.append(("Employees", str(emp_raw)))

                parts = [
                    str(v).strip()
                    for v in (info.get("city"), info.get("state"), info.get("country"))
                    if v
                ]
                rows.append(("Headquarters", ", ".join(parts) if parts else "—"))

            # Render rows as stacked header + field
            for label, value in rows:
                st.markdown(f"**{label}**")
                st.write(value if (value not in (None, "")) else "—")

            # Company website button (ETF websites often absent; skip)
            if not is_fund:
                url = (info.get("website") or "").strip()
                if url:
                    if not (
                        url.lower().startswith("http://") or url.lower().startswith("https://")
                    ):
                        url = "https://" + url
                    st.link_button("Company website", url, use_container_width=True)
                else:
                    st.caption("Website: —")
