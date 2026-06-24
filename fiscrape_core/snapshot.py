# fiscrape_core/snapshot.py:
"""Stateless snapshot orchestrator (UI-facing)

Assembles live quote and optional history via services, enriches a profile-like dict,
and returns (metrics, hist, info). No UI state, no formatting, no provider logic here.
Defers price-derived math to metrics_engine; avoids rounding/pretty-printing.
"""

from typing import Any, Dict, Optional, Tuple

import pandas as pd
from pandas import DataFrame

from .metrics_engine import compute_live_metrics
from .pipeline_registry import PROFILE_PASS_KEYS
from .services import emit_service_warning, get_history, get_live_quote

TRACE = False  # flip True locally when debugging


def snapshot(
    sym: str,
    period: str = "1d",
    interval: str = "1m",
    *,
    profile: Optional[Dict[str, Any]] = None,
    include_history: bool = True,  # kept name as your app_core uses it
    **kwargs,  # safety for any future named args
) -> tuple[dict[str, Any], dict[str, Any], DataFrame]:
    """
    Stateless live snapshot:
      - metrics: live price, marketCap/PE/yield/PS/PB (via compute_live_metrics),
                 volume z-score vs 3mo (fallback 10d), and a UI bundle for Price
      - hist:    optional price history DataFrame (empty df if not requested/unavailable)
      - info:    profile enriched with live price & % change; session fields included

    Return shape preserved: (metrics: dict, hist: DataFrame, info: dict)
    """
    metrics: Dict[str, Any] = {}
    hist: pd.DataFrame = pd.DataFrame()

    # ---- Local history cache (avoid redundant network calls) ----
    _hist_cache: Dict[Tuple[str, str], pd.DataFrame] = {}

    def _get_hist(p: str, i: str) -> pd.DataFrame:
        key = (p, i)
        if key in _hist_cache:
            return _hist_cache[key]
        try:
            h = get_history(sym, p, i)
        except Exception as e:
            emit_service_warning(
                sym,
                code="HISTORY_FAIL",
                message=str(e),
                where="snapshot",
                context={"period": p, "interval": i},
            )
            h = pd.DataFrame()
        _hist_cache[key] = h
        return h

    def _trace(label, **k):
        if TRACE:
            items = ", ".join(f"{kk}={vv!r}" for kk, vv in k.items())
            print(f"[snapshot] {label}: {items}")

    # ---------- A) Live quote (REG + PRE/AH) ----------
    try:
        q = get_live_quote(sym)
    except Exception as e:
        emit_service_warning(sym, code="LIVE_QUOTE_FAIL", message=str(e), where="snapshot")
        q = {}

    _trace("quote", price=q.get("regularMarketPrice"), state=q.get("marketState"))

    # ---------- B) History (optional; fallback to daily if 1m series unusable) ----------
    if include_history:
        hist = _get_hist(period, interval)
        if (hist.empty or "Close" not in hist) and interval != "1d":
            hist = _get_hist(period, "1d")

    # ---------- C) Active price selection (no math, no UI) ----------
    reg_p, reg_t = q.get("regularMarketPrice"), q.get("regularMarketTime")
    pre_p, pre_t = q.get("preMarketPrice"), q.get("preMarketTime")
    post_p, post_t = q.get("postMarketPrice"), q.get("postMarketTime")

    def pick_active_price_by_recency(cands):
        if (pre_p is not None) and (post_p is not None):
            if post_t and pre_t:
                return (post_p, "post") if post_t >= pre_t else (pre_p, "pre")
            if post_t and not pre_t:
                return (post_p, "post")
            if pre_t and not post_t:
                return (pre_p, "pre")
        for p, t, tag in cands:
            if p is not None:
                return p, tag
        return None, None

    active_price, active_src = pick_active_price_by_recency(
        [
            (post_p, post_t, "post"),
            (pre_p, pre_t, "pre"),
            (reg_p, reg_t, "regular"),
        ]
    )

    # ---------- D) Build info from quote + registry-driven profile fallbacks ----------
    info = {k: v for k, v in (q or {}).items() if v is not None}

    if active_src:
        info["activePriceSource"] = active_src  # UI badge; engine ignores

    if profile:
        for k in PROFILE_PASS_KEYS:
            v = profile.get(k)
            if v is not None and k not in info:  # provider-first
                info[k] = v

    # ---------- E) Compute live metrics (engine owns the math) ----------
    metrics_new = compute_live_metrics(
        active_price,
        info,
        volume_history_daily=_get_hist("3mo", "1d"),  # for z-score
        intraday_history=hist if interval.endswith("m") else None,  # today vol fallback
    )
    metrics.update(metrics_new)
    _trace("metrics", keys=list(metrics.keys())[:6], price=metrics.get("price"))

    # ---------- F) Return ----------
    return info, metrics, hist
