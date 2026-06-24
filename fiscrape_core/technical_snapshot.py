"""Technical stuff, still being worked on"""

from __future__ import annotations
from typing import Any, Dict, List, Tuple, Union, Optional
import math
import re
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

# Reuse your existing history service:
from .services import get_history
from .math_tools import sma, ema, wilder_rsi, bollinger_bands, macd, vwap


def extend_history_for_warmup(
    symbol: str,
    period: str,
    interval: str,
    requests: Union[List[Union[str, Dict[str, Any]]], Tuple[Union[str, Dict[str, Any]], ...]],
    *,
    tz: Optional[str] = None,  # if you have a display TZ; None → use naive UTC
    warmup_margin_pct: float = 0.10,  # 10% buffer beyond strict warm-up
    retry_on_shortfall: bool = True,  # one adaptive retry if preload is short
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Return OHLCV history that *includes* the lookback needed to compute indicators strictly,
    plus a meta dict describing warm-up sufficiency. This function does NOT compute indicators.

    Inputs
    ------
    symbol: e.g., "AAPL"
    period: e.g., "1mo", "3mo", "6mo", "1y", "ytd", "max"  (visible range requested)
    interval: e.g., "1m", "5m", "15m", "1h", "1d", "1wk", "1mo"
    requests: list/tuple of indicator specs; strings like "SMA(200)", "EMA(16)", "MACD(12,26,9)"
              or dicts like {"name":"SMA","length":200}

    Returns
    -------
    df_full: pd.DataFrame indexed by timestamp (ascending), with at least:
             ["Open","High","Low","Close","Volume"] columns.
             It *covers*: [lookback_start, visible_end] (or as much as provider allows).
    meta: {
        "visibleStart": pd.Timestamp,
        "visibleEnd":   pd.Timestamp,
        "requiredWarmupBars": int,     # strict warm-up bars required by selected indicators
        "bufferBars":         int,     # with extra margin applied
        "availablePreload":   int,     # rows strictly before visibleStart
        "warmupComplete":     bool,
        "messages":           List[str],
        "usedProviderPeriod": Optional[str],  # if start/end not supported
        "attempts":           int,
        "interval":           str,
        "period":             str,
        "symbol":             str,
    }
    """

    # -----------------------
    # Nested helpers (scoped)
    # -----------------------
    def _now_utc() -> pd.Timestamp:
        return pd.Timestamp(datetime.now(timezone.utc))

    def _to_tz(ts: pd.Timestamp) -> pd.Timestamp:
        # Keep datetimes timezone-aware in UTC unless a TZ is given
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        if tz:
            try:
                return ts.tz_convert(tz)
            except Exception:
                return ts  # fallback if tz not recognized in this runtime
        return ts

    def _parse_and_dedupe(reqs) -> List[Dict[str, Any]]:
        """
        Normalize to dicts and dedupe by (name.upper(), canonical params tuple).
        Supported strings:
          - "SMA(200)", "EMA(16)", "RSI(14)", "BB(20,2)", "MACD(12,26,9)"
        """
        normd: List[Dict[str, Any]] = []

        def _from_str(s: str) -> Dict[str, Any]:
            s = s.strip()
            m = re.match(r"^\s*([A-Za-z]+)\s*\(([^)]*)\)\s*$", s)
            if not m:
                return {"name": s.strip().upper()}
            name = m.group(1).strip().upper()
            args = [a.strip() for a in m.group(2).split(",") if a.strip() != ""]
            # map common indicators
            if name in {"SMA", "EMA", "RSI"} and len(args) >= 1:
                return {"name": name, "length": int(args[0])}
            if name in {"BB", "BBANDS"} and len(args) >= 1:
                k = float(args[1]) if len(args) >= 2 else 2.0
                return {"name": "BB", "length": int(args[0]), "k": k}
            if name == "MACD":
                # MACD(fast, slow, signal) default to (12,26,9) if omitted
                fast = int(args[0]) if len(args) >= 1 else 12
                slow = int(args[1]) if len(args) >= 2 else 26
                signal = int(args[2]) if len(args) >= 3 else 9
                return {"name": "MACD", "fast": fast, "slow": slow, "signal": signal}
            return {"name": name}

        def _canon_key(d: Dict[str, Any]) -> Tuple:
            name = d.get("name", "").upper()
            # canonicalize params
            if name in {"SMA", "EMA", "RSI"}:
                return (name, int(d.get("length", 0)))
            if name == "BB":
                return (name, int(d.get("length", 0)), float(d.get("k", 2.0)))
            if name == "MACD":
                return (
                    name,
                    int(d.get("fast", 12)),
                    int(d.get("slow", 26)),
                    int(d.get("signal", 9)),
                )
            # generic
            # include sorted items for uniqueness
            return (name, tuple(sorted((k, d[k]) for k in d.keys() if k != "name")))

        # normalize
        for r in reqs or []:
            d = _from_str(r) if isinstance(r, str) else {**r, "name": r.get("name", "").upper()}
            normd.append(d)

        # dedupe
        seen = set()
        uniq = []
        for d in normd:
            k = _canon_key(d)
            if k not in seen:
                seen.add(k)
                uniq.append(d)
        return uniq

    def _indicator_warmup_bars(spec: Dict[str, Any]) -> int:
        name = spec.get("name", "").upper()
        if name in {"SMA", "EMA", "RSI"}:
            return int(spec.get("length", 0))
        if name == "VWAP":
            return 0  # session-anchored; we recompute from start-of-day, no extra bars needed
        if name == "BB":
            return int(spec.get("length", 0))
        if name == "MACD":
            return int(max(int(spec.get("fast", 12)), int(spec.get("slow", 26))))
        # default: assume zero warmup if unknown
        return 0

    def _compute_buffer_bars(reqs_norm: List[Dict[str, Any]]) -> Tuple[int, int]:
        strict = 0
        for spec in reqs_norm:
            strict = max(strict, _indicator_warmup_bars(spec))
        buffer_bars = int(math.ceil(strict * (1.0 + max(0.0, warmup_margin_pct))))
        return strict, buffer_bars

    def _interval_to_timedelta_per_bar(interval_str: str) -> timedelta:
        table = {
            "1m": timedelta(minutes=1),
            "2m": timedelta(minutes=2),
            "5m": timedelta(minutes=5),
            "15m": timedelta(minutes=15),
            "30m": timedelta(minutes=30),
            "60m": timedelta(minutes=60),
            "90m": timedelta(minutes=90),
            "1h": timedelta(hours=1),
            "1d": timedelta(days=1),
            "1wk": timedelta(weeks=1),
            "1mo": timedelta(days=30),
            "3mo": timedelta(days=90),
        }
        return table.get(interval_str, timedelta(days=1))

    def _bars_to_calendar_delta(interval_str: str, buffer_bars: int) -> timedelta:
        per_bar = _interval_to_timedelta_per_bar(interval_str)
        # add slack to cover weekends/holidays/sessions
        if per_bar <= timedelta(hours=1):
            # intraday: 1.5x slack and minimum 2 calendar days
            delta = per_bar * buffer_bars * 1.5
            return max(delta, timedelta(days=2))
        elif per_bar <= timedelta(days=1):
            # daily: 1.5x slack, min 7 days
            delta = per_bar * buffer_bars * 1.5
            return max(delta, timedelta(days=7))
        else:
            # weekly/monthly: linear slack
            return per_bar * (buffer_bars + 2)

    def _derive_visible_window(
        period_str: str, end_hint: Optional[pd.Timestamp] = None
    ) -> Tuple[pd.Timestamp, pd.Timestamp]:
        """
        Resolve 'period' to absolute [visible_start, visible_end] in UTC.
        """
        end_ts = end_hint or _now_utc()
        end_ts = end_ts.tz_localize("UTC") if end_ts.tzinfo is None else end_ts.tz_convert("UTC")

        # Map common period buckets to approximate durations
        p = (period_str or "").lower()
        days_map = {
            "5d": 5,
            "1mo": 30,
            "3mo": 90,
            "6mo": 180,
            "1y": 365,
            "2y": 730,
            "5y": 1825,
            "10y": 3650,
        }
        if p == "ytd":
            start_ts = pd.Timestamp(year=end_ts.year, month=1, day=1, tz="UTC")
        elif p == "max":
            # With 'max' we cannot know the earliest; pick 10y as a sane default, provider will clamp earlier.
            start_ts = end_ts - timedelta(days=3650)
        elif p in days_map:
            start_ts = end_ts - timedelta(days=days_map[p])
        else:
            # Fallback: try to parse like "Xmo", "Xy"
            m = re.match(r"^\s*(\d+)\s*(d|mo|y|w)\s*$", p)
            if m:
                n = int(m.group(1))
                unit = m.group(2)
                dur = {"d": "days", "w": "weeks", "mo": "days", "y": "days"}[unit]
                factor = {"d": 1, "w": 7, "mo": 30, "y": 365}[unit]
                start_ts = end_ts - timedelta(**{dur: n * factor})
            else:
                # Unknown → default 3 months
                start_ts = end_ts - timedelta(days=90)

        return start_ts, end_ts

    def _choose_provider_period(total_span: timedelta) -> str:
        """
        Choose a yfinance-like 'period' string that covers total_span.
        """
        days = max(1, int(math.ceil(total_span.total_seconds() / 86400)))
        if days <= 5:
            return "5d"
        if days <= 30:
            return "1mo"
        if days <= 90:
            return "3mo"
        if days <= 180:
            return "6mo"
        if days <= 365:
            return "1y"
        if days <= 730:
            return "2y"
        if days <= 1825:
            return "5y"
        if days <= 3650:
            return "10y"
        return "max"

    def _fetch_history(
        start_ts: pd.Timestamp, end_ts: pd.Timestamp, interval_str: str
    ) -> Tuple[pd.DataFrame, Optional[str], int]:
        """
        Try start/end first. If the service signature doesn't support that, fall back to period.
        Returns (df, used_period_str_if_any, attempts=1).
        """
        used_period = None
        attempts = 1
        try:
            df = get_history(symbol=symbol, start=start_ts, end=end_ts, interval=interval_str)  # type: ignore
        except TypeError:
            # Some wrappers only accept 'period'
            total_span = end_ts - start_ts
            used_period = _choose_provider_period(total_span)
            df = get_history(symbol=symbol, period=used_period, interval=interval_str)  # type: ignore
        except Exception:
            # Last resort: try period
            total_span = end_ts - start_ts
            used_period = _choose_provider_period(total_span)
            df = get_history(symbol=symbol, period=used_period, interval=interval_str)  # type: ignore

        if not isinstance(df.index, pd.DatetimeIndex):
            # Try to coerce date column to index if necessary
            for col in ("Datetime", "Date", "date", "timestamp"):
                if col in df.columns:
                    df = df.set_index(pd.to_datetime(df[col], utc=True)).drop(columns=[col])
                    break
        # Ensure UTC, ascending
        if isinstance(df.index, pd.DatetimeIndex):
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")
            df = df.sort_index()
        return df, used_period, attempts

    def _count_preload_rows(df: pd.DataFrame, visible_start: pd.Timestamp) -> int:
        if not isinstance(df.index, pd.DatetimeIndex):
            return 0
        return int((df.index < visible_start).sum())

    # -----------------------
    # Main body
    # -----------------------
    msgs: List[str] = []
    attempts_total = 0

    # 1) Normalize + dedupe requests, compute strict warm-up and buffer
    reqs_norm = _parse_and_dedupe(requests)
    required_warmup, buffer_bars = _compute_buffer_bars(reqs_norm)

    # 2) Resolve visible window
    vis_start, vis_end = _derive_visible_window(period)

    # 3) Translate buffer bars -> calendar delta for interval
    delta = _bars_to_calendar_delta(interval, buffer_bars)
    lookback_start = vis_start - delta

    # 4) Fetch with lookback
    df, used_period, attempts = _fetch_history(lookback_start, vis_end, interval)
    attempts_total += attempts

    # 5) Measure preload sufficiency
    available_preload = _count_preload_rows(df, vis_start)
    warmup_complete = available_preload >= required_warmup

    # 6) Optional adaptive retry if short
    if required_warmup > 0 and not warmup_complete and retry_on_shortfall:
        # double the delta once, within reasonable limits
        extra_delta = delta * 2
        lookback_start2 = vis_start - extra_delta
        df2, used_period2, attempts2 = _fetch_history(lookback_start2, vis_end, interval)
        attempts_total += attempts2

        avail2 = _count_preload_rows(df2, vis_start)
        if avail2 > available_preload:
            df = df2
            available_preload = avail2
            used_period = used_period2
            warmup_complete = available_preload >= required_warmup

    # 7) Basic sanity on columns
    needed_cols = ["Close"]
    if not all(c in df.columns for c in needed_cols):
        msgs.append(
            "History missing required 'Close' column; upstream service may have changed schema."
        )

    # 8) Build meta
    meta: Dict[str, Any] = {
        "symbol": symbol,
        "period": period,
        "interval": interval,
        "visibleStart": _to_tz(vis_start),
        "visibleEnd": _to_tz(vis_end),
        "requiredWarmupBars": int(required_warmup),
        "bufferBars": int(buffer_bars),
        "availablePreload": int(available_preload),
        "warmupComplete": bool(warmup_complete),
        "messages": msgs,
        "usedProviderPeriod": used_period,
        "attempts": attempts_total,
    }

    return df, meta


def technical_snapshot(
    symbol: str,
    period: str,
    interval: str,
    requests: Union[List[Union[str, Dict[str, Any]]], Tuple[Union[str, Dict[str, Any]], ...]],
    *,
    style_label: str = "line",
    tz: Optional[str] = None,
    warmup_margin_pct: float = 0.10,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Compute-on-demand, strict-math technical snapshot for UI consumption.

    Returns:
        df_plot: One DataFrame with OHLCV + ONLY requested indicators + __x_idx__ (contiguous, 0..N-1).
        meta:    Warm-up diagnostics merged with compute results (computedSignals, skippedSignals, messages).
    """

    # -----------------------
    # Nested helpers (scoped)
    # -----------------------
    def _parse_and_dedupe(reqs) -> List[Dict[str, Any]]:
        """Accept strings like 'SMA(200)' or dicts {'name':'SMA','length':200}; dedupe by canonical key."""

        def _from_str(s: str) -> Dict[str, Any]:
            s = s.strip()
            m = re.match(r"^\s*([A-Za-z]+)\s*\(([^)]*)\)\s*$", s)
            if not m:
                return {"name": s.strip().upper()}
            name = m.group(1).strip().upper()
            args = [a.strip() for a in m.group(2).split(",") if a.strip() != ""]
            if name in {"SMA", "EMA", "RSI"} and len(args) >= 1:
                return {"name": name, "length": int(args[0])}
            if name in {"BB", "BBANDS"} and len(args) >= 1:
                k = float(args[1]) if len(args) >= 2 else 2.0
                return {"name": "BB", "length": int(args[0]), "k": k}
            if name == "MACD":
                fast = int(args[0]) if len(args) >= 1 else 12
                slow = int(args[1]) if len(args) >= 2 else 26
                signal = int(args[2]) if len(args) >= 3 else 9
                return {"name": "MACD", "fast": fast, "slow": slow, "signal": signal}
            return {"name": name}

        def _canon_key(d: Dict[str, Any]) -> Tuple:
            name = d.get("name", "").upper()
            if name in {"SMA", "EMA", "RSI"}:
                return (name, int(d.get("length", 0)))
            if name == "BB":
                return (name, int(d.get("length", 0)), float(d.get("k", 2.0)))
            if name == "MACD":
                return (
                    name,
                    int(d.get("fast", 12)),
                    int(d.get("slow", 26)),
                    int(d.get("signal", 9)),
                )
            return (name, tuple(sorted((k, d[k]) for k in d.keys() if k != "name")))

        normd: List[Dict[str, Any]] = []
        for r in reqs or []:
            d = _from_str(r) if isinstance(r, str) else {**r, "name": r.get("name", "").upper()}
            normd.append(d)

        seen, uniq = set(), []
        for d in normd:
            k = _canon_key(d)
            if k not in seen:
                seen.add(k)
                uniq.append(d)
        return uniq

    def _compute_requested_indicators(
        df_full: pd.DataFrame,
        reqs_norm: List[Dict[str, Any]],
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
        """
        Compute only requested indicators (strict math), attach columns to df_full.
        Returns: (df_full_with_cols, computed, skipped, msgs)
        """
        msgs: List[str] = []
        computed: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []
        memo: Dict[Tuple, Any] = {}

        # Ensure we have a proper Close series
        if "Close" not in df_full.columns:
            msgs.append("Missing 'Close' column; cannot compute indicators.")
            return df_full, computed, skipped, msgs

        close = pd.to_numeric(df_full["Close"], errors="coerce")
        idx = df_full.index

        def _series_like(x: Any) -> pd.Series:
            """Coerce output to a Series aligned to df_full.index."""
            if isinstance(x, pd.Series):
                return x.reindex(idx)
            # numpy array or list
            return pd.Series(x, index=idx, dtype="float64")

        def _key(name: str, **p) -> Tuple:
            # cast params to builtin types for stable hashing & JSON
            canon = tuple(
                sorted(
                    (
                        k,
                        (
                            int(v)
                            if isinstance(v, (np.integer, int))
                            else float(v)
                            if isinstance(v, (np.floating, float))
                            else v
                        ),
                    )
                    for k, v in p.items()
                )
            )
            return (name.upper(), canon)

        def _attach(colname: str, series: pd.Series) -> None:
            df_full[colname] = _series_like(series)

        for spec in reqs_norm:
            name = str(spec.get("name", "")).upper()
            try:
                if name == "SMA":
                    n = int(spec.get("length", 0))
                    if n <= 0:
                        raise ValueError("length must be > 0")
                    k = _key("SMA", length=n)
                    if k not in memo:
                        memo[k] = sma(close, n)
                    col = f"sma{n}"
                    _attach(col, memo[k])
                    computed.append({"name": "SMA", "length": int(n), "columns": [col]})

                elif name == "EMA":
                    n = int(spec.get("length", 0))
                    if n <= 0:
                        raise ValueError("length must be > 0")
                    k = _key("EMA", length=n)
                    if k not in memo:
                        memo[k] = ema(close, n)
                    col = f"ema{n}"
                    _attach(col, memo[k])
                    computed.append({"name": "EMA", "length": int(n), "columns": [col]})

                elif name == "RSI":
                    n = int(spec.get("length", 0))
                    if n <= 0:
                        raise ValueError("length must be > 0")
                    k = _key("RSI", length=n)
                    if k not in memo:
                        memo[k] = wilder_rsi(close, n)  # facade alias
                    col = f"rsi{n}"
                    _attach(col, memo[k])
                    computed.append({"name": "RSI", "length": int(n), "columns": [col]})

                elif name == "VWAP":
                    basis = str(spec.get("basis", "hlc3")).lower()

                    # --- choose price basis safely ---
                    if basis == "close" or not all(
                        c in df_full.columns for c in ("High", "Low", "Close")
                    ):
                        price = pd.to_numeric(df_full.get("Close"), errors="coerce")
                        out_col = "vwapClose"
                    else:
                        H = pd.to_numeric(df_full.get("High"), errors="coerce")
                        L = pd.to_numeric(df_full.get("Low"), errors="coerce")
                        C = pd.to_numeric(df_full.get("Close"), errors="coerce")
                        price = (H + L + C) / 3.0  # HLC3
                        out_col = "vwap"

                    vol = pd.to_numeric(df_full.get("Volume"), errors="coerce")

                    # guard non-positive / NaN volume in the cumulative
                    pv = (price * vol).where(vol > 0)
                    vv = vol.where(vol > 0)

                    # --- anchor policy by interval ---
                    # intraday if interval is minute/hour granularity
                    _ival = (interval or "").lower()
                    is_intraday = _ival.endswith(("m", "h")) or _ival in {
                        "1m",
                        "2m",
                        "5m",
                        "15m",
                        "30m",
                        "60m",
                        "90m",
                        "1h",
                    }

                    if is_intraday:
                        # group by calendar day in the same tz used elsewhere
                        ix = df_full.index
                        if tz and isinstance(ix, pd.DatetimeIndex) and ix.tz is not None:
                            daykey = ix.tz_convert(tz).floor("D")
                        else:
                            daykey = ix.floor("D")

                        num = pv.groupby(daykey, sort=False).cumsum()
                        den = vv.groupby(daykey, sort=False).cumsum()
                    else:
                        # daily/weekly/monthly → anchored cumulative over the window
                        num = pv.cumsum()
                        den = vv.cumsum()

                    vwap = num / den.replace(0, np.nan)

                    _attach(out_col, vwap)
                    computed.append(
                        {
                            "name": "VWAP",
                            "basis": ("hlc3" if out_col == "vwap" else "close"),
                            "mode": ("session" if is_intraday else "anchored"),
                            "columns": [out_col],
                        }
                    )

                elif name in {"BB", "BBANDS"}:
                    n = int(spec.get("length", 0))
                    if n <= 0:
                        raise ValueError("length must be > 0")
                    k_std = float(spec.get("k", 2.0))
                    kk = _key("BB", length=n, k=k_std)
                    if kk not in memo:
                        mid, upper, lower = bollinger_bands(close, n, k_std)
                        # ensure all are series
                        memo[kk] = (_series_like(mid), _series_like(upper), _series_like(lower))
                    mid, upper, lower = memo[kk]
                    cols = (f"bbMid{n}", f"bbUpper{n}", f"bbLower{n}")
                    _attach(cols[0], mid)
                    _attach(cols[1], upper)
                    _attach(cols[2], lower)
                    computed.append(
                        {"name": "BB", "length": int(n), "k": float(k_std), "columns": list(cols)}
                    )

                elif name == "MACD":
                    fast = int(spec.get("fast", 12))
                    slow = int(spec.get("slow", 26))
                    sig = int(spec.get("signal", 9))
                    if min(fast, slow, sig) <= 0:
                        raise ValueError("fast/slow/signal must be > 0")
                    kk = _key("MACD", fast=fast, slow=slow, signal=sig)
                    if kk not in memo:
                        macd_line, signal_line, hist = macd(close, fast, slow, sig)
                        memo[kk] = (
                            _series_like(macd_line),
                            _series_like(signal_line),
                            _series_like(hist),
                        )
                    macd_line, signal_line, hist = memo[kk]
                    cols = (
                        f"macd_{fast}_{slow}_{sig}",
                        f"macdSignal_{fast}_{slow}_{sig}",
                        f"macdHist_{fast}_{slow}_{sig}",
                    )
                    _attach(cols[0], macd_line)
                    _attach(cols[1], signal_line)
                    _attach(cols[2], hist)
                    computed.append(
                        {
                            "name": "MACD",
                            "fast": int(fast),
                            "slow": int(slow),
                            "signal": int(sig),
                            "columns": list(cols),
                        }
                    )

                else:
                    skipped.append({**spec, "reason": "Unsupported indicator"})

            except Exception as e:
                skipped.append({**spec, "reason": f"{type(e).__name__}: {e}"})

        # Ensure returns match the annotation exactly
        return df_full, computed, skipped, msgs

    def _slice_visible(
        df_full: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp
    ) -> pd.DataFrame:
        mask = (df_full.index >= start_ts) & (df_full.index <= end_ts)
        return df_full.loc[mask].copy()

    def _ensure_x_idx(df_vis: pd.DataFrame) -> pd.DataFrame:
        df_vis = df_vis.sort_index()
        # contiguous 0..N-1 index for the chart
        df_vis["__x_idx__"] = np.arange(len(df_vis), dtype=int)
        return df_vis

    # -----------------------
    # Main pipeline
    # -----------------------
    # 1) Pull extended history for strict warm-up (trimmed to lookback_start..visible_end)
    df_full, hist_meta = extend_history_for_warmup(
        symbol=symbol,
        period=period,
        interval=interval,
        requests=requests,
        tz=tz,
        warmup_margin_pct=warmup_margin_pct,
        retry_on_shortfall=True,
    )

    # 2) Compute only what was requested (strict math), on the FULL frame
    reqs_norm = _parse_and_dedupe(requests)
    df_full, computed, skipped, msgs = _compute_requested_indicators(df_full, reqs_norm)

    # 3) Slice back to the visible window AFTER computation
    vis_start = pd.Timestamp(hist_meta["visibleStart"]).tz_convert("UTC")
    vis_end = pd.Timestamp(hist_meta["visibleEnd"]).tz_convert("UTC")
    df_vis = _slice_visible(df_full, vis_start, vis_end)

    # 4) Single alignment pass: add __x_idx__
    df_plot = _ensure_x_idx(df_vis)

    # Optional helper for robust color fallback in UI (line mode / missing Open)
    if "Close" in df_plot.columns and "prevClose" not in df_plot.columns:
        df_plot["prevClose"] = pd.to_numeric(df_plot["Close"], errors="coerce").shift(1)

    # 5) Meta merge
    meta: Dict[str, Any] = {
        **hist_meta,
        "computedSignals": computed,
        "skippedSignals": skipped,
        "messages": (hist_meta.get("messages", []) + msgs),
        "indicatorApiVersion": "v1",
    }

    return df_plot, meta
