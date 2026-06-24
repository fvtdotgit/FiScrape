# fiscrape_core/math_tools.py
"""Numerical primitives for finance (pure, tiny)

Safe ratio, percent change, z-score, 52-week helpers, and small utilities used across
core. No provider awareness, no I/O, and no display rounding.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

from .utils import clamp, to_float

import pandas as pd
import numpy as np


# --- Math ---
def change_pct(
    current: Any,
    reference: Any,
    *,
    clamp_low: float | None = None,
    clamp_high: float | None = None,
) -> float | None:
    r = safe_ratio(current, reference)
    if r is None:
        return None
    pct = (r - 1.0) * 100.0
    if clamp_low is not None and clamp_high is not None:
        pct = clamp(pct, clamp_low, clamp_high)
    return pct


def safe_ratio(
    num: float | int | None,
    denom: float | int | None,
    *,
    bad: Optional[float] = None,  # e.g., use 0.0 where tests expect zero-on-bad
    allow_zero: bool = False,  # True => denom != 0; False => denom > 0
) -> Optional[float]:
    try:
        n = float(num)
        d = float(denom)
    except (TypeError, ValueError):
        return bad
    if (d == 0.0 and not allow_zero) or (d < 0.0 and not allow_zero):
        return bad
    if d == 0.0:
        return bad
    return n / d


# ---------- Price-derived metrics ----------
def compute_market_cap(
    price: Any, shares_outstanding: Any = None, implied_shares: Any = None, float_shares: Any = None
) -> float | None:
    p = to_float(price)
    if p is None:
        return None
    s: float | None = None
    for candidate in (shares_outstanding, implied_shares, float_shares):
        v = to_float(candidate)
        if v is not None:
            s = v
            break
    return p * s if s is not None else None


def compute_trailing_pe(price: Any, eps_ttm: Any) -> float | None:
    p = to_float(price)
    e = to_float(eps_ttm)
    if p is None or e is None or e <= 0:
        return None
    return p / e


def compute_dividend_yield(price: Any, annual_div: Any) -> float | None:
    p = to_float(price)
    d = to_float(annual_div)
    if p is None or p <= 0 or d is None:
        return None
    return (d / p) * 100.0


# ---------- 52-week math ----------
def off_high_pct(price: Any, high: Any) -> float | None:
    r = safe_ratio(price, high)
    return (r - 1.0) * 100.0 if r is not None else None


def position_in_range(price: Any, low: Any, high: Any) -> float | None:
    p = to_float(price)
    lo = to_float(low)
    hi = to_float(high)
    if p is None or lo is None or hi is None:
        return None
    if hi <= lo:
        return None
    r = safe_ratio(p - lo, hi - lo)
    return clamp(r * 100.0, 0.0, 100.0) if r is not None else None


def range_endpoints_as_pct_of_price(
    price: Any, low: Any, high: Any
) -> tuple[float | None, float | None]:
    p = to_float(price)
    lo = to_float(low)
    hi = to_float(high)
    if p is None or p == 0:
        return None, None
    lo_r = safe_ratio(lo, p)
    hi_r = safe_ratio(hi, p)
    return (
        (lo_r * 100.0) if lo_r is not None else None,
        (hi_r * 100.0) if hi_r is not None else None,
    )


# ---------- Z-score ----------
def zscore(value: Any, mean: Any, std: Any) -> float | None:
    v = to_float(value)
    m = to_float(mean)
    s = to_float(std)
    if v is None or m is None or s is None or s == 0:
        return None
    return (v - m) / s


def zscore_from_series(
    value: float | int | np.number,
    series_or_df: pd.Series | pd.DataFrame,
    window: Optional[int] = 20,
    *,
    column: str = "Volume",
    exclude_last: bool = True,
    ddof: int = 0,
) -> float | None:
    """
    Scalar z-score of `value` against the last `window` *completed* points of a series.

    - Accepts either a pd.Series or a pd.DataFrame; if DataFrame, uses `column` (default "Volume").
    - If exclude_last=True, drop the most-recent element in the baseline (e.g., partial 'today').
    - Returns float, or None if insufficient data / invalid std / missing column.
    """
    # coerce scalar
    try:
        v = float(value)
    except Exception:
        return None

    # normalize input to a numeric Series
    if isinstance(series_or_df, pd.DataFrame):
        if column not in series_or_df.columns:
            return None
        s = pd.to_numeric(series_or_df[column], errors="coerce")
    else:
        s = pd.to_numeric(series_or_df, errors="coerce")

    s = s.dropna()
    if exclude_last and len(s) > 0:
        s = s.iloc[:-1]

    # default window if None
    w = int(20 if window is None else window)
    if len(s) < w:
        return None

    base = s.iloc[-w:]
    mu = base.mean()
    sigma = base.std(ddof=ddof)

    if not np.isfinite(sigma) or sigma == 0:
        return None

    return (v - mu) / sigma


# ================================
# Technical Indicators (pure math)
# ================================


def sma(series: pd.Series, length: int) -> pd.Series:
    """Simple Moving Average (no rounding)."""
    s = pd.to_numeric(series, errors="coerce")
    return s.rolling(window=int(length), min_periods=int(length)).mean()


def ema(series: pd.Series, length: int) -> pd.Series:
    """Exponential Moving Average (no rounding; adjust=False for recursive EMA)."""
    s = pd.to_numeric(series, errors="coerce")
    return s.ewm(span=int(length), adjust=False, min_periods=int(length)).mean()


def wilder_rsi(close: pd.Series, length: int = 14) -> pd.Series:
    """
    Wilder's RSI (classic). No rounding. Returns NaN until enough periods.
    Implementation: EMA of gains/losses with alpha=1/length (adjust=False).
    """
    c = pd.to_numeric(close, errors="coerce")
    delta = c.diff()

    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(alpha=1.0 / float(length), adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1.0 / float(length), adjust=False, min_periods=length).mean()

    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi


def macd(
    close: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    MACD line, Signal line, Histogram (no rounding).
    MACD = EMA(fast) - EMA(slow); Signal = EMA(MACD, signal); Hist = MACD - Signal.
    """
    c = pd.to_numeric(close, errors="coerce")
    ema_fast = ema(c, fast)
    ema_slow = ema(c, slow)
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=int(signal), adjust=False, min_periods=int(signal)).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def bollinger_bands(
    close: pd.Series,
    length: int = 20,
    k: float = 2.0,
    ddof: int = 0,
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Bollinger Bands (mid, upper, lower). No rounding.
    mid = SMA(length); dev = std(length, ddof); upper = mid + k*dev; lower = mid - k*dev.
    """
    c = pd.to_numeric(close, errors="coerce")
    mid = sma(c, int(length))
    dev = c.rolling(window=int(length), min_periods=int(length)).std(ddof=ddof)
    upper = mid + float(k) * dev
    lower = mid - float(k) * dev
    return mid, upper, lower


def typical_price_hlc3(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    return (high.astype("float64") + low.astype("float64") + close.astype("float64")) / 3.0


def typical_price_ohlc4(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series
) -> pd.Series:
    return (
        open_.astype("float64")
        + high.astype("float64")
        + low.astype("float64")
        + close.astype("float64")
    ) / 4.0


def cumulative_weighted_mean(x: pd.Series, w: pd.Series, *, min_weight: float = 0.0) -> pd.Series:
    """
    Pure cumulative weighted mean: cumsum(x*w) / cumsum(w).
    - Returns NaN until cumulative weight > min_weight.
    - Treats non-positive weights as 0.
    """
    x = x.astype("float64")
    w = w.astype("float64").where(w > 0, 0.0)  # clamp negatives/NaNs to 0 in the weight stream
    num = (x * w).cumsum()
    den = w.cumsum()
    out = num / den.replace(0.0, np.nan)
    valid = den > min_weight
    return out.where(valid)


def vwap(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    *,
    basis: str = "hlc3",
    open_: pd.Series | None = None,
) -> pd.Series:
    """
    Anchored (window-cumulative) VWAP using a chosen price basis.
    Stateless: no sessions/timezones/grouping.
    basis: 'hlc3' (default), 'close', 'ohlc4'
    """
    if basis == "close":
        price = close
    elif basis == "ohlc4":
        if open_ is None:
            raise ValueError("basis='ohlc4' requires open_ series")
        price = typical_price_ohlc4(open_, high, low, close)
    else:  # 'hlc3'
        price = typical_price_hlc3(high, low, close)

    # Propagate NaNs from inputs into the final series
    valid_rows = high.notna() & low.notna() & close.notna() & volume.notna()
    if basis == "ohlc4":
        valid_rows &= open_.notna()

    out = cumulative_weighted_mean(price.where(valid_rows), volume.where(valid_rows))
    return out
