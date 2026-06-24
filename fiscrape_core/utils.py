# fiscrape_core/utils.py
"""Generic helpers (no provider state or network)

Parsing/coercion (to_float, df_or_none), list ops, TTL/time helpers, and small guards.
Also exposes classify_market_session(raw) → {PRE, POST, REGULAR, OVERNIGHT, CLOSED, OTHER}.
Keep this module dependency-light.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any, Literal, Optional

import numpy as np
import pandas as pd


def clamp(x: float, lo: float, hi: float) -> float:
    """Clamp x into [lo, hi]."""
    return max(lo, min(hi, x))


MarketSession = Literal["PRE", "POST", "REGULAR", "OVERNIGHT", "CLOSED", "OTHER"]


def classify_market_session(raw: Optional[str]) -> MarketSession:
    """
    Collapse Yahoo's raw marketState strings into:
    PRE | POST | REGULAR | OVERNIGHT | CLOSED | OTHER
    (OVERNIGHT = PREPRE/POSTPOST; OTHER = any unknowns, e.g., HALTED)
    """
    s = (raw or "").upper()
    if not s:
        return "REGULAR"
    if s in ("PREPRE", "POSTPOST"):
        return "OVERNIGHT"
    if s.startswith("PRE"):
        return "PRE"
    if s.startswith("POST"):
        return "POST"
    if s == "REGULAR":
        return "REGULAR"
    if s == "CLOSED":
        return "CLOSED"
    return "OTHER"


def df_or_none(obj) -> Optional[pd.DataFrame]:
    try:
        if isinstance(obj, pd.DataFrame):
            # Normalize: ensure descending time columns if columns look like dates
            df = obj.copy()
            try:
                # YF statements have date-like column labels
                try:
                    cols = pd.to_datetime(df.columns)
                except Exception:
                    cols = df.columns  # keep as-is if conversion fails
                # Only sort if conversion produced (some) datetimes
                if any(hasattr(c, "to_pydatetime") for c in cols):
                    df = df.loc[
                        :,
                        sorted(
                            df.columns,
                            key=lambda c: pd.to_datetime(c, errors="coerce"),
                            reverse=True,
                        ),
                    ]
            except Exception:
                pass
            return df
    except Exception:
        return None
    return None


# utils.py
def finite_positive(x) -> Optional[float]:
    try:
        v = float(x)
        return v if math.isfinite(v) and v > 0 else None
    except Exception:
        return None


def first_non_none(*vals):
    """First non-None value."""
    for v in vals:
        if v is not None:
            return v
    return None


def json_default(o):
    if isinstance(o, np.generic):
        return o.item()
    if isinstance(o, pd.Timestamp):
        return o.isoformat()
    return str(o)


def json_digest(obj: Any) -> str:
    return json.dumps(obj or {}, sort_keys=True, default=json_default)


def list_move_inplace(lst: list[str], src: int, dst: int):
    """Move item at index src to index dst within the same list (dst is the final index)."""
    if src == dst or src < 0 or dst < 0 or src >= len(lst) or dst >= len(lst):
        return
    item = lst.pop(src)
    lst.insert(dst, item)


def to_epoch_int(ts: Any) -> Optional[int]:
    """Int-ify epoch-ish timestamps safely; returns None if not coercible."""
    try:
        return int(ts) if ts is not None else None
    except Exception:
        return None


def to_float(
    *xs: Any,
    finite: bool = True,
    min_value: float | None = None,
    include_min: bool = False,
    max_value: float | None = None,
    include_max: bool = False,
) -> Optional[float]:
    """
    Return the first argument that cleanly converts to float and satisfies the constraints.
    - Coalesces across multiple candidates (left to right).
    - By default rejects NaN/±inf (finite=True).
    - Optional bounds: min/max with inclusive/exclusive edges.
    """
    for x in xs:
        try:
            v = float(x)
        except (TypeError, ValueError):
            continue
        if finite and not math.isfinite(v):
            continue
        if min_value is not None:
            if (v < min_value) or (not include_min and v == min_value):
                continue
        if max_value is not None:
            if (v > max_value) or (not include_max and v == max_value):
                continue
        return v
    return None


def ttl_bucket(ts: datetime, ttl_seconds: int) -> int:
    """Integer bucket index for a TTL (drives cache invalidation cadence)."""
    ttl = max(1, int(ttl_seconds))
    return int(ts.timestamp() // ttl)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# --- Timeframe mapping (UI <-> yfinance API) -------------------------------

# UI -> API (authoritative)
PERIOD_UI2API: dict[str, str] = {
    "1D": "1d",
    "5D": "5d",
    "1M": "1mo",
    "3M": "3mo",
    "6M": "6mo",
    "YTD": "ytd",
    "1Y": "1y",
    "5Y": "5y",
    "MAX": "max",
}
INTERVAL_UI2API: dict[str, str] = {
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

# Auto-generated reverse maps: API -> UI
PERIOD_API2UI: dict[str, str] = {v: k for k, v in PERIOD_UI2API.items()}
INTERVAL_API2UI: dict[str, str] = {v: k for k, v in INTERVAL_UI2API.items()}

# Common alias normalization (lowercased keys)
_PERIOD_ALIASES = {
    "1d": "1D",
    "5d": "5D",
    "1mo": "1M",
    "3mo": "3M",
    "6mo": "6M",
    "ytd": "YTD",
    "1y": "1Y",
    "5y": "5Y",
    "max": "MAX",
}
_INTERVAL_ALIASES = {
    "1m": "1m",
    "2m": "2m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "60m": "60m",
    "90m": "90m",
    "1d": "1D",
    "5d": "5D",
    "1wk": "1W",
    "1w": "1W",
    "1mo": "1M",
    "3mo": "3M",
}


def normalize_period_key(ui_key: str | None) -> str:
    """Return a valid UI period key; default to '1D' on invalid."""
    if not ui_key:
        return "1D"
    k = ui_key.strip()
    if k in PERIOD_UI2API:  # exact UI match
        return k
    lk = k.lower()
    if lk in _PERIOD_ALIASES:  # UI-like or API-like in lowercase
        return _PERIOD_ALIASES[lk]
    # Try direct API -> UI (if user passed '1d', 'ytd', etc)
    return PERIOD_API2UI.get(lk, "1D")


def normalize_interval_key(ui_key: str | None) -> str:
    """Return a valid UI interval key; default to '1m' on invalid."""
    if not ui_key:
        return "1m"
    k = ui_key.strip()
    if k in INTERVAL_UI2API:  # exact UI match
        return k
    lk = k.lower()
    if lk in _INTERVAL_ALIASES:
        return _INTERVAL_ALIASES[lk]
    # Try API -> UI (if user passed '1wk', '1mo', etc)
    return INTERVAL_API2UI.get(lk, "1m")


def period_to_api(ui_key: str | None) -> str:
    """Map UI period to API string, defaulting via '1D'."""
    ui = normalize_period_key(ui_key)
    return PERIOD_UI2API[ui]


def interval_to_api(ui_key: str | None) -> str:
    """Map UI interval to API string, defaulting via '1m'."""
    ui = normalize_interval_key(ui_key)
    return INTERVAL_UI2API[ui]


def resolve_timeframe(period_ui: str | None, interval_ui: str | None) -> dict[str, str]:
    """
    Return a canonical dict with both UI and API forms, with defaults applied.
    Keys: uiPeriod, uiInterval, apiPeriod, apiInterval.
    """
    ui_p = normalize_period_key(period_ui)
    ui_i = normalize_interval_key(interval_ui)
    return {
        "uiPeriod": ui_p,
        "uiInterval": ui_i,
        "apiPeriod": PERIOD_UI2API[ui_p],
        "apiInterval": INTERVAL_UI2API[ui_i],
    }
