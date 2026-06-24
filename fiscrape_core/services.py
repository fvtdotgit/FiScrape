# fiscrape_core/services.py
"""Network boundary for Yahoo Finance (provider edge only)

Single responsibility: fetch provider payloads with minimal shaping.
- get_live_quote(symbol): canonical live fields (REG + PRE/AH). No recompute of ratios.
- get_history(symbol, period, interval): price/volume history (DataFrame) for charts/signals.
- get_financial_statements(symbol): income/balance/cashflow DataFrames.
Emits service-edge warnings; higher layers compose or format results.
"""

from typing import Any, Dict, Optional

import pandas as pd
import yfinance as yf

from fiscrape_core.pipeline_registry import LIVE_PASS_KEYS, apply_aliases
from fiscrape_core.utils import df_or_none, to_epoch_int, to_float

# in-memory, last-run warnings keyed by symbol
LAST_SERVICE_WARNINGS: dict[str, list[dict[str, Any]]] = {}


def emit_service_warning(
    symbol: str, *, code: str, message: str, where: str, context: Dict[str, Any] | None = None
) -> None:
    lst = LAST_SERVICE_WARNINGS.setdefault(symbol, [])
    lst.append({"code": code, "message": message, "where": where, "context": context or {}})


def pop_service_warnings(symbol: str) -> list[dict[str, Any]]:
    return LAST_SERVICE_WARNINGS.pop(symbol, [])


# --- Boolean classifiers ---
def hasRetrievablePrice(symbol: str) -> bool:
    """
    Simple + robust:
      1) Try fast_info keys and accept ANY finite numeric.
      2) Fall back to recent daily history (5d) and accept any finite close.
    """
    try:
        t = yf.Ticker(symbol)

        fi = getattr(t, "fast_info", None) or {}
        for k in ("regularMarketPrice", "lastPrice", "previousClose", "regularMarketPreviousClose"):
            if to_float(fi.get(k)) is not None:
                return True

        h = t.history(period="5d", interval="1d", auto_adjust=True, actions=False, repair=True)
        if h is not None and not h.empty and "Close" in h:
            c = h["Close"].dropna()
            if c.size and to_float(c.iloc[-1]) is not None:
                return True

        return False
    except Exception:
        return False


# --- get_live_quote ---
def get_live_quote(symbol: str) -> Dict[str, Any]:
    """
    Canonical live quote (REG + PRE/AH) using Yahoo/yfinance field names.
    - No recomputation of % changes, volumes, or ratios.
    - No fallback to last daily close; gating happens in snapshot.
    - Prioritize fast_info over info for overlapping live fields.
    - Pass through only keys in LIVE_PASS_KEYS (pipeline_registry).
    """
    t = yf.Ticker(symbol)

    # --- A) Pull provider payloads ---
    fi = getattr(t, "fast_info", {}) or {}
    try:
        info_raw = (
            getattr(t, "get_info", t.info)()
            if callable(getattr(t, "get_info", None))
            else (t.info or {})
        )
    except Exception:
        info_raw = t.info or {}

    # --- B) Canonical alias map (both directions into yfinance canonical names) ---

    fi_can = apply_aliases(fi)
    info_can = apply_aliases(info_raw)

    # --- C) Merge with precedence: info → fast_info (fast_info wins) ---
    raw: Dict[str, Any] = dict(info_can)
    raw.update(fi_can)

    # --- D) Seed symbol explicitly (cheap & useful for tracing) ---
    raw["symbol"] = symbol

    # --- E) Allowlist pass-through (no manual re-keying) ---
    q: Dict[str, Any] = {k: raw[k] for k in LIVE_PASS_KEYS if k in raw and raw[k] is not None}

    # --- F) Coerce time fields to epoch ints (keep provider names) ---
    for tk in ("regularMarketTime", "preMarketTime", "postMarketTime"):
        if tk in q:
            q[tk] = to_epoch_int(q[tk])

    # --- G) Sanitize non-finite floats (avoid NaN/inf downstream) ---
    try:
        import math

        for k, v in list(q.items()):
            if isinstance(v, float) and not math.isfinite(v):
                q[k] = None
    except Exception:
        pass

    return q


def get_history(symbol: str, period: str, interval: str) -> pd.DataFrame:
    """
    Fetch price history for `symbol` with the original 4-arg signature.
    `hist_bucket` is intentionally unused here; it's passed through so callers
    (e.g., snapshot / cached wrappers) can bust caches.

    Returns:
        pd.DataFrame: same shape as before, empty DataFrame on failure.
    """
    try:
        t = yf.Ticker(symbol)  # let yfinance manage its own session/crumb
        df = t.history(
            period=period,
            interval=interval,
            prepost=interval.endswith("m"),
            auto_adjust=True,
            actions=False,
            repair=True,
        )
        if df is None or df.empty:
            return pd.DataFrame()
        # normalize to UTC if tz-aware (keeps downstream consistent)
        if getattr(df.index, "tz", None) is not None:
            df = df.tz_convert("UTC")
        return df
    except Exception:
        # match original behavior: fail closed, no side effects
        return pd.DataFrame()


def get_financial_statements(symbol: str) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Fetch Yahoo Finance statements for a symbol via yfinance.Ticker.
    Returns a dict of DataFrames or None when unavailable.
    Keys:
      income   := Ticker.income_stmt
      balance  := Ticker.balance_sheet
      cashflow := Ticker.cashflow
      q_income := Ticker.quarterly_income_stmt
      q_balance:= Ticker.quarterly_balance_sheet
      q_cashflow:=Ticker.quarterly_cashflow
    """
    t = yf.Ticker(symbol)
    try:
        inc = df_or_none(getattr(t, "income_stmt", None))
        bal = df_or_none(getattr(t, "balance_sheet", None))
        cfs = df_or_none(getattr(t, "cashflow", None))
        qinc = df_or_none(getattr(t, "quarterly_income_stmt", None))
        qbal = df_or_none(getattr(t, "quarterly_balance_sheet", None))
        qcfs = df_or_none(getattr(t, "quarterly_cashflow", None))
    except Exception:
        # Defensive: return Nones if yfinance raises
        inc = bal = cfs = qinc = qbal = qcfs = None

    return {
        "income": inc,
        "balance": bal,
        "cashflow": cfs,
        "q_income": qinc,
        "q_balance": qbal,
        "q_cashflow": qcfs,
    }
