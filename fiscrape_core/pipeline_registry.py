# fiscrape_core/pipeline_registry.py
"""Provider field registry (allowlists and aliases)

Defines:
- LIVE_PASS_KEYS / PROFILE_PASS_KEYS: canonical allowlists for pass-through fields.
- ALIAS_MAP and helpers (apply_aliases, keys_for) to normalize provider key names.
No network, no formatting; used by services/technical to keep payloads consistent.
"""

from __future__ import annotations

from typing import Any, Dict, Set

# -------------------------------
# LIVE (quote/session) allowlist
# -------------------------------
LIVE_PASS_KEYS: Set[str] = {
    # Identity/trace (cheap & useful)
    "symbol",
    "quoteType",
    # Session & timing
    "marketState",
    "regularMarketTime",
    "preMarketTime",
    "postMarketTime",
    # Prices (reg, pre, and post) & deltas (provider-reported, no recompute)
    "regularMarketPrice",
    "regularMarketPreviousClose",
    "regularMarketChange",
    "regularMarketChangePercent",
    "preMarketPrice",
    "preMarketChange",
    "preMarketChangePercent",
    "postMarketPrice",
    "postMarketChange",
    "postMarketChangePercent",
    # Market cap, net assets, and shares
    "marketCap",
    "netAssets",
    "sharesOutstanding",
    "impliedSharesOutstanding",
    "floatShares",
    "navPrice",
    # Volumes (provider-first; engine may compute ratios/z-scores)
    "regularMarketVolume",
    "averageDailyVolume3Month",
    "averageDailyVolume10Day",
    # 52-week
    "fiftyTwoWeekHigh",
    "fiftyTwoWeekLow",
    # To be organized
    "trailingPE",
    "forwardPE",
    "dividendRate",
    "dividendYield",
    "exDividendDate",
}

# --------------------------------
# PROFILE (stable issuer/listing)
# --------------------------------
PROFILE_PASS_KEYS: Set[str] = {
    # Names & identity
    "longName",
    "shortName",
    # Currencies (profile-first policy)
    "currency",
    "financialCurrency",
    # Close/Open
    "previousClose",
    "open",
    # Bid/Ask
    "bid",
    "ask",
    # Beta
    "beta",
    # Price/Earnings
    "trailingPE",
    # Price/Sales
    "priceToSalesTrailing12Months",
    # Ex-dividend Date
    "exDividendDate",
    # Summary
    "longBusinessSummary",
    # Description (Equity)
    "fullExchangeName",
    "sectorDisp",
    "industryDisp",
    "fullTimeEmployees",
    "city",
    "state",
    "country",
    "website",
    # Description (ETF)
    "fundFamily",
    "category",
    "fundInceptionDate",
    # Misc stable facts
    "firstTradeDateEpochSeconds",
}

ALIAS_MAP = {
    # prices & baseline
    "lastPrice": "regularMarketPrice",
    "previousClose": "regularMarketPreviousClose",
    "dayHigh": "regularMarketDayHigh",
    "dayLow": "regularMarketDayLow",
    "open": "regularMarketOpen",
    "volume": "regularMarketVolume",
    # volumes (averages)
    "averageVolume": "averageDailyVolume3Month",
    "averageVolume10days": "averageDailyVolume10Day",
}


# -------------------------------
# Helpers
# -------------------------------
def keys_for(route: str) -> Set[str]:
    if route not in ("live", "profile"):
        raise ValueError(f"Unknown route {route!r}")
    return LIVE_PASS_KEYS if route == "live" else PROFILE_PASS_KEYS


def apply_aliases(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        canonical = ALIAS_MAP.get(k, k)
        out[canonical] = v
    return out
