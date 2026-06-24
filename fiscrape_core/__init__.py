# fiscrape_core/__init__.py
"""fiscrape_core — public API surface

Stable, app-facing re-exports for the core layer. Keep network at service edges, keep
compute pure, and keep UI formatting out of core. All public scalar keys use camelCase.
"""

from __future__ import annotations

from .fundamentals import FundamentalsAnalyzer
from .math_tools import (
    change_pct,
    compute_dividend_yield,
    compute_market_cap,
    compute_trailing_pe,
    off_high_pct,
    position_in_range,
    range_endpoints_as_pct_of_price,
    zscore,
)
from .metrics_engine import (
    compute_live_metrics,
)
from .pipeline_registry import LIVE_PASS_KEYS, PROFILE_PASS_KEYS, apply_aliases, keys_for
from .portfolio import Portfolio, ScrapedTicker
from .services import get_financial_statements, get_history, get_live_quote, hasRetrievablePrice
from .snapshot import snapshot
from .technical_snapshot import technical_snapshot

# Timeframe mapping & helpers (single source of truth)
from .utils import (
    INTERVAL_API2UI,
    INTERVAL_UI2API,
    PERIOD_API2UI,
    PERIOD_UI2API,
    clamp,
    classify_market_session,
    interval_to_api,
    json_digest,
    list_move_inplace,
    normalize_interval_key,
    normalize_period_key,
    period_to_api,
    resolve_timeframe,
    to_float,
    ttl_bucket,
    utc_now,
)

__all__ = (
    # fundamentals
    "FundamentalsAnalyzer",
    # metrics
    "change_pct",
    "compute_dividend_yield",
    "compute_market_cap",
    "compute_trailing_pe",
    "off_high_pct",
    "position_in_range",
    "range_endpoints_as_pct_of_price",
    "zscore",
    # metrics_engine
    "compute_live_metrics",
    # pipeline_registry
    "LIVE_PASS_KEYS",
    "PROFILE_PASS_KEYS",
    "keys_for",
    "apply_aliases",
    # portfolio
    "ScrapedTicker",
    "Portfolio",
    # services
    "get_financial_statements",
    "get_history",
    "get_live_quote",
    "hasRetrievablePrice",
    # snapshot
    "snapshot",
    # technical_snapshot
    "technical_snapshot",
    # utils
    "INTERVAL_API2UI",
    "INTERVAL_UI2API",
    "PERIOD_API2UI",
    "PERIOD_UI2API",
    "clamp",
    "classify_market_session",
    "interval_to_api",
    "json_digest",
    "list_move_inplace",
    "normalize_interval_key",
    "normalize_period_key",
    "period_to_api",
    "resolve_timeframe",
    "to_float",
    "ttl_bucket",
    "utc_now",
)
