# fiscrape_core/metrics_engine.py
"""Live metrics composer (pure compute, no network)

Derives price-based scalars (marketCap, trailingPE, PB/PS fallbacks), 52-week helpers,
and volume signals (incl. z-score from history). Accepts optional 'extras' (from
FundamentalsAnalyzer) to prefer analyzer scalars where present. No formatting/state.
"""

from __future__ import annotations

from typing import Any

from .math_tools import compute_market_cap, safe_ratio, zscore_from_series
from .utils import finite_positive, first_non_none, to_float


# Compute live metrics to process fast-moving metrics from quote data pulled from get_live_quote
def compute_live_metrics(price: float | None, info: dict[str, Any], **kwargs) -> dict[str, Any]:
    """
    Provider-first:
      - If Yahoo supplies a ratio (trailingPE, priceToBook, priceToSalesTrailing12Months, dividendYield), pass it through.
      - Otherwise compute a fallback here using Yahoo info (exception rule) with denominator <= 0 -> 0.0.
      - P/CF TTM is NOT computed here (statement-based) — we only pass it through if Fundamentals computed it.
    Also adds:
      - priceToSalesTtm (alias)
      - 52-week helpers: offHighPct, positionInRange, activeOver52wLow, activeOver52wHigh
      - fundAssetsEst (if netAssets is missing)
    """
    p = to_float(price)

    # Shares — for market cap & fallbacks
    sh_out = to_float(first_non_none(info.get("sharesOutstanding")))
    sh_impl = to_float(first_non_none(info.get("impliedSharesOutstanding")))
    sh_float = to_float(first_non_none(info.get("floatShares")))

    # Denominator sources (Yahoo-first per your exception rule)
    trailing_eps = to_float(
        first_non_none(
            info.get("epsTrailingTwelveMonths"),
            info.get("trailingEps"),
        )
    )
    book_ps = to_float(first_non_none(info.get("bookValue")))
    total_rev = to_float(first_non_none(info.get("totalRevenue")))
    div_rate = to_float(
        first_non_none(
            info.get("trailingAnnualDividendRate"),
            info.get("dividendRate"),
        )
    )

    # Provider values (prefer these if present)
    pe_yf = to_float(info.get("trailingPE"))
    pb_yf = to_float(info.get("priceToBook"))
    ps_yf = to_float(info.get("priceToSalesTrailing12Months"))
    dy_yf = to_float(info.get("dividendYield"))

    out: dict[str, Any] = {
        "price": p if isinstance(p, (int, float)) else None,
        "marketCap": compute_market_cap(p, sh_out, sh_impl, sh_float),
    }

    # --- trailingPE (provider-first; else fallback from Yahoo EPS) ---
    if pe_yf is not None:
        out["trailingPE"] = pe_yf
    else:
        out["trailingPE"] = safe_ratio(p, trailing_eps)

    # --- priceToBook (provider-first; else p / bookValue_per_share) ---
    if pb_yf is not None:
        out["priceToBook"] = pb_yf
    else:
        out["priceToBook"] = safe_ratio(p, book_ps)

    # --- priceToSalesTrailing12Months (provider-first; else marketCap/Revenue or (p*shares)/Revenue) ---
    if ps_yf is not None:
        out["priceToSalesTrailing12Months"] = ps_yf
    else:
        if total_rev is not None:
            mcap = out["marketCap"] if isinstance(out["marketCap"], (int, float)) else None
            num = (
                mcap
                if mcap is not None
                else (
                    float(p) * float(sh_out) if p is not None and sh_out not in (None, 0) else None
                )
            )
            out["priceToSalesTrailing12Months"] = safe_ratio(num, total_rev)
        else:
            out["priceToSalesTrailing12Months"] = None

    # --- Alias: priceToSalesTtm ---
    out["priceToSalesTtm"] = out.get("priceToSalesTrailing12Months")

    # --- dividendYield (provider-first; else dividendRate / price) ---
    if dy_yf is not None:
        out["dividendYield"] = dy_yf
    else:
        out["dividendYield"] = safe_ratio(
            div_rate, p
        )  # compute_dividend_yield equivalent; denom<=0→0 via _safe_div

    # --- priceToCashFlowTtm (pass-through only; Fundamentals owns the compute) ---
    out["priceToCashFlowTtm"] = to_float(info.get("priceToCashFlowTtm"))

    # --- 52-week helpers (active price vs range) ---
    low = to_float(info.get("fiftyTwoWeekLow"))
    high = to_float(info.get("fiftyTwoWeekHigh"))

    # activeOver52wLow / activeOver52wHigh
    out["activeOver52wLow"] = safe_ratio(p, low)
    out["activeOver52wHigh"] = safe_ratio(p, high)

    # offHighPct = (p / high) - 1
    _ratio_high = safe_ratio(p, high)
    out["offHighPct"] = (_ratio_high - 1.0) if isinstance(_ratio_high, (int, float)) else None

    # positionInRange = (p - low) / (high - low) clipped to [0, 1]
    if p is not None and low is not None and high is not None and float(high) > float(low):
        pos = (float(p) - float(low)) / (float(high) - float(low))
        out["positionInRange"] = max(0.0, min(1.0, pos))
    else:
        out["positionInRange"] = None

    # --- ETF NAV premium/discount: (price / nav) - 1 ---
    nav = to_float(info.get("navPrice"))
    _ratio_nav = safe_ratio(p, nav)  # denom<=0 -> 0, None if either missing
    out["navPremiumPct"] = (_ratio_nav - 1.0) if isinstance(_ratio_nav, (int, float)) else None

    # --- ETF fund assets estimate (fallback) ---
    net_assets = to_float(info.get("netAssets"))
    if net_assets is None and p is not None and sh_out not in (None, 0):
        out["fundAssetsEst"] = float(p) * float(sh_out)
    else:
        out["fundAssetsEst"] = None

    # ----- Volume signals (provider-first; history-assisted) -----
    # today volume: prefer provider, fallback to intraday aggregation
    today_vol = finite_positive(info.get("regularMarketVolume") or info.get("volume"))
    if today_vol is None:
        intra_df = kwargs.get("intraday_history")
        last_date = intra_df.index.max().date()
        v = intra_df.loc[intra_df.index.date == last_date, "Volume"].dropna().astype(float).sum()
        today_vol = float(v) if v > 0 else None
    # % vs avg (provider baselines)
    avg3m = finite_positive(info.get("averageDailyVolume3Month"))
    avg10d = finite_positive(info.get("averageDailyVolume10Day"))

    if today_vol is not None and avg3m is not None:
        out["volumePctVsAvg"] = (today_vol / avg3m) - 1.0
        out["volumeBaselineLabel"] = "3-mo"
        out["volumeAvgUsed"] = avg3m
    elif today_vol is not None and avg10d is not None:
        out["volumePctVsAvg"] = (today_vol / avg10d) - 1.0
        out["volumeBaselineLabel"] = "10-day"
        out["volumeAvgUsed"] = avg10d

    # z-score from daily history if provided
    z = zscore_from_series(today_vol, kwargs.get("volume_history_daily"))

    if z is not None:
        out["volumeZScore"] = z
        # if we didn't set a baseline label via provider, note 'hist'
        out.setdefault("volumeBaselineLabel", "hist")

    return out
