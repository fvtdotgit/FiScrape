import numpy as np
import pandas as pd
import pytest


# ----------------------------
# Helpers shared by many tests
# ----------------------------
def _mk_intraday(vols=(100, 200, 300)):
    idx = pd.date_range("2025-01-01 09:30", periods=len(vols), freq="1min", tz="UTC")
    return pd.DataFrame({"Close": np.linspace(100.0, 101.0, len(vols)), "Volume": vols}, index=idx)


# ============================
# SERVICES: live quote surface
# ============================
def test_get_live_quote_falls_back_to_daily_close(monkeypatch):
    """If live price is missing, services should use the last daily close."""
    df = pd.DataFrame(
        {"Close": [100.0, 101.0, 102.0, 103.0, 104.0]},
        index=pd.date_range("2025-08-25", periods=5, freq="D"),
    )

    class FakeTicker:
        fast_info = {}
        info = {}

        def history(self, *a, **k):
            return df

    import fiscrape_core.services as services

    monkeypatch.setattr(services.yf, "Ticker", lambda *_a, **_k: FakeTicker())

    from fiscrape_core.services import get_live_quote

    q = get_live_quote("AAPL", quote_bucket=0)
    assert q.get("regularMarketPrice") == pytest.approx(104.0)


def test_get_live_quote_normalizes_volume_alias_to_3m(monkeypatch):
    """Provider may give `averageVolume` only; we should emit canonical `averageDailyVolume3Month`."""

    class FakeTicker:
        fast_info = {}
        info = {
            "averageVolume": 2_345_678,
            "regularMarketPrice": 100.0,
            "regularMarketPreviousClose": 99.0,
        }

        def history(self, *a, **k):
            return pd.DataFrame()

    import fiscrape_core.services as services

    monkeypatch.setattr(services.yf, "Ticker", lambda *_a, **_k: FakeTicker())

    from fiscrape_core.services import get_live_quote

    q = get_live_quote("AAPL", quote_bucket=0)
    assert q.get("averageDailyVolume3Month") == 2_345_678
    assert "averageVolume" not in q  # canonical-only output


# ============================================
# ENGINE: provider-first + computed fallbacks
# ============================================
def test_engine_provider_first_passthrough():
    from fiscrape_core.metrics_engine import compute_live_metrics

    info = {
        "trailingPE": 19.8,
        "priceToBook": 4.2,
        "priceToSalesTrailing12Months": 3.1,
        "dividendYield": 0.0123,
        "sharesOutstanding": 50_000_000,
    }
    out = compute_live_metrics(100.0, info, extras={})
    assert out["trailingPE"] == pytest.approx(19.8)
    assert out["priceToBook"] == pytest.approx(4.2)
    assert out["priceToSalesTrailing12Months"] == pytest.approx(3.1)
    assert out["priceToSalesTtm"] == pytest.approx(3.1)  # alias
    assert out["dividendYield"] == pytest.approx(0.0123)


@pytest.mark.parametrize(
    "total_rev, expect",
    [
        (10_000.0, 0.5),  # mcap 5,000 / 10,000
        (0.0, 0.0),  # denom <= 0 -> 0 per rule
        (None, None),  # missing denom -> None
    ],
)
def test_engine_ps_fallback_cases(total_rev, expect):
    from fiscrape_core.metrics_engine import compute_live_metrics

    price = 100.0
    info = {"sharesOutstanding": 50.0}  # mcap = 5,000
    extras = {"totalRevenue": total_rev} if total_rev is not None else {}
    out = compute_live_metrics(price, info, extras)
    assert out["priceToSalesTrailing12Months"] == (
        pytest.approx(expect) if expect is not None else None
    )
    assert out["priceToSalesTtm"] == out["priceToSalesTrailing12Months"]


def test_engine_price_to_cash_flow_is_passthrough_from_fundamentals():
    from fiscrape_core.metrics_engine import compute_live_metrics

    out = compute_live_metrics(100.0, info={}, extras={"priceToCashFlowTtm": 12.34})
    assert out["priceToCashFlowTtm"] == pytest.approx(12.34)


def test_engine_52w_helpers_from_active_price():
    from fiscrape_core.metrics_engine import compute_live_metrics

    out = compute_live_metrics(
        110.0, {"fiftyTwoWeekLow": 100.0, "fiftyTwoWeekHigh": 200.0}, extras={}
    )
    assert out["activeOver52wLow"] == pytest.approx(1.10)
    assert out["activeOver52wHigh"] == pytest.approx(0.55)
    assert out["offHighPct"] == pytest.approx(-0.45)
    assert out["positionInRange"] == pytest.approx((110.0 - 100.0) / (200.0 - 100.0))


def test_engine_nav_premium_is_computed():
    from fiscrape_core.metrics_engine import compute_live_metrics

    out = compute_live_metrics(100.0, {"navPrice": 99.0}, extras={})
    assert out["navPremiumPct"] == pytest.approx((100.0 / 99.0) - 1.0)


# =========================
# SNAPSHOT: end-to-end glue
# =========================
@pytest.mark.parametrize(
    "market_state, reg, pre, post, expect_price, expect_src",
    [
        ("PRE", 100.0, 101.0, None, 101.0, "pre"),
        ("POST", 100.0, None, 102.0, 102.0, "post"),
        ("REGULAR", 100.0, 99.0, 103.0, 100.0, "regular"),
    ],
)
def test_snapshot_active_price_selection(
    monkeypatch, market_state, reg, pre, post, expect_price, expect_src
):
    def fake_get_live_quote(sym, quote_bucket=None):
        now = 1_700_000_000
        return {
            "marketState": market_state,
            "regularMarketPrice": reg,
            "regularMarketTime": now,
            "preMarketPrice": pre,
            "preMarketTime": now - 100 if pre is not None else None,
            "postMarketPrice": post,
            "postMarketTime": now + 100 if post is not None else None,
            "regularMarketPreviousClose": 99.5,
        }

    def fake_get_history(sym, period, interval, hist_bucket=None):
        return _mk_intraday()

    import fiscrape_core.snapshot as technical

    monkeypatch.setattr(technical, "get_live_quote", fake_get_live_quote)
    monkeypatch.setattr(technical, "get_history", fake_get_history)

    metrics, hist, info = technical.TechnicalAnalyzer.snapshot(
        "FAKE", "1d", "1m", quote_bucket=0, hist_bucket=0, extras={}
    )
    assert metrics.get("price") == pytest.approx(expect_price)
    assert metrics.get("activePriceSource") == expect_src


def test_snapshot_gapfills_info_from_engine(monkeypatch):
    """Provider omits ratios; snapshot should gap-fill from engine outputs into info."""

    def fake_get_live_quote(sym, quote_bucket=None):
        return {
            "marketState": "REGULAR",
            "regularMarketPrice": 100.0,
            "regularMarketTime": 1_700_000_000,
            "regularMarketPreviousClose": 100.0,
            # No provider P/S or P/B, etc.
            "sharesOutstanding": 1_000.0,
        }

    def fake_get_history(sym, period, interval, hist_bucket=None):
        return _mk_intraday()

    import fiscrape_core.snapshot as technical

    monkeypatch.setattr(technical, "get_live_quote", fake_get_live_quote)
    monkeypatch.setattr(technical, "get_history", fake_get_history)

    # Provide totalRevenue so engine can compute P/S; bookValue missing → PB None
    extras = {"totalRevenue": 200_000.0}
    metrics, hist, info = technical.TechnicalAnalyzer.snapshot(
        "SYM", "1d", "1m", quote_bucket=0, hist_bucket=0, extras=extras
    )

    # engine computed: marketCap = 100 * 1,000 = 100,000; PS = 100,000 / 200,000 = 0.5
    assert metrics["marketCap"] == pytest.approx(100_000.0)
    assert info.get("priceToSalesTtm") == pytest.approx(0.5)
    assert info.get("priceToBook") is None  # no bookValue to compute fallback


def test_snapshot_volume_baseline_ladder(monkeypatch):
    """3mo → 10d → history ladder should produce a baseline label and vs-avg %."""

    # Case: no 3m avg, but 10d avg present
    def fake_get_live_quote(sym, quote_bucket=None):
        return {
            "marketState": "REGULAR",
            "regularMarketPrice": 100.0,
            "regularMarketTime": 1_700_000_000,
            "regularMarketPreviousClose": 99.0,
            "regularMarketVolume": 1_000_000,
            "averageDailyVolume10Day": 2_000_000,
        }

    def fake_get_history(sym, period, interval, hist_bucket=None):
        # Provide daily-only baseline for the last resort branch, too
        if period == "10d" and interval == "1d":
            idx = pd.date_range("2025-01-01", periods=10, freq="D", tz="UTC")
            return pd.DataFrame(
                {"Close": np.linspace(99, 101, 10), "Volume": np.linspace(900_000, 1_100_000, 10)},
                index=idx,
            )
        return _mk_intraday()

    import fiscrape_core.snapshot as technical

    monkeypatch.setattr(technical, "get_live_quote", fake_get_live_quote)
    monkeypatch.setattr(technical, "get_history", fake_get_history)

    metrics, hist, info = technical.TechnicalAnalyzer.snapshot(
        "FAKE", "1d", "1m", quote_bucket=0, hist_bucket=0, extras={}
    )
    assert metrics["volumeBaselineLabel"] in {"10-day avg", "3-month avg"}
    assert isinstance(metrics["volumeVsAvgPct"], float)


def test_snapshot_etf_helpers_end_to_end(monkeypatch):
    """ETF premium & fund assets estimate should appear end-to-end."""

    def fake_get_live_quote(sym, quote_bucket=None):
        return {
            "marketState": "REGULAR",
            "regularMarketPrice": 100.0,
            "regularMarketTime": 1_700_000_000,
            "navPrice": 99.0,
            "netAssets": None,
            "sharesOutstanding": 1_000.0,
            "regularMarketPreviousClose": 100.0,
        }

    def fake_get_history(sym, period, interval, hist_bucket=None):
        return _mk_intraday()

    import fiscrape_core.snapshot as technical

    monkeypatch.setattr(technical, "get_live_quote", fake_get_live_quote)
    monkeypatch.setattr(technical, "get_history", fake_get_history)

    metrics, hist, info = technical.TechnicalAnalyzer.snapshot(
        "ETF", "1d", "1m", quote_bucket=0, hist_bucket=0, extras={}
    )
    assert metrics["fundAssetsEst"] == pytest.approx(100.0 * 1_000.0)
    assert info["navPremiumPct"] == pytest.approx((100.0 / 99.0) - 1.0, rel=1e-6)
