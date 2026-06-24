# fiscrape_core/fundamentals.py
"""Statement-based fundamentals analyzer (stable metrics only)

Reads statement DataFrames from ScrapedTicker (or via services fallback) and computes
camelCase scalars: margins, ROA/ROE/ROIC, leverage, liquidity, coverage, and growth.
Writes results back to ScrapedTicker. Avoids live/price-derived calculations.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from fiscrape_core.math_tools import safe_ratio
from fiscrape_core.services import get_financial_statements
from fiscrape_logger import logger


class FundamentalsAnalyzer:
    @staticmethod
    def _search_parameter(
        df: pd.DataFrame | None,
        parameters: str | list[str],
        output_column: int | slice,
        mode: str | None = "sum",
    ) -> Any:
        """
        If output_column is an int → returns a scalar from that position.
        If it's a slice → returns a Series; when mode is provided, aggregate it.
        """
        if df is None or df.empty:
            return None
        if isinstance(parameters, str):
            parameters = [parameters]
        results: list[Any] = []
        for p in parameters:
            if p not in df.index:
                continue
            row = df.loc[p]
            val = row.iloc[output_column]
            # Only aggregate genuine sequences (Series/list/tuple), not strings/scalars.
            if mode is not None and isinstance(val, (pd.Series, list, tuple)):
                m = mode.lower()
                if m == "sum":
                    val = pd.Series(val).sum()
                elif m in ("avg", "average"):
                    val = pd.Series(val).mean()
                elif m in ("sd", "std_dev", "standard_deviation"):
                    val = pd.Series(val).std()
            results.append(val)
        if not results:
            return None
        return results[0] if len(results) == 1 else results

    @staticmethod
    def _compute_growth_percent(
        current_value: float | None,
        previous_value: float | None,
        years: int = 1,
    ) -> dict[str, Any] | None:
        """
        Return {"growth": <percent float>, "sign": <transition str>} with NO rounding.
        - years == 1 -> simple % change
        - years > 1  -> CAGR
        """
        if years <= 0:
            return None
        if current_value is None or previous_value in (None, 0):
            return None

        # sign transition
        if current_value * previous_value < 0:
            sign = "negative-to-positive" if current_value > 0 else "positive-to-negative"
            curr_abs = abs(float(current_value))
            prev_abs = abs(float(previous_value))
        else:
            if current_value > 0 and previous_value > 0:
                sign = "positive-to-positive"
            elif current_value < 0 and previous_value < 0:
                sign = "negative-to-negative"
            else:
                sign = ""
            curr_abs = float(current_value)
            prev_abs = float(previous_value)

        try:
            if years > 1:
                growth_pct = ((curr_abs / prev_abs) ** (1.0 / years) - 1.0) * 100.0
            else:
                growth_pct = ((curr_abs - prev_abs) / prev_abs) * 100.0
            return {"growth": growth_pct, "sign": sign}
        except Exception:
            return None

    def analyze(self, portfolio: Any, period: str = "yearly") -> None:
        """
        Populate ScrapedTicker attributes:
          - Equities: fetch YF statements via Services and run existing computations.
        Uses Yahoo-native keys end-to-end.
        """
        tickers: list[str] = list(getattr(portfolio, "tickers", []))
        instances: dict[str, Any] = getattr(portfolio, "ticker_instances", {}) or {}

        logger.info(f"[Fundamentals] Analyzing {len(tickers)} tickers; period={period}")

        def _iter_portfolio(pf):
            """
            Yield (symbol, inst, allow_service_fallback).
            - list/tuple: iterate instances, no network fallback (offline-friendly)
            - dict: iterate items, no network fallback
            - portfolio-like: uses pf.tickers + (pf.ticker_instances or pf.instances), allows fallback
            """
            # list/tuple of instances
            if isinstance(pf, (list, tuple)):
                for i, inst in enumerate(pf):
                    sym = getattr(inst, "ticker", None) or getattr(inst, "symbol", None) or f"#{i}"
                    yield sym, inst, False
                return

            # dict mapping symbol -> instance
            if isinstance(pf, dict):
                for sym, inst in pf.items():
                    yield str(sym), inst, False
                return

            # portfolio-like object
            tickers = list(getattr(pf, "tickers", [])) or list(getattr(pf, "symbols", []))
            inst_map = getattr(pf, "ticker_instances", None) or getattr(pf, "instances", None) or {}
            for sym in tickers or list(getattr(inst_map, "keys", lambda: [])()):
                inst = inst_map[sym] if isinstance(inst_map, dict) and sym in inst_map else None
                if inst is not None:
                    yield sym, inst, True

        yearly: bool = period == "yearly"
        col: int | slice = 0 if yearly else slice(0, 4)

        # ... inside FundamentalsAnalyzer.analyze(...)

        for sym, inst, _allow_services in _iter_portfolio(portfolio):
            info = getattr(inst, "ticker_info", {}) or {}
            qt = str(info.get("quoteType") or "").upper()

            # --- ETF path: no equity fundamentals; skip entirely BEFORE fetching statements ---
            if qt == "ETF":
                logger.info(f"{sym}: ETF detected → skipping equity fundamentals.")
                continue

            yearly = period == "yearly"

            # --- Equity path: prefer statements already attached to the instance; fallback to Services ---
            def _get_first_not_none(obj, *names):
                for n in names:
                    if hasattr(obj, n):
                        v = getattr(obj, n)
                        if v is not None:
                            return v
                return None

            inc = _get_first_not_none(inst, "df_income_stmt", "income_stmt")
            bal = _get_first_not_none(inst, "df_balancesheet", "balance_sheet")
            cfs = _get_first_not_none(inst, "df_cashflow", "cashflow")
            qinc = _get_first_not_none(inst, "df_quarterly_income_stmt", "quarterly_income_stmt")
            qbal = _get_first_not_none(inst, "df_quarterly_balancesheet", "quarterly_balance_sheet")
            qcfs = _get_first_not_none(inst, "df_quarterly_cashflow", "quarterly_cashflow")

            def _empty(df):
                return not isinstance(df, pd.DataFrame) or df.empty

            # If everything is empty on the instance, fall back to Services once
            if all(_empty(df) for df in (inc, bal, cfs, qinc, qbal, qcfs)):
                if _allow_services and isinstance(sym, str) and sym and sym.isascii():
                    st = get_financial_statements(sym)
                    inc = st.get("income")
                    bal = st.get("balance")
                    cfs = st.get("cashflow")
                    qinc = st.get("q_income")
                    qbal = st.get("q_balance")
                    qcfs = st.get("q_cashflow")

            if all(_empty(df) for df in (inc, bal, cfs)):
                logger.warning(f"{sym}: No financials found; skipping equity fundamentals.")
                continue

            # Yearly vs quarterly source selection
            src_inc = inc if yearly else qinc
            src_bal = bal if yearly else qbal
            src_cfs = cfs if yearly else qcfs

            # scalars from profile-like info
            trailingEps = info.get("trailingEps") or info.get("epsTrailingTwelveMonths")
            bookValue = info.get("bookValue")
            totalRevenue = info.get("totalRevenue")
            sharesOutstanding = info.get("sharesOutstanding")
            impliedSharesOutstanding = info.get("impliedSharesOutstanding")
            floatShares = info.get("floatShares")
            ebitda = info.get("ebitda")
            freeCashFlow = info.get("freeCashFlow")

            # statement-derived (current period / last 4 quarters)
            dilutedEps = self._search_parameter(src_inc, "Diluted EPS", col)
            operatingCashFlow = self._search_parameter(src_cfs, "Operating Cash Flow", col)
            operatingIncome = self._search_parameter(src_inc, "Operating Income", col)
            netIncome = self._search_parameter(src_inc, "Net Income", col)
            ebit = self._search_parameter(src_inc, "EBIT", col)
            interestExpense = self._search_parameter(src_inc, "Interest Expense", col)
            taxProvision = self._search_parameter(src_inc, "Tax Provision", col)
            investedCapital = self._search_parameter(src_bal, "Invested Capital", col)

            # ---------- Valuation fallbacks (provider-missing; use REGULAR session price) ----------
            # Rule: prefer Yahoo's own ratios; compute here only if provider value is missing/None.
            # Denominator <= 0 -> 0. Missing numerators -> None.
            reg_price = info.get("regularMarketPrice")
            sh = sharesOutstanding or impliedSharesOutstanding or floatShares

            # P/E (trailing) fallback: regular price / EPS TTM
            trailingPE_calc = (
                None if info.get("trailingPE") is not None else safe_ratio(reg_price, trailingEps)
            )

            # P/B fallback: regular price / book value per share
            priceToBook_calc = (
                None if info.get("priceToBook") is not None else safe_ratio(reg_price, bookValue)
            )

            # P/S (TTM) fallback: (regular price * shares) / revenue TTM
            priceToSales_calc = None
            if info.get("priceToSalesTrailing12Months") is None:
                try:
                    mc_num = (
                        float(reg_price) * float(sh)
                        if reg_price is not None and sh is not None
                        else None
                    )
                except Exception:
                    mc_num = None
                priceToSales_calc = safe_ratio(
                    mc_num, totalRevenue
                )  # uses Yahoo totalRevenue when available

            # P/CF (TTM) fallback: (regular price * shares) / Operating Cash Flow (TTM)
            # No native Yahoo ratio here; compute when CFO is available.
            priceToCashFlow_calc = None
            try:
                mc_num_cf = (
                    float(reg_price) * float(sh)
                    if reg_price is not None and sh is not None
                    else None
                )
            except Exception:
                mc_num_cf = None
            if operatingCashFlow is not None:
                priceToCashFlow_calc = safe_ratio(mc_num_cf, operatingCashFlow)

            # Growth helper: fetch single prior-period value N steps back (3y or 2y)
            def _prev(df: pd.DataFrame | None, name: str) -> tuple[float | None, int | None]:
                if df is None or df.empty:
                    return None, None
                for idx in (3, 2):
                    try:
                        v = FundamentalsAnalyzer._search_parameter(df, name, idx, mode=None)
                        return (float(v) if v is not None else None), idx
                    except Exception:
                        continue
                return None, None

            totalRevenuePrev, trYears = _prev(inc, "Total Revenue")
            operatingIncomePrev, oiYears = _prev(inc, "Operating Income")
            netIncomePrev, niYears = _prev(inc, "Net Income")
            dilutedEpsPrev, deYears = _prev(inc, "Diluted EPS")
            operatingCfPrev, ocfYears = _prev(cfs, "Operating Cash Flow")

            # Growth calculations
            revenueGrowth = (
                self._compute_growth_percent(totalRevenue, totalRevenuePrev, trYears or 1)
                if totalRevenue is not None and totalRevenuePrev not in (None, 0)
                else None
            )
            operatingIncomeGrowth = (
                self._compute_growth_percent(operatingIncome, operatingIncomePrev, oiYears or 1)
                if operatingIncome is not None and operatingIncomePrev not in (None, 0)
                else None
            )
            netIncomeGrowth = (
                self._compute_growth_percent(netIncome, netIncomePrev, niYears or 1)
                if netIncome is not None and netIncomePrev not in (None, 0)
                else None
            )
            dilutedEpsGrowth = (
                self._compute_growth_percent(dilutedEps, dilutedEpsPrev, deYears or 1)
                if dilutedEps is not None and dilutedEpsPrev not in (None, 0)
                else None
            )
            operatingCashFlowGrowth = (
                self._compute_growth_percent(operatingCashFlow, operatingCfPrev, ocfYears or 1)
                if operatingCashFlow is not None and operatingCfPrev not in (None, 0)
                else None
            )

            # profitability / strength (unrounded)
            if ebit is not None and interestExpense not in (None, 0):
                try:
                    denom = abs(float(interestExpense))
                    interestCoverage = (float(ebit) / denom) if denom else None
                except Exception:
                    interestCoverage = None
            else:
                interestCoverage = None

            if ebit is not None and taxProvision is not None and investedCapital not in (None, 0):
                try:
                    returnOnInvestedCapital = (float(ebit) - float(taxProvision)) / float(
                        investedCapital
                    )
                except Exception:
                    returnOnInvestedCapital = None
            else:
                returnOnInvestedCapital = None

            if ebitda is not None and totalRevenue not in (None, 0):
                try:
                    ebitdaMargins = float(ebitda) / float(totalRevenue)
                except Exception:
                    ebitdaMargins = None
            else:
                ebitdaMargins = None

            if freeCashFlow is not None and totalRevenue not in (None, 0):
                try:
                    freeCashFlowMargins = float(freeCashFlow) / float(totalRevenue)
                except Exception:
                    freeCashFlowMargins = None
            else:
                freeCashFlowMargins = None

            # Attach valuation fallbacks without overriding provider values
            _val = {}
            if trailingPE_calc is not None:
                _val["trailingPE"] = trailingPE_calc
            if priceToBook_calc is not None:
                _val["priceToBook"] = priceToBook_calc
            if priceToSales_calc is not None:
                _val["priceToSalesTrailing12Months"] = priceToSales_calc
            if priceToCashFlow_calc is not None:
                _val["priceToCashFlowTtm"] = priceToCashFlow_calc
            if _val:
                inst.set_attr(**_val)

            # export (camelCase keys for outputs)
            inst.set_attr(
                interestCoverage=interestCoverage,
                returnOnInvestedCapital=returnOnInvestedCapital,
                ebitdaMargins=ebitdaMargins,
                freeCashFlowMargins=freeCashFlowMargins,
                revenueGrowth=revenueGrowth,
                operatingIncomeGrowth=operatingIncomeGrowth,
                netIncomeGrowth=netIncomeGrowth,
                dilutedEpsGrowth=dilutedEpsGrowth,
                operatingCashFlowGrowth=operatingCashFlowGrowth,
            )
