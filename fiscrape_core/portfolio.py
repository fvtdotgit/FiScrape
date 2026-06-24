# fiscrape_core/portfolio.py
"""Portfolio containers and ticker wrapper (no network)

Defines:
- ScrapedTicker: yfinance.Ticker subclass with set_attr/get_attr and warnings plumbing.
- Portfolio: container for tickers, tags, and scrape orchestration (no history fetch).
Boundary: do not talk to the network here beyond calling services.* helpers.
"""

from __future__ import annotations

import concurrent.futures
import time
from typing import Any

import pandas as pd
import requests
import yfinance as yf

from fiscrape_logger import logger


class ScrapedTicker(yf.Ticker):
    """
    A subclass of yfinance's Ticker that adds custom functionality for setting and retrieving
    financial attributes.

    Methods:
        set_attr(**kwargs)
            Sets financial attributes on this ticker instance.
        get_attr(key: str = None)
            Retrieves a specific financial attribute if a key is provided,
            or returns a dictionary of all non-internal, non-callable attributes if no key is provided.
    """

    _warnings: list[dict[str, Any]] = []

    def add_warning(
        self, code: str, message: str, *, where: str, context: dict[str, Any] | None = None
    ) -> None:
        if not hasattr(self, "_warnings") or self._warnings is None:
            self._warnings = []
        self._warnings.append(
            {"code": code, "message": message, "where": where, "context": context or {}}
        )

    def get_warnings(self) -> list[dict[str, Any]]:
        return list(self._warnings or [])

    def clear_warnings(self) -> None:
        self._warnings = []

    def adopt_service_warnings(self) -> None:
        """
        Pull any pending service-edge warnings (network/history) into this ticker.
        """
        try:
            from .services import pop_service_warnings  # local import to avoid cycles

            sym = getattr(self, "ticker", None) or getattr(self, "symbol", None)
            if not sym:
                return
            for w in pop_service_warnings(sym):
                self.add_warning(
                    w.get("code", "WARN"),
                    w.get("message", ""),
                    where=w.get("where", "services"),
                    context=w.get("context", {}),
                )
        except Exception:
            # Never break pipelines for warnings plumbing
            pass

    def set_attr(self, **kwargs: Any) -> None:
        """
        Sets financial attributes for this ticker instance.

        :param kwargs: Keyword arguments representing financial attributes (e.g., market_cap, net_income).
        :type kwargs: dict
        """
        for key, value in kwargs.items():
            if isinstance(value, pd.DataFrame) and value.empty:
                final_value = None
            else:
                final_value = value
            setattr(self, key, final_value)

    def get_attr(self, search_key: str = None) -> Any | dict[str, Any]:
        """
        Retrieves financial attributes from this ticker instance.

        If a key is provided, returns the value of that attribute (or None if not set).
        If key is None, returns a dictionary of all non-internal and non-callable attributes.

        :param search_key: The attribute name to retrieve. If None, retrieves all non-internal attributes.
        :type search_key: str, optional
        :return: The value of the specified attribute, or a dictionary of attributes if key is None.
        :rtype: Any or dict
        """
        if search_key:
            return getattr(self, search_key, None)
        else:
            return {
                attr: getattr(self, attr)
                for attr in dir(self)
                if not (
                    (
                        attr.startswith("__")
                        or not (attr not in ["basic_info", "earnings", "shares"])
                    )
                    or callable(getattr(self, attr))
                )
            }


class Portfolio:
    """
    A container for a collection of stock tickers and their associated financial data. The Portfolio class is
    responsible for organizing and collecting data on your specified tickers grouped by specific needs.

    This class separates container-level tags (e.g., potential_to_buy, strategy) from ticker-level financial attributes.
    Ticker-level attributes are always applied to the underlying ScrapedTicker objects, which extend yfinance's Ticker.

    Attributes:
        tickers (list): List of ticker symbols.
        ticker_instances (dict): Mapping of ticker symbols (str) to ScrapedTicker objects.
        tags (dict): Container-level tags (e.g., strategic flags, screening information).

    Methods:
        set_tag(**kwargs)
            Sets container-level tags.
        get_tag(key: str = None)
            Retrieves a specific tag or all tags if key is None.
        clear_tag(key: str = None)
            Clears a specific tag or clears all tags if key is None.
        get_attr(key: str = None)
            Retrieves a specific financial attribute from each ticker object, or all attributes if key is None.
        scrape(tickers)
            Scrapes multiple tickers and stores their ScrapedTicker objects.
    """

    def __init__(self) -> None:
        """
        Initializes the Portfolio container.

        Creates an empty container with no ticker symbols, ticker instances, or container-level tags.
        """
        self.tickers: list[str] = []  # List of ticker symbols
        self.ticker_instances: dict[
            str, ScrapedTicker
        ] = {}  # Mapping: ticker symbol -> ScrapedTicker object
        self.tags: dict[str, Any] = {}  # Container-level tags as a dictionary

    # Container-level tag methods
    def set_tag(self, **kwargs: Any) -> None:
        """
        Sets container-level tags (strategic metadata).

        Example:
            portfolio.set_tag(potential_to_buy="yes", strategy="value")

        :param kwargs: Keyword arguments representing container-level tags.
        :type kwargs: dict
        """
        for key, value in kwargs.items():
            final_value = None if value is None or pd.isna(value) else value
            self.tags[key] = final_value
            setattr(self, key, final_value)

    def get_tag(self, search_key: str | None = None) -> Any | dict[str, Any]:
        """
        Retrieves container-level tags.

        :param search_key: If provided, returns the value for that specific tag;
        if None, returns the complete tags dictionary.
        :type search_key: str, optional
        :return: The value of the specified tag or the complete tag's dictionary.
        :rtype: any or dict
        """
        if search_key:
            return self.tags.get(search_key, None)
        else:
            return self.tags

    def clear_tag(self, search_key: str | None = None) -> None:
        """
        Clears container-level tags.

        :param search_key: If provided, clears the specified tag; if None, clears all tags.
        :type search_key: str, optional
        """
        if search_key:
            if search_key in self.tags:
                del self.tags[search_key]
            if hasattr(self, search_key):
                delattr(self, search_key)
        else:
            for tag in list(self.tags.keys()):
                if hasattr(self, tag):
                    delattr(self, tag)
            self.tags.clear()

    def get_attr(self, search_key: str | None = None) -> dict[str, Any] | dict[str, dict[str, Any]]:
        """
        Retrieves financial attributes from each underlying ticker object.

        - If 'search_key' is provided, returns a dictionary mapping each ticker symbol to the value of that attribute.
        - If 'search_key' is None, returns a nested dictionary mapping each ticker symbol to a dictionary of all
          non-internal and non-callable attributes.

        :param search_key: The attribute name to retrieve. If None, retrieves all non-internal, non-callable attributes.
        :type search_key: str, optional
        :return: A dictionary mapping ticker symbols to the attribute value or to a dictionary of attributes.
        :rtype: dict
        """
        if not self.ticker_instances:
            return {}
        if search_key is not None:
            return {
                ticker: getattr(ticker_obj, search_key, None)
                for ticker, ticker_obj in self.ticker_instances.items()
            }
        else:
            return {
                ticker: {
                    attr: getattr(ticker_obj, attr)
                    for attr in dir(ticker_obj)
                    if not attr.startswith("__") and not callable(getattr(ticker_obj, attr))
                }
                for ticker, ticker_obj in self.ticker_instances.items()
            }

    def scrape(self, tickers: list[str], parallel: bool = True, max_workers: int = 5) -> None:
        """
        Scrapes multiple tickers and creates corresponding ScrapedTicker objects.

        Each ticker symbol is added to the container, and a ScrapedTicker object is created for it.
        If an error occurs while creating a ticker object, it is logged and skipped.

        When parallel is True, ticker scraping is executed concurrently using a ThreadPoolExecutor.
        Otherwise, tickers are scraped sequentially.

        :param tickers: A list of ticker symbols to scrape.
        :type tickers: list of str
        :param parallel: Whether to run scraping in parallel (default True).
        :type parallel: bool
        :param max_workers: The maximum number of threads to use concurrently (only if parallel is True).
        :type max_workers: int
        :return: None
        """
        logger.info(f"Starting scrape process for tickers: {tickers}")
        self.tickers = tickers

        def scrape_ticker(ticker: str) -> tuple[str, Any]:
            try:
                # Create the ScrapedTicker instance for this ticker.
                ticker_instance = ScrapedTicker(ticker)

                # Obtaining information on ticker
                ticker_info = ticker_instance.info

                # Handling case where ticker_info might be incomplete
                if ticker_info is None:
                    raise ValueError(f"Ticker info for {ticker} is missing or incomplete.")

                quote_type = ticker_info.get("quoteType", "").upper()
                is_etf = quote_type == "ETF"

                start_df_scrape = time.time()

                # Commit these DataFrames to the ticker instance using set_attr if equity
                if is_etf:
                    ticker_instance.set_attr(
                        ticker=ticker,
                        ticker_info=ticker_info,
                        df_income_stmt=pd.DataFrame(),
                        df_balancesheet=pd.DataFrame(),
                        df_cashflow=pd.DataFrame(),
                        df_quarterly_income_stmt=pd.DataFrame(),
                        df_quarterly_balancesheet=pd.DataFrame(),
                        df_quarterly_cashflow=pd.DataFrame(),
                    )
                else:
                    ticker_instance.set_attr(
                        ticker=ticker,
                        ticker_info=ticker_info,
                        df_income_stmt=ticker_instance.income_stmt,
                        df_balancesheet=ticker_instance.balancesheet,
                        df_cashflow=ticker_instance.cashflow,
                        df_quarterly_income_stmt=ticker_instance.quarterly_income_stmt,
                        df_quarterly_balancesheet=ticker_instance.quarterly_balancesheet,
                        df_quarterly_cashflow=ticker_instance.quarterly_cashflow,
                    )

                # Attach any service-edge warnings collected during this scrape
                try:
                    ticker_instance.adopt_service_warnings()
                except Exception:
                    pass

                end_df_scrape = time.time()

                logger.info(
                    f"Scraped DataFrames for {ticker} in {end_df_scrape - start_df_scrape:.2f} seconds"
                )

                return ticker, ticker_instance
            except requests.exceptions.RequestException as req_exc:
                logger.error(f"Network error occurred while scraping {ticker}: {req_exc}")
                return ticker, None
            except ValueError as val_exc:
                logger.error(f"Value error while processing ticker {ticker}: {val_exc}")
                return ticker, None
            except TypeError as type_exc:
                logger.error(f"Type error while processing ticker {ticker}: {type_exc}")
                return ticker, None
            except Exception as exc:
                logger.error(f"Unexpected error while scraping {ticker}: {exc}")
                return ticker, None

        if parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_ticker = {
                    executor.submit(scrape_ticker, ticker): ticker for ticker in tickers
                }
                for future in concurrent.futures.as_completed(future_to_ticker):
                    ticker, scraped_obj = future.result()
                    if scraped_obj is not None:
                        self.ticker_instances[ticker] = scraped_obj
                    else:
                        logger.warning(f"Scraping failed for ticker: {ticker}")
            logger.info("Parallel scraping completed.")
        else:
            # Sequential scraping — use the same logic as the parallel worker
            for ticker in tickers:
                try:
                    t, scraped_obj = scrape_ticker(ticker)
                    if scraped_obj is not None:
                        self.ticker_instances[t] = scraped_obj
                    else:
                        logger.warning(f"Scraping failed for ticker: {ticker}")
                except Exception as e:
                    logger.error(f"Error scraping ticker {ticker}: {e}")
            logger.info("Sequential scraping completed.")
