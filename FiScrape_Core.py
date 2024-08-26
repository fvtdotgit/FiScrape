"""
This module provides tools for scraping and analyzing financial data from Yahoo Finance. It utilizes Selenium for
web scraping, multiprocessing for concurrent processing of multiple tickers, and pandas for data handling.

Modules and classes:
- Driver: Manages the creation of Selenium WebDriver instances.
- Ticker: Stores and handles attributes related to individual stock tickers.
- Scraper: Manages the web scraping of financial data.
- Analyzer: Provides methods for analyzing the scraped financial data.
- Compiler: Handles the compilation and visualization of analyzed data.
- Exporter: Exports the analyzed data to CSV files.
"""

import pandas as pd
import random
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
import multiprocessing
from multiprocessing import Manager
import os

from selenium.common import TimeoutException, WebDriverException

from fiscrape_logger import logger
from itertools import chain
from math import floor

import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

# Setting options for pandas
pd.options.display.float_format = '{:.0f}'.format
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.option_context('display.precision', 5)


class Driver:
    """
    A utility class for managing the creation and configuration of Selenium WebDriver instances.

    Methods:
    - create_driver(head=None): Creates and returns a Selenium WebDriver instance with optional headless mode.

    """
    @staticmethod
    def create_driver(head=None):
        """
        Creates and returns a Selenium WebDriver instance configured for Firefox.

        :param head: If set to None, the WebDriver runs in headless mode (without opening a browser window).
                     Any other value will run the WebDriver with the browser window visible.
        :type head: any, optional
        :return: A configured Selenium WebDriver instance.
        :rtype: webdriver.Firefox
        """
        opt = webdriver.FirefoxOptions()
        if not head:
            opt.add_argument('-headless')
        driver = webdriver.Firefox(options=opt)  # Create headless driver
        driver.delete_all_cookies()

        return driver


class Ticker:
    """
    Represents a stock ticker with associated attributes and URLs for scraping financial data from Yahoo Finance.

    Attributes:
    - ticker: The stock ticker symbol (e.g., 'AAPL' for Apple Inc.).
    - data: A dictionary storing additional attributes and values related to the ticker.
    - summary_link: URL for the summary page of the ticker on Yahoo Finance.
    - statistics_link: URL for the statistics page of the ticker on Yahoo Finance.
    - fs_link: List of URLs for the financial statement pages (income statement, balance sheet, cash flow) of the
    ticker.
    - profile_link: URL for the profile page of the ticker on Yahoo Finance.
    - holders_link: URL for the major holders page of the ticker on Yahoo Finance.
    - insider_roster_link: URL for the insider roster page of the ticker on Yahoo Finance.
    - insider_transactions_link: URL for the insider transactions page of the ticker on Yahoo Finance.

    Methods:
    - set_attr(**kwargs): Sets attributes for the ticker based on keyword arguments.
    - get_attr(attribute_name): Retrieves the value of a specified attribute.
    - get_all_attr(): Returns a dictionary of all attributes for the ticker.
    """
    def __init__(self, ticker, **kwargs):
        """
        Initializes a Ticker instance with the provided stock ticker symbol and additional attributes.

        :param ticker: The stock ticker symbol (e.g., 'AAPL' for Apple Inc.).
        :type ticker: str
        :param kwargs: Additional attributes to set for the ticker, provided as keyword arguments.
        :type kwargs: dict
        """
        self.ticker = ticker
        self.set_attr(**kwargs)
        self.data = kwargs

        self.summary_link = f'https://finance.yahoo.com/quote/{self.ticker}'
        self.statistics_link = f'https://finance.yahoo.com/quote/{self.ticker}/key-statistics'
        self.fs_link = [f'https://finance.yahoo.com/quote/{self.ticker}/{document}?p={self.ticker}'
                        for document in ['financials', 'balance-sheet', 'cash-flow']]
        self.profile_link = f'https://finance.yahoo.com/quote/{self.ticker}/profile/'
        self.holders_link = f'https://finance.yahoo.com/quote/{self.ticker}/holders/'
        self.insider_roster_link = f'https://finance.yahoo.com/quote/{self.ticker}/insider-roster/'
        self.insider_transactions_link = f'https://finance.yahoo.com/quote/{self.ticker}/insider-transactions/'

    def set_attr(self, **kwargs):
        """
        Sets attributes for the ticker based on the provided keyword arguments. If the value is None or an invalid
        string, the attribute is set to None.

        :param kwargs: Attributes to set, provided as keyword arguments (e.g., market_cap=1000000).
        :type kwargs: dict
        """
        for key, value in kwargs.items():
            if value is None or (isinstance(value, str) and value in ['--', '-- ', '---']):
                setattr(self, key, None)
            else:
                setattr(self, key, value)

    def get_attr(self, attribute_name):
        """
        Retrieves the value of a specified attribute for the ticker.

        :param attribute_name: The name of the attribute to retrieve.
        :type attribute_name: str
        :return: The value of the specified attribute, or None if it doesn't exist.
        :rtype: any
        """
        return getattr(self, attribute_name, None)

    def get_all_attr(self):
        """
        Returns a dictionary of all attributes for the ticker, excluding internal methods and predefined attributes.

        :return: A dictionary containing all attributes of the ticker.
        :rtype: dict
        """
        for attr in dir(self):
            if not attr.startswith('__') and attr not in ['set_attr', 'get_attr', 'data', 'get_all_attr',
                                                          'summary_link', 'statistics_link', 'fs_link', 'profile_link',
                                                          'holders_link']:
                self.data.update({attr: getattr(self, attr)})
        return self.data


class Scraper:
    """
    Manages the process of scraping financial data from Yahoo Finance for multiple stock tickers.

    This class is designed to handle the complexities of web scraping by managing retries, handling
    different versions of the web pages, and ensuring resource management by controlling the lifecycle
    of Selenium WebDriver instances. By keeping the same WebDriver instance active during the scraping
    process (mostly for fundamentals), the class minimizes the risk of older versions of the web page being loaded,
    which can interfere with the scraping process. This approach also helps in preventing memory leaks by
    ensuring that WebDriver instances are properly closed after use.

    Attributes:
    - ticker_instances: A dictionary that stores all ticker data important for calculations and documentation.
    - sleep_time: The time to sleep between actions to mimic human behavior and avoid detection.
    - retries: The maximum number of retries allowed for loading pages.
    - max_click_retries: The maximum number of retries allowed for clicking elements on the page.
    - se_version_indicator: The HTML element tag used to identify the version of the page.
    - se_class_version_indicator: The class attribute associated with the version indicator element.
    - indicator_text: The text content that confirms the correct version of the page is loaded.
    - Various other attributes related to the HTML structure of Yahoo Finance pages.

    Methods:
    - load_and_check_version(url, driver, ticker): Loads a URL and checks if the correct version of the page is loaded.
    - find_expand_all_button(driver, ticker): Attempts to find and click the 'Expand All' button on financial pages.
    - obtain_recommendation(recommended_ticker, number_of_recommendations=3, head=None): Obtains stock recommendations.
    - fundamentals(ticker, shared_dict, lock, head=None): Scrapes fundamental financial data for a given ticker.
    - profile(ticker, shared_dict, lock, head=None): Scrapes profile data for a given ticker.
    - holders(ticker, shared_dict, lock, head=None): Scrapes holders data for a given ticker.
    - insider_transactions(ticker, shared_dict, lock, head=None): Scrapes insider transactions data for a given ticker.
    - scrape(ticker_string, target='fundamentals', max_processes_capacity=1): Scrapes data for multiple tickers
    using multiprocessing.
    """
    def __init__(self,
                 sleep_time=random.uniform(0.5, 1.5),
                 retries=10,
                 max_click_retries=10,
                 # Load and check
                 se_version_indicator='a',
                 se_class_version_indicator='rapid-noclick-resp opt-in-link',
                 indicator_text='\nBack to classic\n\n\n\n\n',
                 # Recommender
                 se_ticker_and_name='a',
                 se_class_ticker_and_name='loud-link fin-size-large yf-13p9sh2',
                 se_ticker='span',
                 # Summary page
                 se_name='div',
                 se_class_name='longName yf-15b2o7n',
                 se_price='fin-streamer',
                 se_class_price='livePrice yf-mgkamr',
                 se_change='fin-streamer',
                 se_class_change='priceChange yf-mgkamr',
                 se_summary_label='span',
                 se_class_summary_label='label yf-tx3nkj',
                 se_summary_content='span',
                 se_class_summary_content='value yf-tx3nkj',
                 # Statistics page
                 se_statistics_valuation_table_header='th',
                 se_class_statistics_valuation_table_header='yf-104jbnt',
                 se_statistics_valuation_table_row='tr',
                 se_class_statistics_valuation_table_row='yf-104jbnt',
                 se_statistics_valuation_table_column='td',
                 se_class_statistics_valuation_table_column='yf-104jbnt',
                 se_statistics_hgl_n_info_header='header',  # Unused
                 se_class_statistics_hgl_n_info_header='yf-13ievhf',  # Unused
                 se_statistics_hgl_n_info_content='table',  # Unused
                 se_class_statistics_hgl_n_info_content='table yf-vaowmx',  # Unused
                 se_statistics_hgl_n_info_row='tr',
                 se_class_statistics_hgl_n_info_row='row yf-vaowmx',
                 se_statistics_hgl_n_info_column='td',
                 se_class_statistics_hgl_n_info_column='yf-vaowmx',
                 # Financials page(s)
                 expand_all_button_xpath='//*[@id="nimbus-app"]/section/section/section/article/article/div/div['
                                         '2]/button/span',
                 se_financials_header_row='div',
                 se_class_financials_header_row='row yf-1ezv2n5',
                 se_financials_header_column='div',
                 se_class_financials_header_column='column yf-1ezv2n5',  # Unused
                 se_financials_content_row='div',
                 se_class_financials_content_row='.row.lv-0.yf-1xjz32c, .row.lv-1.yf-1xjz32c, '
                                                 '.row.lv-2.yf-1xjz32c, .row.lv-3.yf-1xjz32c, '
                                                 '.row.lv-4.yf-1xjz32c',
                 se_financials_content_column='div',
                 se_class_financials_content_column='yf-1xjz32c',
                 # Profile
                 se_sector_and_industry='a',
                 se_class_sector_and_industry='subtle-link fin-size-large yf-13p9sh2',
                 se_employees='dd',
                 se_class_employees='',  # Unused
                 se_profile_header_row='th',
                 se_class_profile_header_row='yf-mj92za',
                 se_profile_header_column='th',
                 se_class_profile_header_column='yf-mj92za',
                 se_profile_content_row='tr',  # Unused
                 se_class_profile_content_row='yf-mj92za',
                 se_profile_content_column='td',
                 se_class_profile_content_column='yf-mj92za',
                 # Holders
                 se_major_holders='td',
                 se_class_major_holders='majorHolders yf-1toamfi',
                 # Insider transaction
                 se_insider_purchase_row='tr',
                 se_class_insider_purchase_row='yf-1toamfi',
                 se_insider_purchase_header_cell='th',
                 se_class_insider_purchase_header_cell='yf-1toamfi',
                 se_insider_purchase_cell='td',
                 se_class_insider_purchase_cell='yf-1toamfi'
                 ):
        self.ticker_instances = {}  # Will contain all ticker data important for calculations and documentation
        self.sleep_time = sleep_time
        self.retries = retries
        self.max_click_retries = max_click_retries
        # Load and check
        self.se_version_indicator = se_version_indicator
        self.se_class_version_indicator = se_class_version_indicator
        self.indicator_text = indicator_text
        # Recommender
        self.se_ticker_and_name = se_ticker_and_name
        self.se_class_ticker_and_name = se_class_ticker_and_name
        self.se_ticker = se_ticker
        # Summary page
        self.se_name = se_name
        self.se_class_name = se_class_name
        self.se_price = se_price
        self.se_class_price = se_class_price
        self.se_change = se_change
        self.se_class_change = se_class_change
        self.se_summary_label = se_summary_label
        self.se_class_summary_label = se_class_summary_label
        self.se_summary_content = se_summary_content
        self.se_class_summary_content = se_class_summary_content
        # Statistics page
        self.se_statistics_valuation_table_header = se_statistics_valuation_table_header
        self.se_class_statistics_valuation_table_header = se_class_statistics_valuation_table_header
        self.se_statistics_valuation_table_row = se_statistics_valuation_table_row
        self.se_class_statistics_valuation_table_row = se_class_statistics_valuation_table_row
        self.se_statistics_valuation_table_column = se_statistics_valuation_table_column
        self.se_class_statistics_valuation_table_column = se_class_statistics_valuation_table_column
        self.se_statistics_hgl_n_info_header = se_statistics_hgl_n_info_header
        self.se_class_statistics_hgl_n_info_header = se_class_statistics_hgl_n_info_header
        self.se_statistics_hgl_n_info_content = se_statistics_hgl_n_info_content
        self.se_class_statistics_hgl_n_info_content = se_class_statistics_hgl_n_info_content
        self.se_statistics_hgl_n_info_row = se_statistics_hgl_n_info_row
        self.se_class_statistics_hgl_n_info_row = se_class_statistics_hgl_n_info_row
        self.se_statistics_hgl_n_info_column = se_statistics_hgl_n_info_column
        self.se_class_statistics_hgl_n_info_column = se_class_statistics_hgl_n_info_column
        # Financials page(s)
        self.expand_all_button_xpath = expand_all_button_xpath
        self.se_financials_header_row = se_financials_header_row
        self.se_class_financials_header_row = se_class_financials_header_row
        self.se_financials_header_column = se_financials_header_column
        self.se_class_financials_header_column = se_class_financials_header_column
        self.se_financials_content_row = se_financials_content_row
        self.se_class_financials_content_row = se_class_financials_content_row
        self.se_financials_content_column = se_financials_content_column
        self.se_class_financials_content_column = se_class_financials_content_column
        self.se_sector_and_industry = se_sector_and_industry
        # Profile
        self.se_class_sector_and_industry = se_class_sector_and_industry
        self.se_employees = se_employees
        self.se_class_employees = se_class_employees
        self.se_profile_header_row = se_profile_header_row
        self.se_class_profile_header_row = se_class_profile_header_row
        self.se_profile_header_column = se_profile_header_column
        self.se_class_profile_header_column = se_class_profile_header_column
        self.se_profile_content_row = se_profile_content_row
        self.se_class_profile_content_row = se_class_profile_content_row
        self.se_profile_content_column = se_profile_content_column
        self.se_class_profile_content_column = se_class_profile_content_column
        # Holders
        self.se_major_holders = se_major_holders
        self.se_class_major_holders = se_class_major_holders
        # Insider transaction
        self.se_insider_purchase_row = se_insider_purchase_row
        self.se_class_insider_purchase_row = se_class_insider_purchase_row
        self.se_insider_purchase_header_cell = se_insider_purchase_header_cell
        self.se_class_insider_purchase_header_cell = se_class_insider_purchase_header_cell
        self.se_insider_purchase_cell = se_insider_purchase_cell
        self.se_class_insider_purchase_cell = se_class_insider_purchase_cell
        self.lock = multiprocessing.Lock()  # Lock for thread safety

    def request(self, url, headers=None):
        """
        Sends an HTTP GET request to the specified URL and attempts to parse the HTML content using BeautifulSoup.
        The method checks if the correct version of the webpage is loaded based on specific indicators within the HTML.
        If the incorrect version is detected, it will retry the request up to `self.retries` times, with a pause
        between each attempt defined by `self.sleep_time`.

        :param url: The URL to send the GET request to.
        :type url: str
        :param headers: Optional HTTP headers to include in the request. Defaults to a user-agent header mimicking
        a standard browser.
        :type headers: dict, optional
        :return: A BeautifulSoup object representing the parsed HTML content if the correct version is detected.
                 Raises a ValueError if the maximum retries are reached without loading the correct version.
        :rtype: BeautifulSoup or None
        :raises ValueError: If the incorrect version of the webpage is loaded after all retry attempts.
        """
        if headers is None:
            headers = {'User-agent': 'Mozilla/5.0'}

        for attempt in range(self.retries):
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            soup = BeautifulSoup(response.text, 'lxml')

            return soup  # Correct version detected

    def load_and_check_version(self, url, driver, ticker):
        """
        Loads the specified URL in the given WebDriver instance and checks if the correct version of the page is loaded.
        If the page is not correctly loaded, logs an error and returns None.

        This method does not create a separate retry mechanism to ensure that the WebDriver instance is reused
        consistently throughout the scraping process. Reusing the same WebDriver helps in preventing the loading
        of older versions of the web page, which can cause inconsistencies in the scraped data. Moreover, it ensures
        that the WebDriver is properly closed after use, preventing potential memory leaks.

        :param url: The URL to load.
        :type url: str
        :param driver: The WebDriver instance to use for loading the page.
        :type driver: webdriver.Firefox
        :param ticker: The stock ticker symbol associated with the page being loaded.
        :type ticker: str
        :return: A BeautifulSoup object if the correct version of the page is loaded, otherwise None.
        :rtype: BeautifulSoup or None
        """
        try:
            driver.get(url)
            html = driver.execute_script('return document.body.innerHTML;')
            soup = BeautifulSoup(html, 'lxml')

            sleep(float(self.sleep_time))

            # Check for the correct version
            indicator_texts = [entry.text for entry in soup.find_all(
                self.se_version_indicator, class_=self.se_class_version_indicator)]

            if self.indicator_text in indicator_texts:
                return soup  # Correct version detected
            else:
                logger.error(f'{ticker}: Incorrect version detected.')
                return None

        except TimeoutException as te:
            logger.error(f'{ticker}: Timeout while loading the page - {te}.')
            return None

        except WebDriverException as we:
            logger.error(f'{ticker}: WebDriver exception occurred - {we}.')
            return None

        except Exception as e:
            logger.error(f'{ticker}: General error occurred while loading the page - {e}.')
            return None

    def find_expand_all_button(self, driver, ticker):
        """
        Attempts to find and click the 'Expand All' button on the financials page. If the button cannot be clicked
        after the maximum number of retries, logs an error and returns False.

        :param driver: The WebDriver instance to use for finding and clicking the button.
        :type driver: webdriver.Firefox
        :param ticker: The stock ticker symbol associated with the page being interacted with.
        :type ticker: str
        :return: True if the 'Expand All' button was successfully clicked, otherwise False.
        :rtype: bool
        """
        click_retry_count = 0
        logger.info(f"{ticker}: Attempting to find and click the 'Expand All' button.")

        while click_retry_count < self.max_click_retries:
            try:
                # Attempt to locate and click the 'Expand All' button
                expand_all_button = WebDriverWait(driver, 20).until(
                    ec.element_to_be_clickable((By.XPATH, self.expand_all_button_xpath))
                )
                expand_all_button.click()
                logger.info(f"{ticker}: 'Expand All' button clicked successfully.")
                return True  # Success
            except (TimeoutException, WebDriverException) as e:
                click_retry_count += 1
                logger.warning(
                    f"{ticker}: Attempt {click_retry_count} - Failed to click 'Expand All' button due to {e}."
                    f" Retrying...")
                sleep(self.sleep_time)

        logger.error(f"{ticker}: Failed to click the 'Expand All' button after {self.max_click_retries} attempts.")
        return False  # Failed after max retries

    def obtain_recommendation(self, recommended_ticker, number_of_recommendations=3):
        """
        Obtains stock recommendations related to the specified ticker from Yahoo Finance.

        :param recommended_ticker: The stock ticker symbol to obtain recommendations for.
        :type recommended_ticker: str
        :param number_of_recommendations: The number of recommended tickers to scrape.
        :type number_of_recommendations: int, optional
        :return: A string of recommended tickers separated by spaces.
        :rtype: str
        """

        logger.info(f"{recommended_ticker}: Starting to obtain recommendations.")
        try:
            soup = Scraper.request(self, Ticker(recommended_ticker).summary_link)

            recommendation_content = [entry for entry in soup.find_all(self.se_ticker_and_name,
                                                                       class_=self.se_class_ticker_and_name)]

            recommended_ticker_outputs = [[entry.text for entry in
                                           recommendation_content[recommendation_iteration].find_all(self.se_ticker)]
                                          for recommendation_iteration in range(len(recommendation_content))
                                          ][:number_of_recommendations + 1]

            recommended_ticker_outputs = list(chain(*recommended_ticker_outputs))

            ticker_string = ' '.join(recommended_ticker_outputs)
            logger.info(f"{recommended_ticker}: Successfully obtained recommendations: {ticker_string}.")
            return ticker_string

        except Exception as e:
            logger.error(f"{recommended_ticker}: An error occurred during scraping recommendations - {e}.",
                         exc_info=True)

        finally:
            logger.info(f"{recommended_ticker}: Driver closed after obtaining recommendations.")

    def fundamentals(self, ticker, shared_dict, lock, head=None):
        """
        Scrapes fundamental financial data for the specified ticker. The method attempts to load relevant Yahoo
        Finance pages (summary, statistics, financials) and extract data such as price, change, financial ratios,
        and growth metrics. This method encompasses scraping balance sheet, income statement, and cash flow statement.

        The method ensures that the same WebDriver instance scrapes the same ticker during the scraping process.
        This consistent reuse of the WebDriver helps in avoiding issues related to loading older versions of the page.
        Additionally, the method ensures that the WebDriver is properly closed after use to prevent memory leaks.

        :param ticker: The stock ticker symbol to scrape data for.
        :type ticker: str
        :param shared_dict: A shared dictionary to store the scraped data, used in multiprocessing.
        :type shared_dict: multiprocessing.Manager().dict
        :param lock: A lock to synchronize access to shared resources in a multiprocessing environment.
        :type lock: multiprocessing.Lock
        :param head: If set to None, the WebDriver runs in headless mode (without opening a browser window).
                     Any other value will run the WebDriver with the browser window visible.
        :type head: any, optional
        """
        logger.info(f"{ticker}: Starting fundamentals scraping.")
        driver = Driver.create_driver(head)
        try:
            # Initialize variables that might not be available (e.g., index funds)
            df_statistics_valuations = None
            df_statistics_hgl_n_info = None
            df_income_statement = None
            df_balance_sheet = None
            df_cash_flow = None

            logger.info(f"{ticker}: Price, change, and summary scraping initiated.")

            # Try loading the summary page with retries
            for attempt in range(self.retries):
                logger.info(f"{ticker}: Attempt {attempt + 1} to load summary page.")
                soup = self.load_and_check_version(Ticker(ticker).summary_link, driver, ticker)
                if soup is not None:
                    logger.info(f"{ticker}: Successfully loaded the summary page on attempt {attempt + 1}.")
                    break  # Break out of the loop if the correct version is loaded
                logger.warning(f"{ticker}: Failed to load the summary page on attempt {attempt + 1}. Retrying...")
                driver.quit()
                driver = Driver.create_driver(head)  # Create a new driver for the next attempt
            else:
                raise Exception(f'{ticker}: Failed to load the correct summary page after {self.retries} retries.')

            # Real-time price and change (also a good test whether the web version is loaded)
            name = [entry.text for entry in soup.find(self.se_name, class_=self.se_class_name)]  # First one only!
            price = [entry.text for entry in soup.find_all(self.se_price, class_=self.se_class_price)]
            change = [entry.text for entry in soup.find_all(self.se_change, class_=self.se_class_change)]

            change_intraday = (change[0], change[1])
            change_afterhours = (change[2], change[3]) if len(change) > 2 else None

            summary_label = [entry.text for entry in soup.find_all(
                self.se_summary_label, class_=self.se_class_summary_label)]
            summary_content = [entry.text for entry in soup.find_all(
                self.se_summary_content, class_=self.se_class_summary_content)]

            raw_summary_table = [[summary_label[summary_iteration], summary_content[summary_iteration]]
                                 for summary_iteration in range(len(summary_label))]

            df_summary = pd.DataFrame(raw_summary_table)

            logger.info(f"{ticker}: Price, change, and summary fetched successfully.")

            # Try loading the statistics page with retries
            for attempt in range(self.retries):
                logger.info(f"{ticker}: Attempt {attempt + 1} to load statistics page.")
                soup = self.load_and_check_version(Ticker(ticker).statistics_link, driver, ticker)
                if soup is not None:
                    logger.info(f"{ticker}: Successfully loaded the statistics page on attempt {attempt + 1}.")
                    break  # Break out of the loop if the correct version is loaded
                logger.warning(f"{ticker}: Failed to load the statistics page on attempt {attempt + 1}. Retrying...")
                driver.quit()
                driver = Driver.create_driver(head)  # Create a new driver for the next attempt
            else:
                raise Exception(f'{ticker}: Failed to load the correct statistics page after {self.retries} retries.')

            # Identifying whether statistics & financial information are available through the side tabs
            side_tab_labels = [entry.text.strip() for entry in soup.select('a[category]')]

            # Statistics valuation table generator if "Statistics" tab is present
            if 'Statistics' in side_tab_labels:
                logger.info(f'{ticker}: Statistics scraping initiated.')

                # Generating the header for the statistics valuation table
                statistics_valuation_header = [entry.text for entry in soup.find_all(
                    self.se_statistics_valuation_table_header, class_=self.se_class_statistics_valuation_table_header)]
                statistics_valuation_header[0] = 'Breakdown'

                # Generating the statistics valuation table
                statistics_valuation_table = soup.find_all(self.se_statistics_valuation_table_row,
                                                           class_=self.se_class_statistics_valuation_table_row)

                # Note: the statistics valuation table begins with the header
                raw_statistics_valuation_table = [statistics_valuation_header]
                # Extract the individual elements from the row generated in statistics table
                raw_statistics_valuation_table.extend([[entry.text.strip() for entry in
                                                        statistics_valuation_table[valuation_iteration].find_all(
                                                            self.se_statistics_valuation_table_column,
                                                            class_=self.se_class_statistics_valuation_table_column)]
                                                       for valuation_iteration in
                                                       range(len(statistics_valuation_table))])

                df_statistics_valuations = pd.DataFrame(raw_statistics_valuation_table)

                # Cleaning up statistics valuation table
                df_statistics_valuations_T = df_statistics_valuations.T  # Transpose
                df_statistics_valuations = df_statistics_valuations_T.T

                # Generating the statistics financial highlights
                statistics_hgl_n_info = soup.find_all(self.se_statistics_hgl_n_info_row,
                                                      class_=self.se_class_statistics_hgl_n_info_row)
                raw_statistics_hgl_n_info = [[entry.text.strip() for entry in
                                              statistics_hgl_n_info[statistics_iteration].find_all(
                                                  self.se_statistics_hgl_n_info_column,
                                                  class_=self.se_class_statistics_hgl_n_info_column)]
                                             for statistics_iteration in range(len(statistics_hgl_n_info))]
                df_statistics_hgl_n_info = pd.DataFrame(raw_statistics_hgl_n_info)

                logger.info(f'{ticker}: Statistics scraping completed.')
            else:
                logger.warning(f'{ticker}: No statistics tab found.')

            # Financials page scraping
            if 'Financials' in side_tab_labels:
                for link_iteration in range(len(Ticker(ticker).fs_link)):
                    # Try loading the financials page with retries
                    for attempt in range(self.retries):
                        logger.info(
                            f"{ticker}: Attempt {attempt + 1} to load financials page "
                            f"(link {link_iteration + 1}).")
                        soup = self.load_and_check_version(Ticker(ticker).fs_link[link_iteration], driver, ticker)
                        if soup is not None:
                            logger.info(
                                f"{ticker}: Successfully loaded the financials page on attempt {attempt + 1} "
                                f"(link {link_iteration + 1}).")
                            break  # Break out of the loop if the correct version is loaded
                        logger.warning(
                            f"{ticker}: Failed to load the financials page on attempt {attempt + 1} "
                            f"(link {link_iteration + 1}). Retrying...")
                        driver.quit()
                        driver = Driver.create_driver(head)  # Create a new driver for the next attempt
                    else:
                        raise Exception(
                            f'{ticker}: Failed to load the correct financials page after {self.retries} retries.')

                    # Initialize the maximum number of retries
                    Scraper.find_expand_all_button(self, driver, ticker)

                    html_expanded = driver.execute_script('return document.body.innerHTML;')
                    soup_expanded = BeautifulSoup(html_expanded, 'lxml')

                    # Generating the header for the financial statements
                    fs_header_row = soup_expanded.find_all(self.se_financials_header_row,
                                                           class_=self.se_class_financials_header_row)
                    # Note: only the main headers are extracted without other features
                    fs_header_row = fs_header_row[0]
                    fs_header = [entry.text for entry in fs_header_row.find_all(self.se_financials_header_column)]
                    raw_fs_table = [fs_header]  # Note: The raw financial table begins with the headers

                    # Generating the contents for the financial statements
                    # (Note: splicing is used to pop repetitive column)
                    fs_content = soup_expanded.select(self.se_class_financials_content_row)

                    raw_fs_table.extend([entry.text for entry in
                                         fs_content[fs_iteration].find_all(
                                             self.se_financials_content_column,
                                             class_=self.se_class_financials_content_column)][1:]
                                        for fs_iteration in range(len(fs_content)))

                    df_financial_statement = pd.DataFrame(raw_fs_table)

                    if 'financials' in Ticker(ticker).fs_link[link_iteration]:
                        df_income_statement = df_financial_statement.copy()
                    elif 'balance-sheet' in Ticker(ticker).fs_link[link_iteration]:
                        df_balance_sheet = df_financial_statement.copy()
                    elif 'cash-flow' in Ticker(ticker).fs_link[link_iteration]:
                        df_cash_flow = df_financial_statement.copy()

                    logger.info(f'{ticker}: Financial statements scraping completed for link {link_iteration + 1}.')

            else:
                logger.warning(f'{ticker}: No financials tab found.')

            # Update the shared dictionary with synchronization
            with lock:
                logger.info(f'{ticker}: Updating shared dictionary with scraped data.')
                shared_dict[ticker] = {
                    'ticker': ticker,
                    'name': name[0],
                    'price': price[0] if price[0] is not None else None,
                    'change_intraday': change_intraday if change_intraday is not None else None,
                    'change_afterhours': change_afterhours if change_afterhours is not None else None,
                    'df_summary': df_summary.to_dict() if df_summary is not None else None,
                    'df_statistics_valuations': df_statistics_valuations.to_dict() if
                    df_statistics_valuations is not None else None,
                    'df_statistics_highlights': df_statistics_hgl_n_info.to_dict() if
                    df_statistics_hgl_n_info is not None else None,
                    'df_income_statement': df_income_statement.to_dict() if df_income_statement is not None else None,
                    'df_balance_sheet': df_balance_sheet.to_dict() if df_balance_sheet is not None else None,
                    'df_cash_flow': df_cash_flow.to_dict() if df_cash_flow is not None else None
                }
                logger.info(f'{ticker}: Shared dictionary updated successfully.')

        except Exception as e:
            logger.error(f'{ticker}: An error occurred during scraping fundamentals - {e}.', exc_info=True)

        finally:
            driver.quit()
            logger.info(f'{ticker}: Driver closed after fundamentals scraping.')

    def profile(self, ticker, shared_dict, lock):
        """
        Scrapes profile data for the specified ticker from Yahoo Finance. This includes sector, industry,
        number of employees, and key executive information.

        :param ticker: The stock ticker symbol to scrape data for.
        :type ticker: str
        :param shared_dict: A shared dictionary to store the scraped data, used in multiprocessing.
        :type shared_dict: multiprocessing.Manager().dict
        :param lock: A lock to synchronize access to shared resources in a multiprocessing environment.
        :type lock: multiprocessing.Lock
        """
        logger.info(f"{ticker}: Starting profile scraping.")
        try:
            soup = Scraper.request(self, Ticker(ticker).profile_link)

            logger.info(f"{ticker}: Extracting sector, industry, and employees data from profile page.")

            sector_and_industry = [entry.text for entry in soup.find_all(
                self.se_sector_and_industry, class_=self.se_class_sector_and_industry)]

            employees = [entry.text for entry in soup.find_all(self.se_employees)]
            # Note: it is the second entry if exists

            logger.info(f"{ticker}: Extracting key executives data from profile page.")

            # Generating the header for the profile table
            profile_header_row = [entry.text for entry in soup.find_all(self.se_profile_header_row,
                                                                        class_=self.se_class_profile_header_row)]

            raw_profile_table = [profile_header_row]  # Note: The raw profile table begins with the headers

            # Generating the contents for the profile table
            profile_content = [entry for entry in soup.find_all(self.se_profile_content_row,
                                                                class_=self.se_class_profile_content_row)]

            raw_profile_table.extend([entry.text for entry in
                                      profile_content[profile_iteration].find_all(
                                          self.se_profile_content_column,
                                          class_=self.se_class_profile_content_column)]
                                     for profile_iteration in range(len(profile_content)))

            # Remove out useless and empty first two rows
            raw_profile_table = raw_profile_table[2:]

            df_key_executives = pd.DataFrame(raw_profile_table)

            logger.info(f"{ticker}: Profile data extraction completed successfully.")

            # Update the shared dictionary with synchronization
            with lock:
                logger.info(f"{ticker}: Updating shared dictionary with profile data.")
                shared_dict[ticker] = {
                    'ticker': ticker,
                    'sector': sector_and_industry[0] if sector_and_industry else None,
                    'industry': sector_and_industry[1] if len(sector_and_industry) > 1 else None,
                    'employees': employees[1]  # The second entry with structural element 'dd'
                    if employees is not None and len(employees) > 1 else None,
                    'df_key_executives': df_key_executives.to_dict() if df_key_executives is not None else None
                }
                logger.info(f"{ticker}: Shared dictionary updated successfully with profile data.")

        except Exception as e:
            logger.error(f"{ticker}: An error occurred during scraping profile - {e}.", exc_info=True)

        finally:
            logger.info(f"{ticker}: Driver closed after profile scraping.")

    def holders(self, ticker, shared_dict, lock):
        """
        Scrapes major holders' data for the specified ticker from Yahoo Finance. This includes data on the percentage
        of shares held by insiders, institutions, and the number of institutions holding shares.

        :param ticker: The stock ticker symbol to scrape data for.
        :type ticker: str
        :param shared_dict: A shared dictionary to store the scraped data, used in multiprocessing.
        :type shared_dict: multiprocessing.Manager().dict
        :param lock: A lock to synchronize access to shared resources in a multiprocessing environment.
        :type lock: multiprocessing.Lock
        """
        logger.info(f"{ticker}: Starting holders data scraping.")
        try:
            # Try loading the holders page with retries
            soup = Scraper.request(self, Ticker(ticker).holders_link)

            logger.info(f"{ticker}: Extracting major holders data from holders page.")

            # Major holders is much simpler than other tables, thank god
            major_holders = [entry.text for entry in
                             soup.find_all(self.se_major_holders, class_=self.se_class_major_holders)]
            insider_shares_hold = major_holders[0] if len(major_holders) > 0 else None
            institution_shares_hold = major_holders[2] if len(major_holders) > 0 else None
            institution_float_hold = major_holders[4] if len(major_holders) > 0 else None
            num_institution_holding_shares = major_holders[6] if len(major_holders) > 0 else None

            logger.info(f"{ticker}: Holders data extraction completed successfully.")

            # Update the shared dictionary with synchronization
            with lock:
                logger.info(f"{ticker}: Updating shared dictionary with holders data.")
                shared_dict[ticker] = {
                    'ticker': ticker,
                    'insider_shares_hold': insider_shares_hold if insider_shares_hold is not None else None,
                    'institution_shares_hold': institution_shares_hold if institution_shares_hold is not None else None,
                    'institution_float_hold': institution_float_hold if institution_float_hold is not None else None,
                    'num_institution_holding_shares': num_institution_holding_shares if
                    num_institution_holding_shares is not None else None
                }
                logger.info(f"{ticker}: Shared dictionary updated successfully with holders data.")

        except Exception as e:
            logger.error(f"{ticker}: An error occurred during scraping holders data - {e}.", exc_info=True)

        finally:
            logger.info(f"{ticker}: Driver closed after holders data scraping.")

    def insider_transactions(self, ticker, shared_dict, lock):
        """
        Scrapes insider transactions data for the specified ticker from Yahoo Finance. This includes data on
        insider purchases, sales, and net changes in shares held by insiders.

        :param ticker: The stock ticker symbol to scrape data for.
        :type ticker: str
        :param shared_dict: A shared dictionary to store the scraped data, used in multiprocessing.
        :type shared_dict: multiprocessing.Manager().dict
        :param lock: A lock to synchronize access to shared resources in a multiprocessing environment.
        :type lock: multiprocessing.Lock
        """
        logger.info(f"{ticker}: Starting insider transactions data scraping.")
        try:
            # Try loading the insider transactions page
            soup = Scraper.request(self, Ticker(ticker).insider_transactions_link)

            logger.info(f"{ticker}: Extracting insider transactions data from page.")

            # Extract insider transaction data
            insider_transaction_header = [entry.text for entry in soup.find_all(
                self.se_insider_purchase_header_cell, class_=self.se_class_insider_purchase_header_cell)][:3]

            raw_insider_transaction = [insider_transaction_header]
            # Note: The raw insider table begins with the headers

            insider_transaction_content = [entry for entry in soup.find_all(
                self.se_insider_purchase_row, class_=self.se_class_insider_purchase_row)]

            raw_insider_transaction.extend([entry.text for entry in insider_transaction_content[insider_iteration]
                                           .find_all(self.se_insider_purchase_cell,
                                                     class_=self.se_class_insider_purchase_cell)][:3]
                                           for insider_iteration in range(1, len(insider_transaction_content) - 3))
            # Note: List splicing prevents spillover scraping, and minus 3 prevents scraping tables below

            df_insider_transactions = pd.DataFrame(raw_insider_transaction)

            logger.info(f"{ticker}: Insider transactions data extraction completed successfully.")

            # Update the shared dictionary with synchronization
            with lock:
                logger.info(f"{ticker}: Updating shared dictionary with insider transactions data.")
                shared_dict[ticker] = {
                    'ticker': ticker,
                    'df_insider_transactions': df_insider_transactions.to_dict() if
                    df_insider_transactions is not None else None
                }
                logger.info(f"{ticker}: Shared dictionary updated successfully with insider transactions data.")

        except Exception as e:
            logger.error(f"{ticker}: An error occurred during scraping insider transactions data - {e}.", exc_info=True)

        finally:
            logger.info(f"{ticker}: Driver closed after insider transactions data scraping.")

    def scrape(self, ticker_string, target='fundamentals', max_processes_capacity=1):
        """
        Initiates the scraping process for multiple tickers using multiprocessing, allowing concurrent scraping of
        data for increased efficiency. The target can be fundamentals, holders, profile, insider transactions, or all.

        This method determines the number of processes to run based on the maximum processing capacity, then
        dispatches separate processes for each batch of tickers. The WebDriver instances are managed to ensure
        consistency across scraping sessions, preventing issues related to loading older versions of pages and
        avoiding memory leaks.

        :param ticker_string: A string of stock ticker symbols separated by spaces (e.g., 'AAPL MSFT GOOGL').
        :type ticker_string: str
        :param target: The type of data to scrape. Options include 'fundamentals', 'holders', 'profile',
        'insider transactions', or 'all'.
        :type target: str, optional
        :param max_processes_capacity: Determines the fraction of available CPU cores to use for multiprocessing
        (e.g., 0.75 for 75%).
        :type max_processes_capacity: float, optional
        :return: A dictionary containing the scraped data for each ticker, with ticker symbols as keys.
        :rtype: dict
        """
        logger.info(f"Starting scrape process for tickers: {ticker_string} with target: {target}")

        # Determining the number of processes your computer should run to the number of available CPU cores
        max_processes = floor(multiprocessing.cpu_count() * max_processes_capacity)
        logger.info(f"Max processes capacity set to: {max_processes}")

        with Manager() as manager:
            shared_dict = manager.dict()
            lock = manager.Lock()  # Create a lock for synchronization
            processes = []
            tickers = ticker_string.split()

            if any(x in target.lower() for x in ['fundamentals', 'all']):
                logger.info(f"Starting fundamentals scraping for {len(tickers)} tickers.")
                for i in range(0, len(tickers), max_processes):
                    batch = tickers[i:i + max_processes]
                    logger.info(f"Processing batch {i // max_processes + 1}: {batch}")
                    for ticker in batch:
                        process = multiprocessing.Process(
                            target=self.fundamentals,
                            args=(ticker, shared_dict, lock))
                        processes.append(process)
                        process.start()

                    for process in processes:
                        process.join()

                    processes = []

                logger.info(f"Fundamentals scraping completed for all tickers.")

                for ticker in tickers:
                    if ticker in shared_dict:
                        data = shared_dict[ticker]
                        if ticker not in self.ticker_instances:
                            self.ticker_instances[ticker] = Ticker(ticker=ticker)

                        self.ticker_instances[ticker].set_attr(
                            price=data.get('price'),
                            name=data.get('name'),
                            change_intraday=data.get('change_intraday'),
                            change_afterhours=data.get('change_afterhours'),
                            df_summary=pd.DataFrame(data.get('df_summary')) if data.get('df_summary') else None,
                            df_statistics_valuations=pd.DataFrame(data.get('df_statistics_valuations')) if data.get(
                                'df_statistics_valuations') else None,
                            df_statistics_highlights=pd.DataFrame(data.get('df_statistics_highlights')) if data.get(
                                'df_statistics_highlights') else None,
                            df_income_statement=pd.DataFrame(data.get('df_income_statement')) if data.get(
                                'df_income_statement') else None,
                            df_balance_sheet=pd.DataFrame(data.get('df_balance_sheet')) if data.get(
                                'df_balance_sheet') else None,
                            df_cash_flow=pd.DataFrame(data.get('df_cash_flow')) if data.get('df_cash_flow') else None
                        )
                    else:
                        logger.warning(f"No data found for ticker {ticker} in shared_dict")

            if any(x in target.lower() for x in ['profile', 'all']):
                logger.info(f"Starting profile scraping for {len(tickers)} tickers.")
                for i in range(0, len(tickers), max_processes):
                    batch = tickers[i:i + max_processes]
                    logger.info(f"Processing batch {i // max_processes + 1}: {batch}")
                    for ticker in batch:
                        process = multiprocessing.Process(
                            target=self.profile,
                            args=(ticker, shared_dict, lock))
                        processes.append(process)
                        process.start()

                    for process in processes:
                        process.join()

                    processes = []

                logger.info(f"Profile scraping completed for all tickers.")

                for ticker in tickers:
                    if ticker in shared_dict:
                        data = shared_dict[ticker]
                        if ticker not in self.ticker_instances:
                            self.ticker_instances[ticker] = Ticker(ticker=ticker)

                        self.ticker_instances[ticker].set_attr(
                            sector=data.get('sector'),
                            industry=data.get('industry'),
                            employees=data.get('employees'),
                            df_key_executives=pd.DataFrame(data.get('df_key_executives'))
                            if data.get('df_key_executives') else None
                        )
                    else:
                        logger.warning(f"No data found for ticker {ticker} in shared_dict")

            if any(x in target.lower() for x in ['holders', 'all']):
                logger.info(f"Starting holders scraping for {len(tickers)} tickers.")
                for i in range(0, len(tickers), max_processes):
                    batch = tickers[i:i + max_processes]
                    logger.info(f"Processing batch {i // max_processes + 1}: {batch}")
                    for ticker in batch:
                        process = multiprocessing.Process(
                            target=self.holders,
                            args=(ticker, shared_dict, lock))
                        processes.append(process)
                        process.start()

                    for process in processes:
                        process.join()

                    processes = []

                logger.info(f"Holders scraping completed for all tickers.")

                for ticker in tickers:
                    if ticker in shared_dict:
                        data = shared_dict[ticker]
                        if ticker not in self.ticker_instances:
                            self.ticker_instances[ticker] = Ticker(ticker=ticker)

                        self.ticker_instances[ticker].set_attr(
                            insider_shares_hold=data.get('insider_shares_hold'),
                            institution_shares_hold=data.get('institution_shares_hold'),
                            institution_float_hold=data.get('institution_float_hold'),
                            num_institution_holding_shares=data.get('num_institution_holding_shares')
                        )
                    else:
                        logger.warning(f"No data found for ticker {ticker} in shared_dict")

            if any(x in target.lower() for x in ['insider transactions', 'all']):
                logger.info(f"Starting insider transactions scraping for {len(tickers)} tickers.")
                for i in range(0, len(tickers), max_processes):
                    batch = tickers[i:i + max_processes]
                    logger.info(f"Processing batch {i // max_processes + 1}: {batch}")
                    for ticker in batch:
                        process = multiprocessing.Process(
                            target=self.insider_transactions,
                            args=(ticker, shared_dict, lock))
                        processes.append(process)
                        process.start()

                    for process in processes:
                        process.join()

                    processes = []

                logger.info(f"Insider transactions scraping completed for all tickers.")

                for ticker in tickers:
                    if ticker in shared_dict:
                        data = shared_dict[ticker]
                        if ticker not in self.ticker_instances:
                            self.ticker_instances[ticker] = Ticker(ticker=ticker)

                        self.ticker_instances[ticker].set_attr(
                            df_insider_transactions=pd.DataFrame(data.get('df_insider_transactions'))
                            if data.get('df_insider_transactions') else None,
                        )
                    else:
                        logger.warning(f"No data found for ticker {ticker} in shared_dict")

            logger.info(f"Scraping process completed for all tickers with target: {target}.")
            return self.ticker_instances


class Analyzer:
    """
    The Analyzer class is responsible for processing and analyzing the data scraped by the Scraper class.
    It provides methods for extracting specific financial metrics from the DataFrames and performing calculations
    to derive key financial ratios, growth rates, and other indicators.

    Attributes:
    - ticker_instances: A dictionary to store ticker data, including both raw and analyzed data.
    - period: The period for which financial data is analyzed (1 for TTM, 2 for 10K).
    - round_int: The number of decimal places to round numeric results to.

    Methods:
    - search_parameter(df_example, parameters, output_column, search_column=0): Searches for a specific parameter
    in a DataFrame.
    - abbr_to_number(number_string): Converts a string with an abbreviation (k, M, B, T) into a numeric value.
    - join_comma(comma_number): Converts a string with commas into a float.
    - analyze(ticker_string, scraper_output, target='fundamentals', financial_data_period='TTM'): Analyzes financial
    data for the specified tickers.
    """
    def __init__(self,
                 period=1,
                 round_int=2):
        self.ticker_instances = {}  # Will contain all ticker data copied from scraper class above and analyzed data
        self.period = period
        self.round_int = round_int
        logger.info(f"Analyzer initialized with period: {period}, rounding: {round_int}")

    @staticmethod
    def search_parameter(df_example, parameters, output_column, search_column=0):
        """
        Searches for a specific parameter in a DataFrame and returns the corresponding value from the specified
        output column.

        This method allows you to search a DataFrame for a particular parameter (e.g., "Inventory," "Operating
        Cash Flow") and retrieve the value from a specified column. It is designed to handle cases where the DataFrame
        might be empty or where the desired parameter is not found.

        :param df_example: The DataFrame to search.
        :type df_example: pd.DataFrame
        :param parameters: The parameter(s) to search for in the DataFrame. If a single string is provided, it is
        converted to a list.
        :type parameters: str or list of str
        :param output_column: The column index from which to retrieve the value.
        :type output_column: int
        :param search_column: The column index in which to search for the parameter, default is 0.
        :type search_column: int, optional
        :return: The value from the specified output column if found, otherwise None.
        :rtype: any
        """
        logger.debug(f"Searching parameter '{parameters}'.")

        # Ensure parameters is a list; if a single string is provided, convert it to a list
        if isinstance(parameters, str):
            parameters = [parameters]

        # Check if the DataFrame is empty or none of the parameters are found
        if df_example.empty:
            logger.warning(f"{df_example} is empty. No parameters found.")
            return None

        # Iterate through each parameter and check if any matches in the search_column
        for parameter in parameters:
            matching_rows = df_example[df_example[search_column].str.contains(parameter, na=False)]
            if not matching_rows.empty:
                result_list = matching_rows.values.tolist()
                if result_list and not any(x in result_list[0][output_column] for x in ['--', '-- ', '---']):
                    return result_list[0][output_column].strip()

        logger.warning(f"None of the parameters '{parameters}' found.")
        # Return None if no match is found for any of the parameters
        return None

    # Use primarily for market cap and operating cash flow
    @staticmethod
    def abbr_to_number(number_string):
        """
        Converts a string containing a number with an abbreviation (k, M, B, T) into a numeric value (float).

        This method is useful for converting values like "12k" to 12000 or "3.5B" to 3,500,000,000.

        :param number_string: The number string containing an abbreviation (e.g., "12k", "3.5B").
        :type number_string: str
        :return: The numeric value as a float, or the original string if no abbreviation is found.
        :rtype: float or str
        """
        logger.debug(f"Converting abbreviated number string '{number_string}' to float.")

        if number_string is None:
            logger.warning('Number string is None.')
            return None
        elif 'k' in number_string:
            return float(number_string.replace('k', '')) * 10 ** 3
        elif 'M' in number_string:
            return float(number_string.replace('M', '')) * 10 ** 6
        elif 'B' in number_string:
            return float(number_string.replace('B', '')) * 10 ** 9
        elif 'T' in number_string:
            return float(number_string.replace('T', '')) * 10 ** 12
        logger.warning(f"Unrecognized abbreviation in number string: {number_string}")
        return number_string

    @staticmethod
    def join_comma(comma_number):
        """
        Converts a string containing a number with commas into a float.

        This method is useful for converting values like "1,200.01" to 1200.01. If the input is None or
        a placeholder string like "--", it returns None.

        :param comma_number: The number string containing commas (e.g., "1,200.01").
        :type comma_number: str
        :return: The numeric value as a float, or None if the input is invalid.
        :rtype: float or None
        """
        logger.debug(f"Converting comma-separated string '{comma_number}' to float.")

        if comma_number in [None, '--', '-- ', '---']:
            logger.warning(f"Comma-separated number '{comma_number}' is not a valid number.")
            return None
        else:
            return float(comma_number.replace(',', ''))

    @staticmethod
    def calculate_growth_rate(current_value, previous_value, period):
        """
        Calculates the growth rate for a given financial metric over a specified period.

        If the previous and current values have opposite signs, the function calculates the growth rate
        using the absolute values and appends an indicator showing the sign change (e.g., "- -> +" or "+ -> -").

        :param current_value: The value of the financial metric in the current period.
        :type current_value: float
        :param previous_value: The value of the financial metric in the previous period.
        :type previous_value: float
        :param period: The number of periods (years) over which the growth is calculated. Defaults to 1.
        :type period: int
        :return: The growth rate expressed as a percentage string (e.g., "5.23%"), with a sign change indicator
                 if applicable, or None if the calculation is not possible.
        :rtype: str or None
        """
        # Handle cases where values are None or zero
        if current_value is None or previous_value in [None, 0]:
            return None

        sign_change = None

        # Check for opposite signs and calculate with absolute values
        if current_value * previous_value < 0:
            sign_change = "- -> +" if current_value > 0 else "+ -> -"
            current_value = abs(current_value)
            previous_value = abs(previous_value)

        try:
            if period > 1:
                # Calculate CAGR
                cagr = ((current_value / previous_value) ** (1 / period)) - 1
                growth_rate = round(cagr * 100, 2)
            else:
                # Calculate simple growth rate
                growth_rate = round(((current_value - previous_value) / previous_value) * 100, 2)

            # Append sign change indicator if applicable
            growth_rate_str = f"{growth_rate}%"
            if sign_change:
                growth_rate_str += f" ({sign_change})"

            return growth_rate_str

        except (ZeroDivisionError, ValueError):
            # Return None if calculation is not possible due to a zero or invalid input
            return None

    def analyze(self, ticker_string, scraper_output, target='fundamentals', financial_data_period='TTM'):
        """
        Analyzes the financial data for the specified tickers, processing the data extracted by the Scraper class
        and calculating key financial metrics such as growth rates, financial ratios, and profitability measures.

        The method can analyze different types of data, including fundamentals, profile, holders, and insider
        transactions.
        It also handles cases where certain data is unavailable or needs to be derived from other available data.

        For financial data, this method will determine whether to use the trailing twelve months (TTM) data or annual
        10K data based on the 'financial_data_period' parameter. The method also ensures that calculations are
        appropriately rounded and that missing data is handled gracefully.

        :param ticker_string: A string of ticker symbols separated by spaces (e.g., 'AAPL MSFT GOOGL').
        :type ticker_string: str
        :param scraper_output: The output from the Scraper class containing the raw scraped data.
        :type scraper_output: dict
        :param target: The type of data to analyze (fundamentals, profile, holders, insider transactions, or all).
        :type target: str, optional
        :param financial_data_period: The period for financial data analysis (TTM or 10K).
        :type financial_data_period: str, optional
        :return: A dictionary containing the analyzed data for each ticker, with ticker symbols as keys.
        :rtype: dict
        """
        logger.info(f"Starting analysis for tickers: {ticker_string} with target: {target}, "
                    f"period: {financial_data_period}")

        self.ticker_instances = scraper_output.copy()

        if any(x in target.lower() for x in ['fundamentals', 'all']):
            if financial_data_period.upper() in [2, '10K']:
                self.period = 2
                calculation_mode = '10K'
            else:
                self.period = 1
                calculation_mode = 'TTM'

            logger.info(f"Analyzing fundamentals for tickers: {ticker_string}")

            for ticker in ticker_string.split():
                logger.info(f"Analyzing fundamentals for ticker: {ticker}")

                # Retrieve the ticker instance safely
                ticker_instance = scraper_output.get(ticker)

                if ticker_instance:
                    logger.info(f"{ticker}: Data found. Proceeding with analysis.")

                    # Pulling the DataFrames
                    df_summary = ticker_instance.df_summary
                    df_statistics_valuations = ticker_instance.df_statistics_valuations
                    df_statistics_highlights = ticker_instance.df_statistics_highlights
                    df_income_statement = ticker_instance.df_income_statement
                    df_balance_sheet = ticker_instance.df_balance_sheet
                    df_cash_flow = ticker_instance.df_cash_flow

                    # Search for data from DataFrames if statistics or financials pages are not present
                    if df_summary is None or df_summary.empty:
                        logger.warning(f"{ticker}: No summary data available for analysis.")

                        # Note: Variables used in Show() class must be declared in all pathways
                        self.ticker_instances[ticker].set_attr(
                            summary_availability='x',
                            statistics_availability='x',
                            fs_availability='x',
                        )

                        continue

                    elif df_statistics_valuations is None or df_statistics_valuations.empty:
                        logger.warning(f"{ticker}: No statistics data available. Analyzing summary data only.")

                        forward_dividend_and_yield = Analyzer.search_parameter(df_summary, 'Yield', 1)
                        net_assets = Analyzer.search_parameter(df_summary, 'Net Assets', 1)
                        # Sometimes PE ratio is reported, sometimes it is not
                        try:
                            price_to_earnings = Analyzer.search_parameter(df_summary, 'PE Ratio', 1)
                        except IndexError:
                            price_to_earnings = None

                        # Note: Variables used in Show() class must be declared in all pathways
                        self.ticker_instances[ticker].set_attr(
                            summary_availability='✓',
                            statistics_availability='x',
                            fs_availability='x',

                            forward_dividend_and_yield=forward_dividend_and_yield,
                            net_assets=net_assets,
                            price_to_earnings=price_to_earnings,
                        )

                        continue

                    # Search for data from DataFrames if both the statistics and financials pages are present
                    elif df_statistics_valuations is not None and not df_statistics_valuations.empty:
                        logger.info(f"{ticker}: Summary and statistics data found. Proceeding with detailed analysis.")

                        # These following values do not need to be converted to float as they are reported immediately
                        # Key Statistics
                        forward_dividend_and_yield = Analyzer.search_parameter(
                            df_summary, 'Forward Dividend & Yield', 1)
                        market_cap = Analyzer.search_parameter(df_summary, 'Market Cap', 1)
                        eps = Analyzer.search_parameter(df_summary, 'EPS', 1)
                        diluted_eps = Analyzer.search_parameter(df_statistics_highlights, 'Diluted EPS', 1)

                        # Valuation
                        price_to_book = Analyzer.search_parameter(df_statistics_valuations,
                                                                  'Price/Book', self.period)
                        price_to_sales = Analyzer.search_parameter(df_statistics_valuations,
                                                                   'Price/Sales', self.period)
                        price_to_earnings = Analyzer.search_parameter(df_statistics_valuations,
                                                                      'Trailing P/E', self.period)

                        # Financial Strength
                        current_ratio = Analyzer.search_parameter(df_statistics_highlights,
                                                                  'Current Ratio', 1)

                        # Profitability
                        return_on_assets = Analyzer.search_parameter(df_statistics_highlights,
                                                                     'Return on Assets', 1)
                        return_on_equity = Analyzer.search_parameter(df_statistics_highlights,
                                                                     'Return on Equity', 1)
                        profit_margin = Analyzer.search_parameter(df_statistics_highlights,
                                                                  'Profit Margin', 1)

                        # These following values need to be converted to float as more calculations are needed
                        # Price-to-cash flow data search (abbr_to_number only used here since they are summary and
                        # stats data)
                        market_cap_float = Analyzer.abbr_to_number(
                            Analyzer.search_parameter(df_summary, 'Market Cap', 1))
                        operating_cash_flow = Analyzer.abbr_to_number(
                            Analyzer.search_parameter(df_statistics_highlights, 'Operating Cash Flow', 1))

                        # Growth metrics search (join_comma used here from now on since it is financials section data)
                        # Note: The try except block prevents out of range error when only 4 columns are displayed
                        # instead of 5.
                        try:
                            # Total revenue growth, 3-year TTM
                            total_revenue = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Total Revenue', self.period))
                            total_revenue_prev = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Total Revenue', 5))

                            total_revenue_period = 3

                        except IndexError:
                            logger.warning(
                                f'{ticker}: insufficient total revenue data, 2-year TTM data provided in place of '
                                f'3-year TTM data.')

                            # Total revenue growth, 2-year TTM
                            total_revenue = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Total Revenue', self.period))
                            total_revenue_prev = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Total Revenue', 4))

                            total_revenue_period = 2

                        try:
                            # Operating income growth, 3-year TTM
                            operating_income = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Operating Income', self.period))
                            operating_income_prev = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Operating Income', 5))

                            operating_income_period = 3

                        except IndexError:
                            logger.warning(
                                f'{ticker}: insufficient operating income data, 2-year TTM data provided in place of '
                                f'3-year TTM data.')

                            # Operating income growth, 2-year TTM
                            operating_income = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Operating Income', self.period))
                            operating_income_prev = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Operating Income', 4))

                            operating_income_period = 2

                        try:
                            # Net income growth, 3-year TTM
                            net_income = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Net Income', self.period))
                            net_income_prev = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Net Income', 5))

                            net_income_period = 3

                        except IndexError:
                            logger.warning(f'{ticker}: insufficient net income data, 2-year TTM data provided in place '
                                           f'of 3-year TTM data.')

                            # Net income growth, 2-year TTM
                            net_income = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Net Income', self.period))
                            net_income_prev = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Net Income', 4))

                            net_income_period = 2

                        try:
                            # Diluted EPS growth, 3-year TTM
                            diluted_eps_fs = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Diluted EPS', self.period))
                            diluted_eps_fs_prev = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Diluted EPS', 5))

                            diluted_eps_period = 3

                        except IndexError:
                            logger.warning(f'{ticker}: insufficient diluted EPS data, 2-year TTM data provided in place'
                                           f' of 3-year TTM data.')

                            # Diluted EPS growth, 2-year TTM
                            diluted_eps_fs = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Diluted EPS', self.period))
                            diluted_eps_fs_prev = Analyzer.join_comma(
                                Analyzer.search_parameter(df_income_statement, 'Diluted EPS', 4))

                            diluted_eps_period = 2

                        # Quick ratio search
                        current_assets = Analyzer.join_comma(
                            Analyzer.search_parameter(df_balance_sheet, 'Current Assets', self.period))
                        current_liabilities = Analyzer.join_comma(
                            Analyzer.search_parameter(df_balance_sheet, 'Current Liabilities', self.period))
                        inventory = Analyzer.join_comma(
                            Analyzer.search_parameter(df_balance_sheet, 'Inventory', self.period))

                        # Interest coverage search
                        EBIT = Analyzer.join_comma(
                            Analyzer.search_parameter(df_income_statement, 'EBIT', self.period))
                        interest_expense = Analyzer.join_comma(
                            Analyzer.search_parameter(df_income_statement, 'Interest Expense', self.period))

                        # Debt-to-equity search
                        total_debt = Analyzer.join_comma(
                            Analyzer.search_parameter(df_balance_sheet, 'Total Debt', self.period))
                        stockholders_equity = Analyzer.join_comma(
                            Analyzer.search_parameter(df_balance_sheet, 'Stockholders\' Equity', self.period))

                        # Return on invested capital search
                        tax_provision = Analyzer.join_comma(
                            Analyzer.search_parameter(df_income_statement, 'Tax Provision', self.period))
                        invested_capital = Analyzer.join_comma(
                            Analyzer.search_parameter(df_balance_sheet, 'Invested Capital', self.period))

                        # Other miscellaneous data search (for back-up calculations)
                        tangible_book_value = Analyzer.join_comma(
                            Analyzer.search_parameter(df_balance_sheet, 'Tangible Book Value', self.period))
                        total_assets = Analyzer.join_comma(Analyzer.search_parameter(
                            df_balance_sheet, 'Total Assets', self.period))

                        # Summary and statistics data calculations
                        # Price-to-cash flow
                        if market_cap_float and operating_cash_flow and operating_cash_flow != 0:
                            price_to_cash_flow = str(round(market_cap_float / operating_cash_flow, self.round_int))
                        else:
                            price_to_cash_flow = None

                        # Financial statement data calculations
                        # Average revenue growth (3-year or 2-year TTM)
                        if total_revenue is not None and total_revenue_prev not in [None, 0]:
                            revenue_growth = Analyzer.calculate_growth_rate(total_revenue, total_revenue_prev,
                                                                            total_revenue_period)
                        else:
                            revenue_growth = None

                        # Operating income growth (3-year or 2-year TTM)
                        if operating_income is not None and operating_income_prev not in [None, 0]:
                            operating_income_growth = Analyzer.calculate_growth_rate(operating_income,
                                                                                     operating_income_prev,
                                                                                     operating_income_period)
                        else:
                            operating_income_growth = None

                        # Net income growth (3-year or 2-year TTM)
                        if net_income is not None and net_income_prev not in [None, 0]:
                            net_income_growth = Analyzer.calculate_growth_rate(net_income, net_income_prev,
                                                                               net_income_period)
                        else:
                            net_income_growth = None

                        # Diluted EPS growth (3-year or 2-year TTM)
                        if diluted_eps_fs is not None and diluted_eps_fs_prev not in [None, 0]:
                            diluted_eps_growth = Analyzer.calculate_growth_rate(diluted_eps_fs, diluted_eps_fs_prev,
                                                                                diluted_eps_period)
                        else:
                            diluted_eps_growth = None

                        # Quick ratio
                        if (current_assets is not None and current_liabilities not in [None, 0]
                                and inventory is not None):
                            quick_ratio = str(round((current_assets - inventory) / current_liabilities, self.round_int))
                        else:
                            quick_ratio = None

                        # Interest coverage
                        if EBIT is not None and interest_expense not in [None, 0]:
                            interest_coverage = str(round(EBIT / interest_expense, self.round_int))
                        else:
                            interest_coverage = None

                        # Debt-to-equity
                        if total_debt is not None and stockholders_equity not in [None, 0]:
                            debt_to_equity = str(round(total_debt / stockholders_equity, self.round_int))
                        else:
                            debt_to_equity = None

                        # Return on invested capital
                        if EBIT is not None and tax_provision is not None and invested_capital not in [None, 0]:
                            return_on_invested_capital = (str(round((EBIT - tax_provision) / invested_capital * 100,
                                                                    self.round_int)) + '%')
                        else:
                            return_on_invested_capital = None

                        # Back-up financial statement data search if summary and stats fail to provide up-to-date data
                        if diluted_eps is None:
                            diluted_eps = Analyzer.search_parameter(df_income_statement, 'Diluted EPS', self.period)
                            logger.info(f'{ticker}: TTD diluted EPS unavailable '
                                        f'(alternative source: income statement).')

                        # The following data needs to be converted to float by join_comma
                        if operating_cash_flow is None:
                            operating_cash_flow = Analyzer.join_comma(
                                Analyzer.search_parameter(df_cash_flow, 'Operating Cash Flow', self.period))
                            logger.info(f'{ticker}: Operating cash flow obtained from cash flow '
                                        f'instead of statistics highlights.')

                        # Back-up financial statement data calculations and replacement (if statistics has no data)
                        # Note: when data not from financials are calculated with data from financials,
                        # remember factor of 1000
                        if (price_to_book is None and market_cap_float is not None and tangible_book_value
                                not in [None, 0]):
                            price_to_book = str(round(market_cap_float / (tangible_book_value * 1000), 2))
                            logger.info(f'{ticker}: TTD price/book unavailable (alternative source: balance sheet).')

                        if price_to_sales is None and market_cap_float is not None and total_revenue not in [None, 0]:
                            price_to_sales = str(round(market_cap_float / (total_revenue * 1000), 2))
                            logger.info(f'{ticker}: TTD price/sales unavailable '
                                        f'(alternative source: income statement).')

                        if price_to_earnings is None and market_cap_float is not None and net_income not in [None, 0]:
                            price_to_earnings = str(round(market_cap_float / (net_income * 1000), 2))
                            logger.info(f'{ticker}: TTD price/earnings unavailable '
                                        f'(alternative source: income statement).')

                        if (price_to_cash_flow is None and market_cap_float is not None and operating_cash_flow not in
                                [None, 0]):
                            price_to_cash_flow = str(round(market_cap_float / (operating_cash_flow * 1000), 2))
                            logger.info(f'{ticker}: TTD price/cash flow unavailable (alternative source: cash flow).')

                        if (current_ratio is None and current_assets is not None and current_liabilities not in
                                [None, 0]):
                            current_ratio = str(round(current_assets / current_liabilities, 2))
                            logger.info(f'{ticker}: TTD current ratio unavailable (alternative source: balance sheet).')

                        if return_on_assets is None and net_income is not None and total_assets not in [None, 0]:
                            return_on_assets = str(round(net_income / total_assets * 100, 2)) + '%'
                            logger.info(f'{ticker}: TTD return on assets unavailable '
                                        f'(alternative source: income statement, balance sheet).')

                        if return_on_equity is None and net_income is not None and stockholders_equity not in [None, 0]:
                            return_on_equity = str(round(net_income / stockholders_equity * 100, 2)) + '%'
                            logger.info(f'{ticker}: TTD return on equity unavailable '
                                        f'(alternative source: income statement, balance sheet).')

                        if profit_margin == '0.00%' and net_income is not None and total_revenue not in [None, 0]:
                            profit_margin = str(round(net_income / total_revenue * 100, 2)) + '%'
                            logger.info(f'{ticker}: TTD profit margin unavailable '
                                        f'(alternative source: income statement).')

                        self.ticker_instances[ticker].set_attr(
                            # Main variables used in the Show() class
                            summary_availability='✓',
                            fs_availability='✓',
                            latest_10Q=df_statistics_valuations[2][0].strip(),
                            latest_10K=df_income_statement[2][0].strip(),

                            forward_dividend_and_yield=forward_dividend_and_yield,
                            market_cap=market_cap,
                            eps=eps,
                            diluted_eps=diluted_eps,

                            # Valuation
                            price_to_book=price_to_book,
                            price_to_sales=price_to_sales,
                            price_to_earnings=price_to_earnings,
                            price_to_cash_flow=price_to_cash_flow,

                            # Growth
                            revenue_growth=revenue_growth,
                            operating_income_growth=operating_income_growth,
                            net_income_growth=net_income_growth,
                            diluted_eps_growth=diluted_eps_growth,

                            # Financial Strength
                            quick_ratio=quick_ratio,
                            current_ratio=current_ratio,
                            interest_coverage=interest_coverage,
                            debt_to_equity=debt_to_equity,

                            # Profitability
                            return_on_assets=return_on_assets,
                            return_on_equity=return_on_equity,
                            return_on_invested_capital=return_on_invested_capital,
                            profit_margin=profit_margin,

                            # Other variables for storage and reference
                            operating_cash_flow=operating_cash_flow,
                            market_cap_float=market_cap_float,
                            tangible_book_value=tangible_book_value,
                            total_assets=total_assets,
                            calculation_mode=calculation_mode
                        )

                else:
                    logger.warning(f"{ticker}: No data found in scraper_output for this ticker.")

        if any(x in target.lower() for x in ['profile', 'all']):
            logger.info(f"Analyzing profile data for tickers: {ticker_string}")

            for ticker in ticker_string.split():
                logger.info(f"Analyzing profile data for ticker: {ticker}")

                # Pulling the DataFrames
                df_key_executives = scraper_output[ticker].df_key_executives

                if df_key_executives is None or df_key_executives.empty:
                    logger.warning(f"{ticker}: No key executives data available for analysis.")
                    continue

                elif df_key_executives is not None and not df_key_executives.empty:
                    # Search for data from DataFrames
                    chairman = Analyzer.search_parameter(df_key_executives, ['Chairman'], 0, 1)
                    director = Analyzer.search_parameter(df_key_executives, ['Director'], 0, 1)
                    ceo = Analyzer.search_parameter(df_key_executives, ['CEO', 'Chief Executing Officer'], 0, 1)
                    cfo = Analyzer.search_parameter(df_key_executives, ['CFO', 'Chief Financial Officer'], 0, 1)
                    clo = Analyzer.search_parameter(df_key_executives, ['CLO', 'Chief Legal Officer'], 0, 1)
                    cmo = Analyzer.search_parameter(df_key_executives, ['CMO', 'Chief Marketing Officer'], 0, 1)
                    coo = Analyzer.search_parameter(df_key_executives, ['COO', 'Chief Operating Officer'], 0, 1)
                    cso = Analyzer.search_parameter(df_key_executives, ['CSO', 'Chief Strategy Officer'], 0, 1)

                    chairman_year = Analyzer.search_parameter(df_key_executives, ['Chairman'], 4, 1)
                    director_year = Analyzer.search_parameter(df_key_executives, ['Director'], 4, 1)
                    ceo_year = Analyzer.search_parameter(df_key_executives, ['CEO', 'Chief Executing Officer'], 4, 1)
                    cfo_year = Analyzer.search_parameter(df_key_executives, ['CFO', 'Chief Financial Officer'], 4, 1)
                    clo_year = Analyzer.search_parameter(df_key_executives, ['CLO', 'Chief Legal Officer'], 4, 1)
                    cmo_year = Analyzer.search_parameter(df_key_executives, ['CMO', 'Chief Marketing Officer'], 4, 1)
                    coo_year = Analyzer.search_parameter(df_key_executives, ['COO', 'Chief Operating Officer'], 4, 1)
                    cso_year = Analyzer.search_parameter(df_key_executives, ['CSO', 'Chief Strategy Officer'], 4, 1)

                    chairman_salary = Analyzer.search_parameter(df_key_executives, ['Chairman'], 2, 1)
                    director_salary = Analyzer.search_parameter(df_key_executives, ['Director'], 2, 1)
                    ceo_salary = Analyzer.search_parameter(df_key_executives, ['CEO', 'Chief Executing Officer'], 2, 1)
                    cfo_salary = Analyzer.search_parameter(df_key_executives, ['CFO', 'Chief Financial Officer'], 2, 1)
                    clo_salary = Analyzer.search_parameter(df_key_executives, ['CLO', 'Chief Legal Officer'], 2, 1)
                    cmo_salary = Analyzer.search_parameter(df_key_executives, ['CMO', 'Chief Marketing Officer'], 2, 1)
                    coo_salary = Analyzer.search_parameter(df_key_executives, ['COO', 'Chief Operating Officer'], 2, 1)
                    cso_salary = Analyzer.search_parameter(df_key_executives, ['CSO', 'Chief Strategy Officer'], 2, 1)

                    self.ticker_instances[ticker].set_attr(
                        # Main variables used in the Show() class
                        chairman=chairman,
                        director=director,
                        ceo=ceo,
                        cfo=cfo,
                        clo=clo,
                        cmo=cmo,
                        coo=coo,
                        cso=cso,
                        chairman_year=chairman_year,
                        director_year=director_year,
                        ceo_year=ceo_year,
                        cfo_year=cfo_year,
                        clo_year=clo_year,
                        cmo_year=cmo_year,
                        coo_year=coo_year,
                        cso_year=cso_year,
                        chairman_salary=chairman_salary,
                        director_salary=director_salary,
                        ceo_salary=ceo_salary,
                        cfo_salary=cfo_salary,
                        clo_salary=clo_salary,
                        cmo_salary=cmo_salary,
                        coo_salary=coo_salary,
                        cso_salary=cso_salary
                    )

        if any(x in target.lower() for x in ['holders', 'all']):
            logger.info(f"Analyzing holders data for tickers: {ticker_string}")
            # Holders data usually doesn't require much processing, but add logs if any transformations are needed.

        if any(x in target.lower() for x in ['insider transactions', 'all']):
            logger.info(f"Analyzing insider transactions data for tickers: {ticker_string}")

            for ticker in ticker_string.split():
                logger.info(f"Analyzing insider transactions data for ticker: {ticker}")

                # Pulling the DataFrames
                df_insider_transactions = scraper_output[ticker].df_insider_transactions

                if df_insider_transactions is None or df_insider_transactions.empty:
                    logger.warning(f"{ticker}: No insider transactions data available for analysis.")
                    continue

                elif df_insider_transactions is not None and not df_insider_transactions.empty:
                    # Search for data from DataFrames
                    # Note to self: do not put brackets as inputs, it breaks search_parameter somehow
                    total_insider_shares_held = Analyzer.search_parameter(
                        df_insider_transactions, 'Total Insider Shares Held', 1)
                    net_shares_purchased = Analyzer.search_parameter(
                        df_insider_transactions, 'Purchases', 1)
                    net_shares_sold = Analyzer.search_parameter(
                        df_insider_transactions, 'Sales', 1)
                    net_shares_change = Analyzer.search_parameter(
                        df_insider_transactions, 'Net Shares Purchased', 1)
                    net_transactions = Analyzer.search_parameter(
                        df_insider_transactions, 'Net Shares Purchased', 2)
                    purchase_transactions = Analyzer.search_parameter(
                        df_insider_transactions, 'Purchases', 2)
                    sell_transactions = Analyzer.search_parameter(
                        df_insider_transactions, 'Sales', 2)
                    percent_net_shares_change = Analyzer.search_parameter(
                        df_insider_transactions, '% Net Shares Purchased', 1)

                    self.ticker_instances[ticker].set_attr(
                        # Main variables used in the Show() class
                        net_shares_purchased=net_shares_purchased,
                        purchase_transactions=purchase_transactions,
                        net_shares_sold=net_shares_sold,
                        sell_transactions=sell_transactions,
                        net_shares_change=net_shares_change,
                        net_transactions=net_transactions,
                        total_insider_shares_held=total_insider_shares_held,
                        percent_net_shares_change=percent_net_shares_change
                    )

        logger.info(f"Analysis completed for tickers: {ticker_string}")
        return self.ticker_instances


class Compiler:
    """
    The Compiler class is responsible for organizing and visualizing the analyzed financial data. It transforms
    the data from the Analyzer class or a CSV file into structured DataFrames that can be easily reviewed or
    exported. The class provides functionality to compile various types of financial data, such as fundamentals,
    profile data, holders data, and insider transactions, into a user-friendly format.

    Methods:
    - compile(analyzer_output_or_filepath, target='fundamentals'): Compiles the analyzed data into a structured
    DataFrame.
    """
    @staticmethod
    def compile(analyzer_output_or_filepath, target='fundamentals'):
        """
        Compiles the analyzed data or CSV data into a structured DataFrame based on the specified target. This method
        organizes the data for easier visualization, with different options for displaying fundamentals, profile data,
        holders data, and insider transactions.

        The method takes either an instance of the Analyzer class or a filepath to a CSV file containing the
        analyzed data. It then generates a DataFrame that groups the relevant financial metrics for each target
        category.

        :param analyzer_output_or_filepath: An instance of the Analyzer class containing analyzed data, or a filepath to
         a CSV file.
        :type analyzer_output_or_filepath: Analyzer or str
        :param target: The type of data to compile into a DataFrame. Options include 'fundamentals', 'profile',
        'holders', 'insider transactions', or 'all'. Default is 'fundamentals'.
        :type target: str, optional
        :return: A DataFrame containing the compiled data for the specified target.
        :rtype: pd.DataFrame
        """

        logger.info(f"Starting compilation for target: {target}")

        df_fundamentals = None
        df_profile = None
        df_holders = None
        df_insider_transactions = None

        def replace_none_with_dash(value):
            """
            Replaces None or NaN values with a placeholder ('---') for better visualization in the DataFrame.
            Numeric values are rounded to six significant digits to avoid premature rounding of statistics.

            :param value: The input value from the DataFrame, which could be None, NaN, or a numeric value.
            :type value: any
            :return: The same value, or '---' if the input was None or NaN, or a rounded numeric value.
            :rtype: str or float
            """
            if pd.isna(value) or value is None:
                return '---'
            # Ensure we keep full precision for numeric values
            if isinstance(value, (int, float)):
                return f"{value:.6g}"  # Adjust precision as needed (here it's set to 6 significant digits)
            return value

        def extract_value(df_or_ticker_instance_input, column_name):
            """
            Extracts a value from a pandas Series or DataFrame and replaces None with dashes ('---').

            This function is designed to handle both DataFrames and Ticker instances. It retrieves the value from the
            specified column in a DataFrame or the corresponding attribute in a Ticker instance. If the value is None
            or NaN, it returns a placeholder ('---') for better visualization.

            :param df_or_ticker_instance_input: The DataFrame or Ticker instance from which to extract the value.
            :type df_or_ticker_instance_input: pd.DataFrame or Ticker
            :param column_name: The name of the column or attribute to extract the value from.
            :type column_name: str
            :return: The extracted value, or '---' if the value was None or NaN.
            :rtype: str or float
            """
            logger.debug(f"Extracting value for column '{column_name}'.")

            # The case of DataFrame
            if isinstance(df_or_ticker_instance_input, pd.DataFrame):
                if column_name in df_or_ticker_instance_input.columns:
                    value = df_or_ticker_instance_input[column_name].values[0]
                    return replace_none_with_dash(value)
                return '---'

            # The case of Ticker
            elif isinstance(df_or_ticker_instance_input, Ticker):
                value = getattr(df_or_ticker_instance_input, column_name, None)
                return replace_none_with_dash(value)

            # The case where it is neither Ticker nor DataFrame
            return replace_none_with_dash(df_or_ticker_instance_input)

        # Generate list of tickers from analyzer_output
        if isinstance(analyzer_output_or_filepath, pd.DataFrame):
            analyzer_output = analyzer_output_or_filepath.copy()
            tickers = analyzer_output_or_filepath['ticker'].unique().tolist()
            logger.info(f"Loaded data from DataFrame with {len(tickers)} tickers.")
        elif isinstance(analyzer_output_or_filepath, str):
            analyzer_output = pd.read_csv(analyzer_output_or_filepath)
            tickers = analyzer_output['ticker'].unique().tolist()
            logger.info(f"Loaded data from file '{analyzer_output_or_filepath}' with {len(tickers)} tickers.")
        else:
            logger.error('Incorrect input type. Must be a DataFrame or filepath string.')
            return

        # Check if analyzer_output is a DataFrame
        is_dataframe = isinstance(analyzer_output, pd.DataFrame)

        if target.lower() in ['fundamentals', 'all']:
            """
            Compiles fundamental financial data into a DataFrame. This section extracts and organizes metrics like 
            intraday price changes, financial statement availability, valuation ratios, growth metrics, financial 
            strength, and profitability measures. The DataFrame is structured for easy visualization of these 
            key metrics.
            """
            logger.info(f"Compiling fundamentals data for {len(tickers)} tickers.")
            compiled_data = []
            for ticker in tickers:
                if is_dataframe:
                    ticker_obj = analyzer_output[analyzer_output['ticker'] == ticker]
                    if ticker_obj.empty:
                        logger.warning(f"No data found for ticker {ticker} in analyzer_output")
                        continue
                else:
                    ticker_obj = analyzer_output.get(ticker)
                    if ticker_obj is None:
                        logger.warning(f"No data found for ticker {ticker} in analyzer_output")
                        continue

                compiled_data.append({
                    '   ••• INFORMATION •••   ': '••••••••••',

                    'Ticker': extract_value(ticker_obj, 'ticker'),
                    'Name': extract_value(ticker_obj, 'name'),

                    '   ••• INTRADAY DATA •••   ': '••••••••••',
                    'Price': extract_value(ticker_obj, 'price'),
                    'Change (Intraday)': extract_value(ticker_obj, 'change_intraday'),
                    'Change (After Hours)': extract_value(ticker_obj, 'change_afterhours'),

                    '   ••• DATA AVAILABILITY •••   ': '••••••••••',
                    'Summary Availability': extract_value(ticker_obj, 'summary_availability'),
                    'Financial Statements Availability': extract_value(ticker_obj, 'fs_availability'),
                    'Latest 10-Q': extract_value(ticker_obj, 'latest_10Q'),
                    'Latest 10-K': extract_value(ticker_obj, 'latest_10K'),

                    '   ••• OVERVIEW •••   ': '••••••••••',
                    'Forward Dividend & Yield': extract_value(ticker_obj, 'forward_dividend_and_yield'),
                    'Market Cap / Net Assets': extract_value(ticker_obj, 'market_cap'),
                    'EPS': extract_value(ticker_obj, 'eps'),
                    'Diluted EPS': extract_value(ticker_obj, 'diluted_eps'),

                    f'   ••• VALUATION - {extract_value(ticker_obj, "calculation_mode")} •••   ': '••••••••••',
                    'Price-to-Book': extract_value(ticker_obj, 'price_to_book'),
                    'Price-to-Sales': extract_value(ticker_obj, 'price_to_sales'),
                    'Price-to-Earnings': extract_value(ticker_obj, 'price_to_earnings'),
                    'Price-to-Cash Flow': extract_value(ticker_obj, 'price_to_cash_flow'),

                    f'   ••• GROWTH - {extract_value(ticker_obj, "calculation_mode")} •••   ': '••••••••••',
                    'Revenue Growth': extract_value(ticker_obj, 'revenue_growth'),
                    'Operating Income Growth': extract_value(ticker_obj, 'operating_income_growth'),
                    'Net Income Growth': extract_value(ticker_obj, 'net_income_growth'),
                    'Diluted EPS Growth': extract_value(ticker_obj, 'diluted_eps_growth'),

                    f'   ••• FINANCIAL STRENGTH - {extract_value(ticker_obj, "calculation_mode")} •••   ': '••••••••••',
                    'Quick Ratio': extract_value(ticker_obj, 'quick_ratio'),
                    'Current Ratio': extract_value(ticker_obj, 'current_ratio'),
                    'Interest Coverage': extract_value(ticker_obj, 'interest_coverage'),
                    'Debt/Equity': extract_value(ticker_obj, 'debt_to_equity'),

                    f'   ••• PROFITABILITY - {extract_value(ticker_obj, "calculation_mode")} •••   ': '••••••••••',
                    'Return on Assets': extract_value(ticker_obj, 'return_on_assets'),
                    'Return on Equity': extract_value(ticker_obj, 'return_on_equity'),
                    'Return on Invested Capital': extract_value(ticker_obj, 'return_on_invested_capital'),
                    'Profit Margin': extract_value(ticker_obj, 'profit_margin')
                })

            df_fundamentals = pd.DataFrame(compiled_data).transpose()
            logger.info(f'Fundamentals compilation completed.')

        if target.lower() in ['profile', 'all']:
            """
            Compiles company profile data into a DataFrame. This section focuses on key executive information, including 
            names, birth years, and salaries of top executives like the Chairman, CEO, CFO, and others. The DataFrame 
            is structured to provide a comprehensive view of the company's leadership.
            """
            logger.info(f"Compiling profile data for {len(tickers)} tickers.")
            profile_data = []
            for ticker in tickers:
                if is_dataframe:
                    ticker_obj = analyzer_output[analyzer_output['ticker'] == ticker]
                    if ticker_obj.empty:
                        logger.warning(f"No data found for ticker {ticker} in analyzer_output")
                        continue
                else:
                    ticker_obj = analyzer_output.get(ticker)
                    if ticker_obj is None:
                        logger.warning(f"No data found for ticker {ticker} in analyzer_output")
                        continue

                profile_data.append({
                    'Ticker': extract_value(ticker_obj, 'ticker'),
                    'Chairman (Birth Year) - Salary': f"{extract_value(ticker_obj, 'chairman')} "
                                                      f"({extract_value(ticker_obj, 'chairman_year')}) - "
                                                      f"${extract_value(ticker_obj, 'chairman_salary')}",
                    'Director (Birth Year) - Salary': f"{extract_value(ticker_obj, 'director')} "
                                                      f"({extract_value(ticker_obj, 'director_year')}) - "
                                                      f"${extract_value(ticker_obj, 'director_salary')}",
                    'CEO (Birth Year) - Salary': f"{extract_value(ticker_obj, 'ceo')} "
                                                 f"({extract_value(ticker_obj, 'ceo_year')}) - "
                                                 f"${extract_value(ticker_obj, 'ceo_salary')}",
                    'CFO (Birth Year) - Salary': f"{extract_value(ticker_obj, 'cfo')} "
                                                 f"({extract_value(ticker_obj, 'cfo_year')}) - "
                                                 f"${extract_value(ticker_obj, 'cfo_salary')}",
                    'CLO (Birth Year) - Salary': f"{extract_value(ticker_obj, 'clo')} "
                                                 f"({extract_value(ticker_obj, 'clo_year')}) - "
                                                 f"${extract_value(ticker_obj, 'clo_salary')}",
                    'CMO (Birth Year) - Salary': f"{extract_value(ticker_obj, 'cmo')} "
                                                 f"({extract_value(ticker_obj, 'cmo_year')}) - "
                                                 f"${extract_value(ticker_obj, 'cmo_salary')}",
                    'COO (Birth Year) - Salary': f"{extract_value(ticker_obj, 'coo')} "
                                                 f"({extract_value(ticker_obj, 'coo_year')}) - "
                                                 f"${extract_value(ticker_obj, 'coo_salary')}",
                    'CSO (Birth Year) - Salary': f"{extract_value(ticker_obj, 'cso')} "
                                                 f"({extract_value(ticker_obj, 'cso_year')}) - "
                                                 f"${extract_value(ticker_obj, 'cso_salary')}"
                })

            df_profile = pd.DataFrame(profile_data).transpose()
            logger.info(f"Profile compilation completed.")

        if target.lower() in ['holders', 'all']:
            """
            Compiles major holders data into a DataFrame. This section extracts and organizes information about insider 
            shareholding, institutional shareholding, and the number of institutions holding shares. The DataFrame is 
            structured to provide insights into the ownership distribution of the company.
            """
            logger.info(f"Compiling holders data for {len(tickers)} tickers.")
            holders_data = []
            for ticker in tickers:
                if is_dataframe:
                    ticker_obj = analyzer_output[analyzer_output['ticker'] == ticker]
                    if ticker_obj.empty:
                        logger.warning(f"No data found for ticker {ticker} in analyzer_output")
                        continue
                else:
                    ticker_obj = analyzer_output.get(ticker)
                    if ticker_obj is None:
                        logger.warning(f"No data found for ticker {ticker} in analyzer_output")
                        continue

                holders_data.append({
                    'Ticker': extract_value(ticker_obj, 'ticker'),
                    '% Shares (Insider)': extract_value(ticker_obj, 'insider_shares_hold'),
                    '% Shares (Institution)': extract_value(ticker_obj, 'institution_shares_hold'),
                    '% Float (Institution)': extract_value(ticker_obj, 'institution_float_hold'),
                    '# of Institutions Holding Shares': extract_value(ticker_obj, 'num_institution_holding_shares')
                })

            df_holders = pd.DataFrame(holders_data).transpose()
            logger.info(f'Holders compilation completed.')

        if target.lower() in ['insider transactions', 'all']:
            """
            Compiles insider transactions data into a DataFrame. This section focuses on insider trading activities, 
            including net shares purchased, sold, and the overall net change. The DataFrame is structured to highlight 
            insider trading patterns and trends within the company.
            """
            logger.info(f"Compiling insider transactions data for {len(tickers)} tickers.")
            insider_transactions_data = []
            for ticker in tickers:
                if is_dataframe:
                    ticker_obj = analyzer_output[analyzer_output['ticker'] == ticker]
                    if ticker_obj.empty:
                        logger.warning(f"No data found for ticker {ticker} in analyzer_output")
                        continue
                else:
                    ticker_obj = analyzer_output.get(ticker)
                    if ticker_obj is None:
                        logger.warning(f"No data found for ticker {ticker} in analyzer_output")
                        continue

                insider_transactions_data.append({
                    'Ticker': extract_value(ticker_obj, 'ticker'),
                    'Total Insider Shared Held': extract_value(ticker_obj, 'total_insider_shares_held'),
                    'Net Shares Purchased': extract_value(ticker_obj, 'net_shares_purchased'),
                    'Net Shares Sold': extract_value(ticker_obj, 'net_shares_sold'),
                    'Net Shares Change': extract_value(ticker_obj, 'net_shares_change'),
                    '% Net Shares Change': extract_value(ticker_obj, 'percent_net_shares_change'),
                    'Purchase Transactions': extract_value(ticker_obj, 'purchase_transactions'),
                    'Sell Transactions': extract_value(ticker_obj, 'sell_transactions'),
                    'Net Transactions': extract_value(ticker_obj, 'net_transactions')
                })

            df_insider_transactions = pd.DataFrame(insider_transactions_data).transpose()
            logger.info(f"Insider transactions compilation completed.")

        return df_fundamentals, df_profile, df_holders, df_insider_transactions


class Exporter:
    """
    The Compiler class is responsible for organizing and visualizing the analyzed financial data. It transforms
    the data from the Analyzer class or a CSV file into structured DataFrames that can be easily reviewed or
    exported. The class provides functionality to compile various types of financial data, such as fundamentals,
    profile data, holders data, and insider transactions, into a user-friendly format.

    Methods:
    - compile(analyzer_output_or_filepath, target='fundamentals'): Compiles the analyzed data into a
    structured DataFrame.
    """

    @staticmethod
    def export_to_csv(ticker_string, analyzer_instance, filepath, mode='write'):
        """
        Exports the analyzed data from the Analyzer instance to a CSV file. The user can choose to either write a new
        file or append and update an existing file.

        :param ticker_string: A string of space-separated ticker symbols.
        :type ticker_string: str
        :param analyzer_instance: An instance of the Analyzer class containing analyzed data.
        :type analyzer_instance: dict
        :param filepath: The file path where the CSV should be saved.
        :type filepath: str
        :param mode: The mode of export: 'write' to create a new file or overwrite existing data, 'append' to update
        existing data.
        :type mode: str, optional
        :return: None
        :rtype: None
        """
        logger.info(f'Starting export to CSV for tickers: {ticker_string}')
        data_to_export = []

        # Split the ticker_string into individual tickers
        tickers = ticker_string.split()

        # Iterate through each ticker in the ticker_string
        for ticker in tickers:
            ticker_obj = analyzer_instance.get(ticker)
            if ticker_obj:
                logger.info(f"Processing {ticker} for CSV export.")
                ticker_data = {
                    # Information
                    'ticker': ticker_obj.get_attr('ticker'),

                    # Intraday Data
                    'price': ticker_obj.get_attr('price'),
                    'name': ticker_obj.get_attr('name'),
                    'change_intraday': ticker_obj.get_attr('change_intraday'),
                    'change_afterhours': ticker_obj.get_attr('change_afterhours'),

                    # Data Availability
                    'summary_availability': ticker_obj.get_attr('summary_availability'),
                    'fs_availability': ticker_obj.get_attr('fs_availability'),
                    'latest_10Q': ticker_obj.get_attr('latest_10Q'),
                    'latest_10K': ticker_obj.get_attr('latest_10K'),

                    # Overview
                    'forward_dividend_and_yield': ticker_obj.get_attr('forward_dividend_and_yield'),
                    'market_cap': ticker_obj.get_attr('market_cap'),
                    'eps': ticker_obj.get_attr('eps'),
                    'diluted_eps': ticker_obj.get_attr('diluted_eps'),

                    # Valuation
                    'price_to_book': ticker_obj.get_attr('price_to_book'),
                    'price_to_sales': ticker_obj.get_attr('price_to_sales'),
                    'price_to_earnings': ticker_obj.get_attr('price_to_earnings'),
                    'price_to_cash_flow': ticker_obj.get_attr('price_to_cash_flow'),

                    # Growth
                    'revenue_growth': ticker_obj.get_attr('revenue_growth'),
                    'operating_income_growth': ticker_obj.get_attr('operating_income_growth'),
                    'net_income_growth': ticker_obj.get_attr('net_income_growth'),
                    'diluted_eps_growth': ticker_obj.get_attr('diluted_eps_growth'),

                    # Financial Strength
                    'quick_ratio': ticker_obj.get_attr('quick_ratio'),
                    'current_ratio': ticker_obj.get_attr('current_ratio'),
                    'interest_coverage': ticker_obj.get_attr('interest_coverage'),
                    'debt_to_equity': ticker_obj.get_attr('debt_to_equity'),

                    # Profitability
                    'return_on_assets': ticker_obj.get_attr('return_on_assets'),
                    'return_on_equity': ticker_obj.get_attr('return_on_equity'),
                    'return_on_invested_capital': ticker_obj.get_attr('return_on_invested_capital'),
                    'profit_margin': ticker_obj.get_attr('profit_margin'),

                    # Additional Financial Data
                    'operating_cash_flow': ticker_obj.get_attr('operating_cash_flow'),
                    'market_cap_float': ticker_obj.get_attr('market_cap_float'),
                    'tangible_book_value': ticker_obj.get_attr('tangible_book_value'),
                    'total_assets': ticker_obj.get_attr('total_assets'),
                    'calculation_mode': ticker_obj.get_attr('calculation_mode'),

                    # Executive Information (Profile Data)
                    'chairman': ticker_obj.get_attr('chairman'),
                    'chairman_year': ticker_obj.get_attr('chairman_year'),
                    'chairman_salary': ticker_obj.get_attr('chairman_salary'),
                    'director': ticker_obj.get_attr('director'),
                    'director_year': ticker_obj.get_attr('director_year'),
                    'director_salary': ticker_obj.get_attr('director_salary'),
                    'ceo': ticker_obj.get_attr('ceo'),
                    'ceo_year': ticker_obj.get_attr('ceo_year'),
                    'ceo_salary': ticker_obj.get_attr('ceo_salary'),
                    'cfo': ticker_obj.get_attr('cfo'),
                    'cfo_year': ticker_obj.get_attr('cfo_year'),
                    'cfo_salary': ticker_obj.get_attr('cfo_salary'),
                    'clo': ticker_obj.get_attr('clo'),
                    'clo_year': ticker_obj.get_attr('clo_year'),
                    'clo_salary': ticker_obj.get_attr('clo_salary'),
                    'cmo': ticker_obj.get_attr('cmo'),
                    'cmo_year': ticker_obj.get_attr('cmo_year'),
                    'cmo_salary': ticker_obj.get_attr('cmo_salary'),
                    'coo': ticker_obj.get_attr('coo'),
                    'coo_year': ticker_obj.get_attr('coo_year'),
                    'coo_salary': ticker_obj.get_attr('coo_salary'),
                    'cso': ticker_obj.get_attr('cso'),
                    'cso_year': ticker_obj.get_attr('cso_year'),
                    'cso_salary': ticker_obj.get_attr('cso_salary'),

                    # Insider Transactions
                    'total_insider_shares_held': ticker_obj.get_attr('total_insider_shares_held'),
                    'net_shares_purchased': ticker_obj.get_attr('net_shares_purchased'),
                    'net_shares_sold': ticker_obj.get_attr('net_shares_sold'),
                    'net_shares_change': ticker_obj.get_attr('net_shares_change'),
                    'percent_net_shares_change': ticker_obj.get_attr('percent_net_shares_change'),
                    'purchase_transactions': ticker_obj.get_attr('purchase_transactions'),
                    'sell_transactions': ticker_obj.get_attr('sell_transactions'),
                    'net_transactions': ticker_obj.get_attr('net_transactions'),

                    # Holders Data
                    'insider_shares_hold': ticker_obj.get_attr('insider_shares_hold'),
                    'institution_shares_hold': ticker_obj.get_attr('institution_shares_hold'),
                    'institution_float_hold': ticker_obj.get_attr('institution_float_hold'),
                    'num_institution_holding_shares': ticker_obj.get_attr('num_institution_holding_shares')
                }
                data_to_export.append(ticker_data)
            else:
                logger.warning(f"No data found for ticker {ticker} in analyzer_instance")

        # Convert the new data to a DataFrame
        df_new_data = pd.DataFrame(data_to_export)

        if mode == 'append' and os.path.exists(filepath):
            logger.info(f"Appending to existing CSV file {filepath}.")
            df_existing = pd.read_csv(filepath)

            # Combine new data with the existing data, aligning columns
            df_combined = df_existing.set_index('ticker').combine_first(df_new_data.set_index('ticker')).reset_index()

            # Save the updated DataFrame back to the CSV file
            df_combined.to_csv(filepath, index=False)
            logger.info(f"Data successfully appended to {filepath}")
        else:
            # If mode is 'write' or file does not exist, create or overwrite the CSV file
            df_new_data.to_csv(filepath, index=False)
            logger.info(f"Data successfully written to {filepath}")
