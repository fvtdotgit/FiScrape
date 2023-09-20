# Last updated: 20230920 by FVT (fvtdotgit)

# If first time MacBook user, enter into Terminal (⌥ + F12), use pip or pip3 as needed:
#   1/ pip install pandas
#   2/ pip install numpy
#   3/ pip install beautifulsoup4
#   4/ pip install selenium
#   5/ pip install lxml
#   6/ safaridriver --enable (enter password from administrator's account)

# --- From this point below, everything should be fully automated ---

# Necessary databases for web scraping, tabling, and basic maths
import pandas as pd
from numpy import cbrt
from datetime import datetime
from time import time
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
import logging
from fiscrape_logger import logger

logging.basicConfig(filename='fiscrape_logging.log', filemode='w', level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')

logger.info('FiScrape: application initiated.')

pd.options.display.float_format = '{:.0f}'.format
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.option_context('display.precision', 5)

# Data storage boxes
ticker_store = ['Ticker']
summary_availability_store = ['Summary']
statistics_availability_store = ['Statistics']
fs_availability_store = ['Financial Statement(s)']
latest_10Q_store = ['Latest 10-Q']
latest_10K_store = ['Latest 10-K']

realtime_price_store = ['Price/Share ($)']
yield_store = ['Forward Dividend & Yield']
market_cap_store = ['Market Cap']
eps_store = ['EPS']
diluted_eps_store = ['Diluted EPS']

price_to_book_store = ['Price/Book']
price_to_cash_flow_store = ['Price/Cash Flow']
price_to_sales_store = ['Price/Sales']
price_to_earnings_store = ['Price/Earnings']

rev_3yr_growth_store = ['Rev 3-Yr Growth (%)']
oi_3yr_growth_store = ['Operating Income 3-Yr Growth (%)']
ni_3yr_growth_store = ['Net Income 3-Yr Growth (%)']
diluted_eps_growth_store = ['Diluted EPS Growth (%)']

current_ratio_store = ['Current Ratio']
quick_ratio_store = ['Quick Ratio']
interest_coverage_store = ['Interest Coverage']
debt_to_equity_store = ['Debt/Equity']

return_on_equity_store = ['Return on Equity (%)']
return_on_assets_store = ['Return on Assets (%)']
roic_store = ['Return on Invested Capital (%)']
profit_margin_store = ['Profit Margin (%)']


# Function to search for summary or statistics data
def search_sum_stat_parameter(df_example, parameter, column):
    if not df_example[df_example[0].str.match(parameter)].values.tolist():
        return '---'
    else:
        return df_example[df_example[0].str.match(parameter)].values.tolist()[0][column].replace('N/A', '---')
        # Note: for more historical data in the following columns, change the last value to 2, 3, or 4


# Functions to search for financial statement data
def search_fs_parameter(df_example, parameter, column):
    if not df_example[df_example[0].str.match(parameter)].values.tolist():
        return 'Null'  # Should be 0 theoretically but would run into dividing by 0...
    else:
        return df_example[df_example[0].str.match(parameter)].values.tolist()[0][column]
        # Note: for more historical data in the following columns, change the last value to 2, 3, or 4


# Function to convert millions, billions, and trillions abbreviations into numerical values
# Note: this can only be used for the statistics section
def abbr_to_number(number_example):
    if 'M' in number_example:
        return float(number_example.replace('M', '000000')) * 10 ** 6
    elif 'B' in number_example:
        return float(number_example.replace('B', '000000000')) * 10 ** 9
    elif 'T' in number_example:
        return float(number_example.replace('T', '000000000000')) * 10 ** 12


# Convert comma-split numbers into numbers
def join_comma(comma_number):
    if comma_number in ['Null', '-']:
        return 'Null'
    else:
        return float(comma_number.replace(',', ''))


print('''\nWelcome to Financial Scrape! I can help you pull financial information from Yahoo Finance,
as well as automating calculations of financial ratios.

Note: please observe your web browser while the program is running,
or the financial statements on Yahoo Finance will not expand properly.''')
print('\n---')

driver = webdriver.Safari()
driver.maximize_window()

while True:
    # Various inputs to configure FiScrape
    export_data_mode = input('\nTo append to current data or write new data to csv file export, enter \"A\" or \"W\". '
                             'To do neither, leave empty: ')
    ticker_list = input('\nEnter ticker(s) with spaces in between them (e.g. AAPL AXP META): ').split()
    print_boolean = input('\nPrint financial statements (yes/no): ')

    # Sleep time inputs and error handling to prevent letters or negative numeric values
    while True:
        sleep_time = input('\nEnter average time for a website to load completely in seconds (e.g. 2): ')
        try:
            float(sleep_time)
            assert float(sleep_time) >= 0
            break
        except ValueError:
            print('\nOnly numeric inputs are allowed. Please try again.')
        except AssertionError:
            print('\nOnly zero or positive values are allowed. Please try again.')

    print('\n---')

    for ticker in ticker_list:
        start = time()
        
        logger.info(f'{ticker}: analysis initiated.')

        print('\nTICKER: ' + ticker)

        # Error handling for unexpected redirection to Yahoo Finance mobile version
        while True:
            # Summary page to html soup converter
            summary_link = f'https://finance.yahoo.com/quote/{ticker}?p={ticker}'
            driver.get(summary_link)
            html = driver.execute_script('return document.body.innerHTML;')
            soup = BeautifulSoup(html, 'lxml')

            sleep(float(sleep_time))

            # Real time price (also a good test whether the web version is loaded)
            realtime_price = [entry.text for entry in soup.find_all('fin-streamer',
                                                                    class_='Fw(b) Fz(36px) Mb(-4px) D(ib)')]

            try:
                mobile_test = realtime_price[0]
                break
            except IndexError:
                logger.error(f'{ticker}: driver directed user to Yahoo Finance mobile version.')

                driver.close()
                driver = webdriver.Safari()
                driver.maximize_window()

        # Error handling for incorrectly typed ticker
        if not realtime_price[0]:
            logger.warning(f'{ticker}: potentially misspelled ticker.')

            realtime_price = ['---']

            sleep(float(sleep_time))

            continue

        today_change = [entry.text for entry in soup.find_all('fin-streamer', class_='Fw(500) Pstart(8px) Fz(24px)')]
        now = datetime.now()

        print('\n' + str(now))
        print(str(realtime_price) + str(today_change))

        # Summary table generator
        logger.info(f'{ticker}: summary scraping initiated.')

        summary_header = [entry.text for entry in soup.find_all('td', class_='C($primaryColor) W(51%)')]
        summary_content = [entry.text for entry in soup.find_all('td', class_='Ta(end) Fw(600) Lh(14px)')]

        raw_summary_table = [[summary_header[summary_iteration], summary_content[summary_iteration]]
                             for summary_iteration in range(1, len(summary_header))]

        df_summary_table = pd.DataFrame(raw_summary_table)

        logger.info(f'{ticker}: summary scraping completed.')

        print('\nSUMMARY: Completed')

        # Boolean statement to be printed in data storage table
        if realtime_price == ['N/A']:
            summary_availability_store.extend(['x'])
        else:
            summary_availability_store.extend(['✓'])

        # Summary output
        if print_boolean.lower() == 'yes' and summary_availability_store[-1] == '✓':
            print('\nREAL TIME SUMMARY')
            print('\n' + df_summary_table.to_string(index=True, header=False) + '\n')

        # Statistics page to html soup converter
        statistics_link = f'https://finance.yahoo.com/quote/{ticker}/key-statistics?p={ticker}'
        driver.get(statistics_link)
        html = driver.execute_script('return document.body.innerHTML;')
        soup = BeautifulSoup(html, 'lxml')

        sleep(float(sleep_time))

        # Identifying whether statistics information is available through the "Statistics" tab
        statistics_label = [entry.text for entry in soup.find_all('li', class_='IbBox Fw(500) fin-tab-item H(44px) '
                                                                               'desktop_Bgc($hoverBgColor):h '
                                                                               'desktop-lite_Bgc($hoverBgColor):h '
                                                                               'selected')]

        # Statistics table generator if "Statistics" tab is present
        if statistics_label[0] == 'Statistics':
            logger.info(f'{ticker}: statistics scraping initiated.')

            # Generating the header for the statistics section
            statistics_header = [entry.text for entry in soup.find_all('th', class_='Fw(b)')]
            statistics_header[0] = statistics_header[0].replace('Current', '')
            statistics_header.insert(0, 'Breakdown')

            # Generating the statistics table
            statistics_features = soup.find_all('tr', class_='Bxz(bb)')

            raw_statistics_table = [statistics_header]  # Note: the statistics table begins with the header

            raw_statistics_table.extend([[entry.text for entry in
                                          statistics_features[statistics_iteration].find_all
                                          ('td', class_='fi-row:h_Bgc(''$hoverBgColor)')]
                                         for statistics_iteration in range(9)])  # There are always 9 rows in statistics

            df_statistics_table = pd.DataFrame(raw_statistics_table)

            # Generating the statistics information and financial highlights

            raw_statistics_info = [[entry.text for entry in statistics_features[statistics_iteration]]
                                   for statistics_iteration in range(9, 60)]

            df_statistics_info = pd.DataFrame(raw_statistics_info)

            logger.info(f'{ticker}: statistics scraping completed.')

            print('STATISTICS: Completed')

            statistics_availability_store.extend(['✓'])

            # Statistics output
            if print_boolean.lower() == 'yes' and statistics_availability_store[-1] == '✓':
                print('\nREAL TIME & HISTORIC STATISTICS')
                print('\n' + df_statistics_table.to_string(index=True, header=False))
                print('\n' + df_statistics_info.to_string(index=True, header=False) + '\n')

            # Search and calculate financial statement data
            logger.info(f'{ticker}: statistics data processing initiated.')

            market_cap = abbr_to_number(search_sum_stat_parameter(df_statistics_table, 'Market Cap', 1))
            operating_cash_flow = abbr_to_number(search_sum_stat_parameter
                                                 (df_statistics_info, 'Operating Cash Flow', 1))

            if market_cap and operating_cash_flow and operating_cash_flow != 0:
                price_to_cash_flow = str(round(market_cap / operating_cash_flow, 2))
            else:
                price_to_cash_flow = '---'

            # Extending pre-calculated data to data storage
            latest_10Q_store.extend([df_statistics_table[2][0]])
            market_cap_store.extend([search_sum_stat_parameter(df_statistics_table, 'Market Cap', 1)])
            price_to_book_store.extend([search_sum_stat_parameter(df_statistics_table, 'Price/Book', 1)])
            price_to_sales_store.extend([search_sum_stat_parameter(df_statistics_table, 'Price/Sales', 1)])
            price_to_earnings_store.extend([search_sum_stat_parameter(df_statistics_table, 'Trailing P/E', 1)])
            price_to_cash_flow_store.extend([price_to_cash_flow])
            diluted_eps_store.extend([search_sum_stat_parameter(df_statistics_info, 'Diluted EPS', 1)])
            current_ratio_store.extend([search_sum_stat_parameter(df_statistics_info, 'Current Ratio', 1)])
            return_on_assets_store.extend([search_sum_stat_parameter(df_statistics_info, 'Return on Assets', 1)])
            return_on_equity_store.extend([search_sum_stat_parameter(df_statistics_info, 'Return on Equity', 1)])
            profit_margin_store.extend([search_sum_stat_parameter(df_statistics_info, 'Profit Margin', 1)])

            logger.info(f'{ticker}: statistics data processing completed.')

        else:
            print('STATISTICS: No statistics available')

            statistics_availability_store.extend(['x'])
            latest_10Q_store.extend(['---'])
            market_cap_store.extend(['---'])
            price_to_book_store.extend(['---'])
            price_to_sales_store.extend(['---'])
            # Note: Extending price-to-earnings store for negative cases is present below
            price_to_cash_flow_store.extend(['---'])
            diluted_eps_store.extend(['---'])
            current_ratio_store.extend(['---'])
            return_on_assets_store.extend(['---'])
            return_on_equity_store.extend(['---'])
            profit_margin_store.extend(['---'])

        # Types of financial statements to generate
        df_income_statement = []
        df_balance_sheet = []
        df_cash_flow = []

        logger.info(f'{ticker}: financial statements scraping initiated.')

        for document in ['financials', 'balance-sheet', 'cash-flow']:
            # Financial statement pages to html converter
            fs_link = (f'https://finance.yahoo.com/quote/{ticker}/'
                       f'{document}?p={ticker}')
            driver.get(fs_link)
            html = driver.execute_script('return document.body.innerHTML;')
            soup = BeautifulSoup(html, 'lxml')

            sleep(float(sleep_time))

            # Identifying whether financial information is available through the "Financials" tab
            financial_label = [entry.text for entry in soup.find_all('li', class_='IbBox Fw(500) fin-tab-item H(44px) '
                                                                                  'desktop_Bgc($hoverBgColor):h '
                                                                                  'desktop-lite_Bgc($hoverBgColor):h '
                                                                                  'selected')]

            # Clicking button module and financial table generator
            if financial_label[0] == 'Financials':
                clickable = WebDriverWait(driver, 10).until(
                    ec.element_to_be_clickable(
                        (By.XPATH, '//section[@data-test=\'qsp-financial\']//span[text()=\'Expand All\']')))

                clickable.click()

                html_expanded = driver.execute_script('return document.body.innerHTML;')
                soup_expanded = BeautifulSoup(html_expanded, 'lxml')

                fs_features = soup_expanded.find_all('div', class_='D(tbr)')

                # Generating the header for the financial statements
                fs_header = [entry.text for entry in fs_features[0].find_all('div', class_='D(ib)')]
                raw_fs_table = [fs_header]  # Note: The raw financial table begins with the headers

                # Generating the contents for the financial statements
                raw_fs_table.extend([entry.text for entry in
                                     fs_features[fs_iteration].find_all('div', class_='D(tbc)')]
                                    for fs_iteration in range(1, len(fs_features)))

                df_financial_statement = pd.DataFrame(raw_fs_table)

                if document in 'financials':
                    df_income_statement = df_financial_statement.copy()
                    continue
                elif document in 'balance-sheet':
                    df_balance_sheet = df_financial_statement.copy()
                    continue
                elif document in 'cash-flow':
                    df_cash_flow = df_financial_statement.copy()

                    logger.info(f'{ticker}: financial statements scraping completed.')

                    print('FINANCIAL STATEMENTS: Completed')

                    fs_availability_store.extend(['✓'])

                # Financial statement data search
                logger.info(f'{ticker}: financial statements data processing initiated.')

                # The try except block prevents out of range error when only 4 columns are displayed instead of 5.
                try:
                    total_revenue = join_comma(search_fs_parameter(df_income_statement, 'Total Revenue', 1))
                    total_revenue03 = join_comma(search_fs_parameter(df_income_statement, 'Total Revenue', 5))

                    operating_income = join_comma(search_fs_parameter(df_income_statement, 'Operating Income', 1))
                    operating_income03 = join_comma(search_fs_parameter(df_income_statement, 'Operating Income', 5))

                    net_income = join_comma(search_fs_parameter(df_income_statement, 'Net Income', 1))
                    net_income03 = join_comma(search_fs_parameter(df_income_statement, 'Net Income', 5))

                    diluted_eps = join_comma(search_fs_parameter(df_income_statement, 'Diluted EPS', 2))
                    diluted_eps03 = join_comma(search_fs_parameter(df_income_statement, 'Diluted EPS', 5))
                except IndexError:
                    logger.warning(f'{ticker}: insufficient historic data, 2-year TTM data provided in place of '
                                   f'3-year TTM data.')

                    total_revenue = join_comma(search_fs_parameter(df_income_statement, 'Total Revenue', 1))
                    total_revenue03 = join_comma(search_fs_parameter(df_income_statement, 'Total Revenue', 4))

                    operating_income = join_comma(search_fs_parameter(df_income_statement, 'Operating Income', 1))
                    operating_income03 = join_comma(search_fs_parameter(df_income_statement, 'Operating Income', 4))

                    net_income = join_comma(search_fs_parameter(df_income_statement, 'Net Income', 1))
                    net_income03 = join_comma(search_fs_parameter(df_income_statement, 'Net Income', 4))

                    diluted_eps = join_comma(search_fs_parameter(df_income_statement, 'Diluted EPS', 2))
                    diluted_eps03 = join_comma(search_fs_parameter(df_income_statement, 'Diluted EPS', 4))

                current_assets = join_comma(search_fs_parameter(df_balance_sheet, 'Current Assets', 1))
                current_liabilities = join_comma(search_fs_parameter(df_balance_sheet, 'Current Liabilities', 1))
                inventory = join_comma(search_fs_parameter(df_balance_sheet, 'Inventory', 1))

                EBIT = join_comma(search_fs_parameter(df_income_statement, 'EBIT', 1))
                interest_expense = join_comma(search_fs_parameter(df_income_statement, 'Interest Expense', 1))

                total_debt = join_comma(search_fs_parameter(df_balance_sheet, 'Total Debt', 1))
                stockholders_equity = join_comma(search_fs_parameter(df_balance_sheet, 'Stockholders\' Equity', 1))

                tax_provision = join_comma(search_fs_parameter(df_income_statement, 'Tax Provision', 1))
                invested_capital = join_comma(search_fs_parameter(df_balance_sheet, 'Invested Capital', 1))

                # Financial statement data calculations
                if total_revenue != 'Null' and total_revenue03 not in ['Null', 0]:
                    rev_3yr_growth = str(round((cbrt(total_revenue / total_revenue03) - 1) * 100, 2)) + '%'
                else:
                    rev_3yr_growth = '---'

                if operating_income != 'Null' and operating_income03 not in ['Null', 0]:
                    oi_3yr_growth = str(round((cbrt(operating_income / operating_income03) - 1) * 100, 2)) + '%'
                else:
                    oi_3yr_growth = '---'

                if net_income != 'Null' and net_income03 not in ['Null', 0]:
                    ni_3yr_growth = str(round((cbrt(net_income / net_income03) - 1) * 100, 2)) + '%'
                else:
                    ni_3yr_growth = '---'

                if diluted_eps != 'Null' and diluted_eps03 not in ['Null', 0]:
                    diluted_eps_growth = str(round((cbrt(diluted_eps / diluted_eps03) - 1) * 100, 2)) + '%'
                else:
                    diluted_eps_growth = '---'

                if current_assets != 'Null' and current_liabilities not in ['Null', 0] and inventory != 'Null':
                    quick_ratio = str(round((current_assets - inventory) / current_liabilities, 2))
                else:
                    quick_ratio = '---'

                if EBIT != 'Null' and interest_expense not in ['Null', 0]:
                    interest_coverage = str(round(EBIT / interest_expense, 2))
                else:
                    interest_coverage = '---'

                if total_debt != 'Null' and stockholders_equity not in ['Null', 0]:
                    debt_to_equity = str(round(total_debt / stockholders_equity, 2))
                else:
                    debt_to_equity = '---'

                if EBIT != 'Null' and tax_provision != 'Null' and invested_capital not in ['Null', 0]:
                    return_on_invested_capital = str(round((EBIT - tax_provision) / invested_capital * 100, 2)) + '%'
                else:
                    return_on_invested_capital = '---'

                # Extending financial data and ratios to data storage
                latest_10K_store.extend([df_financial_statement[2][0]])
                rev_3yr_growth_store.extend([rev_3yr_growth])
                oi_3yr_growth_store.extend([oi_3yr_growth])
                ni_3yr_growth_store.extend([ni_3yr_growth])
                diluted_eps_growth_store.extend([diluted_eps_growth])
                quick_ratio_store.extend([quick_ratio])
                interest_coverage_store.extend([interest_coverage])
                debt_to_equity_store.extend([debt_to_equity])
                roic_store.extend([return_on_invested_capital])

                # Financial statement back-up data search
                tangible_book_value = join_comma(search_fs_parameter(df_balance_sheet, 'Tangible Book Value', 1))
                total_assets = join_comma(search_fs_parameter(df_balance_sheet, 'Total Assets', 1))
                operating_cash_flow = join_comma(search_fs_parameter(df_cash_flow, 'Operating Cash Flow', 1))

                # Back-up financial statement data calculations and replacement (if statistics does not provide data)
                if diluted_eps_store[-1] == '---':
                    diluted_eps_store[-1] = join_comma(search_fs_parameter(df_income_statement, 'Diluted EPS', 2))
                    logger.info(f'{ticker}: TTD diluted EPS unavailable (alternative source: income statement).')

                if price_to_book_store[-1] == '---' and market_cap_store[-1] != '---' \
                        and tangible_book_value not in ['---', 0]:
                    price_to_book = str(round(float(abbr_to_number(market_cap_store[-1]))
                                              / (tangible_book_value * 1000), 2))
                    price_to_book_store[-1] = price_to_book_store[-1].replace('---', price_to_book)
                    logger.info(f'{ticker}: TTD price/book unavailable (alternative source: balance sheet).')

                if price_to_sales_store[-1] == '---' and market_cap_store[-1] != '---' \
                        and total_revenue not in ['---', 0]:
                    price_to_sales = str(round(float(abbr_to_number(market_cap_store[-1]))
                                               / (total_revenue * 1000), 2))
                    price_to_sales_store[-1] = price_to_sales_store[-1].replace('---', price_to_sales)
                    logger.info(f'{ticker}: TTD price/sales unavailable (alternative source: income statement).')

                if price_to_earnings_store[-1] == '---' and market_cap_store[-1] != '---' \
                        and net_income not in ['---', 0]:
                    price_to_earnings = str(round(float(abbr_to_number(market_cap_store[-1]))
                                                  / (net_income * 1000), 2))
                    price_to_earnings_store[-1] = price_to_earnings_store[-1].replace('---', price_to_earnings)
                    logger.info(f'{ticker}: TTD price/earnings unavailable (alternative source: income statement).')

                if price_to_cash_flow_store[-1] == '---' and market_cap_store[-1] != '---' \
                        and operating_cash_flow not in ['---', 0]:
                    price_to_cash_flow = str(round(float(abbr_to_number(market_cap_store[-1]))
                                                   / (operating_cash_flow * 1000), 2))
                    price_to_cash_flow_store[-1] = price_to_cash_flow_store[-1].replace('---', price_to_cash_flow)
                    logger.info(f'{ticker}: TTD price/cash flow unavailable (alternative source: cash flow).')

                if current_ratio_store[-1] == '---' and current_assets != 'Null' and \
                        current_liabilities not in ['---', 0]:
                    current_ratio = str(round(current_assets / current_liabilities, 2))
                    current_ratio_store[-1] = current_ratio_store[-1].replace('---', current_ratio)
                    logger.info(f'{ticker}: TTD current ratio unavailable (alternative source: balance sheet).')

                if return_on_assets_store[-1] == '---' and net_income != 'Null' \
                        and total_assets not in ['---', 0]:
                    return_on_assets = str(round(net_income / total_assets * 100, 2)) + '%'
                    return_on_assets_store[-1] = return_on_assets_store[-1].replace('---', return_on_assets)
                    logger.info(f'{ticker}: TTD return on assets unavailable '
                                f'(alternative source: income statement, balance sheet).')

                if return_on_equity_store[-1] == '---' and net_income != 'Null' and \
                        stockholders_equity not in ['---', 0]:
                    return_on_equity = str(round(net_income / stockholders_equity * 100, 2)) + '%'
                    return_on_equity_store[-1] = return_on_equity_store[-1].replace('---', return_on_equity)
                    logger.info(f'{ticker}: TTD return on equity unavailable '
                                f'(alternative source: income statement, balance sheet).')

                if profit_margin_store[-1] == '0.00%' and net_income != 'Null' and \
                        total_revenue not in ['---', 0]:
                    profit_margin = str(round(net_income / total_revenue * 100, 2)) + '%'
                    profit_margin_store[-1] = profit_margin_store[-1].replace('0.00%', profit_margin)
                    logger.info(f'{ticker}: TTD profit margin unavailable (alternative source: income statement).')

                # Financial statement output
                if print_boolean.lower() == 'yes' and fs_availability_store[-1] == '✓':
                    print('\nINCOME STATEMENT')
                    print('\n' + df_income_statement.to_string(index=True, header=False))
                    print('\nBALANCE SHEET')
                    print('\n' + df_balance_sheet.to_string(index=True, header=False))
                    print('\nCASH FLOW')
                    print('\n' + df_cash_flow.to_string(index=True, header=False) + '\n')

                logger.info(f'{ticker}: financial statements data processing concluded.')

            else:
                print('FINANCIAL STATEMENTS: No financial statements available')

                # Extending empty financial ratios to data storage
                fs_availability_store.extend(['x'])

                latest_10K_store.extend(['---'])
                rev_3yr_growth_store.extend(['---'])
                oi_3yr_growth_store.extend(['---'])
                ni_3yr_growth_store.extend(['---'])
                diluted_eps_growth_store.extend(['---'])
                quick_ratio_store.extend(['---'])
                interest_coverage_store.extend(['---'])
                debt_to_equity_store.extend(['---'])
                roic_store.extend(['---'])

                break

        # Extending summary data to data storage
        logger.info(f'{ticker}: data export initiated.')
        
        ticker_store.extend([ticker])
        realtime_price_store.extend(realtime_price)
        if statistics_label[0] == 'Statistics':
            yield_store.extend([search_sum_stat_parameter(df_summary_table, 'Forward Dividend & Yield', 1)])
            eps_store.extend([search_sum_stat_parameter(df_summary_table, 'EPS', 1)])
        else:
            yield_store.extend([search_sum_stat_parameter(df_summary_table, 'Yield', 1)])
            eps_store.extend(['---'])
            try:
                price_to_earnings_store.extend([search_sum_stat_parameter(df_summary_table, 'PE Ratio', 1)])
            except IndexError:
                price_to_earnings_store.extend(['---'])

        # Data storage table generator
        raw_data_export = [ticker_store,
                           summary_availability_store,
                           statistics_availability_store,
                           fs_availability_store,
                           latest_10Q_store,
                           latest_10K_store,

                           realtime_price_store,
                           yield_store,
                           market_cap_store,
                           eps_store,
                           diluted_eps_store,

                           price_to_book_store,
                           price_to_sales_store,
                           price_to_earnings_store,
                           price_to_cash_flow_store,

                           rev_3yr_growth_store,
                           oi_3yr_growth_store,
                           ni_3yr_growth_store,
                           diluted_eps_growth_store,

                           quick_ratio_store,
                           current_ratio_store,
                           interest_coverage_store,
                           debt_to_equity_store,

                           return_on_assets_store,
                           return_on_equity_store,
                           roic_store,
                           profit_margin_store]

        df_data_export = pd.DataFrame(raw_data_export)

        if export_data_mode.upper() == 'A':
            fiscrape_export = open('fiscrape_export.txt', 'a')
            fiscrape_export.write(df_data_export.transpose()[(len(ticker_store) - 1):].
                                  to_csv(index=False, header=False))
            fiscrape_export.close()

        elif export_data_mode.upper() == 'W':
            fiscrape_export = open('fiscrape_export.txt', 'w')
            fiscrape_export.write(df_data_export.transpose().to_csv(index=False, header=True))
            fiscrape_export.close()
            
        logger.info(f'{ticker}: data export concluded.')

        print('DATA EXPORT: Completed')
        print('\n---')

        # Data storage output
        if print_boolean == 'yes' and ticker == ticker_list[-1]:
            print('\nDATA EXPORT')
            print('\n' + df_data_export.to_string(index=True, header=False))
            
        end = time()
        
        logger.info(f'{ticker}: analysis concluded (total time: {end - start} seconds).')

    if input('\nDo you wish to append more data? Enter \"yes\" or \"no\": ').lower() == 'no':
        break

driver.quit()

print('\nPlease check fiscrape_logging.log for more detailed information on alternative calculations '
      'of certain financial ratios based on availability.')

logger.info('FiScrape: closing application.')
