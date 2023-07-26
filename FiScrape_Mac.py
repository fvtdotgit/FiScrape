# Last updated: 20230726 by FVT  (fvtdotgit)

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
import numpy as np
import datetime
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

# Choose a browser of your choice if needed
driver = webdriver.Safari()
driver.maximize_window()

pd.options.display.float_format = '{:.0f}'.format
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.option_context('display.precision', 5)

# Data storage boxes
ticker_store = ["Ticker"]
realtime_price_store = ["Price/Share ($)"]
yield_store = ["Forward Dividend & Yield"]
market_cap_store = ["Market Cap"]

eps_store = ["EPS (TTM)"]
diluted_eps_store = ["Diluted EPS"]

price_to_book_store = ["Price/Book"]
price_to_cash_flow_store = ["Price/Cash Flow"]
price_to_sales_store = ["Price/Sale"]
price_to_earnings_store = ["Price/Earnings"]

rev_3yr_growth_store = ["Rev 3-Yr Growth (%)"]
oi_3yr_growth_store = ["Operating Income 3-Yr Growth (%)"]
ni_3yr_growth_store = ["Net Income 3-Yr Growth (%)"]
diluted_eps_growth_store = ["Diluted EPS Growth (%)"]

current_ratio_store = ["Current Ratio"]
quick_ratio_store = ["Quick Ratio"]
interest_coverage_store = ["Interest Coverage"]
debt_to_equity_store = ["Debt/Equity"]

return_on_equity_store = ["Return on Equity (%)"]
return_on_assets_store = ["Return on Assets (%)"]
roic_store = ["Return on Invested Capital (%)"]
profit_margin_store = ["Profit Margin (%)"]

summary_availability_store = ["Summary"]
statistics_availability_store = ["Statistics"]
fs_availability_store = ["Financial Statement(s)"]

print("""
Welcome to Financial Scrape! I can help you pull financial information from Yahoo Finance,
as well as automating calculations of financial ratios.

Note 1: please observe your web browser while the program is running,
or the financial documents on Yahoo Finance will not expand properly.
""")

can_input = True

while can_input:
    # Stock ticker input
    print("---")
    print("")
    ticker = input("Enter ticker(s) with spaces in between them (e.g. AAPL AXP META): ").split()
    print("")
    print_boolean = input("Print financial statements (yes/no): ")
    print("")
    sleep_time = input("Enter average time for a website to load completely in seconds (e.g. 2): ")
    print("")
    print("---")

    for ticker_index in ticker:

        print("")
        print("STOCK TICKER: " + ticker_index)
        print("")

        # Summary page to html soup converter
        summary_link = 'https://finance.yahoo.com/quote/' + ticker_index + '?p=' + ticker_index
        driver.get(summary_link)
        html = driver.execute_script('return document.body.innerHTML;')
        soup = BeautifulSoup(html, 'lxml')

        time.sleep(float(sleep_time))

        # Real time price
        realtime_price = [entry.text for entry in soup.find_all('fin-streamer', class_='Fw(b) Fz(36px) Mb(-4px) D(ib)')]
        today_change = [entry.text for entry in soup.find_all('fin-streamer', class_='Fw(500) Pstart(8px) Fz(24px)')]
        now = datetime.datetime.now()

        try:
            realtime_price[0]
        except IndexError:
            print("Yahoo Finance may have redirected you to the mobile version instead of the web version. If this is "
                  "the case, you will need to restart FiScrape.")

        if not realtime_price[0]:
            realtime_price = ["N/A"]

        print(now)
        print(str(realtime_price) + str(today_change))

        # Summary table generator
        summary_header = [entry.text for entry in soup.find_all('td', class_='C($primaryColor) W(51%)')]
        summary_content = [entry.text for entry in soup.find_all('td', class_='Ta(end) Fw(600) Lh(14px)')]

        raw_summary_table = []

        def summary_combined(sum_iteration):
            return [summary_header[sum_iteration], summary_content[sum_iteration]]

        for summary_n_row in range(1, len(summary_header)):
            raw_summary_table.append(summary_combined(summary_n_row))

        df_summary_table = pd.DataFrame(raw_summary_table)

        # Boolean statement to be printed in data storage table
        if realtime_price == ["N/A"]:
            summary_availability_store.append("x")
        else:
            summary_availability_store.append("✓")

        # Summary output
        if print_boolean.lower() == "yes" and summary_availability_store[-1] == "✓":
            print("")
            print("REAL TIME SUMMARY")
            print("")
            print(df_summary_table.to_string(index=True, header=True))

        # Statistics page to html soup converter
        statistics_link = 'https://finance.yahoo.com/quote/' + ticker_index + '/key-statistics?p=' \
                          + ticker_index
        driver.get(statistics_link)
        html = driver.execute_script('return document.body.innerHTML;')
        soup = BeautifulSoup(html, 'lxml')

        time.sleep(float(sleep_time))

        # Identifying whether statistics information is available
        statistics_label = [entry.text for entry in soup.find_all('li', class_='IbBox Fw(500) fin-tab-item H(44px) '
                                                                               'desktop_Bgc($hoverBgColor):h '
                                                                               'desktop-lite_Bgc($hoverBgColor):h '
                                                                               'selected')]

        # Statistics table generator
        if statistics_label[0] == "Statistics":

            # Generating the header for the statistics section
            statistics_header = [entry.text for entry in soup.find_all('th', class_='Fw(b) Ta(c) Pstart(6px) Pend(4px) '
                                                                                    'Py(6px) Miw(fc) Miw(fc)--pnclg')]
            statistics_header.insert(0, "Breakdown")

            # Generating the statistics table
            statistics_features = soup.find_all('tr', class_='Bxz(bb)')

            def content(content_iteration):
                return [entry.text for entry in
                        statistics_features[content_iteration].find_all('td', class_='fi-row:h_Bgc('
                                                                                     '$hoverBgColor)')]

            raw_statistics_table = [statistics_header]  # Note: the statistics table begins with the header

            for statistics_iteration in range(9):  # There are always 9 rows in "Statistics"
                raw_statistics_table.append(content(statistics_iteration))

            df_statistics_table = pd.DataFrame(raw_statistics_table)

            # Generating the statistics tradition information and financial highlights
            def statistics_content(content_iteration):
                return [entry.text for entry in statistics_features[content_iteration]]

            raw_statistics = []  # The trading information and financial highlights start out empty

            for statistics_iteration in range(9, 60):
                raw_statistics.append(statistics_content(statistics_iteration))

            df_statistics_info = pd.DataFrame(raw_statistics)

            statistics_availability_store.append("✓")

            # Statistics output
            if print_boolean.lower() == "yes" and statistics_availability_store[-1] == "✓":
                print("")
                print("REAL TIME & HISTORIC STATISTICS")
                print("")
                print(df_statistics_table.to_string(index=True, header=True))
                print("")
                print(df_statistics_info.to_string(index=True, header=True))

            # Function to search for summary or statistics data
            def search_sum_stat_parameter(df_example, parameter, column):
                if not df_example[df_example[0].str.match(parameter)].values.tolist()[0][0]:
                    return "---"
                else:
                    return df_example[df_example[0].str.match(parameter)].values.tolist()[0][column] \
                        .replace("N/A", "---")
                    # Note: for more historical data in the following columns, change the last value to 2, 3, or 4

            # Function to convert millions, billions, and trillions abbreviations into numerical values
            # Note: this can only be used for the statistics section
            def abbr_to_number(number_example):
                if "M" in number_example:
                    return float(number_example.replace("M", "000000")) * 10 ** 6
                elif "B" in number_example:
                    return float(number_example.replace("B", "000000000")) * 10 ** 9
                elif "T" in number_example:
                    return float(number_example.replace("T", "000000000000")) * 10 ** 12

            # Search and calculate financial document data
            market_cap = abbr_to_number(search_sum_stat_parameter(df_statistics_table, "Market Cap", 1))
            operating_cash_flow = abbr_to_number(search_sum_stat_parameter
                                                 (df_statistics_info, "Operating Cash Flow", 1))

            if market_cap and operating_cash_flow and operating_cash_flow != 0:
                price_to_cash_flow = str(round(market_cap / operating_cash_flow, 2))
            else:
                price_to_cash_flow = "---"

            # Appending pre-calculated data to data storage
            market_cap_store.append(str(market_cap))

            price_to_book_store.append(search_sum_stat_parameter(df_statistics_table, "Price/Book", 1))
            price_to_sales_store.append(search_sum_stat_parameter(df_statistics_table, "Price/Sales", 1))
            price_to_earnings_store.append(search_sum_stat_parameter(df_statistics_table, "Trailing P/E", 1))
            price_to_cash_flow_store.append(price_to_cash_flow)

            diluted_eps_store.append(search_sum_stat_parameter(df_statistics_info, "Diluted EPS", 1))

            current_ratio_store.append(search_sum_stat_parameter(df_statistics_info, "Current Ratio", 1))

            return_on_assets_store.append(search_sum_stat_parameter(df_statistics_info, "Return on Assets", 1))
            return_on_equity_store.append(search_sum_stat_parameter(df_statistics_info, "Return on Equity", 1))
            profit_margin_store.append(search_sum_stat_parameter(df_statistics_info, "Profit Margin", 1))

        else:
            statistics_availability_store.append("x")

            price_to_book_store.append("---")
            price_to_sales_store.append("---")
            # Note: price-to-earnings store appendment for negative cases is present below
            price_to_cash_flow_store.append("---")

            diluted_eps_store.append("---")

            current_ratio_store.append("---")

            return_on_assets_store.append("---")
            return_on_equity_store.append("---")
            profit_margin_store.append("---")

        # Types of financial documents to generate
        df_income_statement = []
        df_balance_sheet = []
        df_cash_flow = []

        document = ['financials', 'balance-sheet', 'cash-flow']

        for doc_type in range(len(document)):
            # Financial statement pages to html converter
            fs_link = 'https://finance.yahoo.com/quote/' + ticker_index + '/' + document[
                int(doc_type)] + '?p=' + ticker_index
            driver.get(fs_link)
            html = driver.execute_script('return document.body.innerHTML;')
            soup = BeautifulSoup(html, 'lxml')

            time.sleep(float(sleep_time))

            # Identifying whether financial information is available
            financial_label = [entry.text for entry in soup.find_all('li', class_='IbBox Fw(500) fin-tab-item H(44px) '
                                                                                  'desktop_Bgc($hoverBgColor):h '
                                                                                  'desktop-lite_Bgc($hoverBgColor):h '
                                                                                  'selected')]

            # Clicking button module and financial table generator
            if financial_label[0] == "Financials":
                clickable = WebDriverWait(driver, 10).until(
                    ec.element_to_be_clickable(
                        (By.XPATH, "//section[@data-test='qsp-financial']//span[text()='Expand All']")))

                clickable.click()

                html_expanded = driver.execute_script('return document.body.innerHTML;')
                soup_expanded = BeautifulSoup(html_expanded, 'lxml')

                time.sleep(float(sleep_time))

                fs_features = soup_expanded.find_all('div', class_='D(tbr)')

                # Generating the header for the financial statements
                fs_header = [entry.text for entry in fs_features[0].find_all('div', class_='D(ib)')]
                raw_fs_table = [fs_header]  # Note: The raw financial table begins with the headers

                def fs_content(content_iteration):
                    return [entry.text for entry in fs_features[content_iteration].find_all('div', class_='D(tbc)')]

                for fs_iteration in range(1, len(fs_features)):
                    raw_fs_table.append(fs_content(fs_iteration))

                df_financial_statement = pd.DataFrame(raw_fs_table)

                if doc_type == 0:
                    df_income_statement.append(df_financial_statement)
                    print("")
                    print("INCOME STATEMENT: Completed")
                    doc_type += 1
                    continue
                elif doc_type == 1:
                    df_balance_sheet.append(df_financial_statement)
                    print("BALANCE SHEET: Completed")
                    doc_type += 1
                    continue
                elif doc_type == 2:
                    df_cash_flow.append(df_financial_statement)
                    print("CASH FLOW: Completed")

                    fs_availability_store.append("✓")

                # Functions to search for financial statement data
                def search_fs_parameter(df_example, parameter, column):
                    if not df_example[0][df_example[0][0].str.match(parameter)].values.tolist():
                        return "Null"  # Should be 0 theoretically but would run into dividing by 0...
                    else:
                        return df_example[0][df_example[0][0].str.match(parameter)].values.tolist()[0][column]
                        # Note: for more historical data in the following columns, change the last value to 2, 3, or 4

                # Convert comma-split numbers into numbers
                def join_comma(comma_number):
                    if comma_number == "Null":
                        return "Null"
                    else:
                        return float(comma_number.replace(',', ''))

                # Financial document data search
                total_revenue = join_comma(search_fs_parameter(df_income_statement, "Total Revenue", 1))
                total_revenue03 = join_comma(search_fs_parameter(df_income_statement, "Total Revenue", 5))

                print(total_revenue)
                print(total_revenue03)

                operating_income = join_comma(search_fs_parameter(df_income_statement, "Operating Income", 1))
                operating_income03 = join_comma(search_fs_parameter(df_income_statement, "Operating Income", 5))

                net_income = join_comma(search_fs_parameter(df_income_statement, "Net Income", 1))
                net_income03 = join_comma(search_fs_parameter(df_income_statement, "Net Income", 5))

                diluted_eps = join_comma(search_fs_parameter(df_income_statement, "Diluted EPS", 2))
                diluted_eps03 = join_comma(search_fs_parameter(df_income_statement, "Diluted EPS", 5))

                current_assets = join_comma(search_fs_parameter(df_balance_sheet, "Current Assets", 1))
                current_liabilities = join_comma(search_fs_parameter(df_balance_sheet, "Current Liabilities", 1))
                inventory = join_comma(search_fs_parameter(df_balance_sheet, "Inventory", 1))

                EBIT = join_comma(search_fs_parameter(df_income_statement, "EBIT", 1))
                interest_expense = join_comma(search_fs_parameter(df_income_statement, "Interest Expense", 1))

                total_debt = join_comma(search_fs_parameter(df_balance_sheet, "Total Debt", 1))
                stockholders_equity = join_comma(search_fs_parameter(df_balance_sheet, "Stockholders\' Equity", 1))

                tax_provision = join_comma(search_fs_parameter(df_income_statement, "Tax Provision", 1))
                invested_capital = join_comma(search_fs_parameter(df_balance_sheet, "Invested Capital", 1))

                # Financial document data calculations
                if total_revenue != "Null" and total_revenue03 not in ["Null", 0]:
                    rev_3yr_growth = str(round((np.cbrt(total_revenue / total_revenue03) - 1) * 100, 2)) + '%'
                else:
                    rev_3yr_growth = "---"

                if operating_income != "Null" and operating_income03 not in ["Null", 0]:
                    oi_3yr_growth = str(round((np.cbrt(operating_income / operating_income03) - 1) * 100, 2)) + '%'
                else:
                    oi_3yr_growth = "---"

                if net_income != "Null" and net_income03 not in ["Null", 0]:
                    ni_3yr_growth = str(round((np.cbrt(net_income / net_income03) - 1) * 100, 2)) + '%'
                else:
                    ni_3yr_growth = "---"

                if diluted_eps != "Null" and diluted_eps03 not in ["Null", 0]:
                    diluted_eps_growth = str(round((np.cbrt(diluted_eps / diluted_eps03) - 1) * 100, 2)) + '%'
                else:
                    diluted_eps_growth = "---"

                if current_assets != "Null" and current_liabilities not in ["Null", 0] and inventory != "Null":
                    quick_ratio = str(round((current_assets - inventory) / current_liabilities, 2))
                else:
                    quick_ratio = "---"

                if EBIT != "Null" and interest_expense not in ["Null", 0]:
                    interest_coverage = str(round(EBIT / interest_expense, 2))
                else:
                    interest_coverage = "---"

                if total_debt != "Null" and stockholders_equity not in ["Null", 0]:
                    debt_to_equity = str(round(total_debt / stockholders_equity, 2))
                else:
                    debt_to_equity = "---"

                if EBIT != "Null" and tax_provision != "Null" and invested_capital not in ["Null", 0]:
                    return_on_invested_capital = str(round((EBIT - tax_provision) / invested_capital * 100, 2)) + '%'
                else:
                    return_on_invested_capital = "---"

                # Appending financial data and ratios to data storage
                rev_3yr_growth_store.append(rev_3yr_growth)
                oi_3yr_growth_store.append(oi_3yr_growth)
                ni_3yr_growth_store.append(ni_3yr_growth)
                diluted_eps_growth_store.append(diluted_eps_growth)
                quick_ratio_store.append(quick_ratio)
                interest_coverage_store.append(interest_coverage)
                debt_to_equity_store.append(debt_to_equity)
                roic_store.append(return_on_invested_capital)

                # Financial document back-up data search
                tangible_book_value = join_comma(search_fs_parameter(df_balance_sheet, "Tangible Book Value", 1))
                total_assets = join_comma(search_fs_parameter(df_balance_sheet, "Total Assets", 1))
                operating_cash_flow = join_comma(search_fs_parameter(df_cash_flow, "Operating Cash Flow", 1))

                # Back-up financial document data calculations and replacement (if statistics does not provide data)
                if diluted_eps_store[-1] == "---":
                    diluted_eps_store[-1] = join_comma(search_fs_parameter(df_income_statement, "Diluted EPS", 2))

                if price_to_book_store[-1] == "---" and market_cap_store[-1] != "---" \
                        and tangible_book_value not in ["---", 0]:
                    price_to_book = str(round(float(market_cap_store[-1]) / (tangible_book_value * 1000), 2))
                    price_to_book_store[-1] = price_to_book_store[-1].replace("---", price_to_book)

                if price_to_sales_store[-1] == "---" and market_cap_store[-1] != "---" \
                        and total_revenue not in ["---", 0]:
                    price_to_sales = str(round(float(market_cap_store[-1]) / (total_revenue * 1000), 2))
                    price_to_sales_store[-1] = price_to_sales_store[-1].replace("---", price_to_sales)

                if price_to_earnings_store[-1] == "---" and market_cap_store[-1] != "---" \
                        and net_income not in ["---", 0]:
                    price_to_earnings = str(round(float(market_cap_store[-1]) / (net_income * 1000), 2))
                    price_to_earnings_store[-1] = price_to_earnings_store[-1].replace("---", price_to_earnings)

                if price_to_cash_flow_store[-1] == "---" and market_cap_store[-1] != "---" \
                        and operating_cash_flow not in ["---", 0]:
                    price_to_cash_flow = str(round(float(market_cap_store[-1]) / (operating_cash_flow * 1000), 2))
                    price_to_cash_flow_store[-1] = price_to_cash_flow_store[-1].replace("---", price_to_cash_flow)

                if current_ratio_store[-1] == "---" and current_assets != "Null" and \
                        current_liabilities not in ["---", 0]:
                    current_ratio = str(round(current_assets / current_liabilities, 2))
                    current_ratio_store[-1] = current_ratio_store[-1].replace("---", current_ratio)

                if return_on_assets_store[-1] == "---" and net_income != "Null" \
                        and total_assets not in ["---", 0]:
                    return_on_assets = str(round(net_income / total_assets * 100, 2)) + "%"
                    return_on_assets_store[-1] = return_on_assets_store[-1].replace("---", return_on_assets)

                if return_on_equity_store[-1] == "---" and net_income != "Null" and \
                        stockholders_equity not in ["---", 0]:
                    return_on_equity = str(round(net_income / stockholders_equity * 100, 2)) + "%"
                    return_on_equity_store[-1] = return_on_equity_store[-1].replace("---", return_on_equity)

                if profit_margin_store[-1] == "0.00%" and net_income != "Null" and \
                        total_revenue not in ["---", 0]:
                    profit_margin = str(round(net_income / total_revenue * 100, 2)) + "%"
                    profit_margin_store[-1] = profit_margin_store[-1].replace("0.00%", profit_margin)

                # Financial statement output
                if print_boolean.lower() == "yes" and fs_availability_store[-1] == "✓":
                    print("")
                    print("INCOME STATEMENT")
                    print("")
                    print(df_income_statement[0].to_string(index=True, header=True))
                    print("")
                    print("BALANCE SHEET")
                    print("")
                    print(df_balance_sheet[0].to_string(index=True, header=True))
                    print("")
                    print("CASH FLOW")
                    print("")
                    print(df_cash_flow[0].to_string(index=True, header=True))

            else:
                print("")
                print("No financial documents available")

                # Appending empty financial ratios to data storage
                fs_availability_store.append("x")

                rev_3yr_growth_store.append("---")
                oi_3yr_growth_store.append("---")
                ni_3yr_growth_store.append("---")
                diluted_eps_growth_store.append("---")
                quick_ratio_store.append("---")
                interest_coverage_store.append("---")
                debt_to_equity_store.append("---")
                roic_store.append("---")

                break

        # Appending summary data to data storage
        def search_sum_stat_parameter(df_example, parameter, column):
            if not df_example[df_example[0].str.match(parameter)].values.tolist()[0][0]:
                return "---"
            else:
                return df_example[df_example[0].str.match(parameter)].values.tolist()[0][column]
                # Note: for more historical data in the following columns, change the last value to 2, 3, or 4

        ticker_store.append(ticker_index)
        realtime_price_store.extend(realtime_price)
        if statistics_label[0] == "Statistics":
            yield_store.append(search_sum_stat_parameter(df_summary_table, "Forward Dividend & Yield", 1))
            eps_store.append(search_sum_stat_parameter(df_summary_table, "EPS", 1))
        else:
            yield_store.append(search_sum_stat_parameter(df_summary_table, "Yield", 1))
            eps_store.append("---")
            try:
                price_to_earnings_store.append(search_sum_stat_parameter(df_summary_table, "PE Ratio", 1))
            except IndexError:
                price_to_earnings_store.append("---")

    # Data storage table generator
    raw_data_storage = [summary_availability_store, statistics_availability_store, fs_availability_store,
                        ticker_store, realtime_price_store, yield_store, diluted_eps_store,
                        price_to_book_store, price_to_sales_store, price_to_earnings_store, price_to_cash_flow_store,
                        rev_3yr_growth_store, oi_3yr_growth_store, ni_3yr_growth_store, diluted_eps_growth_store,
                        quick_ratio_store, current_ratio_store, interest_coverage_store, debt_to_equity_store,
                        return_on_assets_store, return_on_equity_store, roic_store, profit_margin_store]
    df_data_storage = pd.DataFrame(raw_data_storage)

    # Data storage output
    print("")
    print("DATA STORAGE")
    print("")
    print(df_data_storage.to_string(index=True, header=True))
    print("")
