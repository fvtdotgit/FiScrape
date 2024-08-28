"""
This is an example of using FiScrape to obtain financial data. As you can see, I am not a front end person. Maybe
someday I will be. Until then, have fun with this!

Also, before you get started, be aware that this uses Mozilla Firefox so install it, please, please, please.
You will also need geckodriver too at https://github.com/mozilla/geckodriver/releases.
"""

import FiScrape_Core
import logging
import time

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)  # Enable debug logging

    # Your inputs
    tickers = input(str('Input your tickers separated by spaces here (e.g. AAPL AXP V): ')).upper()
    targets = (input(str("Choose between 'fundamentals,' 'holders,' 'insider transactions,' 'profile,' or 'all': "))
               .lower())
    export_path = input(str('Choose location for export (e.g. full_output.csv): '))
    recommendation = input(str('yes or no recommendation? ')).lower()

    # Initialize all modules used
    start = time.time()
    scraper = FiScrape_Core.Scraper()
    analyzer = FiScrape_Core.Analyzer()
    exporter = FiScrape_Core.Exporter()
    compiler = FiScrape_Core.Compiler()

    # Obtain recommendations for the very first ticker inputted
    if recommendation == 'yes':
        tickers = scraper.obtain_recommendation(tickers, 3)
        print('Your recommended tickers: ' + tickers)
    else:
        pass

    # Scrape (adjust max capacity to your liking, go over 1 at your own risk)

    scraped = scraper.scrape(tickers, targets, 0.75)

    # For financial_data_period, choose between 'TTM' or '10K'
    analyzed = analyzer.analyze(tickers, scraped, targets, 'TTM')

    # Export to csv for later reference. For mode, you can either 'write' or 'append' to your csv
    exporter.export_to_csv(tickers, analyzed, export_path, 'write')

    # Import and analyze
    compiled = compiler.compile(export_path, targets)
    print(compiled)

    end = time.time()

    print(f'This scraping took {end - start} seconds')