# Create a Ticker class to populate tickers with appropriate links from Yahoo Finance
class Ticker:
    def __init__(self, ticker, **kwargs):
        self.ticker = ticker
        self.set_attr(**kwargs)
        self.data = {}

    def set_attr(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def get_attr(self):
        for attr in dir(self):
            if not attr.startswith('__') and attr not in ['data', 'set_attr', 'get_attr', 'get_summary_link',
                                                          'get_statistics_link', 'get_financials_link']:
                self.data.update({attr: [getattr(self, attr)]})
        return self.data

    def get_summary_link(self):
        return f'https://finance.yahoo.com/quote/{self.ticker}?p={self.ticker}'

    def get_statistics_link(self):
        return f'https://finance.yahoo.com/quote/{self.ticker}/key-statistics?p={self.ticker}'

    def get_financials_link(self, doc_type):
        return f'https://finance.yahoo.com/quote/{self.ticker}/{doc_type}?p={self.ticker}'
