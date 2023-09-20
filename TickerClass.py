class Ticker:
    def __init__(self, ticker, **kwargs):
        self.ticker = ticker
        self.set_attr(**kwargs)
        self.data = {}

    # Note: the following functions are currently unused
    def set_attr(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def get_attr(self):
        for attr in dir(self):
            if not attr.startswith('__') and attr not in ['data', 'set_attr', 'get_attr']:
                self.data.update({attr: [getattr(self, attr)]})
        return self.data

# Implement inheritance and superclass (Ticker => Stocks, ETF)
