import pandas as pd
from TickerClass import Ticker

pd.options.display.float_format = '{:.0f}'.format
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.option_context('display.precision', 5)


csv_data_import = open("fiscrape_export.txt", "r")
raw_data_import = pd.read_csv(csv_data_import, index_col=0)
df_data_import = pd.DataFrame(raw_data_import).transpose()


# Look-up command
def df_look_up(ticker, row_index):
    return df_data_import[ticker][row_index - 1]


print(df_data_import)

# List of analysed tickers
ticker_list = df_data_import.columns.to_list()[1:]

ticker_class = {str(ticker_list[i]): Ticker(ticker_list[i],
                                            price=df_data_import[ticker_list[i]].iloc[5],
                                            dividend=df_data_import[ticker_list[i]].iloc[6],
                                            market_cap=df_data_import[ticker_list[i]].iloc[7],
                                            eps=df_data_import[ticker_list[i]].iloc[8]
                                            ) for i in range(len(ticker_list))}

# Temporarily not finished. Will be updated...
