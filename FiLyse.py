import pandas as pd

pd.options.display.float_format = '{:.0f}'.format
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.option_context('display.precision', 5)

csv_data_storage = open("FiScrape_Export.txt", "r")
raw_data_storage = pd.read_csv(csv_data_storage).values.tolist()
df_data_storage = pd.DataFrame(raw_data_storage).transpose()

print(df_data_storage)
