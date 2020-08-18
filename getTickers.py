#!/Users/vk/miniconda3/bin/python

import pandas as pd
from get_all_tickers import get_tickers as gt

from get_all_tickers import get_tickers as gt
list_of_tickers = gt.get_tickers()

df = pd.DataFrame (list_of_tickers,columns=['Symbol'])
df = df.sort_values(by=['Symbol'])
df.to_csv('/Users/vk/Desktop/SchoolAndResearch/StockResearch/COMMON_CONFIG/stocks.csv', index=False)
