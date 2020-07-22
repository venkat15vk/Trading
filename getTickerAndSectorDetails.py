#!/usr/local/bin/python3

import requests
import pandas as pd
from io import StringIO

nyse_url = 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download'
nasdaq_url = 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'

r1 = requests.get(nyse_url)
nyse = pd.read_csv(StringIO(r1.text))

r2 = requests.get(nasdaq_url)
nasdaq = pd.read_csv(StringIO(r2.text))

stocks = pd.concat([nyse, nasdaq])
stocks = stocks[stocks['MarketCap'].notna()]
stocks = stocks[stocks['Sector'].notna()]
stocks['IPOyear'].fillna(0, inplace=True)

# find stocks between 30 - 450 bucks
stocks = stocks.loc[stocks['LastSale'] > 5]
stocks = stocks.loc[stocks['LastSale'] < 1000]

stocks = stocks[['Symbol','IPOyear', 'Sector', 'industry']]
stocks.rename({'industry': 'Industry'}, axis=1, inplace=True)
stocks["IPOyear"] = pd.to_numeric(stocks["IPOyear"], downcast="integer")
stocks = stocks.sort_values(by='Symbol', ascending=True)

stocks.to_csv('/Users/vk/Desktop/SchoolAndResearch/StockResearch/COMMON_CONFIG/stocks.csv', index=False)

#techStocks = stocks.loc[stocks['Sector'] == 'Technology']
#techStocks.to_csv('/Users/vk/Desktop/SchoolAndResearch/StockResearch/COMMON_CONFIG/stocks.csv', index=False)
