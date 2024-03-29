from yahoo_fin import stock_info as si
import pandas  as pd
from download_helper import get_company_names

tickers = []

tickers += si.tickers_sp500()
tickers += si.tickers_dow()
tickers = list(set(tickers))

short_company_names,long_company_names = get_company_names(tickers)
df = pd.DataFrame({'tickers':tickers,'shortNames':short_company_names,'longNames':long_company_names})
df.to_csv("./tickers/NASDAQ.csv",index=False,header=True)