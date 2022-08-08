from yahoo_fin import stock_info as si
import pandas  as pd
from download_helper import get_company_names

tickers = []
tickers += list(pd.read_html("https://en.wikipedia.org/wiki/DAX")[3]["Ticker symbol"])
tickers = list(set(tickers))

short_company_names,long_company_names = get_company_names(tickers)
df = pd.DataFrame({'tickers':tickers,'shortNames':short_company_names,'longNames':long_company_names})
df.to_csv("./tickers/EUREX.csv",index=False,header=True)