from yahoo_fin import stock_info as si
import pandas  as pd

nasdaq = pd.DataFrame(si.tickers_sp500())
nasdaq.to_csv("./tickers/nasdaq.csv",index=False,header=False)