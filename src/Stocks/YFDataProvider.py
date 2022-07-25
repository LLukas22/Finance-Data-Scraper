import datetime
import pandas as pd
import yfinance as yf

class YFDataProvider(object):
    def __init__(self) -> None:
        pass
    
    def get_data(self,tickers:list[str],start_date:datetime.datetime,end_date:datetime.datetime,interval:str)->pd.DataFrame:
        if len(tickers) < 1:
            return None
        
        ticker_string = " ".join(tickers)
        data = yf.download(ticker_string, start_date, end_date, interval=interval, threads=True, group_by = 'ticker', progress=False)
        return data

    def get_data_from_period(self,tickers:list[str],interval:str,period:str="max")->pd.DataFrame:
        if len(tickers) < 1:
            return None
        
        ticker_string = " ".join(tickers)
        return yf.download(ticker_string, period = period, interval=interval, threads=True, group_by = 'ticker', progress=False)