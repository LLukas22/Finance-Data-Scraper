import datetime
import pandas as pd
import yfinance as yf
from yfinance import shared

class YFDataProvider(object):
    def __init__(self) -> None:
        pass
    
    def get_data(self,tickers:list[str],start_date:datetime.datetime,end_date:datetime.datetime,interval:str)->tuple[pd.DataFrame,dict]:
        if len(tickers) < 1:
            return None
        
        ticker_string = " ".join(tickers)
        data = yf.download(ticker_string, start_date, end_date, interval=interval, threads=True, group_by = 'ticker', progress=True)
        return data,shared._ERRORS

    def get_data_from_period(self,tickers:list[str],interval:str,period:str="max")->tuple[pd.DataFrame,dict]:
        if len(tickers) < 1:
            return None
        
        ticker_string = " ".join(tickers)
        data = yf.download(ticker_string, period = period, interval=interval, threads=True, group_by = 'ticker', progress=True)
        return data,shared._ERRORS