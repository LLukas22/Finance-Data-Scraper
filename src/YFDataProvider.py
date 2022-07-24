import datetime
import pandas as pd
import yfinance as yf

class YFDataProvider(object):
    def __init__(self) -> None:
        pass
    
    def get_data(self,tickers:list[str],start_date:datetime.date,end_date:datetime.date,interval:str)->pd.DataFrame:
        if len(tickers) < 1:
            return None
        
        ticker_string = " ".join(tickers)
        data = yf.download(ticker_string, start_date, end_date, interval=interval, threads=True, group_by = 'ticker', progress=False)
        return data

    def get_max_data(self,tickers:list[str],interval:str)->pd.DataFrame:
        if len(tickers) < 1:
            return None
        
        ticker_string = " ".join(tickers)
        return yf.download(ticker_string, period = "max", interval=interval, threads=True, group_by = 'ticker', progress=False)