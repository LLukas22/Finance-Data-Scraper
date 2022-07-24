import pandas_market_calendars as mcal
from pandas_market_calendars import MarketCalendar
import pandas as pd
import datetime
     
class Ticker(object):
    trading_times:MarketCalendar
    
    def __init__(self,ticker:str,exchange:str):
        self.ticker = ticker.upper()
        self.exchange = exchange.upper()
        self.trading_times = None 
        
    def _init_calendar(self)->None:
        self.trading_times = mcal.get_calendar(self.exchange)
        
    def get_trading_times(self,start_date:datetime.date,end_date:datetime.date)->pd.DataFrame:
        if self.trading_times is None:
            self._init_calendar()
        return self.trading_times.schedule(start_date=start_date, end_date=end_date)
    
    def is_in_trading_times(self,date:datetime.date)->bool:
        schedule = self.get_trading_times(date,date)
        if len(schedule) == 0:
            return False
        return date == schedule.index[0].date()
    
    

    

                
            
        

