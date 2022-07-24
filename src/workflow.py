from ExecutionContext import ExecutionContext
from model.Intervals import INTERVALS,IntervalTypes
from influxdb_client import Point, WritePrecision
import datetime
import pytz
import pandas as pd
from model.Ticker import Ticker
from Config import TICKER_SECTION,TICKER_INTERVALS_KEY

def get_interval(interval:str):
    if interval not in INTERVALS:
        raise ValueError(f"Unknown interval {interval}")
    return INTERVALS[interval]


def difference(existing:list[str],tickers:list[Ticker])->list[Ticker]:
    return [ticker for ticker in tickers if ticker.ticker not in existing]

def create_point(timestamp:datetime,row:pd.Series,ticker:Ticker,interval:str)->Point:
    if timestamp.tzinfo is not None and timestamp.tzinfo.utcoffset(timestamp) is not None:
        timestamp = timestamp.tz_convert(pytz.UTC)
    else:
        timestamp = timestamp.tz_localize(pytz.UTC)
        
    return Point("Stocks")\
        .tag("ticker",ticker.ticker)\
        .tag("exchange",ticker.exchange)\
        .tag("interval",interval)\
        .field("open",row["Open"])\
        .field("high",row["High"])\
        .field("low",row["Low"])\
        .field("close",row["Close"])\
        .field("adj close",row["Adj Close"])\
        .field("volume",row["Volume"])\
        .time(timestamp, WritePrecision.S)
        

def create_points(data:pd.DataFrame,ticker:Ticker,interval:str)->list[Point]:
    points = []
    for timestamp,row in data[ticker.ticker].iterrows():
        try:
            points.append(create_point(timestamp,row,ticker,interval))
        except Exception as e:
            print(e)
    return points
    
def gather_data(exchange:str,executionContext:ExecutionContext):
    """
    Syncs the data of the given Exchange with the database
    """
    for interval in executionContext.config.get(TICKER_SECTION,TICKER_INTERVALS_KEY).split(","):
        try:
            interval_type = get_interval(interval)
            if interval_type == IntervalTypes.Intraday:
                intraday_flow(exchange,interval,executionContext)
            else:
                daily_flow(exchange,interval,executionContext)
        except Exception as e:
            print(e)
                

def intraday_flow(exchange:str,interval:str,excutionContext:ExecutionContext):
    """
    Flow to retrieve intraday data for a given exchange and interval
    """
    # Up to 30 Days of data is available for Intraday data
    # => Sync the data for the last 30 days with the database
    tickers = list(excutionContext.tickerRepository.exchanges[exchange].values())
    if len(tickers) == 0:
        raise ValueError(f"No tickers found for exchange {exchange}")
    end_date = datetime.datetime.now().date()
    start_date = end_date-datetime.timedelta(days=(29))
    
    days_to_sync = []
    for i in range(0,28):
        end = start_date + datetime.timedelta(days=min(i+1,28))
        start = start_date + datetime.timedelta(days=i)
        #Check if the date is a valid trading day for this exchange
        if(tickers[0].is_in_trading_times(start)):
            days_to_sync.append((start,end))

    #Check if the data is already in the database and download it if not
    points_to_write = []
    
    for start,end in days_to_sync:
        already_in_database = excutionContext.influxClient.get_existing_tickers(start,end,interval)
        tickers_to_gather = difference(already_in_database,tickers)
        data = excutionContext.yfDataProcider.get_data([ticker.ticker for ticker in tickers_to_gather],start,end,interval)
        if data is not None:
            for ticker in tickers_to_gather:
                points_to_write.extend(create_points(data,ticker,interval))

    print(f"Storing {len(points_to_write)} Points for exchange {exchange}")
    excutionContext.influxClient.store_points(points_to_write)
    return None

def daily_flow(exchange:str,interval:str,executionContext:ExecutionContext):
    """
    Flow to download daily data for a given exchange and interval
    """
    tickers = list(executionContext.tickerRepository.exchanges[exchange].values())
    if len(tickers) == 0:
        raise ValueError(f"No tickers found for exchange {exchange}")
    
    #First we check if the ticker is in the database if not we download the max from YFinance and add it
    existing_tickers = executionContext.influxClient.get_existing_tickers_for_interval(interval)
    tickers_to_gather = difference(existing_tickers,tickers)
    if len(tickers_to_gather) > 0:
        points_to_write = []
        data = executionContext.yfDataProcider.get_max_data([ticker.ticker for ticker in tickers_to_gather],interval)
        if data is not None:
            for ticker in tickers_to_gather:
                points_to_write.extend(create_points(data,ticker,interval))
                
        print(f"[New Ticker] Storing {len(points_to_write)} Points for exchange {exchange} ...")
        executionContext.influxClient.store_points(points_to_write)
        print(f"[New Ticker] Stored {len(points_to_write)} Points for exchange {exchange}!")       
        
    #If we already have data for an stock we just download the latest data
    #1. We ignore the stocks we just doenloaded
    tickers_to_check = difference([ticker.ticker for ticker in tickers_to_gather],tickers)
    
    #2. Querry the database for the last date we got data for each stock and group by days
    today = datetime.datetime.now().date()
    tickers_to_gather = {}
    for ticker in tickers_to_check:
        last_date = executionContext.influxClient.get_last_entry(ticker.ticker,interval)
        if last_date is not None:
            last_date = last_date.date()
            
            if last_date == today:
                continue
            if last_date not in tickers_to_gather:
                tickers_to_gather[last_date] = [ticker]
            else:
                tickers_to_gather[last_date].append(ticker) 
        else:
            print(f"Can't find last date for {ticker.ticker}!")

    #3. Download the data from YFinance
    points_to_write = []
    for date in tickers_to_gather:
        batched_tickers = tickers_to_gather[date]
        #Advance the date by one day to get the next trading day
        start = date + datetime.timedelta(days=1)
        data = executionContext.yfDataProcider.get_data([ticker.ticker for ticker in batched_tickers],start,today,interval)
        if data is not None:
            for ticker in batched_tickers:
                points_to_write.extend(create_points(data,ticker,interval))
                
    #4. Store the data in the database
    print(f"[Existing Ticker] Storing {len(points_to_write)} Points for exchange {exchange} ...")
    executionContext.influxClient.store_points(points_to_write)
    print(f"[Existing Ticker] Stored {len(points_to_write)} Points for exchange {exchange}!")
    
    return None