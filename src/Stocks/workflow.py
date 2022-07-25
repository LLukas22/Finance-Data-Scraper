from ExecutionContext import ExecutionContext
from model.Intervals import INTERVALS,IntervalTypes
from influxdb_client import Point, WritePrecision
import datetime
import pytz
import pandas as pd
from model.Ticker import Ticker
import os
import logging
import traceback

CONFIGURED_INTERVALS = os.getenv("STOCKSCRAPER_INTERVALS","1m,1d").split(",")

def get_interval(interval:str)->IntervalTypes:
    if interval not in INTERVALS:
        raise ValueError(f"Unknown interval {interval}")
    return INTERVALS[interval]

def interval_to_timedelta(interval:str)->datetime.timedelta:
    match interval:
        case "1m":
            return datetime.timedelta(minutes=1)
        case "2m":
            return datetime.timedelta(minutes=2)
        case "5m":
            return datetime.timedelta(minutes=5)
        case "15m":
            return datetime.timedelta(minutes=15)
        case "30m":
            return datetime.timedelta(minutes=30)
        case "60m":
            return datetime.timedelta(hours=1)
        case "90m":
            return datetime.timedelta(hours=1,minutes=30)
        case "1h":
            return datetime.timedelta(hours=1)
        case "1d":
            return datetime.timedelta(days=1)
        case "5d":
            return datetime.timedelta(days=5)
        case "1wk":
            return datetime.timedelta(weeks=1)
        case "1mo":
            return datetime.timedelta(days=30)
        case "3mo":
            return datetime.timedelta(days=90)
        case _:
            raise ValueError(f"Unknown interval {interval}")
        
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
            logging.error(e)
            logging.debug(traceback.format_exc())
    return points
    
def gather_data(exchange:str,executionContext:ExecutionContext):
    """
    Syncs the data of the given Exchange with the database
    """
    for interval in CONFIGURED_INTERVALS:
        try:
            logging.info(f"Starting {exchange} - {interval}")
            flow(exchange,interval,executionContext)
        except Exception as e:
            logging.error(e)
            logging.debug(traceback.format_exc())
        finally:
            logging.info(f"Finished {exchange} - {interval}")
                
                
def flow(exchange:str,interval:str,executionContext:ExecutionContext):
    """
    Flow to download data for a given exchange and interval
    """
    interval_type = get_interval(interval)
    time_delta = interval_to_timedelta(interval)
    
    tickers = list(executionContext.tickerRepository.exchanges[exchange].values())
    if len(tickers) == 0:
        raise ValueError(f"No tickers found for exchange {exchange}")
    
    #First we check if the ticker is in the database if not we download the max from YFinance and add it
    existing_tickers = executionContext.influxClient.get_existing_tickers_for_interval(interval)
    tickers_to_gather = difference(existing_tickers,tickers)
    if len(tickers_to_gather) > 0:
        points_to_write = []
        data = None
        if interval_type == IntervalTypes.Daily:
            data = executionContext.yfDataProcider.get_data_from_period([ticker.ticker for ticker in tickers_to_gather],interval)
        else:
            #Maximum for Intraday is 30 days
            data = executionContext.yfDataProcider.get_data_from_period([ticker.ticker for ticker in tickers_to_gather],interval,"1mo")    
        if data is not None:
            for ticker in tickers_to_gather:
                points_to_write.extend(create_points(data,ticker,interval))
                
        logging.info(f"[New Tickers] Storing {len(points_to_write)} Points ({interval}) for exchange {exchange} ...")
        executionContext.influxClient.store_points(points_to_write)
        logging.info(f"[New Tickers] Stored {len(points_to_write)} Points ({interval}) for exchange {exchange}!")       
        
    #If we already have data for an stock we just download the latest data
    #1. We ignore the stocks we just downloaded
    tickers_to_check = difference([ticker.ticker for ticker in tickers_to_gather],tickers)
    
    #2. Querry the database for the last date we got data for each stock and group by datetime
    now = datetime.datetime.now().astimezone(pytz.utc)
    tickers_to_gather = {}
    for ticker in tickers_to_check:
        last_date = executionContext.influxClient.get_last_entry(ticker.ticker,interval)
        if last_date is not None:
            #this schould never happen
            if last_date == now:
                continue
            
            if last_date not in tickers_to_gather:
                tickers_to_gather[last_date] = [ticker]
            else:
                tickers_to_gather[last_date].append(ticker) 
        else:
            logging.warning(f"Can't find last date for {ticker.ticker}!")

    #3. Download the data from YFinance
    points_to_write = []
    for date in tickers_to_gather:
        if now-date < time_delta:
            #We cant download data from the future (e.g we want to download an interval of 7days => we can only downlaod 7 days after the last date)
            continue
        
        batched_tickers = tickers_to_gather[date]
        batched_tickers_names = [ticker.ticker for ticker in batched_tickers]
        data = None
        
        if interval_type == IntervalTypes.Intraday and now-date > datetime.timedelta(days=30):
            #we can only get the last 30 days
            data = executionContext.yfDataProcider.get_data_from_period(batched_tickers_names,interval,"1mo")
            logging.warning(f"[WARNING] The last entry for {','.join(batched_tickers_names)} is older than 30 days! Only  the last 30 days will be downloaded!");   
        else:
            data = executionContext.yfDataProcider.get_data(batched_tickers_names,date,now,interval)
        if data is not None:
            for ticker in batched_tickers:
                points_to_write.extend(create_points(data,ticker,interval))
                
    #4. Store the data in the database
    logging.info(f"[Existing Ticker] Storing {len(points_to_write)} Points ({interval}) for exchange {exchange} ...")
    executionContext.influxClient.store_points(points_to_write)
    logging.info(f"[Existing Ticker] Stored {len(points_to_write)} Points ({interval}) for exchange {exchange}!")