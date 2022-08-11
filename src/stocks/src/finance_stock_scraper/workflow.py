from finance_stock_scraper.ExecutionContext import ExecutionContext
from finance_stock_scraper.model.Intervals import INTERVALS,IntervalTypes
from finance_stock_scraper.model.Ticker import Ticker
import pytz
import pandas as pd
import os
import logging
import traceback
from tqdm import tqdm
import ctypes
import numpy as np
from questdb.ingress import TimestampNanos, Buffer
from datetime import datetime,date,timedelta

CONFIGURED_INTERVALS = os.getenv("STOCKSCRAPER_INTERVALS","5m,1d").split(",")
FLUX_PROTOCOL_MAX_INT = 2_147_483_647 #In theory this should be the int64 max but flux-line-protocol in quest db only supports up to int32

def get_interval(interval:str)->IntervalTypes:
    if interval not in INTERVALS:
        raise ValueError(f"Unknown interval {interval}")
    return INTERVALS[interval]

def interval_to_timedelta(interval:str)->timedelta:
    match interval:
        case "1m":
            return timedelta(minutes=1)
        case "2m":
            return timedelta(minutes=2)
        case "5m":
            return timedelta(minutes=5)
        case "15m":
            return timedelta(minutes=15)
        case "30m":
            return timedelta(minutes=30)
        case "60m":
            return timedelta(hours=1)
        case "90m":
            return timedelta(hours=1,minutes=30)
        case "1h":
            return timedelta(hours=1)
        case "1d":
            return timedelta(days=1)
        case "5d":
            return timedelta(days=5)
        case "1wk":
            return timedelta(weeks=1)
        case "1mo":
            return timedelta(days=30)
        case "3mo":
            return timedelta(days=90)
        case _:
            raise ValueError(f"Unknown interval {interval}")
        
def difference(existing:list[str],tickers:list[Ticker])->list[Ticker]:
    return [ticker for ticker in tickers if ticker.ticker not in existing]

def make_datetime_tz_aware(datetime:datetime):
    if datetime.tzinfo is not None and datetime.tzinfo.utcoffset(datetime) is not None:
        return datetime.astimezone(pytz.UTC)
    else:
        return pytz.utc.localize(datetime)
    
def make_timestamp_tz_aware(timestamp:pd.Timestamp):
    if timestamp.tzinfo is not None and timestamp.tzinfo.utcoffset(timestamp) is not None:
        return timestamp.tz_convert(pytz.UTC)
    else:
        return timestamp.tz_localize(pytz.UTC)
    
def create_point(timestamp:pd.Timestamp,row:pd.Series,ticker:Ticker,interval:str,buffer:Buffer,minimal_date:datetime=datetime(1, 1, 1))->bool:
    
    #Invalide Data skip this row
    if row.isnull().values.any():
        return False
    
    timestamp = make_timestamp_tz_aware(timestamp)
    
    if timestamp.value <= 0 or timestamp <= minimal_date:
        return False
    
    buffer.row(
        f"interval_{interval}",
        symbols={
            "exchange":ticker.exchange,
            "ticker":ticker.ticker
            },
        columns={
            "open":float(row["Open"]),
            "high":float(row["High"]),
            "low":float(row["Low"]),
            "close":float(row["Close"]),
            "adj_close":float(row["Adj Close"]),
            "volume": min(int(row["Volume"]),FLUX_PROTOCOL_MAX_INT)
        },
        at=TimestampNanos(timestamp.value)
    )
    return True

def gather_data(exchange:str,executionContext:ExecutionContext,now:datetime=datetime.now()):
    """
    Syncs the data of the given Exchange with the database
    """
    for interval in CONFIGURED_INTERVALS:
        try:
            logging.info(f"Starting {exchange} - {interval}")
            executionContext.questClient.create_table(interval)
            flow(exchange,interval,executionContext,now)
        except Exception as e:
            logging.error(e)
            logging.debug(traceback.format_exc())
        finally:
            logging.info(f"Finished {exchange} - {interval}")
                
    
    
def download_in_slices(tickers:list[Ticker],interval:str,exchange:str,start:datetime,stop:datetime,executionContext:ExecutionContext,include_start:bool=True,slice_size:int=6)->pd.DataFrame:
    """
    Some intraday data can only be downladed in slices of 6 Days at a time => we have to download in slices if we want to pull the last 30 days
    """
    dif = (stop-start).days
    offsets = list(range(0,dif,slice_size))
    if dif not in offsets:
        offsets.append(dif)
       
    for i,offset in enumerate(offsets[:-1]):
        local_start  = start+timedelta(days=offset)
        local_end = start+timedelta(days=offsets[i+1])
        data,errors = executionContext.yfDataProcider.get_data([ticker.ticker for ticker in  tickers],local_start,local_end,interval)
        handle_errors(errors,executionContext)
        #for many tickers (> 10.000) we get a lot of data (> 10GB) => we need to commit it to the database in slices
        if data is not None:
            store_points(data,tickers,f"Intraday Tickers (Slice {i+1}/{len(offsets[:-1])})",executionContext,interval,exchange,datetime(1, 1, 1) if include_start else start)
        else:
            joined_tickers = ",".join(tickers)
            logging.warning(f"Could not download data for {joined_tickers} from {local_start} to {local_end}")
    
    
def store_points(data:pd.DataFrame,tickers:list[Ticker],message:str,executionContext:ExecutionContext,interval:str,exchange:str,minimal_date:datetime=datetime(1, 1, 1))->None:
    logging.info(f"[{message}] Storing Points ({interval}) for exchange {exchange} ...")
    minimal_date = make_datetime_tz_aware(minimal_date)
    stored_points = 0
    current_iteration = 0
    buffer = Buffer(init_capacity=1024*1024)
    for ticker in tqdm(tickers,f"[{message}] Storing Points ({interval}) for exchange {exchange} ..."):
        if ticker.ticker in data.columns:
            try:
                for timestamp,row in data[ticker.ticker].iterrows():
                    if create_point(timestamp,row,ticker,interval,buffer,minimal_date):
                        current_iteration += 1
                    
                    if current_iteration > 30_000:
                        executionContext.questClient.store_points(buffer)  
                        stored_points += current_iteration
                        current_iteration = 0
            except Exception as e:
                logging.error(e)
                logging.debug(traceback.format_exc())
                    
    if current_iteration > 0:
        executionContext.questClient.store_points(buffer)
        stored_points += current_iteration
        current_iteration = 0
        
    logging.info(f"[{message}] Stored {stored_points} Points ({interval}) for exchange {exchange}!")
    
    
    
def handle_errors(errors:dict,executionContext:ExecutionContext):
    """
    Handles the yFinanced errors and removes faulted tickers from the repository
    """
    if len(errors)>0:
        logging.warning(f"{len(errors)} errors occured")
        #TODO
        # removed = []
        # for ticker,error in errors.items():
        #     if error == "No data found for this date range, symbol may be delisted":
        #         if executionContext.tickerRepository.remove(ticker):
        #             removed.append(ticker)
        # if len(removed) > 0:
        #     logging.debug(f"Removed {','.join(removed)} from the repository!")
    
                
def flow(exchange:str,interval:str,executionContext:ExecutionContext,now:datetime):
    """
    Flow to download data for a given exchange and interval
    """
    interval_type = get_interval(interval)
    time_delta = interval_to_timedelta(interval)
    
    tickers = list(executionContext.tickerRepository.exchanges[exchange].values())
    if len(tickers) == 0:
        raise ValueError(f"No tickers found for exchange {exchange}")
    
    #First we check if the ticker is in the database if not we download the max from YFinance and add it
    existing_tickers = executionContext.questClient.get_existing_tickers_for_interval(interval,exchange)
    tickers_to_gather = difference(existing_tickers,tickers)
    if len(tickers_to_gather) > 0:
        data = None
        if interval_type == IntervalTypes.Daily:
            data,errors = executionContext.yfDataProcider.get_data_from_period([ticker.ticker for ticker in tickers_to_gather],interval)
            handle_errors(errors,executionContext)
            if data is not None:
                store_points(data,tickers_to_gather,"New Tickers",executionContext,interval,exchange)
        else:
            #Maximum for Intraday is 30 days
            download_in_slices(tickers_to_gather,interval,exchange,now-timedelta(days=29),now,executionContext,) 
              
    #If we already have data for an stock we just download the latest data
    #1. We ignore the stocks we just downloaded
    tickers_to_check = difference([ticker.ticker for ticker in tickers_to_gather],tickers)
    
    #2. Querry the database for the last date we got data for each stock and group by datetime
    tickers_to_gather = {}
    last_entries = executionContext.questClient.get_last_entry_dates(interval)
    for ticker in tickers_to_check:
        if ticker.ticker in last_entries:
            last_date = last_entries[ticker.ticker]
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
    for date in tickers_to_gather:
        if now-date < time_delta:
            #We cant download data from the future (e.g we want to download an interval of 7days => we can only downlaod 7 days after the last date)
            continue
        
        batched_tickers = tickers_to_gather[date]
        batched_tickers_names = [ticker.ticker for ticker in batched_tickers]
        data = None
        
        if interval_type == IntervalTypes.Intraday and now-date > timedelta(days=6):
            if now-date > timedelta(days=30):
                #we can only get the last 30 days
                logging.warning(f"[WARNING] The last entry for {','.join(batched_tickers_names)} is older than 30 days! Only  the last 30 days will be downloaded!")
                download_in_slices(batched_tickers,interval,exchange,now-timedelta(days=29),now,executionContext,include_start=False)  
            else:
                #we have to download in slices
                download_in_slices(batched_tickers,interval,exchange,date,now,executionContext,include_start=False)          
        else:
            data,errors = executionContext.yfDataProcider.get_data(batched_tickers_names,date,now,interval)
            handle_errors(errors,executionContext)
            if data is not None:
                store_points(data,batched_tickers,"Existing Tickers",executionContext,interval,exchange,date)
