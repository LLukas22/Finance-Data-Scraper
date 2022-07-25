import time
import pandas as pd
import glob
import os
import pytz
from ExecutionContext import ExecutionContext
from InfluxClient import InfluxClient
from TickerRepository import TickerRepository
from YFDataProvider import YFDataProvider
from model.Ticker import Ticker
from workflow import gather_data
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
import logging

TICKERS_DIR = os.path.abspath(os.getenv('STOCKSCRAPER_TICKERS_DIR',"./Tickers"))
DEBUG = os.getenv('STOCKSCRAPER_DEBUG',"True").upper() == "TRUE"
MODE = os.getenv('STOCKSCRAPER_MODE',"Single").upper() # Single or Scheduled
SLEEP_TIME = int(os.getenv('STOCKSCRAPER_SLEEPTIME',60*30)) # 15 minutes


if __name__ == "__main__":
    
    if DEBUG:
        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s',level=logging.DEBUG)
    else:
        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s',level=logging.INFO)
    logging.debug(f"TICKERS_DIR:{TICKERS_DIR}")
    logging.debug(f"MODE:{MODE}")
    logging.debug(f"SLEEP_TIME:{SLEEP_TIME}")
        
    influxClient = InfluxClient()
    ticker_repo = TickerRepository(influxClient)
    ticker_repo.load_tickers(TICKERS_DIR)
    
    if DEBUG:
        logging.info("Loaded Tickers:")
        for exchange in ticker_repo.exchanges:
            logging.info(f"{exchange}:")
            logging.info(",".join([ticker for ticker in ticker_repo.exchanges[exchange]]))
    
    yfDataProvider = YFDataProvider()
    
    executionContext = ExecutionContext(ticker_repo, yfDataProvider, influxClient)

    # if its in single mode, we will run the gathering process once and then exit
    if MODE == "SINGLE":
        for exchange in ticker_repo.exchanges:
            gather_data(exchange, executionContext)
    else:
        # otherwise, we will run the gathering process in a loop
        last_runs = {} 
        while True:
            try:
                #check all exchanges and if we are are after the tradingtimes we start the gathering process
                now = datetime.now().astimezone(pytz.utc)
                for exchange in ticker_repo.exchanges:
                    
                    #Check if we already run the gathering process for this exchange today
                    if exchange in last_runs:
                        last_run = last_runs[exchange]
                        if now-last_run < timedelta(hours=23,minutes=45):
                            continue
                        
                    calender = mcal.get_calendar(exchange)
                    schedule = calender.schedule(start_date=now, end_date=now)
                    if len(schedule) == 0:
                        #Not a trading day => nothing to fetch
                        continue
                    closing_time = schedule["market_close"][0]
                    #Yahoo Finance can have a delay of 15-30 Minutes for the data  to be available => we add 30 minutes to the closing time
                    closing_time += timedelta(minutes=30)
                    if now > closing_time:
                        #we are after the closing time => we start the gathering process
                        last_runs[exchange] = now
                        logging.info(f"Starting gathering process for exchange {exchange}...")
                        gather_data(exchange, executionContext)
                        logging.info(f"Finished gathering process for exchange {exchange}!")
                        
                #sleep for a while 
                time.sleep(SLEEP_TIME)
            except Exception as e:
                logging.error(e)
                
                    
    
        
    
        
    
        