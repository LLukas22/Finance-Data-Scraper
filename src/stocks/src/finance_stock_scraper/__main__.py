import time
import os
import pytz
import pandas_market_calendars as mcal
import logging
from datetime import datetime, timedelta
from finance_stock_scraper.ExecutionContext import ExecutionContext
from finance_stock_scraper.QuestClient import QuestClient
from finance_stock_scraper.TickerRepository import TickerRepository
from finance_stock_scraper.YFDataProvider import YFDataProvider
from finance_stock_scraper.model.Ticker import Ticker
from finance_stock_scraper.workflow import gather_data



TICKERS_DIR = os.path.abspath(os.getenv('STOCKSCRAPER_TICKERS_DIR',"../../../Tickers"))
DEBUG = os.getenv('STOCKSCRAPER_DEBUG',"False").upper() == "TRUE"
MODE = os.getenv('STOCKSCRAPER_MODE',"Single").upper() # Single or Scheduled
SLEEP_TIME = int(os.getenv('STOCKSCRAPER_SLEEPTIME',60*60*3)) # 3 hours

if __name__ == "__main__":
    
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    if DEBUG:
        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s',level=logging.DEBUG)
    else:
        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s',level=logging.INFO)
        
    logging.info(f"---Starting Scraper---")
    logging.info(f"TICKERS_DIR:{TICKERS_DIR}")
    logging.info(f"MODE:{MODE}")
    logging.info(f"SLEEP_TIME:{SLEEP_TIME}")
        
    # Create QuestClient
    questClient = QuestClient()
    retries = 0
    while True:
        if questClient.health_check():
            break
        else:
            logging.warning(f"Could not establish connection to QuestDB! Retrying ...")
            retries += 1
            time.sleep(2)
            
        if retries > 10:
            raise Exception("Could not connect to QuestDB!")
        
    if not os.path.isdir(TICKERS_DIR):
        raise Exception(f"Tickers Directory '{TICKERS_DIR}' does not exist!")
    
    
    ticker_repo = TickerRepository(questClient)
    ticker_repo.load_tickers(TICKERS_DIR)
    
    logging.info("Loaded Tickers:")
    for exchange in ticker_repo.exchanges:
        logging.info(f"{exchange}:")
        logging.info(",".join([ticker for ticker in ticker_repo.exchanges[exchange]]))
    
    yfDataProvider = YFDataProvider()
    
    executionContext = ExecutionContext(ticker_repo, yfDataProvider, questClient)

    # if its in single mode, we will run the gathering process once and then exit
    if MODE == "SINGLE":
        for exchange in ticker_repo.exchanges:
            now = datetime.now().astimezone(pytz.utc)
            gather_data(exchange, executionContext,now)
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
                        gather_data(exchange, executionContext, now)
                        logging.info(f"Finished gathering process for exchange {exchange}!")
                        
                #sleep for a while
                logging.info(f"Sleeping for {SLEEP_TIME} Seconds!")
                time.sleep(SLEEP_TIME)
            except Exception as e:
                logging.error(e)
                
                    
    
        
    
        
    
        