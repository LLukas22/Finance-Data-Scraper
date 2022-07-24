import pandas as pd
import glob
import os
from Config import save_config, load_config
from ExecutionContext import ExecutionContext
from InfluxClient import InfluxClient
from TickerRepository import TickerRepository
from YFDataProvider import YFDataProvider
from model.Ticker import Ticker
from workflow import gather_data

WORKER_DIR = 'worker'
TICKERS_DIR = 'tickers'
CONFIG = 'config.cfg'

if __name__ == "__main__":

    root_dir = os.path.abspath(WORKER_DIR)
    config_file = os.path.join(root_dir, CONFIG)
    
    #load and rewrite the config file
    config =  load_config(config_file)
    save_config(config, config_file)
    
    influxClient = InfluxClient()

    ticker_repo = TickerRepository(influxClient)
    ticker_repo.load_tickers(os.path.join(root_dir,TICKERS_DIR))
    
    yfDataProvider = YFDataProvider()
    
    executionContext = ExecutionContext(ticker_repo, yfDataProvider, influxClient, config)

    for exchange in ticker_repo.exchanges:
        gather_data(exchange, executionContext)
            
    
        
    
        
    
        