import configparser
import os


TICKER_SECTION = 'tickers'
TICKER_INTERVALS_KEY = 'intervals'

def load_config(path:str)->configparser.ConfigParser:
    config = configparser.SafeConfigParser()
    if os.path.isfile(path):
        config.read(path)
    else:
        #load defaults
        config.add_section(TICKER_SECTION)
        config.set(TICKER_SECTION,TICKER_INTERVALS_KEY,"1m,1d")
        
    return config

def save_config(config:configparser.ConfigParser,path:str):
    with open(path, 'w') as configfile:
        config.write(configfile)

