from TickerRepository import TickerRepository
from  YFDataProvider import YFDataProvider
from InfluxClient import InfluxClient
import configparser

class ExecutionContext(object):
    def __init__(self,tickerRepository:TickerRepository,yfDataProcider:YFDataProvider,influxClient:InfluxClient,config:configparser.ConfigParser) -> None:
        self.tickerRepository = tickerRepository
        self.yfDataProcider = yfDataProcider
        self.influxClient = influxClient
        self.config = config