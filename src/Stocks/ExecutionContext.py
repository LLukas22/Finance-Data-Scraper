from TickerRepository import TickerRepository
from  YFDataProvider import YFDataProvider
from InfluxClient import InfluxClient

class ExecutionContext(object):
    def __init__(self,tickerRepository:TickerRepository,yfDataProcider:YFDataProvider,influxClient:InfluxClient) -> None:
        self.tickerRepository = tickerRepository
        self.yfDataProcider = yfDataProcider
        self.influxClient = influxClient