from finance_stock_scraper.TickerRepository import TickerRepository
from finance_stock_scraper.YFDataProvider import YFDataProvider
from finance_stock_scraper.QuestClient import QuestClient

class ExecutionContext(object):
    def __init__(self,tickerRepository:TickerRepository,yfDataProcider:YFDataProvider,questClient:QuestClient) -> None:
        self.tickerRepository = tickerRepository
        self.yfDataProcider = yfDataProcider
        self.questClient = questClient