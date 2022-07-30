from TickerRepository import TickerRepository
from  YFDataProvider import YFDataProvider
from QuestClient import QuestClient

class ExecutionContext(object):
    def __init__(self,tickerRepository:TickerRepository,yfDataProcider:YFDataProvider,questClient:QuestClient) -> None:
        self.tickerRepository = tickerRepository
        self.yfDataProcider = yfDataProcider
        self.questClient = questClient