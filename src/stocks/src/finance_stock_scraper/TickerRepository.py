from finance_stock_scraper.QuestClient import QuestClient
from finance_stock_scraper.model.Ticker import Ticker
import os
import pandas as pd
from datetime import datetime
import logging

class TickerRepository(object):
    exchanges:dict[str,dict[str,Ticker]]
    
    def __init__(self,quest_client:QuestClient) -> None:
        self.exchanges = {}
        self.quest_client = quest_client
        
    def load_tickers(self,directory:str)->None:
        """
        Load tickers from *.csv files in a directory where the filename is the exchange and the tickers are the rows in the file.
        """
        files = [file for file in os.listdir(directory) if file.endswith(".csv")]
        if len(files) == 0:
            logging.error("Found no *.csv files in the Tickers Directory!")
             
        logging.debug(f"Found files: {','.join(files)}")
                          
        for file in files:
            file = os.path.join(directory,file)
            exchange = os.path.basename(file).split('.')[0].upper()
            tickers = pd.read_csv(file)
            for ticker in tickers.values:
                if ticker[0] is not None and isinstance(ticker[0],str):
                    self.add_ticker(Ticker(ticker[0],exchange))
                
    def add_ticker(self,ticker:Ticker)->None:
        if ticker.exchange not in self.exchanges:
             self.exchanges[ticker.exchange] = {}
        self.exchanges[ticker.exchange][ticker.ticker] = ticker
        
        
    def get_ticker(self,ticker:str)->Ticker|None:
        found_ticker = None
        ticker = ticker.upper()
        for exchage in self.exchanges:
            if ticker in self.exchanges[exchage]:
                found_ticker = self.exchanges[exchage][ticker]
                break 
        return found_ticker
    
    
    def _get_single_value(self,ticker:Ticker,interval:str,values:list[str]=["close"],start_time:datetime|None=None,end_time:datetime|None=None)->pd.DataFrame|None:
        df = None
        result = self.quest_client.get_data(ticker,interval,values,start_time,end_time)
        if result:
            data = {}
            for i,column in enumerate(values):
                data[column] = [entry[i+1] for entry in result['dataset']]
            index = [datetime.strptime(entry[0] ,"%Y-%m-%dT%H:%M:%S.%fZ") for entry in result['dataset']]
            df = pd.DataFrame(data=data,index=index)   
        return df
    
    def _get_multiple_values(self,tickers:list[Ticker],interval:str,values:list[str]=["close"],start_time:datetime|None=None,end_time:datetime|None=None)->dict[str,pd.DataFrame]:
        dataframes = {}
        for ticker in tickers:
            df = self._get_single_value(ticker,interval,values,start_time,end_time)
            if df is not None:
                dataframes[ticker.ticker] = df
        return dataframes
    
    def get_values(self,tickers:list[Ticker]|Ticker,interval:str,values:list[str]=["close"],start_time:datetime|None=None,end_time:datetime|None=None)->pd.DataFrame|dict[str,pd.DataFrame]|None:
        if isinstance(tickers,list):
            return self._get_multiple_values(tickers,interval,values,start_time,end_time)
        else:
            return self._get_single_value(tickers,interval,values,start_time,end_time)
        
    def remove(self,ticker:str)->bool:
        ticker = ticker.upper()
        for exchage in self.exchanges:
            if ticker in self.exchanges[exchage]:
                self.exchanges[exchage].pop(ticker)
                return True
        return False

                
if __name__ == "__main__":
    questClient = QuestClient()
    
    repo = TickerRepository(questClient)
    
    ticker1 = Ticker("A","NASDAQ")
    ticker2 = Ticker("GOOGL","NASDAQ")
    repo.add_ticker(ticker1)
    repo.add_ticker(ticker2)
    data = repo.get_values([ticker1,ticker2],"1d",values=["close","volume"])
    print(data)
    
        