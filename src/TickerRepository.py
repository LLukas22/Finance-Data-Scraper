from InfluxClient import InfluxClient,URL,TOKEN
from model.Ticker import Ticker
import os
import pandas as pd
import glob
from datetime import datetime

class TickerRepository(object):
    exchanges:dict[str,dict[str,Ticker]]
    
    def __init__(self,influx_client:InfluxClient) -> None:
        self.exchanges = {}
        self.influx_client = influx_client
        
    def load_tickers(self,directory:str)->None:
        for file in glob.glob(f"{directory}\\*.csv"):
            exchange = os.path.basename(file).split('.')[0].upper()
            tickers = pd.read_csv(file, header=None)
            for ticker in tickers.values:
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
        tables = self.influx_client.get_data(ticker,interval,values,start_time,end_time)
        if tables:
            data = {}
            for table in tables:
                name = table.records[0].values["_field"]
                values = [record.values["_value"] for record in table.records]
                data[name] = values
                
            index = [record.values["_time"] for record in tables[0].records]
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
        

                
if __name__ == "__main__":
    influxClient = InfluxClient()
    
    repo = TickerRepository(influxClient)
    
    ticker1 = Ticker("AAPL","NASDAQ")
    ticker2 = Ticker("GOOGL","NASDAQ")
    repo.add_ticker(ticker1)
    data = repo.get_values([ticker1,ticker2],"1d",values=["close","volume"])
    print(data)
    
        