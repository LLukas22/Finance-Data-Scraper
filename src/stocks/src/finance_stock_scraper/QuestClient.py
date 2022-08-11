from datetime import datetime,date
import os
from questdb.ingress import Sender, TimestampNanos, Buffer
import requests 
from requests import Response
import pytz
from finance_stock_scraper.model.Ticker import Ticker
from finance_stock_scraper.model.Intervals import INTERVALS, IntervalTypes


HOST = os.getenv('STOCKSCRAPER_QUESTDB_HOST','localhost')
INFLUX_LINE_PROTOCOL_PORT = os.getenv('STOCKSCRAPER_QUESTDB_ILP_PORT',9009) 
REST_PORT = os.getenv('STOCKSCRAPER_QUESTDB_PORT',9000) 
MONITORING_PORT = os.getenv('STOCKSCRAPER_QUESTDB_MONITORING_PORT',9003)

class QuestClient(object):
    def __init__(self,host:str=HOST,port:int=REST_PORT,ilp_port:int=INFLUX_LINE_PROTOCOL_PORT,monitoring_port:int=MONITORING_PORT)-> None:
        self.host = host
        self.ilp_port = ilp_port
        self.port = port
        self.monitoring_port = monitoring_port
    
    def health_check(self)-> bool:
        try:
            return requests.get(f"http://{self.host}:{self.monitoring_port}/status").status_code == 200
        except:
            return False
            
    
        
    def _format_time(self,time:datetime)-> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    def create_table(self,interval:str)-> None:
        if interval not in INTERVALS:
            raise Exception(f"Interval {interval} is not supported")
        interval_type = INTERVALS[interval]
        
        query = f"CREATE TABLE IF NOT EXISTS 'interval_{interval}'"\
            "("\
                "exchange Symbol,"\
                "ticker Symbol,"\
                "open float,"\
                "high float,"\
                "low float,"\
                "close float,"\
                "adj_close float,"\
                "volume long,"\
                "timestamp TIMESTAMP"\
            "),"\
            "index (ticker)"\
            "timestamp(timestamp)"\
            f"PARTITION BY {'YEAR' if interval_type == IntervalTypes.Daily else 'MONTH'};"
        self.raw_query(query)
        


          
    def get_existing_tickers_for_interval(self,interval:str,exchange:str)->list[str]:
        query = f"SELECT DISTINCT ticker FROM 'interval_{interval}' WHERE exchange = '{exchange}'"
        response = self.raw_query(query)
        if response.status_code == 200:
            return [ticker[0] for ticker in response.json()['dataset']]
        else:
            return []
        
        
    def get_last_entry_dates(self,interval:str)-> dict[str,datetime]:
        query = f"SELECT ticker, timestamp FROM 'interval_{interval}'"\
                "LATEST ON timestamp PARTITION BY ticker;"
                
        response = self.raw_query(query)
        last_entries = {}
        if response.status_code == 200:
            for ticker,time in response.json()['dataset']:
                last_entries[ticker] = datetime.strptime(time,"%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)
            return last_entries
        else:
            return last_entries
    
    def store_points(self,buffer:Buffer)-> None:
        if len(buffer) > 0:
            with Sender(self.host, self.ilp_port) as sender:
                sender.flush(buffer)
              
    def get_data(self,ticker:Ticker,interval:str,values:list[str]=["close"],start_date:datetime|None=None,end_date:datetime|None=None)-> None|dict:
        """
        Querry data for the given ticker and interval
        """
        selection = ["timestamp"]+values
        selection = ",".join(selection)
        query = f"SELECT {selection} FROM 'interval_{interval}'"
        query += "WHERE "
        if end_date and start_date:
            query += f"timestamp BETWEEN '{self._format_time(start_date)}' AND '{self._format_time(end_date)}' AND "
        elif end_date and not start_date:
            query += f"timestamp <= '{self._format_time(end_date)}' AND "
        elif start_date and not end_date:
            query += f"timestamp >= '{self._format_time(start_date)}' AND "
            
        query += f"ticker='{ticker.ticker}' AND exchange='{ticker.exchange}';"
        
        response = self.raw_query(query)
        if response.status_code == 200:
            return response.json()
        return None
    
    def raw_query(self,query:str)-> Response:
        return requests.get(f"http://{self.host}:{self.port}/exec?query=" + requests.utils.quote(query))
                     
if __name__ == "__main__":
    questClient = QuestClient()
    questClient.get_last_entry_dates("1m")
    ticker = Ticker("GOOGL","NASDAQ")
    result = questClient.get_data(ticker, "1d",values=["close","volume"],start_date=datetime(year = 2010,month=1,day=1),end_date=datetime(year = 2011,month=1,day=1))