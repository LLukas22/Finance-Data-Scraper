from influxdb_client import InfluxDBClient, Point, WritePrecision,WriteOptions
from influxdb_client.client.flux_table import FluxTable
from datetime import datetime,date
from model.Ticker import Ticker
import os


URL = f"http://{os.getenv('STOCKSCRAPER_INFLUXDB_HOST','localhost')}:{os.getenv('STOCKSCRAPER_INFLUXDB_PORT','8086')}"
TOKEN = os.getenv("STOCKSCRAPER_INFLUXDB_TOKEN","OYRyhNIDCQFe1WMJeJnljPV323EWA3GE45CA1Mpdx5TBbw-pxYqfGlFgAvdtrbKgZcJZnQn7oOhLoRbsUOhnuw==") # Don't use the default token
ORG = os.getenv("STOCKSCRAPER_INFLUXDB_ORG","StockScraper")
BUCKET = os.getenv("STOCKSCRAPER_INFLUXDB_BUCKET","Stocks")

class InfluxClient(object):
    def __init__(self,url:str=URL,token:str=TOKEN,org:str=ORG,bucket:str=BUCKET)-> None:
        self.token = token
        self.url = url
        self.org = org
        self.bucket = bucket
        self.client = InfluxDBClient(url=self.url, token=token, org=org)
        
    def _format_time(self,time:datetime)-> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    def get_existing_tickers(self,start_date:date,end_date:date,interval:str)-> list[str]:
        query = (
            f'from(bucket: "{self.bucket}")' 
            f'|> range(start: {self._format_time(start_date)}, stop: {self._format_time(end_date)})'
            f'|> filter(fn: (r) => r.interval == "{interval}")'
            '|> group(columns: ["ticker"])'
            '|> distinct(column: "ticker")'
            '|> keep(columns: ["_value"])'
        )
        already_existing_tickers = []
        querry_api = self.client.query_api()
        result = querry_api.query(query, org=self.org)
        if len(result) == 0:
            return already_existing_tickers
        
        for record in result[0].records:
            already_existing_tickers.append(record["_value"])
        return already_existing_tickers
    
    def get_existing_tickers_for_interval(self,interval:str):
        query = (
            f'from(bucket: "{self.bucket}")'
            f'|> range(start: 0)'
            f'|> filter(fn: (r) => r.interval == "{interval}")'
            '|> group(columns: ["ticker"])'
            '|> distinct(column: "ticker")'
            '|> keep(columns: ["_value"])'
        )
        already_existing_tickers = []
        querry_api = self.client.query_api()
        result = querry_api.query(query, org=self.org)
        if len(result) == 0:
            return already_existing_tickers
        
        for record in result[0].records:
            already_existing_tickers.append(record["_value"])
        return already_existing_tickers
        
        
    def get_last_entry(self,ticker:str,interval:str)-> datetime|None:
        query = (
            f'from(bucket: "{self.bucket}")'
            f'|> range(start: 0)'
            f'|> filter(fn: (r) => r.interval == "{interval}" and r.ticker == "{ticker}" and r._field == "close")'
            f'|> last()'
        )

        query_api = self.client.query_api()
        result = query_api.query(query, org=self.org)
        if len(result) == 0:
            return None
        
        return result[0].records[0]["_time"]
    
    def store_points(self,points:list[Point])-> None:
        options = WriteOptions()
        with self.client.write_api(write_options=options) as api:
            api.write(self.bucket,self.org,points)
        
        
    def get_data(self,ticker:Ticker,interval:str,values:list[str]=["close"],start_date:datetime|None=None,end_date:datetime|None=None)-> list[FluxTable]|None:
        """
        Querry data for the given ticker and interval
        """
        query = f'from(bucket: "{self.bucket}")'
        
        if end_date and start_date:
            query += f'|> range(start: {self._format_time(start_date)}, stop: {self._format_time(end_date)})'
        elif end_date and not start_date:
            query += f'|> range(start: 0, stop: {self._format_time(end_date)})'
        elif not end_date and start_date:
            query += f'|> range(start: {self._format_time(start_date)})'
        else:
             query += f'|> range(start: 0)'
        
        
        query += f'|> filter(fn: (r) => r.interval == "{interval}" and r.exchange == "{ticker.exchange}"  and r.ticker == "{ticker.ticker}")'
        value_fields = " or ".join(["r._field == \"{0}\"".format(value) for value in values])
        query += f'|> filter(fn: (r) => {value_fields})'
        query +=  f'|> keep(columns: ["_time","_field","_value"])'
        
        query_api = self.client.query_api()
        result = query_api.query(query, org=self.org)
        if len(result) == 0:
            return None
        
        return result
            
    def raw_query(self,query:str)-> list[FluxTable]:
        query_api = self.client.query_api()
        result = query_api.query(query, org=self.org)
        return result
            
if __name__ == "__main__":
    influxClient = InfluxClient(URL,TOKEN)
    ticker = Ticker("AAPL","NASDAQ")
    result = influxClient.get_data(ticker, "1d",values=["close","volume"])