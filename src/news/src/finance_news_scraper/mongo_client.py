import pymongo
import os
from finance_news_scraper.news_sources import News_Item
from newspaper import Article
import numpy as np
from pymongo import ASCENDING, DESCENDING
from pymongo.typings import _CollationIn, _DocumentIn, _DocumentType, _Pipeline
from pymongo.cursor import Cursor
from typing import Optional
from datetime import datetime
import pandas as pd

HOST = os.getenv('NEWSSCRAPER_MONGODB_HOST',"localhost")
PORT = int(os.getenv('NEWSSCRAPER_MONGODB_PORT',"27017"))
USERNAME = os.getenv('NEWSSCRAPER_MONGODB_USERNAME',"admin")
PASSWORD = os.getenv('NEWSSCRAPER_MONGODB_PASSWORD',"asda2sdqw12e4asfd")
DB_NAME = os.getenv('NEWSSCRAPER_MONGODB_DBNAME',"news")
ARTICLE_COLLECTION_NAME = os.getenv('NEWSSCRAPER_MONGODB_ARTICLE_COLLECTIONNAME',"articles")
SENTIMENT_COLLECTION_NAME = os.getenv('NEWSSCRAPER_MONGODB_SENTIMENT_COLLECTIONNAME',"sentiments")

class MongoDBClient(object):
    def __init__(self,host:str=HOST,port:int=PORT,username:str=USERNAME,password:str=PASSWORD,better_compression=True) -> None:
        self.client = pymongo.MongoClient(host=host, port=port, username=username, password=password)
        
        #create the db and collections with indexes
        self.db = self.client[DB_NAME]
        collections = self.db.list_collection_names()
        if ARTICLE_COLLECTION_NAME not in collections:
            if better_compression:
                self.db.create_collection(ARTICLE_COLLECTION_NAME,storageEngine={"wiredTiger": {"configString": "block_compressor=zstd"}})
            else:
                self.db.create_collection(ARTICLE_COLLECTION_NAME)
            self.article_collection = self.db[ARTICLE_COLLECTION_NAME]
            self.article_collection.create_index([("date",DESCENDING)],background=True)
            self.article_collection.create_index([("hash",ASCENDING)],background=True)
        else:
            self.article_collection = self.db[ARTICLE_COLLECTION_NAME]

            
        if SENTIMENT_COLLECTION_NAME not in collections:
            if better_compression:
                self.db.create_collection(SENTIMENT_COLLECTION_NAME,storageEngine={"wiredTiger": {"configString": "block_compressor=zstd"}})
            else:
                self.db.create_collection(SENTIMENT_COLLECTION_NAME)
            self.sentiment_collection = self.db[SENTIMENT_COLLECTION_NAME]
            self.sentiment_collection.create_index([("date",DESCENDING)],background=True)
            self.sentiment_collection.create_index([("article_hash",ASCENDING)],background=True)
        else:
            self.sentiment_collection = self.db[SENTIMENT_COLLECTION_NAME]
            
            
    def insert_sentiment(self,sentiment:tuple[int,np.ndarray],hash:str,datetime:datetime,tickers:list[str])->None:
        data={
            'article_hash':hash,
            'tickers':tickers,
            'date':datetime,
            'sentiment':{
                'class':sentiment[0],
                'probabilities':sentiment[1].tolist()
            } 
        }
        self.sentiment_collection.insert_one(data)
        
    def build_document(self,item:News_Item,article:Article)->dict:
        return {
                'url': item.link,
                'hash': item.hash,
                'text': article.text,
                'authors': article.authors,
                'tickers': item.tickers,
                'date': item.pub_date,
                'publisher':  item.publisher,
            }
        
    def insert_article(self,data:dict)->None:
        self.article_collection.insert_one(data)
        
    def insert_articles(self,articles:list[dict])->None:
        if len(articles) > 0:
            self.article_collection.insert_many(articles)
        
    def find_article_by_hash(self,hash:str)->Optional[_DocumentType]:
        """
        Returns an article if it exists in the database
        """
        return self.article_collection.find_one({'hash':hash})
    
    def find_sentiment_by_hash(self,hash:str)->Optional[_DocumentType]:
        """
        Returns a sentiment if it exists in the database
        """
        return self.sentiment_collection.find_one({'article_hash':hash})
    
    def get_all_article_hashes(self)->set[str]:
        return set(self.article_collection.distinct('hash'))
    
    def get_all_sentiment_hashes(self)->set[str]:
        return set(self.sentiment_collection.distinct('article_hash'))
    
    def __update_tickers(self,collection,_id:_DocumentIn,old_tickers:list,new_tickers:list)->bool:
        tickers=[]
        needs_update = False
        
        if new_tickers:
            if not old_tickers:
                needs_update = True
                tickers = list(set(new_tickers))
            else:
                if not set(old_tickers) == set(new_tickers):
                    needs_update = True
                    tickers = list(set(old_tickers+new_tickers))
                    
        if needs_update:
            collection.update_one({'_id':_id},{'$set':{'tickers':tickers}})
            return True
        return False
    
    def update_sentiment_tickers(self,_id:_DocumentIn,old_tickers:list,new_tickers:list)->bool:
        """
        Updates the tickers of an sentiment if needed
        """
        return self.__update_tickers(self.sentiment_collection,_id,old_tickers,new_tickers)
        
        
    def update_article_tickers(self,_id:_DocumentIn,old_tickers:list,new_tickers:list)->bool:
        """
        Updates the tickers of an article if needed
        """
        return self.__update_tickers(self.article_collection,_id,old_tickers,new_tickers)
    
    
    def __get_by_tickers_and_date(self,collection,tickers:list[str],start:datetime=None,end:datetime=None)->Cursor[_DocumentType]:
        """
        Find all documents that match the tickers and the date range
        """
        
        tickers = list(ticker.upper() for ticker in set(tickers))
        if len(tickers) < 1:
            raise Exception("A ticker must be provided!")
        
        if start and end:
            result = collection.find({ 'tickers': { '$in': tickers },'date': {'$gte': start,'$lt': end}})
        elif start and not end:
            result = collection.find({ 'tickers': { '$in': tickers },'date': {'$gte': start}})
        elif end and not start:
            result = collection.find({ 'tickers': { '$in': tickers },'date': {'$lt': end}})
        else:
            result = collection.find({ 'tickers': { '$in': tickers }})
        return result.sort('date',DESCENDING)
    
    
    def get_articles(self,tickers:list[str],start:datetime=None,end:datetime=None)->Cursor[_DocumentType]:
        """
        Find all articles that match the tickers and the date range
        """
        return self.__get_by_tickers_and_date(self.article_collection,tickers,start,end)
    
    def get_raw_sentiments(self,tickers:list[str],start:datetime=None,end:datetime=None)->Cursor[_DocumentType]:
        """
        Find all sentiments by tickers and the date range
        """
        return self.__get_by_tickers_and_date(self.sentiment_collection,tickers,start,end)
    
    

    def __build_sentiment_dataframe(self,sentiments:list[dict])->pd.DataFrame:
        pd_data = []
        for sentiment in sentiments:
            pd_data.append({"date":sentiment["date"],"sentiment":sentiment["sentiment"]["class"],"certainty":max(sentiment["sentiment"]["probabilities"])})
        return pd.DataFrame(pd_data)



    def _get_weighted_sentiment(self,group:pd.DataFrame) -> float:
            if 'sentiment' in group.columns and 'certainty' in group.columns:
                return (group['sentiment'] * group['certainty']).mean()
            else:
                return pd.np.nan
      
      
      
    def _raw_sentiment_to_dataframe(self,
                                    sentiments:list[dict],
                                    frequency:str="d",
                                    fill_blanks:bool=True,
                                    interpolate_values:bool=True,
                                    interpolation:str='linear')->pd.DataFrame:
        #create df
        df = self.__build_sentiment_dataframe(sentiments=sentiments)
        #set the period and mean the sentiment
        df = (df
                .groupby(df['date'].dt.to_period(frequency).dt.start_time)
                .apply(lambda x: self._get_weighted_sentiment(x))
                .reset_index(name='sentiment')
                .set_index('date'))
        
        #create the times we dont have data for
        if fill_blanks:
            df = df.asfreq(frequency)
            
        #interpolate all nan values   
        if interpolate_values:
            df = df.interpolate(method='linear')
                
        return df
        
           
    def get_sentiments(self,tickers:str|list[str],
                       start:datetime=None,
                       end:datetime=None,
                       frequency:str="d",
                       fill_blanks:bool=True,
                       interpolate_values:bool=True,
                       interpolation:str='linear')->dict[str,pd.DataFrame]|pd.DataFrame:
        
        """
        Retrievs the mean sentiment for the tickers and date range and returns a dataframe for each ticker.
        
        'frequency': the frequency used to group the data. Can be 'd' for daily, 'w' for weekly, 'm' for monthly.For more Options see here: https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
        
        'fill_blanks': if True, the dataframe will be expanded to match the provided frequency. All created rows are initized with NaN.
        
        'interpolate_values': if True all NaN values will be interpolated using the interpolation method.
        
        'interpolation': Interpolation method. Can be 'linear' or other methode. See here: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.interpolate.html
        """
        if tickers is None:
            raise Exception("A ticker must be provided!")
        
        if isinstance(tickers,str):
           tickers = [tickers] 
        
        grouped_sentiments = {}
        data = self.get_raw_sentiments(tickers,start,end)
        ticker_set = set(tickers)
        
        #init the grouped sentiments
        for ticker in ticker_set:
            grouped_sentiments[ticker] = []
            
        for sentiment in data:
            #get all tickers that match this sentiment
            sentiment_tickers = set(sentiment['tickers'])
            for matching in ticker_set & sentiment_tickers:
                grouped_sentiments[matching].append(sentiment)
                
        #build the dataframes
        
        dataframes = {}
        for ticker in ticker_set:
            df = self._raw_sentiment_to_dataframe(list(grouped_sentiments[ticker]),frequency,fill_blanks,interpolate_values,interpolation)    
            dataframes[ticker] = df
            
        if len(dataframes) == 1:
            return next(iter(dataframes.values()))
        return dataframes

        
            
            