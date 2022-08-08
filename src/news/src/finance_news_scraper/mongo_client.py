from pymongo import MongoClient, ASCENDING, DESCENDING
import os
from finance_news_scraper.news_sources import News_Item
from newspaper import Article
import numpy as np
from pymongo.typings import _CollationIn, _DocumentIn, _DocumentType, _Pipeline
from pymongo.cursor import Cursor
from typing import Optional
from datetime import datetime

HOST = os.getenv('NEWSSCRAPER_MONGODB_HOST',"localhost")
PORT = int(os.getenv('NEWSSCRAPER_MONGODB_PORT',"27017"))
USERNAME = os.getenv('NEWSSCRAPER_MONGODB_USERNAME',"admin")
PASSWORD = os.getenv('NEWSSCRAPER_MONGODB_PASSWORD',"asda2sdqw12e4asfd")
DB_NAME = os.getenv('NEWSSCRAPER_MONGODB_DBNAME',"news")
ARTICLE_COLLECTION_NAME = os.getenv('NEWSSCRAPER_MONGODB_ARTICLE_COLLECTIONNAME',"articles")
SENTIMENT_COLLECTION_NAME = os.getenv('NEWSSCRAPER_MONGODB_SENTIMENT_COLLECTIONNAME',"sentiments")

class MongoDBClient(object):
    def __init__(self,host:str=HOST,port:int=PORT,username:str=USERNAME,password:str=PASSWORD) -> None:
        self.client = MongoClient(host=host, port=port, username=username, password=password)
        
        #create the db and collections with indexes
        self.db = self.client[DB_NAME]
        collections = self.db.list_collection_names()
        if ARTICLE_COLLECTION_NAME not in collections:
            self.db.create_collection(ARTICLE_COLLECTION_NAME,storageEngine={"wiredTiger": {"configString": "block_compressor=zstd"}})
            self.article_collection = self.db[ARTICLE_COLLECTION_NAME]
            self.article_collection.create_index([("date",DESCENDING)],background=True)
            self.article_collection.create_index([("hash",ASCENDING)],background=True)
        else:
            self.article_collection = self.db[ARTICLE_COLLECTION_NAME]

            
        if SENTIMENT_COLLECTION_NAME not in collections:
            self.db.create_collection(SENTIMENT_COLLECTION_NAME,storageEngine={"wiredTiger": {"configString": "block_compressor=zstd"}})
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
        
    def build_document(self,rss_item:News_Item,article:Article)->dict:
        return {
                'url': rss_item.link,
                'hash': rss_item.hash,
                'text': article.text,
                'authors': article.authors,
                'tickers': rss_item.tickers,
                'date': rss_item.pub_date,
                'publisher':  rss_item.publisher,
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
        return self.__update_tickers(self.sentiment_collection,_id,old_tickers,new_tickers)
        
        
    def update_article_tickers(self,_id:_DocumentIn,old_tickers:list,new_tickers:list)->bool:
        """
        Updates the tickers  of an article if needed
        """
        return self.__update_tickers(self.article_collection,_id,old_tickers,new_tickers)
    
    
    def __get_by_tickers_and_date(self,collection,tickers:list[str],start:datetime=None,end:datetime=None)->Cursor[_DocumentType]:
        """
        Find all articles that match the tickers and the date range
        """
        
        tickers = list(ticker.upper() for ticker in set(tickers))
        if len(tickers) < 1:
            raise Exception("A ticker must be provided!")
        
        if start and end:
            result = collection.find({ 'tickers': { '$all': tickers },'date': {'$gte': start,'$lt': end}})
        elif start and not end:
            result = collection.find({ 'tickers': { '$all': tickers },'date': {'$gte': start}})
        elif end and not start:
            result = collection.find({ 'tickers': { '$all': tickers },'date': {'$lt': end}})
        else:
            result = collection.find({ 'tickers': { '$all': tickers }})
        return result.sort('date',DESCENDING)
    
    
    def get_articles(self,tickers:list[str],start:datetime=None,end:datetime=None)->Cursor[_DocumentType]:
        """
        Find all articles that match the tickers and the date range
        """
        return self.__get_by_tickers_and_date(self.article_collection,tickers,start,end)
    
    def get_sentiments(self,tickers:list[str],start:datetime=None,end:datetime=None)->Cursor[_DocumentType]:
        """
        Find all sentiments by tickers and the date range
        """
        return self.__get_by_tickers_and_date(self.sentiment_collection,tickers,start,end)

            
            