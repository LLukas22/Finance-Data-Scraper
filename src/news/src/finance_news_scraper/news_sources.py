import logging
import hashlib
from bs4 import BeautifulSoup
import requests
from dateutil.parser import parse
import pytz
from google_news_feed import GoogleNewsFeed
from datetime import datetime, timedelta
from tqdm import tqdm
import time
import random 
import urllib.parse

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0'

class News_Item(object):
    def __init__(self,publisher:str,link:str,tickers:list[str],pub_date:datetime) -> None:
        self.publisher = publisher.upper()
        self.link = link
        self.pub_date = pub_date
        self.tickers = tickers
        self.__hash = None
                   
    @property
    def hash(self)->str:
        if self.__hash:
            return self.__hash
        else:
            self.__hash = hashlib.sha512((self.link).encode("UTF-8")).hexdigest()
            return self.__hash

def get_rss_items(rss_url:str,publisher:str) -> list[News_Item]:
    news_items = []
    r = requests.get(rss_url)
    webpage = r.content
    soup = BeautifulSoup(webpage, features='xml')
    items = soup.find_all('item')
    for item in items:
        link =  item.find('link').text
        dt = parse(item.find('pubDate').text).astimezone(pytz.UTC)
        #Some sites give us the tickers of the article
        tickers = None
        if item.find('category'):
            categories = item.find_all('category')
            tickers = [category.text for category in categories]
        news_items.append(News_Item(publisher,link,tickers,dt))
    return news_items

def get_google_news_items(tickers:list[tuple[str,str,str]]) -> list[News_Item]:
    news_items = []
    gnf = GoogleNewsFeed()
    for ticker,shortname,longname in tqdm(tickers,desc="Google News"):
        try:
            time.sleep(random.uniform(0.5,1.0)) # try to avoid being rate limited
            results = gnf.query(f"{ticker} OR {shortname} OR {longname}",when="2w")
            for result in results:
                news_items.append(News_Item(result.source,result.link,[ticker],result.pubDate))
        except Exception as e:
            logging.error(e)
    return news_items

def get_finviz_news_items(tickers:list[tuple[str,str,str]]) -> list[News_Item]:
    news_items = []
    finwiz_url = 'https://finviz.com/quote.ashx?t='
    for ticker,shortname,longname in tqdm(tickers,desc="FinViz News"):
        try:
            time.sleep(random.uniform(0.5,1.0)) # try to avoid being rate limited
            url = finwiz_url + ticker.lower()
            result = requests.get(url,headers = {'User-Agent': USER_AGENT})
            if result.status_code == 200:
                html = BeautifulSoup(result.content, features='html.parser')
                news_table = html.find(id='news-table')
                
                for x in news_table.findAll('tr'):
                    link = x.a.attrs['href']
                    publisher = x.span.get_text().strip()

                    date_scrape = x.td.text.split()
                    datetime = parse(" ".join(date_scrape)).astimezone(pytz.UTC)
                    news_items.append(News_Item(publisher,link,[ticker],datetime))
        except Exception as e:
            logging.error(e)
    return news_items
            

            
    