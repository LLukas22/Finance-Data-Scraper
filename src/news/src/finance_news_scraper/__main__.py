import json
import os
import logging
from finance_news_scraper.mongo_client import MongoDBClient
from tqdm import tqdm 
from newspaper import Article
from newspaper.article import ArticleException
from finance_news_scraper.sentiment import SentimentProvider
from finance_news_scraper.news_sources import get_rss_items,get_finviz_news_items,get_google_news_items
from newspaper import Config
import pandas as pd
from finance_news_scraper.news_sources import News_Item
from tqdm.contrib.concurrent import thread_map
import time

RSS_DIR = os.path.abspath(os.getenv('NEWSSCRAPER_RSS_DIR',"../../../news"))
TICKERS_DIR = os.path.abspath(os.getenv('STOCKSCRAPER_TICKERS_DIR',"../../../Tickers"))
DEBUG = os.getenv('NEWSSCRAPER_DEBUG',"True").upper() == "TRUE"
MODE = os.getenv('NEWSSCRAPER_MODE',"Single").upper() # Single or Scheduled
SLEEP_TIME = int(os.getenv('NEWSSCRAPER_SLEEPTIME',60*60*6)) # 6 hours
SENTIMENT_MODE = os.getenv('NEWSSCRAPER_SENTIMENT_MODE',"ALL").upper() # ALL or NEW
PERFORM_SENTIMENT_ANALYSIS = os.getenv('NEWSSCRAPER_SENTIMENT_ANALYSIS',"TRUE").upper() == "TRUE"
PERFORM_NEWS_SCRAPING = os.getenv('NEWSSCRAPER_SCRAPE_NEWS',"TRUE").upper() == "TRUE"
DOWNLOAD_RSS_FEED = os.getenv('NEWSSCRAPER_DOWNLOAD_RSS_FEED',"FALSE").upper() == "TRUE"
DOWNLOAD_GOOGLE_NEWS = os.getenv('NEWSSCRAPER_DOWNLOAD_GOOGLE_NEWS',"TRUE").upper() == "TRUE"
DOWNLOAD_FINVIZ_NEWS = os.getenv('NEWSSCRAPER_DOWNLOAD_FINVIZ_NEWS',"FALSE").upper() == "TRUE"

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0'
JS_ERROR_TEXTS = [
    "please enable Javascript",
    "Javascript is Disabled"
]

if __name__ == "__main__":
    
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("huggingface").setLevel(logging.WARNING)
    logging.getLogger("newspaper").setLevel(logging.WARNING)
    logging.getLogger("transformers").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    
    if DEBUG:
        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s',level=logging.DEBUG)
    else:
        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s',level=logging.INFO)
        
    logging.info(f"RSS_DIR:{RSS_DIR}")
    logging.info(f"TICKERS_DIR:{TICKERS_DIR}")
    
    os.makedirs(RSS_DIR,exist_ok=True)
    os.makedirs(TICKERS_DIR,exist_ok=True)
    
    rss_feeds = None
    rss_file = os.path.join(RSS_DIR,"rss-feeds.json")
    if os.path.isfile(rss_file):
        with open(rss_file) as f:
            rss_feeds = json.load(f)

    files = [file for file in os.listdir(TICKERS_DIR) if file.endswith(".csv")]
    logging.info(f"Found Ticker files: {','.join(files)}")
       
      
    tickers=[]               
    for file in files:
        file = os.path.join(TICKERS_DIR,file)
        exchange = os.path.basename(file).split('.')[0].upper()
        local_tickers = pd.read_csv(file)
        for ticker in local_tickers.values:
            if ticker[0] is not None and isinstance(ticker[0],str):
                tickers.append((ticker[0],ticker[1],ticker[2]))
                    
    config = Config()
    config.browser_user_agent = USER_AGENT
    config.request_timeout = 7
    config.fetch_images = False
    
    mongoClient = MongoDBClient()
    sentimentProvider = SentimentProvider()
    
    while True:
        news_items:list[News_Item] = []
        
        if PERFORM_NEWS_SCRAPING:
            
            if DOWNLOAD_GOOGLE_NEWS and len(tickers) > 0:
                news_items += get_google_news_items(tickers[:10])
                
            if DOWNLOAD_FINVIZ_NEWS and len(tickers) >  0:
                news_items += get_finviz_news_items(tickers)
                
            if DOWNLOAD_RSS_FEED and rss_feeds:
                for publisher,feed in tqdm(rss_feeds.items(),desc="RSS Feeds"):
                    news_items += get_rss_items(feed,publisher)
            
        #Group by site hash
        grouped_news_items = {}
        for news_item in news_items:
            if news_item.hash in grouped_news_items:
                grouped_news_items[news_item.hash].append(news_item)
            else:
                grouped_news_items[news_item.hash] = [news_item]
                
        cleaned_news_items = []
        
        #Collaps duplicates into a single item
        for hash,news_items in grouped_news_items.items():
            news_item = news_items[0]
            tickers = []
            for item in news_items:
                if item.tickers:
                    tickers += item.tickers
            tickers = list(set(tickers))
            news_item.tickers = tickers
            cleaned_news_items.append(news_item)
        
        def download_news_items(news_items:list[News_Item])->list[str]:
            
            hashes = []
            to_store = []
            for news_item in news_items:
                try:
                    existing_entry = mongoClient.find_article_by_hash(news_item.hash)
                    if existing_entry:
                        #if needed update the tickers in the entry
                        if mongoClient.update_article_tickers(existing_entry["_id"],existing_entry["tickers"],news_item.tickers):
                            hashes.append(news_item.hash)
                        continue
                    
                    article = Article(news_item.link,language="en",config=config, fetch_images=False)
                    article.download()
                    article.parse()
                    
                    for js_error_text in JS_ERROR_TEXTS:
                        if js_error_text in article.text:
                            logging.debug(f"JS ERROR: {news_item.link}")
                            failed_downloads.append(news_item.link)
                            continue
                         
                    to_store.append(mongoClient.build_document(news_item,article))
                    hashes.append(news_item.hash)
                  
                except ArticleException as articleException:
                    logging.debug(f"ArticleError: {articleException}")
                    failed_downloads.append(news_item.link)
                      
                except Exception as e:
                    logging.error(f"{e}")
                    continue
                
            mongoClient.insert_articles(to_store)
            return hashes
          

        hashes = []
        failed_downloads = []
        logging.info(f"Found {len(cleaned_news_items)} articles!")
        if len(cleaned_news_items) > 0:
            list_of_hashes = []
            
            def chunks(lst, n):
                """Yield successive n-sized chunks from lst."""
                for i in range(0, len(lst), n):
                    yield lst[i:i + n]
                
            def flatten(l):
                return [item for sublist in l for item in sublist]
                      
            list_of_hashes = thread_map(download_news_items,list(chunks(cleaned_news_items,25)),max_workers=16,desc="Downloading News Items")
            hashes = flatten(list_of_hashes)
            
            hashes = set([hash for hash in list(set(hashes)) if hash])
            logging.info(f"Failed {len(failed_downloads)} Downloads!")
            logging.info(f"Stored {len(hashes)} items!")
        
        #Sentiment Analysis
        if PERFORM_SENTIMENT_ANALYSIS:
            logging.info(f"Start Sentiment Analysis")
            
            def analyse_article(hash:str):
                try:
                    article = mongoClient.find_article_by_hash(hash)
                    if article:
                        existing_sentiment = mongoClient.find_sentiment_by_hash(hash)
                        if existing_sentiment:
                            #if needed update the tickers in the entry
                            mongoClient.update_sentiment_tickers(existing_sentiment["_id"],existing_sentiment["tickers"],article["tickers"])
                            return
                            
                        sentiment = sentimentProvider.get_sentiment(article["text"])
                        mongoClient.insert_sentiment(sentiment,hash,article["date"],article["tickers"])
                except Exception as e:
                    logging.error(f"{e}")
            
            sentiments_to_calculate = []
            
            if SENTIMENT_MODE == "ALL":
                #Get all Articles that have no sentiment
                sentiments_to_calculate = mongoClient.get_all_article_hashes().difference(mongoClient.get_all_sentiment_hashes())
            else:
                #Only process the new articles
                sentiments_to_calculate = hashes
            
            if len(sentiments_to_calculate) > 0:
                for hash in tqdm(sentiments_to_calculate,desc="Sentiment Analysis"):
                    analyse_article(hash)
            logging.info(f"Finished Sentiment Analysis!")   
               
        if MODE == "SINGLE":
            break
        
        logging.info(f"Sleeping for {SLEEP_TIME} seconds ...")
        sentimentProvider.dispose_model()
        time.sleep(SLEEP_TIME)