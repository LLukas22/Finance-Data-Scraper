from multiprocessing.context import assert_spawning
import mongomock
import pymongo
from finance_news_scraper.mongo_client import MongoDBClient
from finance_news_scraper.news_sources import News_Item
from datetime import datetime
from newspaper import Article
import numpy as np
import pytz
import pandas as pd

@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_creates_collections():
    client = MongoDBClient('server.example.com',27017,better_compression=False)
    assert client.article_collection is not None
    assert client.sentiment_collection is not None

@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_can_build_article():
  client = MongoDBClient('server.example.com',27017,better_compression=False)
  news_item = News_Item('publisher', 'link', ["A", "B"], datetime.now())
  article = Article(news_item.link)
  article.text = "Test Text"
  article.authors = ["Foo","Bar"]
  document = client.build_document(news_item,article)
  assert document['text'] == "Test Text"
  assert document['authors'] == ["Foo","Bar"]
  assert document['url'] == news_item.link
  assert document['hash'] == news_item.hash
  assert document['publisher'] == news_item.publisher
  assert document['tickers'] == news_item.tickers
  assert document['date'] == news_item.pub_date
  
@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_can_insert_article():
  client = MongoDBClient('server.example.com',27017,better_compression=False)
  news_item = News_Item('publisher', 'link', ["A", "B"], datetime.now())
  article = Article(news_item.link)
  article.text = "Test Text"
  article.authors = ["Foo","Bar"]
  document = client.build_document(news_item,article)
  client.insert_article(document)
  inserted = client.article_collection.find_one()
  assert inserted['hash'] == news_item.hash
  
  
@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_can_find_article_by_hash():
  client = MongoDBClient('server.example.com',27017,better_compression=False)
  client.article_collection.insert_one({'hash':'foobar'})
  found = client.find_article_by_hash('foobar')
  assert found is not None
  
@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_can_insert_sentiment():
  client = MongoDBClient('server.example.com',27017,better_compression=False)
  now = datetime.now()
  client.insert_sentiment((1,np.array([1,2,3])),'foobar',now,['A','B'])
  inserted = client.sentiment_collection.find_one()
  assert inserted['article_hash'] == 'foobar'
  assert inserted['tickers'] == ['A','B']
  assert inserted['sentiment']['class'] == 1
  assert inserted['sentiment']['probabilities'] == [1,2,3]

@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_can_get_articles():
  client = MongoDBClient('server.example.com',27017,better_compression=False)
  client.article_collection.insert_many([
    {"hash":"A","tickers":["A"]},
    {"hash":"B","tickers":["A","B"]},
    {"hash":"C","tickers":["B"]}
  ])
  articles = list(client.get_articles(["A"]))
  assert articles is not None
  assert len(articles) == 2

@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_can_get_articles_with_startdate():
  client = MongoDBClient('server.example.com',27017,better_compression=False)

  client.article_collection.insert_many([
    {"hash":"A","tickers":["A"], "date":datetime(2018,1,1)},
    {"hash":"B","tickers":["A"], "date":datetime(2019,1,1)},
    {"hash":"C","tickers":["A"],"date":datetime(2020,1,1)},
    {"hash":"D","tickers":["A"],"date":datetime(2021,1,1)}
  ])
  articles = list(client.get_articles(["A"],start=datetime(2019,6,6)))
  assert articles is not None
  assert len(articles) == 2
  assert articles[0]['hash'] == 'D'
  assert articles[1]['hash'] == 'C'
  
@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_can_get_articles_with_enddate():
  client = MongoDBClient('server.example.com',27017,better_compression=False)

  client.article_collection.insert_many([
    {"hash":"A","tickers":["A"], "date":datetime(2018,1,1)},
    {"hash":"B","tickers":["A"], "date":datetime(2019,1,1)},
    {"hash":"C","tickers":["A"],"date":datetime(2020,1,1)},
    {"hash":"D","tickers":["A"],"date":datetime(2021,1,1)}
  ])
  articles = list(client.get_articles(["A"],end=datetime(2019,6,6)))
  assert articles is not None
  assert len(articles) == 2
  assert articles[0]['hash'] == 'B'
  assert articles[1]['hash'] == 'A'
  
@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_can_get_articles_with_start_and_enddate():
  client = MongoDBClient('server.example.com',27017,better_compression=False)

  client.article_collection.insert_many([
    {"hash":"A","tickers":["A"], "date":datetime(2018,1,1)},
    {"hash":"B","tickers":["A"], "date":datetime(2019,1,1)},
    {"hash":"C","tickers":["A"],"date":datetime(2020,1,1)},
    {"hash":"D","tickers":["A"],"date":datetime(2021,1,1)},
    {"hash":"E","tickers":["A"],"date":datetime(2022,1,1)}
  ])
  articles = list(client.get_articles(["A"],start=datetime(2019,6,6),end=datetime(2021,6,6)))
  assert articles is not None
  assert len(articles) == 2
  assert articles[0]['hash'] == 'D'
  assert articles[1]['hash'] == 'C'
  
@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_can_get_raw_sentiment():
    client = MongoDBClient('server.example.com',27017,better_compression=False)

    client.sentiment_collection.insert_many([
      {"tickers":["A"],"sentiment":{"class":1,'probabilities':[0,0,1] },"date":datetime(2018,1,1)},
      {"tickers":["A"],"sentiment":{"class":1,'probabilities':[0,0,1] },"date":datetime(2019,1,1)},
      {"tickers":["B"],"sentiment":{"class":1,'probabilities':[0,0,1] },"date":datetime(2020,1,1)},
    ])
    articles = list(client.get_raw_sentiments(["A"]))
    assert articles is not None
    assert len(articles) == 2
    
@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_can_get_sentiment():
    client = MongoDBClient('server.example.com',27017,better_compression=False)

    client.sentiment_collection.insert_many([
      {"tickers":["A"],"sentiment":{"class":1,'probabilities':[0,0,1] },"date":datetime(2018,1,1)},
      {"tickers":["A"],"sentiment":{"class":1,'probabilities':[0,0,1] },"date":datetime(2019,1,1)},
      {"tickers":["B"],"sentiment":{"class":1,'probabilities':[0,0,1] },"date":datetime(2020,1,1)},
    ])
    df = client.get_sentiments(["A"],frequency='Y',fill_blanks=False)
    assert isinstance(df,pd.DataFrame)
    assert len(df) == 2
    
    
@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_get_sentiment_interpolates_values():
    client = MongoDBClient('server.example.com',27017,better_compression=False)

    client.sentiment_collection.insert_many([
      {"tickers":["A"],"sentiment":{"class":1,'probabilities':[0,0,1] },"date":datetime(2018,1,1)},
      {"tickers":["A"],"sentiment":{"class":0,'probabilities':[0,1,0] },"date":datetime(2019,1,1)},
      {"tickers":["A"],"sentiment":{"class":-1,'probabilities':[1,0,0] },"date":datetime(2020,1,1)},
    ])
    df = client.get_sentiments("A",frequency='D',fill_blanks=True)
    assert isinstance(df,pd.DataFrame)
    assert len(df) == 731
    assert df.isna().sum().sum() == 0
    
@mongomock.patch(servers=(('server.example.com', 27017),))
def test_client_get_sentiment_can_get_2_sentiments():
    client = MongoDBClient('server.example.com',27017,better_compression=False)

    client.sentiment_collection.insert_many([
      {"tickers":["A"],"sentiment":{"class":1,'probabilities':[0,0,1] },"date":datetime(2018,1,1)},
      {"tickers":["A"],"sentiment":{"class":0,'probabilities':[0,1,0] },"date":datetime(2019,1,1)},
      {"tickers":["B"],"sentiment":{"class":-1,'probabilities':[1,0,0] },"date":datetime(2020,1,1)},
    ])
    result = client.get_sentiments(["A","B"],frequency='Y',fill_blanks=False)
    assert isinstance(result,dict)
    assert len(result) == 2
    assert len(result['B']) == 1
    assert len(result['A']) == 2