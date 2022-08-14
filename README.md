# Finance-Data-Scraper
Packages to collect Stock and News data periodically from different sources and save it to databases.


Also includes python packages to easily access and consume the data.

## Finance-Stock-Scraper
Collects data from the  [Yahoo-Finance API](https://finance.yahoo.com/) and saves it to a [QuestDB](https://questdb.io/) instance.

### Server
Use the [docker-image](https://hub.docker.com/repository/docker/llukas22/finance-stock-scraper) to run the server. The server will collect data after each trading day. The tickers are provided via *.csv files that contain the ticker, name and long-name of the company. The files must be named after the exchange the tickers are listed on. An example can be found [here](tickers/NASDAQ.csv).

An example config can be found in the [docker-compose.yml](docker-compose.yml) file.


### Consume the Data
To consume the data, use the [pip package](https://pypi.org/project/finance-stock-scraper/).

```
pip install finance-stock-scraper
```

Then use the TickerRepository to get the data as a pandas DataFrame.

```python
from finance_stock_scraper.QuestClient import QuestClient
from finance_stock_scraper.TickerRepository import TickerRepository
from finance_stock_scraper.model.Ticker import Ticker

questClient = QuestClient(host=IP)
tickerRepository = TickerRepository(questClient)

#Build a ticker object
ticker = Ticker("GOOGL","NASDAQ")
#Get data from QuestDB
df_daily = tickerRepository.get_values(tickers=ticker,interval="1d",values=["open","close","high","low","volume"])
df_minutly = tickerRepository.get_values(tickers=ticker,interval="5m",values=["open","close","high","low","volume"],start_time=START_TIME,end_time=END_TIME)
```

## Finance-News-Scraper
Scrape news and save them to a [MongoDB](https://www.mongodb.com/) instance.
### Server
Use the [docker-image](https://hub.docker.com/repository/docker/llukas22/finance-news-scraper) to run the server. The server will collect articles from [Google-News](https://news.google.com/topstories), [FinViz](https://finviz.com/) or RSS-Feeds. Then a sentiment analysis will be performed and the articles will be saved to the database.


An example config can be found in the [docker-compose.yml](docker-compose.yml) file.
### Consume the Data
To consume the data, use the [pip package](https://pypi.org/project/finance-news-scraper/).

```
pip install finance-news-scraper
```
Then use the MongoDBClient to get the data as a pandas DataFrame.

```python
from finance_news_scraper.mongo_client import MongoDBClient

mongoClient = MongoDBClient(host=IP)

articles = mongoClient.get_articles(["GOOGL"])
sentiments = mongoClient.get_sentiments("GOOGL",frequency="h",start=START_TIME,end=END_TIME)
```

## Examples
For more examples, see the [demo](demo/demo.ipynb) notebook.