FROM python:3.10

ADD requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

RUN mkdir /var/lib/news-scraper
RUN mkdir /var/lib/news-scraper/tickers
RUN mkdir /var/lib/news-scraper/model
RUN mkdir /var/lib/news-scraper/rss

RUN mkdir /app
ADD ./src /app
WORKDIR /app
CMD ["python","-m","finance_news_scraper"]

