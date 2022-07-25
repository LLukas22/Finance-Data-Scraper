FROM python:3.10

ADD requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

RUN mkdir /var/lib/stock-scraper
RUN mkdir /app
ADD . /app
WORKDIR /app
CMD ["python","-u","server.py"]

