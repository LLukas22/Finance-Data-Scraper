FROM python:3.10

RUN apt update
RUN pip install prefect==2.0b11

RUN mkdir /app
ADD . /app
WORKDIR /app
CMD ["python","-u","test.py"]

