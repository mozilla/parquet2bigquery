FROM python:3-slim

WORKDIR /app

COPY requirements.txt /app/

RUN pip install -r requirements.txt

COPY . ./

RUN python setup.py install

WORKDIR /

ENTRYPOINT ["/usr/local/bin/bulkload"]
