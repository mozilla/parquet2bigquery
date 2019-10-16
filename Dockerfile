FROM python:3-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc

COPY . ./

RUN python setup.py install

WORKDIR /

ENTRYPOINT ["/usr/local/bin/bulkload"]
