FROM python:3-slim

WORKDIR /app

COPY . ./

RUN python setup.py install

WORKDIR /

ENTRYPOINT ["/usr/local/bin/bulkload"]
