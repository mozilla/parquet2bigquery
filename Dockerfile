FROM python:3-slim

WORKDIR /var/tmp/p2b

COPY . ./

RUN python setup.py install

WORKDIR /

ENTRYPOINT ["/usr/local/bin/bulkload"]
