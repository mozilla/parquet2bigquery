FROM python:3

WORKDIR /var/tmp/p2b

COPY . ./

RUN python setup.py install

WORKDIR /

ENTRYPOINT ["/usr/local/bin/bulkload"]
