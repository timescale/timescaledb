FROM postgres:9.6.1

MAINTAINER Matvey Arye

ENV PG_MAJOR 9.6
    
RUN apt-get update && apt-get install -y \
    build-essential \
    daemontools \
    postgresql-server-dev-$PG_MAJOR \
   && rm -rf /var/lib/apt/lists/*

COPY extension /extension
RUN cd /extension && make install && rm -rf /extension