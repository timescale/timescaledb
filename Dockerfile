FROM postgres:9.6.3

MAINTAINER Timescale

ENV PG_MAJOR 9.6

RUN apt-get update && apt-get install -y \
    build-essential \
    daemontools \
    postgresql-server-dev-$PG_MAJOR \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /build/{src,test/results}
COPY sql /build/sql
COPY src/*.c src/*.h build/src/
COPY test/expected /build/test/
COPY test/sql /build/test/
COPY test/runner.sh /build/test/runner.sh
COPY timescaledb.control /build/
COPY Makefile build/Makefile
RUN make -C /build install && rm -rf /build
