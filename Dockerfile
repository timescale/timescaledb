FROM postgres:9.5

MAINTAINER Matvey Arye

ENV PG_MAJOR 9.5

COPY sql /sql

CMD ["psql"]