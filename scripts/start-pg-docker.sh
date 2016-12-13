#!/bin/bash

IMAGE_NAME=${IOBEAMDB_DOCKER_IMAGE:-registry.iobeam.com/iobeam/postgres-9.5-wale:master}
DOCKER_HOST=${DOCKER_HOST:-localhost}

docker run -d --name iobeamdb -p 5432:5432 -e POSTGRES_DB=test  -m 4g \
  -e "NO_BACKUPS=1" \
  $IMAGE_NAME postgres \
  -csynchronous_commit=off -cwal_writer_delay=1000 \
  -cmax_locks_per_transaction=1000 \
  -cshared_preload_libraries=pg_stat_statements \
  -cvacuum_cost_delay=20 -cautovacuum_max_workers=1 \
  -clog_autovacuum_min_duration=1000 -cshared_buffers=1GB \
  -ceffective_cache_size=3GB -cwork_mem=52428kB \
  -cmaintenance_work_mem=512MB -ciobeam.hostname=local \
  -clog_line_prefix="%m [%p]: [%l-1] %u@%d" \
  -clog_error_verbosity=VERBOSE

for i in {1..10}; do 
  sleep 2
  pg_isready -h $DOCKER_HOST -p 5432
  if [[ $? == 0 ]] ; then
    exit 0
  fi

done

