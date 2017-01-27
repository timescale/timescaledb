#!/bin/bash
set -e

if [[ -z "$IMAGE_NAME" ]]; then
  echo "The IMAGE_NAME must be set"
  exit 1
fi

DOCKER_HOST=${DOCKER_HOST:-localhost}
CONTAINER_NAME=${CONTAINER_NAME:-iobeamdb}
DATA_DIR=${DATA_DIR-$PWD/data}
BIN_CMD=${BIN_CMD:-postgres}
PGPORT=${PGPORT:=5432}

VOLUME_MOUNT=""
if [[ -n "$DATA_DIR" ]]; then
  VOLUME_MOUNT="-v $DATA_DIR:/var/lib/postgresql/data"
fi
docker run -d \
  --name $CONTAINER_NAME $VOLUME_MOUNT \
  -p ${PGPORT}:5432 \
  -m 4g \
  -e PGDATA=/var/lib/postgresql/data/iobeam \
  $IMAGE_NAME $BIN_CMD \
  -csynchronous_commit=off -cwal_writer_delay=1000 \
  -cmax_locks_per_transaction=1000 \
  -cshared_preload_libraries=pg_stat_statements,dblink,iobeamdb \
  -cvacuum_cost_delay=20 -cautovacuum_max_workers=1 \
  -clog_autovacuum_min_duration=1000 -cshared_buffers=1GB \
  -ceffective_cache_size=3GB -cwork_mem=52428kB \
  -cmaintenance_work_mem=512MB -ciobeam.hostname=local \
  -clog_line_prefix="%m [%p]: [%l-1] %u@%d" \
  -clog_error_verbosity=VERBOSE

set +e
for i in {1..10}; do 
  sleep 2

  pg_isready -h $DOCKER_HOST

  if [[ $? == 0 ]] ; then
    exit 0
  fi

done

