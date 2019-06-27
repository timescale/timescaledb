#!/usr/bin/env bash
#
# This script test abi-compatibility wrt timescale between minor version of postgres
# It will compile timescale on a release version of postgres ($PG_MAJOR.PG_MINOR_COMPILE)
# and then run that compiled .so on the latest snapshot of postgres ($PG_MAJOR branch).
# This script will then run the regression tests on that machine.
#
set -e

PG_MAJOR=${PG_MAJOR:-10}
PG_MINOR_COMPILE=${PG_MINOR_COMPILE:-2}


SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
PG_IMAGE_TAG_COMPILE=${PG_IMAGE_TAG_COMPILE:-postgres:$PG_MAJOR.$PG_MINOR_COMPILE-alpine}
PG_IMAGE_TAG_RUN=${PG_IMAGE_TAG_RUN:-postgres_snapshot:$PG_MAJOR-snapshot-alpine}
CONTAINER_NAME_COMPILE=${CONTAINER_NAME_COMPILE:-pgtest_compile}
CONTAINER_NAME_RUN=${CONTAINER_NAME_RUN:-pgtest_run}
COMPILE_VOLUME=${COMPILE_VOLUME:-pgtest_compile}

set +e
docker rm -f $(docker ps -a -q -f name=${CONTAINER_NAME_COMPILE} 2>/dev/null) 2>/dev/null
docker rm -f $(docker ps -a -q -f name=${CONTAINER_NAME_RUN} 2>/dev/null) 2>/dev/null
docker volume rm $COMPILE_VOLUME
set -e

#build the snapshot image
docker build --no-cache --build-arg major_version=$PG_MAJOR --build-arg minor_version=$PG_MINOR_COMPILE -t $PG_IMAGE_TAG_RUN -f ${SCRIPT_DIR}/docker_postgres_snapshot/Dockerfile ${SCRIPT_DIR}/docker_postgres_snapshot/

create_base_container() {
  echo "Creating container $1 for tag $2"
  docker rm $1 2>/dev/null || true
  # Run a Postgres container
  docker run -u postgres -d --name $1 -v ${BASE_DIR}:/src -v ${COMPILE_VOLUME}:/compile $2
  # Install build dependencies
  docker exec -u root -it $1 /bin/bash -c "apk add --no-cache --virtual .build-deps coreutils dpkg-dev gcc libc-dev make util-linux-dev diffutils cmake bison flex openssl-dev && mkdir -p /build/debug"

  # Copy the source files to build directory
  docker exec -u root -it $1 /bin/bash -c "cp -a /src/{src,sql,scripts,test,tsl,CMakeLists.txt,timescaledb.control.in,version.config} /build/ && cd /build/debug/ && CFLAGS=-Werror cmake .. -DCMAKE_BUILD_TYPE=Debug -DREGRESS_CHECKS=OFF && make -C /build/debug install && chown -R postgres /build"
}

create_base_container $CONTAINER_NAME_COMPILE $PG_IMAGE_TAG_COMPILE

docker exec -u root -it  $CONTAINER_NAME_COMPILE /bin/bash -c "cp /build/debug/src/*.so /compile && cp /build/debug/src/loader/*.so /compile"
docker rm -f $CONTAINER_NAME_COMPILE

create_base_container $CONTAINER_NAME_RUN $PG_IMAGE_TAG_RUN

docker exec -u root -it $CONTAINER_NAME_RUN /bin/bash -c "cp /compile/* \`pg_config --pkglibdir\`/"


# Run tests, allow continuation after error to get regressions.diffs
set +e
docker exec -u postgres -it ${CONTAINER_NAME_RUN} /bin/bash -c "make -C /build/debug regresscheck TEST_INSTANCE_OPTS='--temp-instance=/tmp/pgdata --temp-config=/build/test/postgresql.conf'"
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    docker exec -it ${CONTAINER_NAME_RUN} cat /build/debug/test/regression.diffs
fi

exit $EXIT_CODE
