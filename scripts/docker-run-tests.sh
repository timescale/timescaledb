#!/usr/bin/env bash
#
# This script runs the TimescaleDB tests through a standard PostgreSQL
# container, first installing the extension via a mounted host volume.
#
SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
PG_IMAGE_TAG=${PG_IMAGE_TAG:-12.0-alpine}
CONTAINER_NAME=${CONTAINER_NAME:-pgtest}

case $1 in
    clean)
        docker rm -f "$(docker ps -a -q -f name=${CONTAINER_NAME} 2>/dev/null)" 2>/dev/null
        ;;
esac

if [ "$(docker ps -q -f name=${CONTAINER_NAME} 2>/dev/null | wc -l)" == "0" ]; then
    echo "Creating container ${CONTAINER_NAME}"
    docker rm ${CONTAINER_NAME} 2>/dev/null
    # Run a Postgres container
    docker run -u postgres -d --name ${CONTAINER_NAME} -v ${BASE_DIR}:/src postgres:${PG_IMAGE_TAG}
    # Install build dependencies
    docker exec -u root -it ${CONTAINER_NAME} /bin/bash -c "apk add --no-cache --virtual .build-deps coreutils dpkg-dev gcc libc-dev make util-linux-dev diffutils cmake bison flex && mkdir -p /build/debug"
fi

# Copy the source files to build directory
docker exec -u root -it ${CONTAINER_NAME} /bin/bash -c "cp -a /src/{src,sql,scripts,test,tsl,CMakeLists.txt,timescaledb.control.in,version.config} /build/ &&cd /build/debug/ && CFLAGS=-Werror cmake .. -DCMAKE_BUILD_TYPE=Debug && make -C /build/debug install && chown -R postgres /build/debug"

# Run tests
if ! docker exec -u postgres -it ${CONTAINER_NAME} /bin/bash -c "make -C /build/debug installcheck TEST_INSTANCE_OPTS='--temp-instance=/tmp/pgdata --temp-config=/build/test/postgresql.conf'"
then
    docker exec -it ${CONTAINER_NAME} cat /build/debug/test/regression.diffs
fi
