#!/usr/bin/env bash
#
# This script builds a development TimescaleDB image from the
# currently checked out source on the host.
#
SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
PG_VERSION=${PG_VERSION:-12.0}
PG_IMAGE_TAG=${PG_IMAGE_TAG:-${PG_VERSION}-alpine}
BUILD_CONTAINER_NAME=${BUILD_CONTAINER_NAME:-pgbuild}
BUILD_IMAGE_NAME=${BUILD_IMAGE_NAME:-$USER/pgbuild}
IMAGE_NAME=${IMAGE_NAME:-$USER/timescaledb}
GIT_ID=$(git -C ${BASE_DIR} describe --dirty --always | sed -e "s|/|_|g")
TAG_NAME=${TAG_NAME:-$GIT_ID}
BUILD_TYPE=${BUILD_TYPE:-Debug}
USE_OPENSSL=${USE_OPENSSL:-true}
PUSH_PG_IMAGE=${PUSH_PG_IMAGE:-false}
GENERATE_DOWNGRADE_SCRIPT=${GENERATE_DOWNGRADE_SCRIPT:-OFF}

# Full image identifiers
PG_IMAGE=${BUILD_IMAGE_NAME}:${PG_IMAGE_TAG}
TS_IMAGE=${IMAGE_NAME}:${TAG_NAME}

trap remove_build_container EXIT

image_exists() {
    [[ $(docker image ls -f "reference=$1" -q) ]]
}

postgres_build_image_exists() {
    image_exists ${PG_IMAGE}
}

timescaledb_image_exists() {
    image_exists ${TS_IMAGE}
}

remove_build_container() {
    local container=${1:-${BUILD_CONTAINER_NAME}}
    docker rm -vf "$(docker ps -a -q -f name=${container} 2>/dev/null)" 2>/dev/null
}

fetch_postgres_build_image() {
    local image=${1:-${PG_IMAGE}}
    docker pull ${image}
}

create_postgres_build_image() {
    local image=${1:-${PG_IMAGE}}

    echo "Creating new PostgreSQL build image ${image}"
    # Run a Postgres container
    docker run -d --name ${BUILD_CONTAINER_NAME} --env POSTGRES_HOST_AUTH_METHOD=trust -v ${BASE_DIR}:/src postgres:${PG_IMAGE_TAG}

    # Install build dependencies
    docker exec -u root ${BUILD_CONTAINER_NAME} /bin/bash -c "apk add --no-cache --virtual .build-deps postgresql-dev gdb coreutils dpkg-dev gcc git libc-dev make cmake util-linux-dev diffutils openssl-dev krb5-dev && mkdir -p /build/debug"
    docker commit -a $USER -m "TimescaleDB build base image version $PG_IMAGE_TAG" ${BUILD_CONTAINER_NAME} ${image}
    remove_build_container ${BUILD_CONTAINER_NAME}

    if ${PUSH_PG_IMAGE}; then
        docker push ${image}
    fi
}

run_postgres_build_image() {
    local image=${1:-${PG_IMAGE}}
    echo "Starting image ${image}"
    docker run -d --name ${BUILD_CONTAINER_NAME} -v ${BASE_DIR}:/src ${image}
}

build_timescaledb()
{
    echo "Building TimescaleDB image \"${TS_IMAGE}\" with USE_OPENSSL=${USE_OPENSSL} BUILD_TYPE=${BUILD_TYPE}"

    run_postgres_build_image ${PG_IMAGE}

    # Build and install the extension with debug symbols and assertions
    tar -c -C ${BASE_DIR} {cmake,src,sql,test,scripts,tsl,version.config,CMakeLists.txt,timescaledb.control.in} | docker cp - ${BUILD_CONTAINER_NAME}:/build/
    if ! docker exec -u root ${BUILD_CONTAINER_NAME} /bin/bash -c " \
    	cd /build/debug \
    	&& cmake -DGENERATE_DOWNGRADE_SCRIPT=${GENERATE_DOWNGRADE_SCRIPT} -DUSE_OPENSSL=${USE_OPENSSL} -DCMAKE_BUILD_TYPE=${BUILD_TYPE} /src \
    	&& make && make install \
    	&& echo \"shared_preload_libraries = 'timescaledb'\" >> /usr/local/share/postgresql/postgresql.conf.sample \
    	&& echo \"timescaledb.telemetry_level=off\" >> /usr/local/share/postgresql/postgresql.conf.sample \
    	&& cd / && rm -rf /build"
    then
      echo "Building timescaledb failed"
      return 1
    fi
    docker commit -a $USER -m "TimescaleDB development image" ${BUILD_CONTAINER_NAME} ${TS_IMAGE}
}

message_and_exit() {
    echo
    echo "Run 'docker run -d --name some-timescaledb -p 5432:5432 ${TS_IMAGE}' to launch"
    exit
}

remove_build_container

if timescaledb_image_exists; then
    echo "Image \"${TS_IMAGE}\" already exists."
    message_and_exit
fi


if ! postgres_build_image_exists; then
    if ! fetch_postgres_build_image "$@"; then
        create_postgres_build_image "$@" || exit 1
    fi
fi

build_timescaledb || exit 1

message_and_exit
