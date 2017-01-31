#!/bin/bash

set -e

if [[ -z "$CONTAINER_NAME" ]]; then
  echo "The CONTAINER_NAME must be set"
  exit 1
fi

matchingStarted=`docker ps --filter="name=$CONTAINER_NAME" -q`
if [[ -n "${matchingStarted}" ]]
then 
  docker stop $matchingStarted 
fi

matching=`docker ps -a --filter="name=$CONTAINER_NAME" -q`
if [[ -n $matching ]]; then 
  docker rm $matching 
fi

CONTAINER_NAME=$CONTAINER_NAME \
DATA_DIR="" \
IMAGE_NAME=$IMAGE_NAME \
PGPORT=${PGPORT:-5432} \
$(dirname $0)/docker-run.sh

TEST_TABLESPACE_PATH=${TEST_TABLESPACE_PATH:-/tmp/tspace1}

# Create data directories for tablespaces tests
docker exec -i $CONTAINER_NAME /bin/bash << EOF
mkdir -p ${TEST_TABLESPACE_PATH}
chown postgres ${TEST_TABLESPACE_PATH}
EOF
