#!/bin/bash

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

CONTAINER_NAME=$CONTAINER_NAME DATA_DIR="" IMAGE_NAME=$IMAGE_NAME $(dirname $0)/docker-run.sh
